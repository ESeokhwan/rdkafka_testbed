#include "abstract_application.h"
#include "service.h"
#include "service_runner.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "producer/producer_service.h"

#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <getopt.h>
#include <vector>
#include <random>
#include <string>
#include <thread>
#include <utility>

#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>
#include <libmoniq/writer/write_strategy/console_monitor_log_write_strategy.h>
#include <libmoniq/writer/write_strategy/monitor_log_write_strategy.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>
#include <libmoniq/adaptor/message_adaptor.h>
#include <vector>

using namespace std;
using namespace common;


// Define structs & classes
struct Arguments {
    string broker;

    string prefix;
    string topic_name;
    int partition_cnt;
    int client_cnt;
    int msg_cnt;
    double interval;
    double interval_noise_stddev;
    int msg_size;
    bool is_sync;
    bool ignore_response;
    bool need_flush;
    bool sample_log;
    bool tag_log;
    bool share_producer;
    int warmup_cnt;
    string warmup_topic;

    int start_barrier_delay;
    int monitoring_batch_size;
    bool prepare_producers = false;
    int prepare_timeout_ms = 30000;

    bool scrapable;
    bool verbose;
};

struct PartitionAssignment {
    int partition;
    int msg_cnt;
};

struct ClientAssignment {
    vector<PartitionAssignment> partition_assignments;
};

struct PreparationTarget {
    rd_kafka_t *handle;
    int client;
    vector<int32_t> partitions;
};

class BasicProducersTest: public AbstractApplication {
protected:
    void cleanup_main() override;

private:
    Arguments args;

    unique_ptr<RdKafka::DeliveryReportCb> dr_cb;
    vector<unique_ptr<ServicesRunner>> services_runners;
    vector<thread> client_threads;
    vector<RdKafka::Producer *> shared_producers;
    vector<PreparationTarget> preparation_targets;

    shared_ptr<moniq::adaptor::IMessageAdaptor> adaptor;
    random_device rd;

    void init_services();
    void init_standalone_services(vector<ClientAssignment> client_assignments);
    void init_sharing_prod_services(vector<ClientAssignment> client_assignments);
    void prepare_measurement_producers();
    void join_clients();

public:
    BasicProducersTest(
        shared_ptr<moniq::MonitorQueue> &monitor_queue,
        shared_ptr<moniq::writer::MonitorLogWriter> &writer,
        shared_ptr<moniq::adaptor::IMessageAdaptor> &adaptor,
        Arguments args
    ): AbstractApplication(monitor_queue, writer), args(args), adaptor(adaptor) {

    }

    virtual ~BasicProducersTest() = default;

    void run() override;
    void close_service_runners();
};

// Define global variables & helper functions
namespace {
    BasicProducersTest *app = nullptr;
    volatile std::sig_atomic_t interrupted = 0;
    volatile std::sig_atomic_t services_initialized = 0;

    constexpr util::OptionWrapper PREPARE_PRODUCERS_OPTION{
        "prepare-producers", required_argument, nullptr, util::CUSTOM_LONG_OPTIONS_VALUE_OFFSET,
        "Prepare the measurement producer connections before starting (on|off, default off)"};
    constexpr util::OptionWrapper PREPARE_TIMEOUT_OPTION{
        "prepare-timeout-ms", required_argument, nullptr, util::CUSTOM_LONG_OPTIONS_VALUE_OFFSET + 1,
        "Overall producer preparation timeout in milliseconds (30000)"};

    vector<ClientAssignment> client_assignment_setup(int client_cnt, int partition_cnt, int total_msg_cnt);
    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
}


// Entry point
int main(int argc, char *argv[]) {
    Arguments args;
    try {
        args = parse_arguments(argc, argv);
    } catch (const std::exception &error) {
        cerr << error.what() << endl;
        return EXIT_FAILURE;
    }

    cout << "single topic partition expr producer starts at " << util::current_time_str() << endl;
    if (args.verbose) {
        cout
            << "Broker: " << args.broker << "\n"
            << "Prefix: " << args.prefix << "\n"
            << "Topic Name: " << args.topic_name << "\n"
            << "Partition Count" << args.partition_cnt << "\n"
            << "Client Count: " << args.client_cnt << "\n"
            << "Message Count per Topic: " << args.msg_cnt << "\n"
            << "Interval: " << args.interval << "\n"
            << "Interval Noise Stddev: " << args.interval_noise_stddev << "\n"
            << "Message Size: " << args.msg_size << "\n"
            << "Is Sync: " << (args.is_sync ? "on" : "off") << "\n"
            << "Ignore Response: " << (args.ignore_response ? "on" : "off") << "\n"
            << "Need Flush: " << (args.need_flush ? "on" : "off") << "\n"
            << "Sample Log: " << (args.sample_log ? "on" : "off") << "\n"
            << "Tag Record: " << (args.tag_log ? "on" : "off") << "\n"
            << "Share Producer: " << (args.share_producer ? "on" : "off") << "\n"
            << "Warmup Count: " << args.warmup_cnt << "\n"
            << "Warmup Topic: " << args.warmup_topic << "\n"
            << "Start Barrier Delay: " << args.start_barrier_delay << "\n"
            << "Monitoring Batch Size: " << args.monitoring_batch_size << "\n"
            << "Scrapable: " << (args.scrapable ? "on" : "off") << endl;
    }

    shared_ptr<moniq::adaptor::IMessageAdaptor> adaptor =
        make_shared<moniq::adaptor::FastJsonBasedLatencyMonitoringMessageGenerator>(args.msg_size, min(args.msg_size, 1000));
    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy =
        make_shared<moniq::writer::ConsoleMonitorLogWriteStrategy>(args.scrapable);
    shared_ptr<moniq::writer::MonitorLogWriter> writer =
        make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, args.monitoring_batch_size, -1);

    auto application = make_unique<BasicProducersTest>(monitor_queue, writer, adaptor, args);
    app = application.get();
    signal(SIGINT, interrupt_handler);
    signal(SIGTERM, interrupt_handler);
    int status = EXIT_SUCCESS;
    try {
        app->run();
    } catch (const std::exception &error) {
        cerr << error.what() << endl;
        status = EXIT_FAILURE;
    }
    app->cleanup();
    services_initialized = 0;
    app = nullptr;
    return interrupted && args.prepare_producers ? 130 : status;
}

void BasicProducersTest::run() {
    init_services();
    if (interrupted) return;
    if (args.prepare_producers) prepare_measurement_producers();

    services_initialized = 1;
    if (interrupted) return;
    try {
        client_threads.reserve(services_runners.size());
        for (const auto &service_runner : services_runners) {
            client_threads.emplace_back(&ServicesRunner::run, service_runner.get());
        }
        start_barrier(args.start_barrier_delay);
    } catch (...) {
        close_service_runners();
        start_signal.count_down();
        throw;
    }
    join_clients();
}

void BasicProducersTest::init_services() {
    vector<ClientAssignment> client_assignment = client_assignment_setup(args.client_cnt, args.partition_cnt, args.msg_cnt);
    if (args.share_producer) init_sharing_prod_services(client_assignment);
    else init_standalone_services(client_assignment);
}

void BasicProducersTest::prepare_measurement_producers() {
    using Queue = unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)>;
    using Event = unique_ptr<rd_kafka_event_t, decltype(&rd_kafka_event_destroy)>;
    using Options = unique_ptr<rd_kafka_AdminOptions_t, decltype(&rd_kafka_AdminOptions_destroy)>;
    using Partitions = unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>;
    using Clock = chrono::steady_clock;

    vector<Queue> queues;
    queues.reserve(preparation_targets.size());
    const auto deadline = Clock::now() + chrono::milliseconds(args.prepare_timeout_ms);
    const auto timeout_for = [&](const PreparationTarget &target) {
        const string prefix = "Producer preparation failed for client " + to_string(target.client) + ": ";
        if (interrupted) throw runtime_error(prefix + "interrupted");
        const auto remaining = chrono::ceil<chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0) {
            throw runtime_error(prefix + "timed out after " + to_string(args.prepare_timeout_ms) + " ms");
        }
        return static_cast<int>(remaining);
    };

    // Submit every probe before waiting so setup time is bounded by one deadline.
    for (const auto &target : preparation_targets) {
        Options options(rd_kafka_AdminOptions_new(target.handle, RD_KAFKA_ADMIN_OP_LISTOFFSETS),
                        rd_kafka_AdminOptions_destroy);
        char errstr[512];
        if (rd_kafka_AdminOptions_set_request_timeout(options.get(), timeout_for(target), errstr, sizeof(errstr))) {
            throw runtime_error("Producer preparation failed for client " + to_string(target.client) + ": " + errstr);
        }
        Partitions partitions(rd_kafka_topic_partition_list_new(static_cast<int>(target.partitions.size())),
                              rd_kafka_topic_partition_list_destroy);
        for (const auto partition : target.partitions) {
            rd_kafka_topic_partition_list_add(partitions.get(), args.topic_name.c_str(), partition)->offset =
                RD_KAFKA_OFFSET_SPEC_LATEST;
        }
        queues.emplace_back(rd_kafka_queue_new(target.handle), rd_kafka_queue_destroy);
        rd_kafka_ListOffsets(target.handle, partitions.get(), options.get(), queues.back().get());
    }

    for (size_t i = 0; i < queues.size(); ++i) {
        const auto &target = preparation_targets[i];
        Event event(nullptr, rd_kafka_event_destroy);
        while (!event) {
            event.reset(rd_kafka_queue_poll(queues[i].get(), std::min(100, timeout_for(target))));
        }
        const string prefix = "Producer preparation failed for client " + to_string(target.client) + ": ";
        if (rd_kafka_event_error(event.get())) throw runtime_error(prefix + rd_kafka_event_error_string(event.get()));
        const auto *result = rd_kafka_event_ListOffsets_result(event.get());
        if (!result) throw runtime_error(prefix + "missing ListOffsets result");
        size_t count = 0;
        const auto **infos = rd_kafka_ListOffsets_result_infos(result, &count);
        if (count != target.partitions.size()) throw runtime_error(prefix + "incomplete ListOffsets result");
        for (size_t j = 0; j < count; ++j) {
            const auto *partition = rd_kafka_ListOffsetsResultInfo_topic_partition(infos[j]);
            if (partition->err) {
                throw runtime_error(prefix + "partition " + to_string(partition->partition) + ": "
                                    + rd_kafka_err2str(partition->err));
            }
        }
    }
}

void BasicProducersTest::init_sharing_prod_services(vector<ClientAssignment> client_assignments) {
    mt19937 rng(rd());
    for (int i = 0; i < args.client_cnt; i++) {
        vector<shared_ptr<IService>> services;

        std::string errstr;
        bool response_enabled = (!args.sample_log || i == 0) && !args.ignore_response;
        dr_cb = make_unique<producer::LoggingDeliveryReportCb>(adaptor, monitor_queue, writer);
        unique_ptr<RdKafka::Conf> conf = producer::ProducerService::create_producer_conf(
            args.broker, args.prefix + "_" + to_string(i), args.is_sync || !args.ignore_response,
            response_enabled ? dr_cb.get(): &producer::NoOpsDeliveryReportCb::get_instance(),
            errstr
        );
        RdKafka::Producer *producer = RdKafka::Producer::create(conf.get(), errstr);
        if (!producer) {
            throw std::runtime_error(errstr);
        }
        shared_producers.push_back(producer);
        if (args.prepare_producers) {
            PreparationTarget target{producer->c_ptr(), i, {}};
            for (const auto &assignment : client_assignments[i].partition_assignments) {
                target.partitions.push_back(assignment.partition);
            }
            preparation_targets.push_back(std::move(target));
        }

        for (PartitionAssignment pa: client_assignments[i].partition_assignments) {
            shared_ptr<IService> service = make_shared<producer::ProducerService>(
                args.prefix + "_" + to_string(i) + "_" + to_string(pa.partition),
                producer,
                args.topic_name,
                pa.partition,
                pa.msg_cnt,
                args.interval,
                args.interval_noise_stddev,
                args.interval / 2,
                rng,
                args.is_sync,
                args.ignore_response,
                args.need_flush,
                (!args.sample_log || i == 0),
                args.tag_log,
                adaptor,
                monitor_queue,
                writer
            );
            services.push_back(service);
        }
        shared_ptr<IService> warmup_service = make_shared<producer::ProducerService>(
            "warmup_" + to_string(i),
            producer,
            args.warmup_topic,
            args.warmup_cnt,
            0,
            0,
            0,
            rng,
            false,
            true,
            true,
            false,
            false,
            adaptor,
            monitor_queue,
            writer
        );

        services_runners.push_back(make_unique<ServicesRunner>(
            services, warmup_service,
            -1, 0, -1, rng, &start_signal
        ));
    }
}

void BasicProducersTest::init_standalone_services(vector<ClientAssignment> client_assignments) {
    std::string errstr;
    unique_ptr<RdKafka::Conf> conf = producer::ProducerService::create_producer_conf(
        args.broker, "warmup_producer", false,
        &producer::NoOpsDeliveryReportCb::get_instance(),
        errstr
    );
    RdKafka::Producer *warmup_producer = RdKafka::Producer::create(conf.get(), errstr);
    if (!warmup_producer) {
        throw std::runtime_error(errstr);
    }
    shared_producers.push_back(warmup_producer);

    mt19937 rng(rd());
    for (int i = 0; i < args.client_cnt; i++) {
        vector<shared_ptr<IService>> services;
        for (PartitionAssignment pa: client_assignments[i].partition_assignments) {
            auto service = make_shared<producer::ProducerService>(
                args.prefix + "_" + to_string(i) + "_" + to_string(pa.partition),
                args.broker,
                args.prefix + "_" + to_string(i),
                args.topic_name,
                pa.partition,
                pa.msg_cnt,
                args.interval,
                args.interval_noise_stddev,
                args.interval / 2,
                rng,
                args.is_sync,
                args.ignore_response,
                args.need_flush,
                (!args.sample_log || i == 0),
                args.tag_log,
                adaptor,
                monitor_queue,
                writer
            );
            if (args.prepare_producers) {
                preparation_targets.push_back({service->client_handle(), i, {pa.partition}});
            }
            services.push_back(service);
        }
        shared_ptr<IService> warmup_service = make_shared<producer::ProducerService>(
            "warmup_" + to_string(i),
            warmup_producer,
            args.warmup_topic,
            args.warmup_cnt,
            0,
            0,
            0,
            rng,
            false,
            true,
            true,
            false,
            false,
            adaptor,
            monitor_queue,
            writer
        );

        services_runners.push_back(make_unique<ServicesRunner>(
            services, warmup_service,
            -1, 0, -1, rng, &start_signal
        ));
    }
}

void BasicProducersTest::close_service_runners() {
    for (auto &service_runner: services_runners) {
        service_runner->close();
    }
}

void BasicProducersTest::join_clients() {
    for (auto &client_thread: client_threads) {
        if (client_thread.joinable()) client_thread.join();
    }
    for (auto *producer: shared_producers) {
        producer->flush(5000);
        delete producer;
    }
    shared_producers.clear();
}

void BasicProducersTest::cleanup_main() {
    close_service_runners();
    join_clients();
}


namespace {

vector<ClientAssignment> client_assignment_setup(int client_cnt, int partition_cnt, int total_msg_cnt) {
    vector<ClientAssignment> output;
    vector<int> msg_cnt_per_client;
    for (int i = 0; i < client_cnt; i++) {
        ClientAssignment tmp;
        tmp.partition_assignments = vector<PartitionAssignment>();

        int tmp_cnt = total_msg_cnt / client_cnt;
        if (i < total_msg_cnt % client_cnt) tmp_cnt += 1;
        output.push_back(tmp);
        msg_cnt_per_client.push_back(tmp_cnt);
    }
    if (client_cnt > partition_cnt) {
        for (int i = 0; i < client_cnt; i++) {
            PartitionAssignment tmp;
            tmp.partition = i % partition_cnt;
            tmp.msg_cnt = 0;
            output[i].partition_assignments.push_back(tmp);
        }
    } else {
        for (int i = 0; i < partition_cnt; i++) {
            PartitionAssignment tmp;
            tmp.partition = i % partition_cnt;
            tmp.msg_cnt = 0;
            output[i % client_cnt].partition_assignments.push_back(tmp);
        }
    }

    for (int i = 0; i < client_cnt; i++) {
        int partition_cnt_on_this_client = output[i].partition_assignments.size();
        for (int j = 0; j < partition_cnt_on_this_client; j++) {
            output[i].partition_assignments[j].msg_cnt = msg_cnt_per_client[i] / partition_cnt_on_this_client;
            if (j < msg_cnt_per_client[i] % partition_cnt_on_this_client) output[i].partition_assignments[j].msg_cnt += 1;
        }
    }
    return output;
}

void interrupt_handler(int signum) {
    interrupted = 1;
    if (services_initialized) app->close_service_runners();
}

Arguments parse_arguments(int argc, char** argv) {
    int opt;
    Arguments args;

    args.prefix = "";
    args.topic_name = "";
    args.partition_cnt = 1;
    args.client_cnt = 1;
    args.msg_cnt = 1;
    args.interval = 1000.0;
    args.interval_noise_stddev = 0;
    args.msg_size = 1000;
    args.is_sync = false;
    args.ignore_response = false;
    args.need_flush = false;
    args.sample_log = false;
    args.tag_log = false;
    args.share_producer = false;
    args.warmup_cnt = 0;
    args.warmup_topic = "test_warmup";
    args.start_barrier_delay = 2;
    args.monitoring_batch_size = -1;
    args.scrapable = false;
    args.verbose = false;
    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::PREFIX_OPTION,
        util::TOPIC_NAME_OPTION,
        util::PARTITION_CNT_OPTION,
        util::CLIENT_CNT_OPTION,
        util::MSG_CNT_OPTION,
        util::INTERVAL_OPTION,
        util::INTERVAL_NOISE_STDDEV_OPTION,
        util::MSG_SIZE_OPTION,
        util::IS_SYNC_OPTION,
        util::IGNORE_RESPONSE_OPTION,
        util::NEED_FLUSH_OPTION,
        util::SAMPLE_LOG_OPTION,
        util::TAG_RECORD_OPTION,
        util::SHARE_PRODUCER_OPTION,
        util::WARMUP_CNT_OPTION,
        util::WARMUP_TOPIC_OPTION,
        util::START_BARRIER_DELAY_OPTION,
        PREPARE_PRODUCERS_OPTION,
        PREPARE_TIMEOUT_OPTION,
        util::MONITORING_BATCH_SIZE_OPTION,
        util::SCRAPABLE_OPTION,
        util::VERBOSE_OPTION,
    };

    const vector<struct option> long_options = make_long_opts(options);
    const string short_options = make_short_opts(options);
    while ((opt = getopt_long(argc, argv, short_options.c_str(), long_options.data(), nullptr)) != -1) {
        switch (opt) {
            case util::HELP_OPTION.get_val(): cout << make_help_message(options) << endl; exit(EXIT_SUCCESS); break;
            case util::BROKER_OPTION.get_val(): args.broker = optarg; break;
            case util::PREFIX_OPTION.get_val(): args.prefix = optarg; break;
            case util::TOPIC_NAME_OPTION.get_val(): args.topic_name = optarg; break;
            case util::PARTITION_CNT_OPTION.get_val(): args.partition_cnt = atoi(optarg); break;
            case util::CLIENT_CNT_OPTION.get_val(): args.client_cnt = atoi(optarg); break;
            case util::MSG_CNT_OPTION.get_val(): args.msg_cnt = atoi(optarg); break;
            case util::INTERVAL_OPTION.get_val(): args.interval = atof(optarg); break;
            case util::INTERVAL_NOISE_STDDEV_OPTION.get_val(): args.interval_noise_stddev = atof(optarg); break;
            case util::MSG_SIZE_OPTION.get_val(): args.msg_size = atoi(optarg); break;
            case util::IS_SYNC_OPTION.get_val(): args.is_sync = true; break;
            case util::IGNORE_RESPONSE_OPTION.get_val(): args.ignore_response = true; break;
            case util::NEED_FLUSH_OPTION.get_val(): args.need_flush = true; break;
            case util::SAMPLE_LOG_OPTION.get_val(): args.sample_log = true; break;
            case util::TAG_RECORD_OPTION.get_val(): args.tag_log = true; break;
            case util::SHARE_PRODUCER_OPTION.get_val(): args.share_producer = true; break;
            case util::WARMUP_CNT_OPTION.get_val(): args.warmup_cnt = atoi(optarg); break;
            case util::WARMUP_TOPIC_OPTION.get_val(): args.warmup_topic = optarg; break;
            case util::START_BARRIER_DELAY_OPTION.get_val(): args.start_barrier_delay = atoi(optarg); break;
            case PREPARE_PRODUCERS_OPTION.get_val(): {
                const string mode = optarg;
                if (mode != "on" && mode != "off") throw runtime_error("prepare-producers must be on or off");
                args.prepare_producers = mode == "on";
                break;
            }
            case PREPARE_TIMEOUT_OPTION.get_val(): {
                size_t used = 0;
                const string value = optarg;
                long timeout = 0;
                try { timeout = std::stol(value, &used); }
                catch (const std::exception &) {}
                if (used != value.size() || timeout < 1 || timeout > 3600000) {
                    throw runtime_error("prepare-timeout-ms must be in 1..3600000");
                }
                args.prepare_timeout_ms = static_cast<int>(timeout);
                break;
            }
            case util::MONITORING_BATCH_SIZE_OPTION.get_val(): args.monitoring_batch_size = atoi(optarg); break;
            case util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case util::VERBOSE_OPTION.get_val(): args.verbose = true; break;
            default:
                cerr << "Error: Unknown option or missing argument." << endl << endl;
                cerr << make_help_message(options) << endl;
                exit(EXIT_FAILURE);
        }
    }

    if (args.topic_name.empty()) args.topic_name = args.prefix;

    return args;
}

}
