#include "abstract_application.h"
#include "monitor/stat_sum_monitor_message_adaptor.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "consumer/consumer_util.h"
#include "monitor/stat_sum_monitor_log.h"
#include "monitor/stat_sum_monitor_log_write_strategy.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <fstream>
#include <memory>
#include <getopt.h>
#include <thread>

#include <librdkafka/rdkafkacpp.h>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>
#include <libmoniq/writer/write_strategy/monitor_log_write_strategy.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>

using namespace std;
using namespace common;

struct Arguments {
    string broker;

    string group_prefix;
    string topic_prefix;
    int client_cnt;
    int start_idx;
    int running_time;
    int poll_timeout;

    bool scrapable;
    bool log_disabled;
    bool read_tagged_only;
    double monitoring_epoch_size;

    bool verbose;

    int start_barrier_delay;

    string outdir;
    string out_prefix;
};

struct ConsumerThreadArg {
    string broker;
    string group_id;
    string client_id;
    string service_name;

    vector<string> topics;

    shared_ptr<moniq::MonitorQueue> monitor_queue;
    shared_ptr<moniq::writer::MonitorLogWriter> writer;
    shared_ptr<common::monitor::IStatSumMonitorMessageAdaptor> message_adaptor;

    latch *start_signal;
    atomic<bool> *end_flag;

    int poll_timeout;
    bool log_disabled;
    bool read_tagged_only;
    bool verbose;
};

struct ServiceArg {
    string name;
    int64_t threshold;
};

class V2xExprConsumerApp: public AbstractApplication {
protected:
    void cleanup_main() override;

private:
    Arguments args;
    vector<struct ServiceArg> service_args;

    vector<ConsumerThreadArg> consumer_thread_args;
    vector<thread> client_threads;

    atomic<bool> end_flag;

    shared_ptr<common::monitor::IStatSumMonitorMessageAdaptor> message_adaptor;

    void init_clients();
    void wait_for_running_time();
    void join_clients();

public:
    V2xExprConsumerApp(
        shared_ptr<moniq::MonitorQueue> &monitor_queue,
        shared_ptr<moniq::writer::MonitorLogWriter> &writer,
        shared_ptr<common::monitor::IStatSumMonitorMessageAdaptor> &message_adaptor,
        Arguments args,
        vector<struct ServiceArg> &service_args
    ): AbstractApplication(monitor_queue, writer), args(args), service_args(service_args), message_adaptor(message_adaptor) {

    }

    virtual ~V2xExprConsumerApp() = default;

    void run() override;
    void send_end_signal_to_threads();
};

// Define global variables & helper functions
namespace {
    atomic<bool> g_signal_received(false);
    V2xExprConsumerApp *app;
    vector<unique_ptr<ostream>> latency_outs;
    vector<unique_ptr<ostream>> per_sec_outs;

    vector<monitor::ServiceInfo> generate_services(vector<struct ServiceArg> &service_args, string outdir, string out_prefix);
    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
    vector<int> assign_services(int num_cars, int num_services);
    void consume_run(struct ConsumerThreadArg *arg);
}


int main(int argc, char *argv[]) {
    Arguments args = parse_arguments(argc, argv);

    cout << "v2x expr consumer starts at " << util::current_time_str() << endl;
    if (args.verbose) {
        cout
            << "Broker: " << args.broker << "\n"
            << "Group Prefix: " << args.group_prefix << "\n"
            << "Topic Prefix: " << args.topic_prefix << "\n"
            << "Client Count: " << args.client_cnt << "\n"
            << "Start Index: " << args.start_idx << "\n"
            << "Running Time: " << args.running_time << "\n"
            << "Poll Timeout: " << args.poll_timeout << "\n"
            << "Start Barrier Delay: " << args.start_barrier_delay << "\n"
            << "Scrapable: " << (args.scrapable ? "on" : "off") << "\n"
            << "No logging: " << (args.log_disabled ? "on" : "off") << "\n"
            << "Log Sampling: " << (args.read_tagged_only ? "on" : "off") << "\n"
            << "Monitoring Epoch Size: " << args.monitoring_epoch_size << "\n"
            << "Output Directory: " << args.outdir
            << "Output File Prefix: " << args.out_prefix << endl;
    }

    vector<struct ServiceArg> service_args = {
        {"S10Hz-Info", 100},
        {"S10Hz-Sensor", 100},
        {"S30Hz", 25},
        {"S50Hz", 20},
        {"S10Hz-Info-for-rsu", 100},
        {"S50Hz-for-rsu", 20},
    };

    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy =
        make_shared<monitor::StatSumPerSecMonitorLogWriteStrategy>(generate_services(service_args, args.outdir, args.out_prefix), args.monitoring_epoch_size);
    shared_ptr<moniq::writer::MonitorLogWriter> writer = make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, -1, -1, 32);
    shared_ptr<common::monitor::IStatSumMonitorMessageAdaptor> message_adaptor = make_shared<common::monitor::ExtractOnlyStatSumMonitorMessageAdaptor>();

    app = new V2xExprConsumerApp(monitor_queue, writer, message_adaptor, args, service_args);
    signal(SIGINT, interrupt_handler);
    signal(SIGTERM, interrupt_handler);
    app->run();
    app->cleanup();

    delete app;
    return 0;
}

void V2xExprConsumerApp::run() {
    init_clients();

    start_barrier(args.start_barrier_delay);
    wait_for_running_time();
}

void V2xExprConsumerApp::init_clients() {
    int client_cnt = args.client_cnt;
    if (client_cnt < 0) client_cnt = service_args.size();

    consumer_thread_args.resize(client_cnt);
    client_threads.reserve(client_cnt);
    vector<int> assigned_idx = assign_services(client_cnt, service_args.size());
    for (int i = 0; i < client_cnt; i++) {
        int cur_idx = args.start_idx + i;
        consumer_thread_args[i].broker = args.broker;
        consumer_thread_args[i].group_id = args.group_prefix + to_string(cur_idx);
        consumer_thread_args[i].client_id = args.group_prefix + to_string(cur_idx);
        consumer_thread_args[i].service_name = service_args[assigned_idx[i]].name;
        consumer_thread_args[i].topics.push_back(args.topic_prefix + service_args[assigned_idx[i]].name);
        consumer_thread_args[i].start_signal = &start_signal;
        consumer_thread_args[i].end_flag = &end_flag;
        consumer_thread_args[i].poll_timeout = args.poll_timeout;
        consumer_thread_args[i].verbose = args.verbose;
        consumer_thread_args[i].log_disabled = args.log_disabled;
        consumer_thread_args[i].read_tagged_only = args.read_tagged_only;
        consumer_thread_args[i].monitor_queue = monitor_queue;
        consumer_thread_args[i].writer = writer;
        consumer_thread_args[i].message_adaptor = message_adaptor;
        client_threads.emplace_back(consume_run, &consumer_thread_args[i]);
    }
}

void V2xExprConsumerApp::wait_for_running_time() {
    auto end_tick = chrono::steady_clock::now() + chrono::seconds(args.running_time);

    while (chrono::steady_clock::now() < end_tick) {
        if (g_signal_received.load()) {
            break;
        }
        this_thread::sleep_for(chrono::seconds(1));
    }
    end_flag.store(true, memory_order_release);
}

void V2xExprConsumerApp::join_clients() {
    for (auto& client_thread: client_threads) {
        if (client_thread.joinable()) client_thread.join();
    }
}

void V2xExprConsumerApp::send_end_signal_to_threads() {
    end_flag.store(true, memory_order_release);
}


void V2xExprConsumerApp::cleanup_main() {
    join_clients();
}

namespace {

vector<monitor::ServiceInfo> generate_services(vector<struct ServiceArg> &service_args, string outdir, string out_prefix) {
    std::string latency_file_postfix = "latency.csv";
    std::string per_sec_file_postfix = "per_sec.csv";

    vector<monitor::ServiceInfo> services;
    for (size_t i = 0; i < service_args.size(); i++) {
        unique_ptr<ostream> latecny_out = make_unique<ofstream>(
            outdir + "/" + out_prefix + service_args[i].name + "_" + latency_file_postfix);
        unique_ptr<ostream> per_sec_out = make_unique<ofstream>(
            outdir + "/" + out_prefix + service_args[i].name + "_" + per_sec_file_postfix);
        services.push_back({
            service_args[i].name, service_args[i].threshold,
            latecny_out.get(), per_sec_out.get(), &cout
        });
        latency_outs.push_back(std::move(latecny_out));
        per_sec_outs.push_back(std::move(per_sec_out));
    }

    return services;
}

vector<int> assign_services(int num_cars, int num_services) {
    vector<int> assigned;
    for (int i = 0; i < num_cars; i++) assigned.push_back(i % num_services);
    return assigned;
}

void consume_run(struct ConsumerThreadArg *arg) {
    string errstr;
    unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", arg->broker, errstr);
    conf->set("group.id", arg->group_id, errstr);
    conf->set("client.id", arg->client_id, errstr);
    conf->set("auto.offset.reset", "latest", errstr);
    conf->set("fetch.min.bytes", "1", errstr);
    conf->set("log_level", "0", errstr);
    conf->set("enable.auto.commit", "false", errstr);
    if (arg->verbose) {
        conf->set("log_level", "7", errstr);
    }
    if (errstr.size() > 0) cout << "Error on creating consumer config: " << errstr << endl;

    unique_ptr<RdKafka::KafkaConsumer> consumer = consumer::create_consumer(conf.get());
    if (consumer.get() == nullptr) return;

    // Wait for the start flag to be set
    arg->start_signal->wait();
    if (!consumer::subscribe_topics(consumer.get(), arg->topics)) return;

    while (!arg->end_flag->load(memory_order_acquire)) {
        RdKafka::Message *msg = consumer::consume_message(consumer.get(), arg->poll_timeout);
        if (msg == nullptr) continue;
        std::string plain_msg = std::string(static_cast<const char*>(msg->payload()));
        RdKafka::MessageTimestamp ts = msg->timestamp();
        int64_t responded_at = ts.timestamp;
        delete msg;

        if (arg->log_disabled) continue;
        if (arg->service_name.find("for-rsu") != string::npos) responded_at -= 3;
        arg->monitor_queue->enqueue(make_unique<monitor::StatSumMonitorLog>(
            arg->message_adaptor.get(),
            plain_msg,
            "Responded",
            responded_at
        ));
        arg->writer->notify_if_needed();
    }
    consumer->close();
}

void interrupt_handler(int signum) {
    g_signal_received.store(true);
}

Arguments parse_arguments(int argc, char** argv) {
    int opt;
    Arguments args;

    args.group_prefix="";
    args.topic_prefix="";
    args.client_cnt = 1;
    args.start_idx = 0;
    args.running_time = 10;
    args.poll_timeout = 0;
    args.start_barrier_delay = 2;
    args.outdir = "";
    args.out_prefix = "";
    args.scrapable = false;
    args.log_disabled = false;
    args.read_tagged_only = false;
    args.monitoring_epoch_size = 1000.0;
    args.verbose = false;

    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::GROUP_PREFIX_OPTION,
        util::TOPIC_PREFIX_OPTION,
        util::CLIENT_CNT_OPTION,
        util::START_IDX_OPTION,
        util::RUNNING_TIME_OPTION,
        util::POLL_TIMEOUT_OPTION,
        util::START_BARRIER_DELAY_OPTION,
        util::SCRAPABLE_OPTION,
        util::NO_LOG_OPTION,
        util::READ_TAGGED_ONLY_OPTION,
        util::MONITORING_EPOCH_SIZE_OPTION,
        util::OUTDIR_OPTION,
        util::OUT_PREFIX_OPTION,
        util::VERBOSE_OPTION,
    };

    const vector<struct option> long_options = make_long_opts(options);
    const string short_options = make_short_opts(options);
    while ((opt = getopt_long(argc, argv, short_options.c_str(), long_options.data(), nullptr)) != -1) {
        switch (opt) {
            case util::HELP_OPTION.get_val(): cout << make_help_message(options) << endl; exit(EXIT_SUCCESS); break;
            case util::BROKER_OPTION.get_val(): args.broker = optarg; break;
            case util::GROUP_PREFIX_OPTION.get_val(): args.group_prefix = optarg; break;
            case util::TOPIC_PREFIX_OPTION.get_val(): args.topic_prefix = optarg; break;
            case util::CLIENT_CNT_OPTION.get_val(): args.client_cnt = atoi(optarg); break;
            case util::START_IDX_OPTION.get_val(): args.start_idx = atoi(optarg); break;
            case util::RUNNING_TIME_OPTION.get_val(): args.running_time = atoi(optarg); break;
            case util::POLL_TIMEOUT_OPTION.get_val(): args.poll_timeout = atoi(optarg); break;
            case util::START_BARRIER_DELAY_OPTION.get_val(): args.start_barrier_delay = atoi(optarg); break;
            case util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case util::NO_LOG_OPTION.get_val(): args.log_disabled = true; break;
            case util::READ_TAGGED_ONLY_OPTION.get_val(): args.read_tagged_only = true; break;
            case util::MONITORING_EPOCH_SIZE_OPTION.get_val(): args.monitoring_epoch_size = atof(optarg); break;
            case util::OUTDIR_OPTION.get_val(): args.outdir = optarg; break;
            case util::OUT_PREFIX_OPTION.get_val(): args.out_prefix = optarg; break;
            case util::VERBOSE_OPTION.get_val(): args.verbose = true; break;
            default:
                cerr << "Error: Unknown option or missing argument." << endl << endl;
                cerr << make_help_message(options) << endl;
                exit(EXIT_FAILURE);
        }
    }

    return args;
}

}