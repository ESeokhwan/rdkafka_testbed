#include "abstract_application.h"
#include "service.h"
#include "service_runner.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "producer/mqtt_producer_service.h"
#include "monitor/stat_sum_monitor_message_adaptor.h"

#include <csignal>
#include <iostream>
#include <memory>
#include <getopt.h>
#include <random>
#include <string>
#include <thread>

#include <mosquitto.h>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>
#include <libmoniq/writer/write_strategy/console_monitor_log_write_strategy.h>
#include <libmoniq/writer/write_strategy/monitor_log_write_strategy.h>
#include <libmoniq/adaptor/message_adaptor.h>

using namespace std;
using namespace common;


// Define structs & classes
struct Arguments {
    string broker;

    string prefix;
    int client_cnt;
    int topic_cnt_per_client;
    int msg_cnt_per_topic;
    double interval;
    double interval_noise_stddev;
    double interval_btw_topic;
    double interval_btw_topic_noise_stddev;
    int msg_size;
    bool sample_log;
    bool tag_log;
    bool share_producer;
    int warmup_cnt;
    string warmup_topic;

    int start_barrier_delay;
    int monitoring_batch_size;
    int service_runner_pool_size;

    bool scrapable;
    bool verbose;

    string outdir;
};

class BasicMqttProducersTest: public AbstractApplication {
protected:
    void cleanup_main() override;

private:
    Arguments args;

    vector<unique_ptr<ServicesRunner>> services_runners;
    vector<thread> client_threads;
    vector<mosquitto *> shared_clients;

    shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor;
    random_device rd;

    void init_services();
    void init_standalone_services();
    void init_sharing_prod_services();
    void join_clients();

public:
    BasicMqttProducersTest(
        shared_ptr<moniq::MonitorQueue> &monitor_queue,
        shared_ptr<moniq::writer::MonitorLogWriter> &writer,
        shared_ptr<common::monitor::StatSumMonitorMessageGenerator> &adaptor,
        Arguments args
    ): AbstractApplication(monitor_queue, writer), args(args), adaptor(adaptor) {

    }

    virtual ~BasicMqttProducersTest() = default;

    void run() override;
};


// Define global variables & helper functions
namespace {
    BasicMqttProducersTest *app;

    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
}


// Entry point
int main(int argc, char *argv[]) {
    Arguments args = parse_arguments(argc, argv);

    cout << "mqtt producers test starts at " << util::current_time_str() << endl;
    if (args.verbose) {
        cout
            << "Broker: " << args.broker << "\n"
            << "Prefix: " << args.prefix << "\n"
            << "Client Count: " << args.client_cnt << "\n"
            << "Topic Count per Client: " << args.topic_cnt_per_client << "\n"
            << "Message Count per Topic: " << args.msg_cnt_per_topic << "\n"
            << "Interval: " << args.interval << "\n"
            << "Interval Noise Stddev: " << args.interval_noise_stddev << "\n"
            << "Interval Between Topics: " << args.interval_btw_topic << "\n"
            << "Interval Between Topics Noise Stddev: " << args.interval_btw_topic_noise_stddev << "\n"
            << "Message Size: " << args.msg_size << "\n"
            << "Sample Log: " << (args.sample_log ? "on" : "off") << "\n"
            << "Tag Record: " << (args.tag_log ? "on" : "off") << "\n"
            << "Share Producer: " << (args.share_producer ? "on" : "off") << "\n"
            << "Warmup Count: " << args.warmup_cnt << "\n"
            << "Warmup Topic: " << args.warmup_topic << "\n"
            << "Start Barrier Delay: " << args.start_barrier_delay << "\n"
            << "Monitoring Batch Size: " << args.monitoring_batch_size << "\n"
            << "Service Runner Pool Size: " << args.service_runner_pool_size << "\n"
            << "Scrapable: " << (args.scrapable ? "on" : "off") << "\n"
            << "Output Directory: " << args.outdir << endl;
    }

    shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor =
        make_shared<common::monitor::StatSumMonitorMessageGenerator>(args.msg_size, min(args.msg_size, 1000));
    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy =
        make_shared<moniq::writer::ConsoleMonitorLogWriteStrategy>(args.scrapable);
    shared_ptr<moniq::writer::MonitorLogWriter> writer =
        make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, args.monitoring_batch_size, -1);

    app = new BasicMqttProducersTest(monitor_queue, writer, adaptor, args);
    signal(SIGINT, interrupt_handler);
    signal(SIGTERM, interrupt_handler);
    app->run();
    app->cleanup();

    delete app;
    return 0;
}

void BasicMqttProducersTest::run() {
    init_services();

    start_barrier(args.start_barrier_delay);
    join_clients();
}

void BasicMqttProducersTest::init_services() {
    if (args.share_producer) init_sharing_prod_services();
    else init_standalone_services();

    for (const auto &service_runner: services_runners) {
        client_threads.push_back(thread(&ServicesRunner::run, service_runner.get()));
    }
}

void BasicMqttProducersTest::init_sharing_prod_services() {
    mt19937 rng(rd());
    for (int i = 0; i < args.client_cnt; i++) {
        vector<shared_ptr<IService>> services;
        mosquitto *mosq_client = producer::MosqProducerService::create_mosq_client(
            args.broker, args.prefix + to_string(i));
        shared_clients.push_back(mosq_client);

        for (int j = 0; j < args.topic_cnt_per_client; j++) {
            shared_ptr<IService> service = make_shared<producer::MosqProducerService>(
                mosq_client,
                args.prefix + to_string(i) + "_" + to_string(j),
                args.prefix + to_string(i) + "_" + to_string(j),
                args.msg_cnt_per_topic,
                args.interval,
                args.interval_noise_stddev,
                args.interval / 2,
                rng,
                (!args.sample_log || i == 0),
                args.tag_log,
                adaptor,
                monitor_queue,
                writer
            );
            services.push_back(service);
        }
        shared_ptr<IService> warmup_service = make_shared<producer::MosqProducerService>(
            mosq_client,
            "warmup",
            args.warmup_topic,
            args.warmup_cnt,
            0,
            0,
            0,
            rng,
            false,
            false,
            adaptor,
            monitor_queue,
            writer
        );

        services_runners.push_back(make_unique<ServicesRunner>(
            services, warmup_service, args.interval_btw_topic, 
            args.interval_btw_topic_noise_stddev, args.interval_btw_topic / 2,
            rng, &start_signal, args.service_runner_pool_size
        ));
    }
}

void BasicMqttProducersTest::init_standalone_services() {
    mt19937 rng(rd());
    for (int i = 0; i < args.client_cnt; i++) {
        vector<shared_ptr<IService>> services;
        for (int j = 0; j < args.topic_cnt_per_client; j++) {
            shared_ptr<IService> service = make_shared<producer::MosqProducerService>(
                args.broker,
                args.prefix + to_string(i),
                args.prefix + to_string(i) + "_" + to_string(j),
                args.prefix + to_string(i) + "_" + to_string(j),
                args.msg_cnt_per_topic,
                args.interval,
                args.interval_noise_stddev,
                args.interval / 2,
                rng,
                (!args.sample_log || i == 0),
                args.tag_log,
                adaptor,
                monitor_queue,
                writer
            );
            services.push_back(service);
        }
        shared_ptr<IService> warmup_service = make_shared<producer::MosqProducerService>(
            args.broker,
            args.prefix + "warmup_" + to_string(i),
            "warmup",
            args.warmup_topic,
            args.warmup_cnt,
            0,
            0,
            0,
            rng,
            false,
            false,
            adaptor,
            monitor_queue,
            writer
        );

        services_runners.push_back(make_unique<ServicesRunner>(
            services, warmup_service, args.interval_btw_topic, 
            args.interval_btw_topic_noise_stddev, args.interval_btw_topic / 2,
            rng, &start_signal, args.service_runner_pool_size
        ));
    }
}

void BasicMqttProducersTest::join_clients() {
    for (auto &client_thread: client_threads) {
        if (client_thread.joinable()) client_thread.join();
    }
    for (auto *mosq_client: shared_clients) {
        producer::MosqProducerService::cleanup_mosq_client(mosq_client, 1000);
    }
    shared_clients.clear();
}

void BasicMqttProducersTest::cleanup_main() {
    for (auto &service_runner: services_runners) {
        service_runner->close();
    }
    join_clients();
}


namespace {

void interrupt_handler(int signum) {
    cout << "Interrupt signal (" << signum << ") received." << endl;
    app->cleanup();
}

Arguments parse_arguments(int argc, char** argv) {
    int opt;
    Arguments args;

    args.client_cnt = 1;
    args.topic_cnt_per_client = 1;
    args.msg_cnt_per_topic = 1;
    args.interval = 1000.0;
    args.interval_noise_stddev = 0;
    args.interval_btw_topic = -1;
    args.interval_btw_topic_noise_stddev = 0;
    args.msg_size = 1000;
    args.sample_log = false;
    args.tag_log = false;
    args.share_producer = false;
    args.warmup_cnt = 0;
    args.warmup_topic = "test_warmup";
    args.start_barrier_delay = 5000;
    args.monitoring_batch_size = -1;
    args.service_runner_pool_size = 8;
    args.scrapable = false;
    args.verbose = false;
    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::PREFIX_OPTION,
        util::CLIENT_CNT_OPTION,
        util::TOPIC_CNT_PER_CLIENT_OPTION,
        util::MSG_CNT_PER_TOPIC_OPTION,
        util::INTERVAL_OPTION,
        util::INTERVAL_NOISE_STDDEV_OPTION,
        util::INTERVAL_BTW_TOPIC_OPTION,
        util::INTERVAL_BTW_TOPIC_NOISE_STDDEV_OPTION,
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
        util::MONITORING_BATCH_SIZE_OPTION,
        util::SERVICE_RUNNER_POOL_SIZE_OPTION,
        util::SCRAPABLE_OPTION,
        util::OUTDIR_OPTION,
        util::VERBOSE_OPTION,
    };

    const vector<struct option> long_options = make_long_opts(options);
    const string short_options = make_short_opts(options);
    while ((opt = getopt_long(argc, argv, short_options.c_str(), long_options.data(), nullptr)) != -1) {
        switch (opt) {
            case util::HELP_OPTION.get_val(): cout << make_help_message(options) << endl; exit(EXIT_SUCCESS); break;
            case util::BROKER_OPTION.get_val(): args.broker = optarg; break;
            case util::PREFIX_OPTION.get_val(): args.prefix = optarg; break;
            case util::CLIENT_CNT_OPTION.get_val(): args.client_cnt = atoi(optarg); break;
            case util::TOPIC_CNT_PER_CLIENT_OPTION.get_val(): args.topic_cnt_per_client = atoi(optarg); break;
            case util::MSG_CNT_PER_TOPIC_OPTION.get_val(): args.msg_cnt_per_topic = atoi(optarg); break;
            case util::INTERVAL_OPTION.get_val(): args.interval = atof(optarg); break;
            case util::INTERVAL_NOISE_STDDEV_OPTION.get_val(): args.interval_noise_stddev = atof(optarg); break;
            case util::INTERVAL_BTW_TOPIC_OPTION.get_val(): args.interval_btw_topic = atof(optarg); break;
            case util::INTERVAL_BTW_TOPIC_NOISE_STDDEV_OPTION.get_val(): args.interval_btw_topic_noise_stddev = atof(optarg); break;
            case util::MSG_SIZE_OPTION.get_val(): args.msg_size = atoi(optarg); break;
            case util::SAMPLE_LOG_OPTION.get_val(): args.sample_log = true; break;
            case util::TAG_RECORD_OPTION.get_val(): args.tag_log = true; break;
            case util::SHARE_PRODUCER_OPTION.get_val(): args.share_producer = true; break;
            case util::WARMUP_CNT_OPTION.get_val(): args.warmup_cnt = atoi(optarg); break;
            case util::WARMUP_TOPIC_OPTION.get_val(): args.warmup_topic = optarg; break;
            case util::START_BARRIER_DELAY_OPTION.get_val(): args.start_barrier_delay = atoi(optarg); break;
            case util::MONITORING_BATCH_SIZE_OPTION.get_val(): args.monitoring_batch_size = atoi(optarg); break;
            case util::SERVICE_RUNNER_POOL_SIZE_OPTION.get_val(): args.service_runner_pool_size = atoi(optarg); break;
            case util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case util::OUTDIR_OPTION.get_val(): args.outdir = optarg; break;
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