#include "abstract_application.h"
#include "monitor/stat_sum_monitor_message_adaptor.h"
#include "service.h"
#include "service_runner.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "producer/mqtt_producer_service.h"

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
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>
#include <libmoniq/adaptor/message_adaptor.h>

using namespace std;
using namespace common;


// Define structs & classes
struct Arguments {
    string broker;

    string client_prefix;
    string topic_prefix;
    int client_cnt;
    int running_time;

    double interval_noise_stddev_rate;
    int warmup_cnt;
    string warmup_topic;

    int start_barrier_delay;
    int monitoring_batch_size;

    bool scrapable;
    bool verbose;

    string outdir;
};

struct ServiceInfo {
    string service_name;
    double interval;
    int msg_size;
};

class V2xMqttExprProducerApp: public AbstractApplication {
protected:
    void cleanup_main() override;

private:
    Arguments args;

    vector<unique_ptr<ServicesRunner>> services_runners;
    vector<thread> client_threads;
    vector<mosquitto *> mosq_clients;

    random_device rd;
    mt19937 rng = mt19937(rd());

    void init_clients();
    shared_ptr<IService> make_service(mosquitto *mosq_client, std::string service_name, std::string topic, double interval, int msg_size);
    shared_ptr<IService> make_warmup_service(mosquitto *mosq_client);
    void join_clients();

public:
    V2xMqttExprProducerApp(
        shared_ptr<moniq::MonitorQueue> &monitor_queue,
        shared_ptr<moniq::writer::MonitorLogWriter> &writer,
        Arguments args
    ): AbstractApplication(monitor_queue, writer), args(args) {

    }

    virtual ~V2xMqttExprProducerApp() = default;

    void run() override;
};


// Define global variables & helper functions
namespace {
    V2xMqttExprProducerApp *app;
    vector<ServiceInfo> service_infos = {
        {"S50Hz", 1000.0/50.0, 6500},
        {"S30Hz", 1000.0/30.0, 400},
        {"S10Hz-Info", 1000.0/10.0, 1600},
        {"S10Hz-Sensor", 1000.0/10.0, 1600},
    };

    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
}


// Entry point
int main(int argc, char *argv[]) {
    Arguments args = parse_arguments(argc, argv);

    cout
        << "client start\n"
        << "Broker: " << args.broker << "\n"
        << "Client Prefix: " << args.client_prefix << "\n"
        << "Topic Prefix: " << args.topic_prefix << "\n"
        << "Client Count: " << args.client_cnt << "\n"
        << "Running Time: " << args.running_time << "\n"
        << "Interval Noise Stddev Rate: " << args.interval_noise_stddev_rate << "\n"
        << "Warmup Count: " << args.warmup_cnt << "\n"
        << "Warmup Topic: " << args.warmup_topic << "\n"
        << "Start Barrier Delay: " << args.start_barrier_delay << "\n"
        << "Monitoring Batch Size: " << args.monitoring_batch_size << "\n"
        << "Scrapable: " << (args.scrapable ? "on" : "off") << "\n"
        << "Output Directory: " << args.outdir << "\n"
        << "Verbose: " << (args.verbose ? "on" : "off") << "\n"
        << "Start time: " << util::current_time_str() << endl;

    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy =
        make_shared<moniq::writer::ConsoleMonitorLogWriteStrategy>(args.scrapable);
    shared_ptr<moniq::writer::MonitorLogWriter> writer =
        make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, args.monitoring_batch_size, -1);

    app = new V2xMqttExprProducerApp(monitor_queue, writer, args);
    signal(SIGINT, interrupt_handler);
    app->run();
    app->cleanup();

    delete app;
    return 0;
}

void V2xMqttExprProducerApp::run() {
    init_clients();

    start_barrier(args.start_barrier_delay);
    join_clients();
}

void V2xMqttExprProducerApp::init_clients() {
    for (int i = 0; i < args.client_cnt; i++) {
        vector<shared_ptr<IService>> services;
        mosquitto *mosq_client = producer::MosqProducerService::create_mosq_client(
            args.broker, args.client_prefix + to_string(i));
        mosq_clients.push_back(mosq_client);

        for (const auto &service_info: service_infos) {
            services.push_back(make_service(
                mosq_client, service_info.service_name,
                args.topic_prefix + service_info.service_name + "/Car" + to_string(i), 
                service_info.interval, service_info.msg_size
            ));
        }
        auto warmup_service = make_warmup_service(mosq_client);

        services_runners.push_back(make_unique<ServicesRunner>(
            services, warmup_service, -1, 0, -1, rng, &start_signal, min(service_infos.size(), 4UL)
        ));
    }

    for (const auto &service_runner: services_runners) {
        client_threads.push_back(thread(&ServicesRunner::run, service_runner.get()));
    }
}

shared_ptr<IService> V2xMqttExprProducerApp::make_service(mosquitto *mosq_client, std::string service_name, std::string topic, double interval, int msg_size) {
    shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor =
        make_shared<common::monitor::StatSumMonitorMessageGenerator>(msg_size, min(msg_size, 1000));
    return make_shared<producer::MosqProducerService>(
        mosq_client,
        service_name,
        topic,
        (args.running_time * 1000) / interval,
        interval,
        interval * args.interval_noise_stddev_rate,
        interval / 2,
        rng,
        true,
        false,
        adaptor,
        monitor_queue,
        writer
    );
}

shared_ptr<IService> V2xMqttExprProducerApp::make_warmup_service(mosquitto *mosq_client) {
    shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor = 
        make_shared<common::monitor::StatSumMonitorMessageGenerator>(10, 100);
    return make_shared<producer::MosqProducerService>(
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
}

void V2xMqttExprProducerApp::join_clients() {
    for (auto &client_thread: client_threads) {
        if (client_thread.joinable()) client_thread.join();
    }
    for (auto *mosq_client: mosq_clients) {
        producer::MosqProducerService::cleanup_mosq_client(mosq_client, 1000);
    }
    mosq_clients.clear();
}

void V2xMqttExprProducerApp::cleanup_main() {
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

    args.client_prefix = "";
    args.topic_prefix = "";
    args.client_cnt = 1;
    args.running_time = 10;
    args.interval_noise_stddev_rate = 0.0;
    args.warmup_cnt = 0;
    args.warmup_topic = "test_warmup";
    args.start_barrier_delay = 5000;
    args.monitoring_batch_size = -1;
    args.scrapable = false;
    args.verbose = false;

    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::CLIENT_PREFIX_OPTION,
        util::TOPIC_PREFIX_OPTION,
        util::CLIENT_CNT_OPTION,
        util::RUNNING_TIME_OPTION,
        util::INTERVAL_NOISE_STDDEV_RATE_OPTION,
        util::WARMUP_CNT_OPTION,
        util::WARMUP_TOPIC_OPTION,
        util::START_BARRIER_DELAY_OPTION,
        util::MONITORING_BATCH_SIZE_OPTION,
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
            case util::CLIENT_PREFIX_OPTION.get_val(): args.client_prefix = optarg; break;
            case util::TOPIC_PREFIX_OPTION.get_val(): args.topic_prefix = optarg; break;
            case util::CLIENT_CNT_OPTION.get_val(): args.client_cnt = atoi(optarg); break;
            case util::RUNNING_TIME_OPTION.get_val(): args.running_time = atoi(optarg); break;
            case util::INTERVAL_NOISE_STDDEV_RATE_OPTION.get_val(): args.interval_noise_stddev_rate = atof(optarg); break;
            case util::WARMUP_CNT_OPTION.get_val(): args.warmup_cnt = atoi(optarg); break;
            case util::WARMUP_TOPIC_OPTION.get_val(): args.warmup_topic = optarg; break;
            case util::START_BARRIER_DELAY_OPTION.get_val(): args.start_barrier_delay = atoi(optarg); break;
            case util::MONITORING_BATCH_SIZE_OPTION.get_val(): args.monitoring_batch_size = atoi(optarg); break;
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