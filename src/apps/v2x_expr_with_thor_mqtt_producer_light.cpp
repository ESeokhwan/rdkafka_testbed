#include "abstract_application.h"
#include "monitor/stat_sum_monitor_message_adaptor.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "producer/mqtt_producer_util.h"

#include <atomic>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <getopt.h>
#include <cmath>

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

    string client_prefix;
    string topic_prefix;
    int client_cnt;
    int start_idx;
    int wakeup_interval;
    int running_time;

    int start_barrier_delay;
    int client_spread_time;
    int client_spread_interval;

    bool log_disabled;

    bool scrapable;
    bool verbose;
};

struct ServiceConf {
    string service_name;
    int data_rate;
    int msg_size;
};

struct ServiceInfo {
    struct ServiceConf conf;
    int max_cnt;
    shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor;

    int cur_idx;
    std::chrono::duration<long double, std::milli> waited_duration;
};

class V2xMqttExprProducerAppV2: public AbstractApplication {
protected:
    void cleanup_main() override;

private:
    Arguments args;
    vector<thread> client_threads;
    vector<mosquitto *> mosq_clients;
    vector<shared_ptr<std::latch>> start_signals;

    void client_run(int car_id, shared_ptr<std::latch> start_signal);
    vector<ServiceInfo> make_services();
    void publish_message(mosquitto *, const std::string &, const std::string &, int, std::shared_ptr<common::monitor::StatSumMonitorMessageGenerator> &);

    void init_clients();
    void start_uniformly();
    void join_clients();

public:
    V2xMqttExprProducerAppV2(
        shared_ptr<moniq::MonitorQueue> &monitor_queue,
        shared_ptr<moniq::writer::MonitorLogWriter> &writer,
        Arguments args
    ): AbstractApplication(monitor_queue, writer), args(args) {

    }

    virtual ~V2xMqttExprProducerAppV2() = default;

    void run() override;
};


// Define global variables & helper functions
namespace {
    V2xMqttExprProducerAppV2 *app;
    vector<ServiceConf> service_confs = {
        {"3Cooperative_driving_for_vehicle_platooning_lower", 50, 6500},
        {"2Cooperative_driving_for_vehicle_platooning_lowest", 30, 400},
        {"1Information_sharing_for_automated_driving", 10, 6500},
        {"0Sensor_information_sharing", 10, 1600},
    };
    atomic<bool> end_flag;

    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
}


// Entry point
int main(int argc, char *argv[]) {
    Arguments args = parse_arguments(argc, argv);

    cout << "v2x expr mqtt producer v2 starts at " << util::current_time_str() << endl;
    if (args.verbose) {
        cout
            << "Broker: " << args.broker << "\n"
            << "Client Prefix: " << args.client_prefix << "\n"
            << "Topic Prefix: " << args.topic_prefix << "\n"
            << "Client Count: " << args.client_cnt << "\n"
            << "Start Index: " << args.start_idx << "\n"
            << "Wakeup Interval: " << args.wakeup_interval << "\n"
            << "Running Time: " << args.running_time << "\n"
            << "Start Barrier Delay: " << args.start_barrier_delay << "\n"
            << "Client Spread Time: " << args.client_spread_time << "ms\n"
            << "Client Spread Interval: " << args.client_spread_interval << "ms\n"
            << "No logging: " << (args.log_disabled ? "on" : "off") << "\n"
            << "Scrapable: " << (args.scrapable ? "on" : "off") << endl;
    }

    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy =
        make_shared<moniq::writer::ConsoleMonitorLogWriteStrategy>(args.scrapable);
    shared_ptr<moniq::writer::MonitorLogWriter> writer =
        make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, -1, -1);

    app = new V2xMqttExprProducerAppV2(monitor_queue, writer, args);
    signal(SIGINT, interrupt_handler);
    signal(SIGTERM, interrupt_handler);
    app->run();
    app->cleanup();

    delete app;
    return 0;
}

void V2xMqttExprProducerAppV2::run() {
    init_clients();
    start_uniformly();
    join_clients();
}

void V2xMqttExprProducerAppV2::init_clients() {
    mosquitto_lib_init();
    for (int i = args.start_idx; i < args.start_idx + args.client_cnt; i++) {
        std::shared_ptr<std::latch> start_signal = std::make_shared<std::latch>(1);
        start_signals.push_back(start_signal);
        client_threads.emplace_back(&V2xMqttExprProducerAppV2::client_run, this, i, start_signal);
    }
}

void V2xMqttExprProducerAppV2::start_uniformly() {
    int possible_spread_step_cnt = 1;
    if (args.client_spread_time > 0 && args.client_spread_interval > 0) {
        possible_spread_step_cnt = args.client_spread_time / args.client_spread_interval;
    }
    int client_cnt_per_spread_step = (int) std::ceil((double) args.client_cnt / (double) possible_spread_step_cnt);
    start_barrier(args.start_barrier_delay);
    for (int i = 0; i < possible_spread_step_cnt; i++) {
        for (int j = 0; j < client_cnt_per_spread_step; j++) {
            int cur_idx = i * client_cnt_per_spread_step + j;
            if (cur_idx >= args.client_cnt) break;
            auto cur_start_signal = start_signals.at(cur_idx);
            cur_start_signal->count_down();
        }
        if (i < possible_spread_step_cnt - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(args.client_spread_interval));
        }
    }
}

void V2xMqttExprProducerAppV2::join_clients() {
    for (auto &t : client_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void V2xMqttExprProducerAppV2::cleanup_main() {
    mosquitto_lib_cleanup();
}

void V2xMqttExprProducerAppV2::client_run(int car_id, shared_ptr<std::latch> start_signal) {
    string client_id = args.client_prefix + to_string(car_id);
    mosquitto *mosq_client = producer::create_mosq_client(args.broker, client_id);
    vector<ServiceInfo> service_infos = make_services();

    start_signal->wait();

    int elasped_tick = 0;
    int end_tick = args.running_time * 1000;
    int max_interval = 0;
    for (auto &service_info: service_infos) {
        max_interval = max(max_interval, (int) ceil(1000.0/(double) service_info.conf.data_rate));
    }
    while (elasped_tick < end_tick + max_interval) {
        if (end_flag.load(memory_order_acquire)) break;
        std::chrono::milliseconds wakeup_interval_duration(args.wakeup_interval);
        for (auto &service_info: service_infos) {
            if (service_info.cur_idx >= service_info.max_cnt) continue;
            auto interval = chrono::duration<long double, milli>(1000.0/(double) service_info.conf.data_rate);
            string name = service_info.conf.service_name;
            service_info.waited_duration += wakeup_interval_duration;
            if (service_info.waited_duration >= interval) {
                publish_message(
                    mosq_client, name,
                    args.topic_prefix + args.topic_prefix + "Car" + to_string(car_id),
                    service_info.cur_idx, service_info.adaptor
                );
                service_info.waited_duration -= interval;
                service_info.cur_idx += 1;
            }
        }

        elasped_tick += args.wakeup_interval;
        std::this_thread::sleep_for(wakeup_interval_duration);
    }

    mosquitto_loop_stop(mosq_client, true);
    mosquitto_destroy(mosq_client);
}

vector<ServiceInfo> V2xMqttExprProducerAppV2::make_services() {
    vector<ServiceInfo> service_infos;
    for (auto service_conf: service_confs) {
        shared_ptr<common::monitor::StatSumMonitorMessageGenerator> adaptor = 
            make_shared<common::monitor::StatSumMonitorMessageGenerator>(service_conf.msg_size, 1000000);
        service_infos.push_back({
            service_conf, args.running_time * service_conf.data_rate, adaptor, 0, 0.0ms
        });
    }
    return service_infos;
}

void V2xMqttExprProducerAppV2::publish_message(
    mosquitto* mosq,
    const std::string &service_name,
    const std::string &topic_name,
    int idx,
    std::shared_ptr<common::monitor::StatSumMonitorMessageGenerator> &adaptor
) {
    std::string core_msg = topic_name + "_" + std::to_string(idx);

    std::string msg = adaptor->generate(core_msg, service_name);
    if (!args.log_disabled) {
        monitor_queue->enqueue(std::make_unique<moniq::MonitorLog>(
            core_msg, "REQUEST", util::get_current_timestamp()
        ));
        writer->notify_if_needed();
    }
    int rc = mosquitto_publish(mosq, nullptr,
        topic_name.c_str(), msg.size(), msg.c_str(), 1, false);
    if (rc != MOSQ_ERR_SUCCESS) {
        throw std::runtime_error("Publish failed for " + topic_name + ": " + mosquitto_strerror(rc));
    }
}


namespace {

void interrupt_handler(int signum) {
    end_flag.store(true, memory_order_release);
}

Arguments parse_arguments(int argc, char **argv) {
    int opt;
    Arguments args;

    // Default values
    args.broker = "localhost:1883";
    args.client_prefix = "";
    args.topic_prefix = "";
    args.client_cnt = 1;
    args.start_idx = 0;
    args.wakeup_interval = 5;
    args.running_time = 10;
    args.start_barrier_delay = 2;
    args.client_spread_time = 100;
    args.client_spread_interval = 5;
    args.log_disabled = false;
    args.scrapable = false;
    args.verbose = false;

    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::CLIENT_PREFIX_OPTION,
        util::TOPIC_PREFIX_OPTION,
        util::CLIENT_CNT_OPTION,
        util::START_IDX_OPTION,
        util::WAKEUP_INTERVAL_OPTION,
        util::RUNNING_TIME_OPTION,
        util::START_BARRIER_DELAY_OPTION,
        util::CLIENT_SPREAD_TIME_OPTION,
        util::CLIENT_SPREAD_INTERVAL_OPTION,
        util::NO_LOG_OPTION,
        util::SCRAPABLE_OPTION,
        util::VERBOSE_OPTION,
    };

    const vector<struct option> long_options = util::make_long_opts(options);
    const string short_options = util::make_short_opts(options);

    while ((opt = getopt_long(argc, argv, short_options.c_str(), long_options.data(), nullptr)) != -1) {
        switch (opt) {
            case util::HELP_OPTION.get_val(): cout << util::make_help_message(options) << endl; exit(EXIT_SUCCESS); break;
            case util::BROKER_OPTION.get_val(): args.broker = optarg; break;
            case util::CLIENT_PREFIX_OPTION.get_val(): args.client_prefix = optarg; break;
            case util::TOPIC_PREFIX_OPTION.get_val(): args.topic_prefix = optarg; break;
            case util::CLIENT_CNT_OPTION.get_val(): args.client_cnt = atoi(optarg); break;
            case util::START_IDX_OPTION.get_val(): args.start_idx = atoi(optarg); break;
            case util::WAKEUP_INTERVAL_OPTION.get_val(): args.wakeup_interval = atoi(optarg); break;
            case util::RUNNING_TIME_OPTION.get_val(): args.running_time = atoi(optarg); break;
            case util::START_BARRIER_DELAY_OPTION.get_val(): args.start_barrier_delay = atoi(optarg); break;
            case util::CLIENT_SPREAD_TIME_OPTION.get_val(): args.client_spread_time = atoi(optarg); break;
            case util::CLIENT_SPREAD_INTERVAL_OPTION.get_val(): args.client_spread_interval = atoi(optarg); break;
            case util::NO_LOG_OPTION.get_val(): args.log_disabled = true; break;
            case util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case util::VERBOSE_OPTION.get_val(): args.verbose = true; break;
            default:
                cerr << "Error: Unknown option or missing argument." << endl << endl;
                cerr << util::make_help_message(options) << endl;
                exit(EXIT_FAILURE);
        }
    }
    return args;
}

}