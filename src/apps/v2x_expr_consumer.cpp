#include "libmoniq/writer/write_strategy/monitor_log_write_strategy.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"
#include "consumer/consumer_util.h"
#include "monitor/stat_sum_monitor_log.h"
#include "monitor/stat_sum_monitor_log_write_strategy.h"

#include <atomic>
#include <csignal>
#include <condition_variable>
#include <iostream>
#include <fstream>
#include <memory>
#include <mutex>
#include <getopt.h>
#include <thread>

#include <librdkafka/rdkafkacpp.h>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>

using namespace std;
using namespace common;

struct Arguments {
    string broker;

    string prefix;
    int client_cnt;
    int running_time;

    bool scrapable;
    bool read_tagged_only;
    bool verbose;

    string outdir;
};

struct ConsumerThreadArg {
    string broker;
    string group_id;
    string client_id;

    vector<string> topics;

    shared_ptr<moniq::MonitorQueue> monitor_queue;
    shared_ptr<moniq::writer::MonitorLogWriter> writer;

    bool read_tagged_only;
    bool verbose;
};

struct ServiceArg {
    string name;
    double threshold;
};

vector<int> assign_services(int num_cars, int num_services);

// Global variables
mutex m;
condition_variable cv;
atomic<bool> start_flag(false);
atomic<bool> end_flag(false);

void interrupt_handler(int signum) {
    cout << "Interrupt signal (" << signum << ") received." << endl;
    end_flag = true;
    cv.notify_all();
}

void parse_arguments(int argc, char** argv, Arguments& args) {
    int opt;

    args.scrapable = false;
    args.read_tagged_only = false;
    args.verbose = false;
    static vector<util::OptionWrapper> options = {
        util::HELP_OPTION,
        util::BROKER_OPTION,
        util::PREFIX_OPTION,
        util::CLIENT_CNT_OPTION,
        util::RUNNING_TIME_OPTION,
        util::SCRAPABLE_OPTION,
        util::READ_TAGGED_ONLY_OPTION,
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
            case util::RUNNING_TIME_OPTION.get_val(): args.running_time = atoi(optarg); break;
            case util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case util::READ_TAGGED_ONLY_OPTION.get_val(): args.read_tagged_only = true; break;
            case util::OUTDIR_OPTION.get_val(): args.outdir = optarg; break;
            case util::VERBOSE_OPTION.get_val(): args.verbose = true; break;
            default:
                cerr << "Error: Unknown option or missing argument." << endl << endl;
                cerr << make_help_message(options) << endl;
                exit(EXIT_FAILURE);
        }
    }
}

void consume_run(struct ConsumerThreadArg *arg) {
    string errstr;
    unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", arg->broker, errstr);
    conf->set("group.id", arg->group_id, errstr);
    conf->set("client.id", arg->client_id, errstr);
    conf->set("auto.offset.reset", "earliest", errstr);
    conf->set("fetch.min.bytes", "1", errstr);
    conf->set("log_level", "0", errstr);
    if (arg->verbose) {
        conf->set("log_level", "7", errstr);
    }
    unique_ptr<RdKafka::KafkaConsumer> consumer = consumer::create_consumer(conf.get());
    if (consumer.get() == nullptr) return;

    // Wait for the start flag to be set
    while (!start_flag.load(memory_order_acquire)) {
        this_thread::yield();
    }

    if (!consumer::subscribe_topics(consumer.get(), arg->topics)) return;

    while (!end_flag.load(memory_order_acquire)) {
        optional<string> plain_msg_opt = consumer::consume_message(consumer.get(), 1);
        if (!plain_msg_opt.has_value()) continue;
        arg->monitor_queue->enqueue(
            make_unique<monitor::StatSumMonitorLog>(
                plain_msg_opt.value(), "Responded", util::get_current_timestamp()
            )
        );
        arg->writer->notify_if_needed();
    }
}

int main(int argc, char *argv[]) {
    Arguments args;
    parse_arguments(argc, argv, args);

    cout
        << "client start\n"
        << "Broker: " << args.broker << "\n"
        << "Prefix: " << args.prefix << "\n"
        << "Client Count: " << args.client_cnt << "\n"
        << "Running Time: " << args.running_time << "\n"
        << "Scrapable: " << (args.scrapable ? "on" : "off") << "\n"
        << "Log Sampling: " << (args.read_tagged_only ? "on" : "off") << "\n"
        << "Output Directory: " << args.outdir << "\n"
        << "Verbose: " << (args.verbose ? "on" : "off") << "\n"
        << "Start time: " << util::current_time_str() << endl;

    std::string latency_file_postfix = "latency.csv";
    std::string per_sec_file_postfix = "per_sec.csv";
    vector<struct ServiceArg> service_args = {
        {"S10Hz-Info", 100},
        {"S10Hz-Sensor", 100},
        {"S30Hz", 25},
        {"S50Hz", 20},
    };

    vector<unique_ptr<ostream>> latency_outs;
    vector<unique_ptr<ostream>> per_sec_outs;
    vector<monitor::ServiceInfo> services;
    for (size_t i = 0; i < service_args.size(); i++) {
        unique_ptr<ostream> latecny_out = make_unique<ofstream>(
            args.outdir + "/" + service_args[i].name + "_" + latency_file_postfix);
        unique_ptr<ostream> per_sec_out = make_unique<ofstream>(
            args.outdir + "/" + service_args[i].name + "_" + per_sec_file_postfix);
        services.push_back({
            service_args[i].name, service_args[i].threshold,
            latecny_out.get(), per_sec_out.get(), &cout
        });
        latency_outs.push_back(std::move(latecny_out));
        per_sec_outs.push_back(std::move(per_sec_out));
    }

    shared_ptr<moniq::MonitorQueue> monitor_queue = make_shared<moniq::MonitorQueue>();
    shared_ptr<moniq::writer::IMonitorLogWriteStrategy> write_strategy = make_shared<monitor::StatSumPerSecMonitorLogWriteStrategy>(services);
    shared_ptr<moniq::writer::MonitorLogWriter> writer = make_shared<moniq::writer::MonitorLogWriter>(monitor_queue, write_strategy, -1, -1);

    thread writer_thread(&moniq::writer::MonitorLogWriter::run, writer);

    vector<struct ConsumerThreadArg> consumer_thread_args(args.client_cnt);
    vector<thread> consumer_threads;

    vector<int> assigned_idx = assign_services(args.client_cnt, services.size());

    for (int i = 0; i < args.client_cnt; i++) {
        consumer_thread_args[i].broker = args.broker;
        consumer_thread_args[i].group_id = args.prefix + "group_" + to_string(i);
        consumer_thread_args[i].topics.push_back(args.prefix + services[assigned_idx[i]].name);
        consumer_thread_args[i].verbose = args.verbose;
        consumer_thread_args[i].read_tagged_only = args.read_tagged_only;
        consumer_thread_args[i].monitor_queue = monitor_queue;
        consumer_thread_args[i].writer = writer;
        consumer_threads.emplace_back(consume_run, &consumer_thread_args[i]);
    }

    cout << "All threads are ready. Starting publishing for " << args.client_cnt << " clients." << endl;
    cout << "Sleep 5s to wait Kakfa" << endl;
    this_thread::sleep_for(chrono::milliseconds(5000));

    signal(SIGINT, interrupt_handler);
    start_flag.store(true, memory_order_release);

    {
        unique_lock<mutex> lock(m);
        cv.wait_for(lock, chrono::milliseconds(args.running_time));
    }
    end_flag.store(true, memory_order_release);

    for (auto& consumer_thread: consumer_threads) {
        consumer_thread.join();
    }
    writer->graceful_shutdown();
    writer_thread.join();

    return 0;
}

vector<int> assign_services(int num_cars, int num_services) {
    vector<int> assigned;
    for (int i = 0; i < num_cars; i++) assigned.push_back(i % num_services);
    return assigned;
}