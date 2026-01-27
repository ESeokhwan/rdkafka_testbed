#include "connector/mqtt_kafka_connector.h"
#include "util/cli_arg_util.h"
#include "util/time_util.h"

#include <csignal>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <mosquitto.h>
#include <librdkafka/rdkafkacpp.h>

using namespace std;

struct Arguments {
    string kafka_broker;
    string mqtt_broker;

    int running_time;

    bool scrapable;
    bool log_disabled;
    bool verbose;
};


// Define global variables & helper functions
namespace {
    atomic<bool> g_signal_received(false);
    Arguments args;
    vector<unique_ptr<MqttKafkaConnector>> connectors;

    Arguments parse_arguments(int argc, char **argv);
    void interrupt_handler(int signum);
}


int main(int argc, char *argv[]) {
    signal(SIGINT, interrupt_handler);
    signal(SIGTERM, interrupt_handler);
    args = parse_arguments(argc, argv);

    cout << "mqtt kafka connector starts at " << common::util::current_time_str() << endl;
    if (args.verbose) {
        cout
            << "Mqtt Broker: " << args.mqtt_broker << "\n"
            << "Kafka Broker: " << args.kafka_broker << "\n"
            << "Running Time: " << args.running_time << "\n"
            << "Scrapable: " << (args.scrapable ? "on" : "off") << "\n"
            << "No logging: " << (args.log_disabled ? "on" : "off") << endl;
    }

    vector<string> service_names = {
        "S50Hz",
        "S30Hz",
        "S10Hz-Info",
        "S10Hz-Sensor",
    };

    for (const string &service_name: service_names) {
        connectors.push_back(make_unique<MqttKafkaConnector>(
            args.kafka_broker,
            args.mqtt_broker,
            service_name,
            service_name + "/#",
            service_name
        ));
    }

    if (!args.log_disabled) {
        std::thread([]() {
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(10));

                cout << "\n================== [Connect Stats] ==================" << endl;

                for (size_t i = 0; i < connectors.size(); i++) {
                    int attempt = connectors[i]->get_total_send_cnt();
                    int success = connectors[i]->get_success_send_cnt();
                    int fail = connectors[i]->get_fail_send_cnt();
                    double loss_rate = (attempt > 0) ? (100.0 * fail / attempt) : 0.0;

                    cout << "[" << connectors[i]->get_id() << "] "
                        << "Attemption: " << attempt
                        << " | Success: " << success
                        << " | Fail: " << fail
                        << " | LOSS: " << fixed << setprecision(2) << loss_rate << "%" << endl;
                }

                cout << "=====================================================" << endl;
            }
        }).detach();
    }

    auto end_time = chrono::steady_clock::now() + chrono::seconds(args.running_time);
    while (chrono::steady_clock::now() < end_time) {
        if (g_signal_received.load()) {
            break;
        }
        this_thread::sleep_for(chrono::seconds(1));
    }
    return 0;
}

namespace {

void interrupt_handler(int signum) {
    g_signal_received.store(true);
}

Arguments parse_arguments(int argc, char** argv) {
    int opt;
    Arguments args;

    args.running_time = 10;
    args.scrapable = false;
    args.log_disabled = false;
    args.verbose = false;

    static vector<common::util::OptionWrapper> options = {
        common::util::HELP_OPTION,
        common::util::KAFKA_BROKER_OPTION,
        common::util::MQTT_BROKER_OPTION,
        common::util::RUNNING_TIME_OPTION,
        common::util::SCRAPABLE_OPTION,
        common::util::NO_LOG_OPTION,
        common::util::VERBOSE_OPTION,
    };

    const vector<struct option> long_options = make_long_opts(options);
    const string short_options = make_short_opts(options);
    while ((opt = getopt_long(argc, argv, short_options.c_str(), long_options.data(), nullptr)) != -1) {
        switch (opt) {
            case common::util::HELP_OPTION.get_val(): cout << make_help_message(options) << endl; exit(EXIT_SUCCESS); break;
            case common::util::KAFKA_BROKER_OPTION.get_val(): args.kafka_broker = optarg; break;
            case common::util::MQTT_BROKER_OPTION.get_val(): args.mqtt_broker = optarg; break;
            case common::util::RUNNING_TIME_OPTION.get_val(): args.running_time = atoi(optarg); break;
            case common::util::SCRAPABLE_OPTION.get_val(): args.scrapable = true; break;
            case common::util::NO_LOG_OPTION.get_val(): args.log_disabled = true; break;
            case common::util::VERBOSE_OPTION.get_val(): args.verbose = true; break;
            default:
                cerr << "Error: Unknown option or missing argument." << endl << endl;
                cerr << make_help_message(options) << endl;
                exit(EXIT_FAILURE);
        }
    }

    return args;
}

}