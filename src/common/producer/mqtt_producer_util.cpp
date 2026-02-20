#include "producer/mqtt_producer_util.h"

#include <stdexcept>

namespace common {
namespace producer {

mosquitto *create_mosq_client(std::string broker, std::string client_id) {
    mosquitto* mosq = mosquitto_new(client_id.c_str(), true, nullptr);
    if (!mosq) {
        throw std::runtime_error("Failed to create Mosquitto instance");
    }

    std::string host;
    int port;
    try {
        host = broker.substr(0, broker.find(':'));
        port = std::stoi(broker.substr(broker.find(':') + 1));
    } catch (...) {
        throw std::runtime_error("Invalid broker format");
    }

    int ret = mosquitto_connect(mosq, host.c_str(), port, 120);
    if (ret) {
        mosquitto_destroy(mosq);
        throw std::runtime_error("Could not connect to Broker");
    }

    mosquitto_loop_start(mosq);
    return mosq;
}

}
}