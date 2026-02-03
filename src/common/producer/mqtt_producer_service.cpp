#include "producer/mqtt_producer_service.h"

#include "util/noise_util.h"
#include "util/time_util.h"

#include <chrono>
#include <thread>
#include <mosquitto.h>
#include <stdexcept>

namespace {
    bool mosq_flush(mosquitto *mosq_client, int timeout_ms) {
        if (!mosq_client) return false;

        int64_t start_tick_ns = common::util::get_current_nano_tick();
        int64_t timeout_ns = timeout_ms * 1000 * 1000;

        while (mosquitto_want_write(mosq_client)) {
            int64_t current_tick_ns = common::util::get_current_nano_tick();
            if ((current_tick_ns - start_tick_ns) > timeout_ns) return false;

            mosquitto_loop(mosq_client, 0, 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }
}

namespace common {
namespace producer {

MosqProducerService::MosqProducerService(
    mosquitto *mosq_client,
    const std::string& service_name,
    const std::string& topic_name,
    size_t round_cnt,
    double interval,
    double interval_noise_stddev,
    double interval_max_abs_noise,
    std::mt19937& rng,
    bool log_enabled,
    bool msg_tagged,
    std::shared_ptr<common::monitor::StatSumMonitorMessageGenerator> &adaptor,
    std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
    std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
): AbstractService(
    round_cnt, interval,
    util::generate_noises(
        interval_noise_stddev, interval_max_abs_noise,
        std::min(round_cnt, util::MAX_NOISE_LIST_LENGTH), rng)
    ), service_name(service_name), topic_name(topic_name), round_cnt(round_cnt),
    log_enabled(log_enabled), msg_tagged(msg_tagged),
    mosq_client(mosq_client), need_to_cleanup_client(false),
    adaptor(adaptor), monitor_queue(monitor_queue), writer(writer) {}

MosqProducerService::MosqProducerService(
    const std::string& host,
    const std::string& client_id,
    const std::string& service_name,
    const std::string& topic_name,
    size_t round_cnt,
    double interval,
    double interval_noise_stddev,
    double interval_max_abs_noise,
    std::mt19937& rng,
    bool log_enabled,
    bool msg_tagged,
    std::shared_ptr<common::monitor::StatSumMonitorMessageGenerator> &adaptor,
    std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
    std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
): AbstractService(
    round_cnt, interval,
    util::generate_noises(
        interval_noise_stddev, interval_max_abs_noise,
        std::min(round_cnt, util::MAX_NOISE_LIST_LENGTH), rng)
    ), service_name(service_name), topic_name(topic_name), round_cnt(round_cnt),
    log_enabled(log_enabled), msg_tagged(msg_tagged),
    mosq_client(create_mosq_client(host, client_id)),
    need_to_cleanup_client(true), adaptor(adaptor),
    monitor_queue(monitor_queue), writer(writer) {}

bool MosqProducerService::is_done() {
    return cur_idx.load(std::memory_order_relaxed) >= round_cnt;
}

void MosqProducerService::work() {
    size_t idx = cur_idx.fetch_add(1, std::memory_order_relaxed);
    std::string core_msg = topic_name + "_" + std::to_string(idx);
    if (msg_tagged && log_enabled) core_msg = "R" + core_msg;

    std::string msg = this->adaptor->generate(core_msg, service_name);
    if (log_enabled) {
        double requested_at = double(util::get_current_timestamp());
        monitor_queue->enqueue(std::make_unique<moniq::MonitorLog>(
            core_msg, "REQUEST", requested_at));
        writer->notify_if_needed();
    }
    int rc = mosquitto_publish(this->mosq_client, nullptr,
        topic_name.c_str(), msg.size(), msg.c_str(), 1, false);
    if (rc != MOSQ_ERR_SUCCESS) {
        throw std::runtime_error("Publish failed for " + topic_name + ": " + mosquitto_strerror(rc));
    }
}

void MosqProducerService::close() {
    if (this->mosq_client != nullptr && need_to_cleanup_client) {
        cleanup_mosq_client(this->mosq_client, 1000);
        this->mosq_client = nullptr;
    }
}

mosquitto *MosqProducerService::create_mosq_client(std::string broker, std::string client_id) {
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

bool MosqProducerService::cleanup_mosq_client(mosquitto *mosq_client, int timeout_ms) {
    if (mosq_client == nullptr) return false;
    mosquitto_disconnect(mosq_client);
    int res = mosq_flush(mosq_client, timeout_ms);
    mosquitto_loop_stop(mosq_client, false);
    mosquitto_destroy(mosq_client);
    return res;
}

}
}