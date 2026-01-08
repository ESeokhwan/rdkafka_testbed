#pragma once

#include "service.h"

#include <libmoniq/adaptor/message_adaptor.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>
#include <memory>
#include <mosquitto.h>

#include <atomic>
#include <cstddef>

namespace common {
namespace producer {

class MosqProducerService: public AbstractService {
private:
    const std::string topic_name;
    const size_t round_cnt;
    const bool log_enabled;
    const bool msg_tagged;

    mosquitto *mosq_client;
    const bool need_to_cleanup_client;

    std::shared_ptr<moniq::adaptor::ILatencyMonitoringMessageAdaptor> adaptor;
    std::shared_ptr<moniq::MonitorQueue> monitor_queue;
    std::shared_ptr<moniq::writer::MonitorLogWriter> writer;

    std::atomic<size_t> cur_idx{0};

public:
    MosqProducerService(
        mosquitto *mosq_client,
        const std::string& topic_name,
        size_t round_cnt,
        int interval,
        double interval_noise_stddev,
        int interval_max_abs_noise,
        std::mt19937& rng,
        bool log_enabled,
        bool msg_tagged,
        std::shared_ptr<moniq::adaptor::ILatencyMonitoringMessageAdaptor> &adaptor,
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
    );

    MosqProducerService(
        const std::string& broker,
        const std::string& client_id,
        const std::string& topic_name,
        size_t round_cnt,
        int interval,
        double interval_noise_stddev,
        int interval_max_abs_noise,
        std::mt19937& rng,
        bool log_enabled,
        bool msg_tagged,
        std::shared_ptr<moniq::adaptor::ILatencyMonitoringMessageAdaptor> &adaptor,
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
    );

    virtual ~MosqProducerService() = default;

    bool is_done() override;

    void work() override;

    void close() override;

    static mosquitto *create_mosq_client(std::string broker, std::string client_id);

    static bool cleanup_mosq_client(mosquitto *mosq_client, int timeout_ms);

};

}
}