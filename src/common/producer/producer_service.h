#pragma once

#include "libmoniq/adaptor/message_adaptor.h"
#include "service.h"

#include <atomic>
#include <cstddef>
#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>
#include <librdkafka/rdkafkacpp.h>

namespace common {
namespace producer {

class ProducerService: public AbstractService {
private:
    const std::string topic_name;
    const size_t round_cnt;
    const bool is_sync;
    const bool ignore_response;
    const bool need_flush;
    const bool log_enabled;
    const bool msg_tagged;

    RdKafka::Producer *producer;
    const bool need_to_cleanup_producer;
    RdKafka::DeliveryReportCb* dr_cb; // it will be set null if `producer` is injected from outside.

    std::shared_ptr<moniq::adaptor::IMessageAdaptor> adaptor;
    std::shared_ptr<moniq::MonitorQueue> monitor_queue;
    std::shared_ptr<moniq::writer::MonitorLogWriter> writer;

    std::atomic<size_t> cur_idx{0};

public:
    ProducerService(
        RdKafka::Producer *producer,
        const std::string& topic_name,
        size_t round_cnt,
        int interval,
        double interval_noise_stddev,
        int interval_max_abs_noise,
        std::mt19937& rng,
        bool is_sync,
        bool ignore_response,
        bool need_flush,
        bool log_enabled,
        bool msg_tagged,
        std::shared_ptr<moniq::adaptor::IMessageAdaptor> &adaptor,
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
    );

    ProducerService(
        const std::string& brokers,
        const std::string& client_id,
        const std::string& topic_name,
        size_t round_cnt,
        int interval,
        double interval_noise_stddev,
        int interval_max_abs_noise,
        std::mt19937& rng,
        bool is_sync,
        bool ignore_response,
        bool need_flush,
        bool log_enabled,
        bool msg_tagged,
        std::shared_ptr<moniq::adaptor::IMessageAdaptor> &adaptor,
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
    );

    virtual ~ProducerService() = default;

    bool is_done() override;

    void work() override;

    void close() override;

    static std::unique_ptr<RdKafka::Conf> create_producer_conf(
        std::string brokers,
        std::string client_id,
        bool need_acks,
        RdKafka::DeliveryReportCb* dr_cb,
        std::string &errstr
    );

};

class LoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
private:
    std::shared_ptr<moniq::adaptor::IMessageAdaptor> adaptor;
    std::shared_ptr<moniq::MonitorQueue> monitor_queue;
    std::shared_ptr<moniq::writer::MonitorLogWriter> writer;

public:
    LoggingDeliveryReportCb(
        std::shared_ptr<moniq::adaptor::IMessageAdaptor> &adaptor,
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
    ): adaptor(adaptor), monitor_queue(monitor_queue), writer(writer) {}

    void dr_cb(RdKafka::Message &message) override;
};

class NoOpsDeliveryReportCb : public RdKafka::DeliveryReportCb {
private:
    NoOpsDeliveryReportCb() {}
    ~NoOpsDeliveryReportCb() {}

public:
    static NoOpsDeliveryReportCb& get_instance() {
        static NoOpsDeliveryReportCb instance;
        return instance;
    }

    void dr_cb(RdKafka::Message &message) override {}

    NoOpsDeliveryReportCb(const NoOpsDeliveryReportCb&) = delete;
    NoOpsDeliveryReportCb& operator=(const NoOpsDeliveryReportCb&) = delete;
    NoOpsDeliveryReportCb(NoOpsDeliveryReportCb&&) = delete;
    NoOpsDeliveryReportCb& operator=(NoOpsDeliveryReportCb&&) = delete;
};

}
}