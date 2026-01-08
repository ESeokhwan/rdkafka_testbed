#include "producer/producer_service.h"

#include "util/noise_util.h"
#include "util/time_util.h"

namespace {

void flush_producer(RdKafka::Producer* producer) {
    while (producer->outq_len() > 0) {
        producer->poll(0);
    }
}

}

namespace common {
namespace producer {

ProducerService::ProducerService(
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
): AbstractService(
    round_cnt, interval, 
    util::generate_noises(
        interval_noise_stddev, interval_max_abs_noise,
        std::min(round_cnt, util::MAX_NOISE_LIST_LENGTH), rng)
    ), topic_name(topic_name), round_cnt(round_cnt),
    is_sync(is_sync), ignore_response(ignore_response), need_flush(need_flush),
    log_enabled(log_enabled), msg_tagged(msg_tagged),
    producer(producer), need_to_cleanup_producer(false),
    adaptor(adaptor), monitor_queue(monitor_queue), writer(writer) {}

ProducerService::ProducerService(
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
): AbstractService(
    round_cnt, interval, 
    util::generate_noises(
        interval_noise_stddev, interval_max_abs_noise,
        std::min(round_cnt, util::MAX_NOISE_LIST_LENGTH), rng)
    ), topic_name(topic_name), round_cnt(round_cnt), is_sync(is_sync),
    ignore_response(ignore_response), need_flush(need_flush),
    log_enabled(log_enabled), msg_tagged(msg_tagged),
    need_to_cleanup_producer(true), adaptor(adaptor),
    monitor_queue(monitor_queue), writer(writer)
{
    std::string errstr;
    dr_cb = new LoggingDeliveryReportCb(adaptor, monitor_queue, writer);
    std::unique_ptr<RdKafka::Conf> conf = create_producer_conf(
        brokers, client_id, is_sync || !ignore_response,
        (log_enabled && !ignore_response) ? dr_cb: &NoOpsDeliveryReportCb::get_instance(),
        errstr
    );
    producer = RdKafka::Producer::create(conf.get(), errstr);
    if (!producer) {
        throw std::runtime_error(errstr);
    }
}

bool ProducerService::is_done() {
    return cur_idx.load(std::memory_order_relaxed) >= round_cnt;
}

void ProducerService::work() {
    size_t idx = cur_idx.fetch_add(1, std::memory_order_relaxed);
    std::string core_msg = topic_name + "_" + std::to_string(idx);
    if (msg_tagged && log_enabled) core_msg = "R" + core_msg;

    std::string msg = this->adaptor->generate(core_msg);
    if (log_enabled) {
        double requested_at = double(util::get_current_timestamp_nano()) / (1000.0 * 1000.0);
        monitor_queue->enqueue(std::make_unique<moniq::MonitorLog>(
            core_msg, "REQUEST", requested_at));
        writer->notify_if_needed();
    }
    producer->produce(
        topic_name, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, 
        const_cast<void*>(static_cast<const void*>(msg.c_str())),
        msg.size(), nullptr, 0, 0, nullptr
    );
    if (need_flush || is_sync) flush_producer(producer);
}

void ProducerService::close() {
    if (producer != nullptr && need_to_cleanup_producer) {
        flush_producer(producer);
        delete producer;
        producer = nullptr;
    }
}

void LoggingDeliveryReportCb::dr_cb(RdKafka::Message &message) {
    double responded_at = double(common::util::get_current_timestamp_nano()) / (1000.0 * 1000.0);
    if (message.err() == RdKafka::ERR_NO_ERROR) {
        std::string message_plain_str = message.payload() ? 
            std::string(static_cast<const char*>(message.payload()), message.len()) : "";
        std::string message_str = adaptor->extract_content(message_plain_str);
        monitor_queue->enqueue(std::make_unique<moniq::MonitorLog>(
            message_str, "RESPONDED", responded_at));
        writer->notify_if_needed();
    }
}

std::unique_ptr<RdKafka::Conf> ProducerService::create_producer_conf(
    std::string brokers,
    std::string client_id,
    bool need_acks,
    RdKafka::DeliveryReportCb* dr_cb,
    std::string &errstr
) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("client.id", client_id, errstr);
    conf->set("batch.size", "1", errstr);
    conf->set("linger.ms", "0", errstr);
    conf->set("acks", need_acks ? "all" : "0", errstr);
    conf->set("dr_cb", dr_cb, errstr);
    return conf;
}

}
}