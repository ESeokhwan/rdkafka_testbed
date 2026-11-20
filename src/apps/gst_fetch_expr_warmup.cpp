#include <librdkafka/rdkafka.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
std::string warmup_id(const char *topic, int32_t partition, int64_t offset) {
    return std::string("gst-fetch-warmup-") + topic + "-" +
        std::to_string(partition) + "-" + std::to_string(offset);
}

struct Delivery {
    int count = 0;

    rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
};

void delivered(rd_kafka_t *, const rd_kafka_message_t *message, void *opaque) {
    auto &state = *static_cast<Delivery *>(opaque);
    if (message->err) {
        state.error = message->err;
        return;
    }
    ++state.count;
}

int remaining(Clock::time_point deadline) {
    const auto ms = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (ms <= 0) throw std::runtime_error("warmup deadline expired");
    return static_cast<int>(ms);
}

void check(rd_kafka_resp_err_t error) {
    if (error) throw std::runtime_error(rd_kafka_err2str(error));
}

bool index_pending(rd_kafka_resp_err_t code) {
    switch (code) {
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE:
            return true;
        default:
            return false;
    }
}

int64_t wait_for_index(rd_kafka_t *kafka, const char *topic, int64_t end,
                       Clock::time_point deadline) {
    std::unique_ptr<rd_kafka_Uuid_t, decltype(&rd_kafka_Uuid_destroy)> uuid(nullptr, rd_kafka_Uuid_destroy);
    bool verify_last_record = false;
    while (true) {
        rd_kafka_global_sequence_page_t *raw = nullptr;
        // Probe the committed end without addressing an uncommitted offset.
        const int64_t first = verify_last_record ? end - 1 : 0;
        const int64_t limit = verify_last_record ? end : 0;
        const int timeout = remaining(deadline);
        std::unique_ptr<rd_kafka_error_t, decltype(&rd_kafka_error_destroy)> error(
            rd_kafka_global_sequence_fetch(kafka, topic, uuid.get(), first, limit, nullptr, &raw, timeout),
            rd_kafka_error_destroy);
        std::unique_ptr<rd_kafka_global_sequence_page_t, decltype(&rd_kafka_global_sequence_page_destroy)>
            page(raw, rd_kafka_global_sequence_page_destroy);
        if (error) throw std::runtime_error(rd_kafka_error_string(error.get()));
        const auto *info = rd_kafka_global_sequence_page_info(page.get());
        if (info->error && !index_pending(info->error)) check(info->error);
        if (info->has_snapshot) {
            if (!uuid) uuid.reset(rd_kafka_Uuid_copy(info->topic_id));
            if (info->committed_global_end_offset > end)
                throw std::runtime_error("GST warmup end exceeded expected offset");
            if (!info->error && verify_last_record && info->committed_global_end_offset < end)
                throw std::runtime_error("GST committed end regressed during warmup verification");
            if (!info->error && info->committed_global_end_offset == end) {
                if (!verify_last_record) {
                    verify_last_record = true;
                    continue;
                }
                size_t count = 0;
                const auto *record = rd_kafka_global_sequence_page_records(page.get(), &count);
                moniq::adaptor::FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor;
                if (count != 1 || record->global_offset != end - 1 || !record->payload ||
                    adaptor.extract_content(std::string(static_cast<const char *>(record->payload), record->len))
                        != warmup_id(topic, record->physical_partition, record->physical_offset))
                    throw std::runtime_error("GST warmup boundary did not contain the final warmup record");
                return end;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(20, remaining(deadline))));
    }
}
using Producer = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;

Producer create_producer(const char *broker, const std::string &client_id, Delivery &state) {
    char reason[512];
    std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> conf(rd_kafka_conf_new(), rd_kafka_conf_destroy);
    const auto set = [&](const char *key, const char *value) {
        if (rd_kafka_conf_set(conf.get(), key, value, reason, sizeof(reason)) != RD_KAFKA_CONF_OK)
            throw std::runtime_error(reason);
    };
    set("bootstrap.servers", broker);
    set("client.id", client_id.c_str());
    set("batch.size", "1");
    set("linger.ms", "0");
    set("allow.auto.create.topics", "false");
    set("enable.idempotence", "false");
    set("acks", "0");
    rd_kafka_conf_set_opaque(conf.get(), &state);
    rd_kafka_conf_set_dr_msg_cb(conf.get(), delivered);
    Producer kafka(
        rd_kafka_new(RD_KAFKA_PRODUCER, conf.get(), reason, sizeof(reason)), rd_kafka_destroy);
    if (!kafka) throw std::runtime_error(reason);
    conf.release();
    return kafka;
}

void warm_partition(rd_kafka_t *producer, const char *topic, int partition, int records,
                    int message_size, std::chrono::milliseconds interval, Delivery &delivery, Clock::time_point deadline) {
    moniq::adaptor::FastJsonBasedLatencyMonitoringMessageGenerator generator(message_size, std::min(message_size, 1000));
    for (int i = 0; i < records; ++i) {
        auto message = generator.generate(warmup_id(topic, partition, i));
        while (true) {
            remaining(deadline);
            const auto error = rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic),
                RD_KAFKA_V_PARTITION(partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_VALUE(message.data(), message.size()), RD_KAFKA_V_END);
            if (error != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                check(error);
                break;
            }
            rd_kafka_poll(producer, std::min(50, remaining(deadline)));
            check(delivery.error);
        }
        const auto next = Clock::now() + interval;
        rd_kafka_poll(producer, 0);
        check(delivery.error);
        if (i + 1 < records) std::this_thread::sleep_until(std::min(deadline, next));
    }
    check(rd_kafka_flush(producer, remaining(deadline)));
    check(delivery.error);
    if (delivery.count != records) throw std::runtime_error("incomplete warmup send");

    while (true) {
        int64_t low = -1, high = -1;
        check(rd_kafka_query_watermark_offsets(producer, topic, partition, &low, &high, remaining(deadline)));
        if (low != 0 || high > records) throw std::runtime_error("unexpected warmup offsets");
        if (high == records) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(20, remaining(deadline))));
    }
}
}

int main(int argc, char **argv) try {
    if (argc < 7 || argc > 9) throw std::runtime_error("usage: gst_fetch_expr_warmup BROKER TOPIC gst|single PARTITIONS RECORDS_PER_PARTITION TIMEOUT_MS [MESSAGE_SIZE [INTERVAL_MS]]");
    const std::string_view mode(argv[3]);
    const int partitions = std::stoi(argv[4]);
    const int records = std::stoi(argv[5]);
    const int timeout = std::stoi(argv[6]);
    const int message_size = argc >= 8 ? std::stoi(argv[7]) : 16;
    const auto interval = std::chrono::milliseconds(argc == 9 ? std::stoi(argv[8]) : 1);
    if (interval.count() <= 0) throw std::runtime_error("warmup interval must be positive");
    if (message_size <= 0 || partitions <= 0 || records <= 0 || timeout <= 0 ||
        (mode != "gst" && (mode != "single" || partitions != 1)))
        throw std::runtime_error("invalid warmup mode, partition count, record count or timeout");

    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
    std::vector<Delivery> deliveries(partitions);
    std::vector<Producer> producers;
    for (int p = 0; p < partitions; ++p) {
        producers.push_back(create_producer(argv[1], std::string("gst-warmup-") + std::to_string(p), deliveries[p]));
    }
    std::vector<std::future<void>> workers;
    for (int p = 0; p < partitions; ++p) {
        workers.push_back(std::async(std::launch::async, [&, p] {
            try {
                warm_partition(producers[p].get(), argv[2], p, records, message_size, interval, deliveries[p], deadline);
            } catch (const std::exception &error) {
                throw std::runtime_error("partition " + std::to_string(p) + ": " + error.what());
            }
        }));
    }
    for (auto &worker : workers) worker.get();
    const int64_t start = mode == "gst"
        ? wait_for_index(producers.front().get(), argv[2], static_cast<int64_t>(partitions) * records, deadline)
        : records;
    producers.clear();
    std::cout << start << '\n';
    return 0;
} catch (const std::exception &error) {
    std::cerr << "warmup failed: " << error.what() << '\n';
    return 1;
}
