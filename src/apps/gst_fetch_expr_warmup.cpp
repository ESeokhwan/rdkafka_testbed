#include <librdkafka/rdkafka.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::string_view payload = "gst-fetch-warmup";

struct Delivery {
    int count = 0;
    int64_t last_offset = -1;
};

struct Deliveries {
    std::vector<Delivery> partitions;
    rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
};

void delivered(rd_kafka_t *, const rd_kafka_message_t *message, void *opaque) {
    auto &state = *static_cast<Deliveries *>(opaque);
    if (message->err) {
        state.error = message->err;
        return;
    }
    auto &partition = state.partitions.at(message->partition);
    ++partition.count;
    partition.last_offset = std::max(partition.last_offset, message->offset);
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
    while (true) {
        rd_kafka_global_sequence_page_t *raw = nullptr;
        const int timeout = remaining(deadline);
        std::unique_ptr<rd_kafka_error_t, decltype(&rd_kafka_error_destroy)> error(
            rd_kafka_global_sequence_fetch(kafka, topic, uuid.get(), end - 1, end, nullptr, &raw, timeout),
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
            if (!info->error && info->committed_global_end_offset == end) {
                size_t count = 0;
                const auto *record = rd_kafka_global_sequence_page_records(page.get(), &count);
                if (count != 1 || record->global_offset != end - 1 || !record->payload ||
                    std::string_view(static_cast<const char *>(record->payload), record->len) != payload)
                    throw std::runtime_error("GST warmup boundary did not contain the final warmup record");
                return end;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(20, remaining(deadline))));
    }
}
}

int main(int argc, char **argv) try {
    if (argc != 7) throw std::runtime_error("usage: gst_fetch_expr_warmup BROKER TOPIC gst|single PARTITIONS RECORDS_PER_PARTITION TIMEOUT_MS");
    const std::string_view mode(argv[3]);
    const int partitions = std::stoi(argv[4]);
    const int records = std::stoi(argv[5]);
    const int timeout = std::stoi(argv[6]);
    if (partitions <= 0 || records <= 0 || timeout <= 0 ||
        (mode != "gst" && (mode != "single" || partitions != 1)))
        throw std::runtime_error("invalid warmup mode, partition count, record count or timeout");

    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
    Deliveries state{std::vector<Delivery>(partitions)};
    char reason[512];
    std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> conf(rd_kafka_conf_new(), rd_kafka_conf_destroy);
    const auto set = [&](const char *key, const char *value) {
        if (rd_kafka_conf_set(conf.get(), key, value, reason, sizeof(reason)) != RD_KAFKA_CONF_OK)
            throw std::runtime_error(reason);
    };
    set("bootstrap.servers", argv[1]);
    set("allow.auto.create.topics", "false");
    set("enable.idempotence", "true");
    set("acks", "all");
    rd_kafka_conf_set_opaque(conf.get(), &state);
    rd_kafka_conf_set_dr_msg_cb(conf.get(), delivered);
    std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> kafka(
        rd_kafka_new(RD_KAFKA_PRODUCER, conf.get(), reason, sizeof(reason)), rd_kafka_destroy);
    if (!kafka) throw std::runtime_error(reason);
    conf.release();
    for (int partition = 0; partition < partitions; ++partition) {
        for (int i = 0; i < records; ++i) {
            remaining(deadline);
            check(rd_kafka_producev(kafka.get(), RD_KAFKA_V_TOPIC(argv[2]),
                RD_KAFKA_V_PARTITION(partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_VALUE(const_cast<char *>(payload.data()), payload.size()), RD_KAFKA_V_END));
        }
    }
    check(rd_kafka_flush(kafka.get(), remaining(deadline)));
    check(state.error);
    for (int partition = 0; partition < partitions; ++partition) {
        const auto &delivery = state.partitions[partition];
        int64_t low = -1, high = -1;
        check(rd_kafka_query_watermark_offsets(kafka.get(), argv[2], partition, &low, &high, remaining(deadline)));
        if (delivery.count != records || delivery.last_offset != records - 1 || low != 0 || high != records)
            throw std::runtime_error("unexpected warmup offsets for partition " + std::to_string(partition));
    }
    const int64_t start = mode == "gst"
        ? wait_for_index(kafka.get(), argv[2], static_cast<int64_t>(partitions) * records, deadline)
        : state.partitions[0].last_offset + 1;
    kafka.reset();
    std::cout << start << '\n';
    return 0;
} catch (const std::exception &error) {
    std::cerr << "warmup failed: " << error.what() << '\n';
    return 1;
}
