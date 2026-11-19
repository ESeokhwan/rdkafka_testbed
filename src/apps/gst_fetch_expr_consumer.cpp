#include <librdkafka/rdkafka.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>
#include <libmoniq/include/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csignal>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using json = nlohmann::json;
using Clock = std::chrono::steady_clock;
using Page = std::unique_ptr<rd_kafka_global_sequence_page_t, decltype(&rd_kafka_global_sequence_page_destroy)>;
using FetchError = std::unique_ptr<rd_kafka_error_t, decltype(&rd_kafka_error_destroy)>;
using Uuid = std::unique_ptr<rd_kafka_Uuid_t, decltype(&rd_kafka_Uuid_destroy)>;
using Kafka = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;

namespace {
volatile std::sig_atomic_t stopping = 0;
void interrupt(int) { stopping = 1; }

int64_t wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

struct Arguments {
    std::string broker = "166.104.110.13:9092", topic, output, ready_file, topic_mode = "gst";
    int64_t start = 0, end = std::numeric_limits<int64_t>::max(), batch_size = 1000;
    bool bounded = false, latency_log = true, prefetch = false;
    int max_batches = 1000, max_bytes = 1048576, isolation = 0;
    int timeout_ms = 10000, poll_ms = 1, interval_ms = 1000, max_errors = 10;
    double duration = 60;
};

struct PartitionConsumer {
    rd_kafka_topic_t *topic = nullptr;
    bool started = false;
    ~PartitionConsumer() {
        if (started) rd_kafka_consume_stop(topic, 0);
        if (topic) rd_kafka_topic_destroy(topic);
    }
};

struct MessageBatch {
    std::vector<rd_kafka_message_t *> values;
    size_t count = 0;
    explicit MessageBatch(size_t capacity) : values(capacity) {}
    ~MessageBatch() {
        for (size_t i = 0; i < count; ++i) rd_kafka_message_destroy(values[i]);
    }
};

double ms_between(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

struct Timing {
    uint64_t samples = 0;
    double total_ms = 0;

    void add(double ms) {
        ++samples;
        total_ms += ms;
    }
};

struct FetchRequest {
    int64_t start, end;
    const rd_kafka_Uuid_t *topic_id;
};

struct FetchResult {
    Page page{nullptr, rd_kafka_global_sequence_page_destroy};
    FetchError error{nullptr, rd_kafka_error_destroy};
    Clock::time_point started, returned;
    int64_t received_at = 0;
};

struct GstFetcher {
    rd_kafka_t *kafka;
    const Arguments &args;
    const rd_kafka_global_sequence_fetch_options_t &options;
    Clock::time_point deadline;

    FetchResult fetch(const FetchRequest &request) const {
        FetchResult result;
        rd_kafka_global_sequence_page_t *raw = nullptr;
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0) return result;
        const int timeout = static_cast<int>(std::min<int64_t>(args.timeout_ms, remaining));
        result.started = Clock::now();
        auto *error = rd_kafka_global_sequence_fetch(kafka, args.topic.c_str(), request.topic_id,
            request.start, request.end, &options, &raw, timeout);
        result.returned = Clock::now();
        result.received_at = wall_ms();
        result.page.reset(raw);
        result.error.reset(error);
        return result;
    }
};

class PrefetchWorker {
public:
    explicit PrefetchWorker(const GstFetcher &fetcher) : fetcher_(fetcher), thread_([this] { run(); }) {}

    ~PrefetchWorker() {
        stop();
        thread_.join();
    }

    void submit(FetchRequest request) {
        {
            std::lock_guard lock(mutex_);
            if (request_ || result_) throw std::logic_error("GST prefetch slot is occupied");
            request_ = request;
        }
        changed_.notify_one();
    }

    FetchResult take() {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [this] { return result_.has_value(); });
        auto result = std::move(*result_);
        result_.reset();
        return result;
    }

    void stop() {
        {
            std::lock_guard lock(mutex_);
            stopped_ = true;
        }
        changed_.notify_one();
    }

private:
    void run() {
        std::unique_lock lock(mutex_);
        for (;;) {
            changed_.wait(lock, [this] { return stopped_ || request_.has_value(); });
            if (stopped_) return;
            const auto request = *request_;
            lock.unlock();
            auto result = fetcher_.fetch(request);
            lock.lock();
            if (stopped_) return;
            request_.reset();
            result_.emplace(std::move(result));
            changed_.notify_one();
        }
    }

    const GstFetcher &fetcher_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::optional<FetchRequest> request_;
    std::optional<FetchResult> result_;
    bool stopped_ = false;
    std::thread thread_;
};

int64_t integer(const std::string &text, const std::string &name, int64_t minimum,
                int64_t maximum = std::numeric_limits<int64_t>::max()) {
    size_t used = 0;
    int64_t value;
    try { value = std::stoll(text, &used); }
    catch (...) { throw std::runtime_error("Invalid " + name + ": " + text); }
    if (used != text.size() || value < minimum || value > maximum)
        throw std::runtime_error("Invalid " + name + ": " + text);
    return value;
}

void usage() {
    std::cout << "Usage: gst_fetch_expr_consumer --topic NAME --output-dir DIR [options]\n"
        "  --broker HOST:PORT          Bootstrap broker\n"
        "  --topic-mode gst|single     API98 or ordinary partition 0 (gst)\n"
        "  --start-offset N            First global/partition position (0)\n"
        "  --end-offset N              Exclusive finite target; otherwise follow until duration\n"
        "  --batch-size N              GST position range / single delivery cap (1000)\n"
        "  --max-batches N             API98 production-batch limit, 1..1000 (1000)\n"
        "  --max-bytes N               API98 soft byte limit, 1..16646144 (1048576)\n"
        "  --isolation-level 0|1       read_uncommitted/read_committed (0)\n"
        "  --duration SEC              Overall measurement deadline (60)\n"
        "  --fetch-timeout-ms N        Deadline per API call (10000)\n"
        "  --poll-interval-ms N        Backoff after no progress/errors (1)\n"
        "  --metrics-interval-ms N     Throughput reporting interval (1000)\n"
        "  --prefetch on|off          Overlap GST processing with one Fetch (off)\n"
        "  --max-consecutive-errors N  Retriable errors before failure (10)\n"
        "  --no-latency-log            Keep exact aggregate latency statistics only\n"
        "  --ready-file PATH          Write after local client/output initialization\n";
}

Arguments parse(int argc, char **argv) {
    Arguments a;
    for (int i = 1; i < argc; ++i) {
        std::string option = argv[i];
        if (option == "--no-latency-log") { a.latency_log = false; continue; }
        if (i + 1 >= argc) throw std::runtime_error("Missing value for " + option);
        std::string value = argv[++i];
        if (option == "--broker") a.broker = value;
        else if (option == "--topic-mode") a.topic_mode = value;
        else if (option == "--topic") a.topic = value;
        else if (option == "--output-dir") a.output = value;
        else if (option == "--ready-file") a.ready_file = value;
        else if (option == "--start-offset") a.start = integer(value, option, 0);
        else if (option == "--end-offset") { a.end = integer(value, option, 0); a.bounded = true; }
        else if (option == "--batch-size") a.batch_size = integer(value, option, 1);
        else if (option == "--max-batches") a.max_batches = static_cast<int>(integer(value, option, 1, 1000));
        else if (option == "--max-bytes") a.max_bytes = static_cast<int>(integer(value, option, 1, 16646144));
        else if (option == "--isolation-level") a.isolation = static_cast<int>(integer(value, option, 0, 1));
        else if (option == "--prefetch") {
            if (value != "on" && value != "off") throw std::runtime_error("prefetch must be on or off");
            a.prefetch = value == "on";
        }
        else if (option == "--fetch-timeout-ms") a.timeout_ms = static_cast<int>(integer(value, option, 1, 2147483647));
        else if (option == "--poll-interval-ms") a.poll_ms = static_cast<int>(integer(value, option, 0, 2147483647));
        else if (option == "--metrics-interval-ms") a.interval_ms = static_cast<int>(integer(value, option, 1, 2147483647));
        else if (option == "--max-consecutive-errors") a.max_errors = static_cast<int>(integer(value, option, 1, 2147483647));
        else if (option == "--duration") {
            size_t used = 0;
            a.duration = std::stod(value, &used);
            if (used != value.size() || !std::isfinite(a.duration) || a.duration <= 0 || a.duration > 1e9)
                throw std::runtime_error("Invalid duration");
        } else throw std::runtime_error("Unknown option: " + option);
    }
    if (a.topic.empty() || a.broker.empty() || a.output.empty() || a.end < a.start)
        throw std::runtime_error("Need topic, broker, output-dir and a valid offset range");
    if (a.topic_mode != "gst" && a.topic_mode != "single") throw std::runtime_error("Invalid topic-mode");
    if (a.topic_mode == "single" && a.prefetch) throw std::runtime_error("prefetch requires GST mode");
    if (a.topic_mode == "single" && (a.batch_size > 2147483647 || a.max_bytes < 1000))
        throw std::runtime_error("single mode needs batch-size <= 2147483647 and max-bytes >= 1000");
    return a;
}

bool retryable(rd_kafka_resp_err_t code) {
    switch (code) {
        case RD_KAFKA_RESP_ERR__TRANSPORT:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
        case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
        case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
        case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
        case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR:
        case RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED:
            return true;
        default: return false;
    }
}

std::string csv_quote(const std::string &text) {
    std::string result = "\"";
    for (char value : text) { if (value == '"') result += '"'; result += value; }
    return result + '"';
}

void write_json(const std::filesystem::path &path, const json &value) {
    std::ofstream output(path);
    if (!output) throw std::runtime_error("Cannot write " + path.string());
    output << value.dump(2) << '\n';
    output.close();
    if (!output) throw std::runtime_error("Failed writing " + path.string());
}

struct Histogram {
    std::map<int64_t, uint64_t> buckets;
    uint64_t count = 0;
    long double sum = 0;
    void add(int64_t value) { ++buckets[value]; ++count; sum += value; }
    json summary() const {
        json result = {{"samples", count}, {"mean_ms", nullptr}, {"p50_ms", nullptr},
                       {"p95_ms", nullptr}, {"p99_ms", nullptr}, {"min_ms", nullptr}, {"max_ms", nullptr}};
        if (!count) return result;
        result["mean_ms"] = static_cast<double>(sum / count);
        result["min_ms"] = buckets.begin()->first;
        result["max_ms"] = buckets.rbegin()->first;
        for (unsigned percentile : {50U, 95U, 99U}) {
            const uint64_t rank = count / 100 * percentile + ((count % 100 * percentile + 99) / 100);
            uint64_t seen = 0;
            for (const auto &[value, frequency] : buckets) {
                seen += frequency;
                if (seen >= rank) { result["p" + std::to_string(percentile) + "_ms"] = value; break; }
            }
        }
        return result;
    }
};

int run(const Arguments &a) {
    const bool single = a.topic_mode == "single";
    std::filesystem::create_directories(a.output);
    const std::filesystem::path out(a.output);
    std::vector<char> latency_buffer(1024 * 1024), throughput_buffer(65536);
    std::ofstream latency, throughput;
    latency.rdbuf()->pubsetbuf(latency_buffer.data(), latency_buffer.size());
    throughput.rdbuf()->pubsetbuf(throughput_buffer.data(), throughput_buffer.size());
    throughput.open(out / "throughput.csv");
    if (!throughput) throw std::runtime_error("Cannot open throughput.csv");
    throughput << "measured_at,measurement_duration_ms,consumed_records,payload_bytes,"
        "throughput_records_per_sec,throughput_payload_bytes_per_sec,fetch_calls,empty_pages,local_errors,broker_errors,"
        "next_global_offset,committed_global_end_offset,next_offset,partition,"
        "nonempty_fetch_samples,nonempty_fetch_total_ms,post_fetch_samples,post_fetch_total_ms,"
        "fetch_wait_samples,fetch_wait_total_ms,page_process_samples,page_process_total_ms,"
        "consume_cycle_samples,consume_cycle_total_ms\n";
    if (a.latency_log) {
        latency.open(out / "latency.csv");
        if (!latency) throw std::runtime_error("Cannot open latency.csv");
        latency << "Content,Status,RequestedAt,RespondedAt,Latency,GlobalOffset,Partition,Offset,PayloadBytes\n";
    }
    std::ofstream errors(out / "fetch_errors.csv");
    if (!errors) throw std::runtime_error("Cannot open fetch_errors.csv");
    errors << "measured_at,scope,error_code,cursor,message\n";
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char reason[512];
    std::map<std::string, std::string> settings{
            {"bootstrap.servers", a.broker}, {"client.id", "gst-fetch-measurement"},
            {"allow.auto.create.topics", "false"}};
    if (single) {
        settings.insert({{"enable.auto.commit", "false"}, {"enable.auto.offset.store", "false"},
            {"enable.partition.eof", "true"}, {"isolation.level", a.isolation ? "read_committed" : "read_uncommitted"},
            {"fetch.message.max.bytes", std::to_string(a.max_bytes)}, {"fetch.max.bytes", std::to_string(a.max_bytes)},
            {"message.max.bytes", std::to_string(std::min(a.max_bytes, 1000000))},
            {"fetch.wait.max.ms", "500"}, {"fetch.min.bytes", "1"}});
    }
    for (const auto &[key, value] : settings) {
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), reason, sizeof(reason)) != RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            throw std::runtime_error(reason);
        }
    }
    // GST uses a producer handle only as API98 transport. The single baseline
    // consumes partition 0 directly without a group, subscription, or commits.
    Kafka kafka(rd_kafka_new(single ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER, conf, reason, sizeof(reason)), rd_kafka_destroy);
    if (!kafka) { rd_kafka_conf_destroy(conf); throw std::runtime_error(reason); }
    PartitionConsumer partition;
    if (single) {
        rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
        for (const auto &[key, value] : std::map<std::string, std::string>{{"auto.commit.enable", "false"}, {"auto.offset.reset", "error"}}) {
            if (rd_kafka_topic_conf_set(topic_conf, key.c_str(), value.c_str(), reason, sizeof(reason)) != RD_KAFKA_CONF_OK) {
                rd_kafka_topic_conf_destroy(topic_conf);
                throw std::runtime_error(reason);
            }
        }
        partition.topic = rd_kafka_topic_new(kafka.get(), a.topic.c_str(), topic_conf);
        if (!partition.topic || rd_kafka_consume_start(partition.topic, 0, a.start) != 0)
            throw std::runtime_error(rd_kafka_err2str(rd_kafka_last_error()));
        partition.started = true;
    }
    rd_kafka_global_sequence_fetch_options_t options;
    rd_kafka_global_sequence_fetch_options_init(&options);
    options.max_batches = a.max_batches;
    options.max_bytes = a.max_bytes;
    options.isolation_level = a.isolation;
    Uuid expected(nullptr, rd_kafka_Uuid_destroy);
    moniq::adaptor::FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor;
    Histogram e2e;
    uint64_t records = 0, bytes = 0, fetches = 0, empty = 0, local_errors = 0, broker_errors = 0;
    uint64_t invalid = 0, negative = 0, skipped = 0, order_errors = 0, max_page_records = 0;
    uint64_t eof_events = 0;
    uint64_t previous_records = 0, previous_bytes = 0, previous_fetches = 0, previous_empty = 0;
    uint64_t previous_local = 0, previous_broker = 0;
    int64_t next = a.start, last_global = a.start - 1, committed = -1;
    int consecutive_errors = 0;
    double fetch_time_ms = 0;
    Timing nonempty_fetch, post_fetch, fetch_wait, page_process, consume_cycle;
    std::optional<Clock::time_point> nonempty_fetch_returned_at, previous_page_processed_at;
    std::string failure;
    const int64_t started_at = wall_ms();
    const auto start = Clock::now();
    auto last_report = start;
    const auto deadline = start + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(a.duration));
    const GstFetcher fetcher{kafka.get(), a, options, deadline};
    std::optional<PrefetchWorker> prefetch;
    if (a.prefetch) prefetch.emplace(fetcher);
    write_json(out / "config.json", {{"broker", a.broker}, {"topic", a.topic}, {"topic_mode", a.topic_mode},
        {"offset_scope", single ? "partition" : "global"}, {"partition", single ? json(0) : json(nullptr)}, {"start_offset", a.start},
        {"end_offset", a.bounded ? json(a.end) : json(nullptr)}, {"batch_size", a.batch_size},
        {"max_batches", single ? json(nullptr) : json(a.max_batches)}, {"max_bytes", a.max_bytes}, {"isolation_level", a.isolation},
        {"prefetch", a.prefetch}, {"single_consumer_settings", single ? json(settings) : json(nullptr)},
        {"single_delivery_strategy", single ? json("wait_one_then_nonblocking_drain") : json(nullptr)},
        {"duration_s", a.duration}, {"fetch_timeout_ms", a.timeout_ms}, {"poll_interval_ms", a.poll_ms},
        {"latency_log", a.latency_log}, {"latency_unit", "ms"}, {"latency_percentile_method", "nearest_rank"}});
    if (!a.ready_file.empty()) {
        auto temporary = a.ready_file + ".tmp";
        write_json(temporary, {{"initialized", true}, {"topic", a.topic}, {"started_at", started_at}});
        std::filesystem::rename(temporary, a.ready_file);
    }
    auto report = [&](bool final) {
        auto now = Clock::now();
        double elapsed_ms = std::chrono::duration<double, std::milli>(now - last_report).count();
        if (!final && elapsed_ms < a.interval_ms) return;
        elapsed_ms = std::max(0.001, elapsed_ms);
        throughput << std::setprecision(15) << wall_ms() << ',' << elapsed_ms << ',' << records - previous_records
            << ',' << bytes - previous_bytes << ',' << (records - previous_records) * 1000.0 / elapsed_ms
            << ',' << (bytes - previous_bytes) * 1000.0 / elapsed_ms << ',' << fetches - previous_fetches
            << ',' << empty - previous_empty << ',' << local_errors - previous_local << ',' << broker_errors - previous_broker
            << ',';
        if (!single) throughput << next;
        throughput << ',';
        if (!single) throughput << committed;
        throughput << ',' << next << ',';
        if (single) throughput << 0;
        for (auto *timing : {&nonempty_fetch, &post_fetch, &fetch_wait, &page_process, &consume_cycle}) {
            throughput << ',';
            if (!single) throughput << timing->samples;
            throughput << ',';
            if (!single) throughput << timing->total_ms;
            *timing = {};
        }
        throughput << '\n';
        if (!throughput || (a.latency_log && !latency) || !errors) throw std::runtime_error("Metric output write failed");
        previous_records = records; previous_bytes = bytes; previous_fetches = fetches;
        previous_empty = empty; previous_local = local_errors; previous_broker = broker_errors;
        last_report = now;
    };
    auto backoff = [&]() {
        const auto until = std::min(deadline, Clock::now() + std::chrono::milliseconds(a.poll_ms));
        while (!stopping && Clock::now() < until)
            std::this_thread::sleep_until(std::min(until, Clock::now() + std::chrono::milliseconds(10)));
    };
    auto collect = [&](int64_t position, int32_t physical_partition, int64_t physical_offset,
                       const void *payload_data, size_t payload_size, int64_t received_at) {
        if (position <= last_global || (a.isolation == 0 && position != last_global + 1)) {
            ++order_errors;
            throw std::runtime_error("Nonmonotonic/duplicate/gapped " + std::string(single ? "partition offset" : "global sequence"));
        }
        last_global = position;
        ++records;
        bytes += payload_size;
        std::string content;
        int64_t requested_at = -1;
        if (payload_data) {
            std::string payload(static_cast<const char *>(payload_data), payload_size);
            content = adaptor.extract_content(payload);
            requested_at = adaptor.extract_requested_at(payload);
        }
        if (content.empty() || requested_at < 0) { ++invalid; return; }
        const int64_t latency_ms = received_at - requested_at;
        if (latency_ms < 0) ++negative;
        else e2e.add(latency_ms);
        if (a.latency_log) {
            latency << csv_quote(content) << (single ? ",SINGLE_FETCH_RECEIVED," : ",GST_FETCH_RECEIVED,")
                << requested_at << ',' << received_at << ',' << latency_ms << ',';
            if (!single) latency << position;
            latency << ',' << physical_partition << ',' << physical_offset << ',' << payload_size << '\n';
        }
    };
    const auto page_end = [&](int64_t offset) {
        return a.batch_size >= a.end - offset ? a.end : offset + a.batch_size;
    };
    const auto fetch_request = [&](int64_t offset) {
        return FetchRequest{offset, page_end(offset), expected.get()};
    };
    bool prefetch_submitted = false;
    try {
        while (!stopping && next < a.end && Clock::now() < deadline) {
            if (single) {
                const auto end = page_end(next);
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
                if (remaining <= 0) break;
                MessageBatch messages(static_cast<size_t>(end - next));
                const auto call_start = Clock::now();
                messages.values[0] = rd_kafka_consume(partition.topic, 0, static_cast<int>(std::min<int64_t>(a.timeout_ms, remaining)));
                rd_kafka_resp_err_t api_error = RD_KAFKA_RESP_ERR_NO_ERROR;
                if (messages.values[0]) {
                    messages.count = 1;
                    if (messages.values.size() > 1) {
                        const auto more = rd_kafka_consume_batch(partition.topic, 0, 0, messages.values.data() + 1, messages.values.size() - 1);
                        if (more < 0) api_error = rd_kafka_last_error();
                        else messages.count += static_cast<size_t>(more);
                    }
                } else if (rd_kafka_last_error() != RD_KAFKA_RESP_ERR__TIMED_OUT) api_error = rd_kafka_last_error();
                const int64_t received_at = wall_ms();
                fetch_time_ms += std::chrono::duration<double, std::milli>(Clock::now() - call_start).count();
                ++fetches; // Application delivery batches, not background wire Fetch requests.
                const uint64_t previous_records_count = records;
                bool had_error = false;
                auto record_error = [&](rd_kafka_resp_err_t code, const std::string &message) {
                    had_error = true;
                    if (code < 0) ++local_errors; else ++broker_errors;
                    errors << received_at << (code < 0 ? ",local," : ",broker,") << code << ',' << next << ',' << csv_quote(message) << '\n';
                    if (!retryable(code) || ++consecutive_errors >= a.max_errors) throw std::runtime_error(message);
                };
                for (size_t i = 0; i < messages.count; ++i) {
                    auto *message = messages.values[i];
                    if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) { ++eof_events; continue; }
                    if (message->err) { record_error(message->err, rd_kafka_message_errstr(message)); continue; }
                    if (message->offset >= a.end) { next = a.end; break; }
                    if (message->offset > next) skipped += message->offset - next;
                    collect(message->offset, message->partition, message->offset, message->payload, message->len, received_at);
                    next = message->offset + 1;
                }
                if (api_error) record_error(api_error, rd_kafka_err2str(api_error));
                if (!had_error) consecutive_errors = 0;
                const auto delivered = records - previous_records_count;
                max_page_records = std::max(max_page_records, delivered);
                if (!delivered) ++empty;
                report(false);
                rd_kafka_poll(kafka.get(), 0);
                if (!delivered || had_error) backoff();
                continue;
            }
            const auto request = fetch_request(next);
            const auto wait_started = Clock::now();
            if (prefetch && !prefetch_submitted) prefetch->submit(request);
            auto result = prefetch ? prefetch->take() : fetcher.fetch(request);
            const auto process_started = Clock::now();
            prefetch_submitted = false;
            if (!result.page && !result.error) break;
            const int64_t received_at = result.received_at;
            const double call_ms = ms_between(result.started, result.returned);
            fetch_time_ms += call_ms;
            ++fetches;
            // Record the gap only once the next call has actually started.
            if (nonempty_fetch_returned_at) {
                post_fetch.add(ms_between(*nonempty_fetch_returned_at, result.started));
                nonempty_fetch_returned_at.reset();
            }
            if (result.error) {
                ++local_errors;
                const auto code = rd_kafka_error_code(result.error.get());
                const std::string message = rd_kafka_error_string(result.error.get());
                errors << received_at << ",local," << code << ',' << next << ',' << csv_quote(message) << '\n';
                report(false);
                if (!retryable(code) || ++consecutive_errors >= a.max_errors) throw std::runtime_error(message);
                backoff();
                continue;
            }
            const auto *info = rd_kafka_global_sequence_page_info(result.page.get());
            if (!expected && info->topic_id) expected.reset(rd_kafka_Uuid_copy(info->topic_id));
            size_t count = 0;
            const auto *values = rd_kafka_global_sequence_page_records(result.page.get(), &count);
            if (count) {
                nonempty_fetch.add(call_ms);
                fetch_wait.add(prefetch ? ms_between(wait_started, process_started) : call_ms);
                nonempty_fetch_returned_at = result.returned;
            }
            max_page_records = std::max<uint64_t>(max_page_records, count);
            const int64_t previous = next;
            const auto page_next = info->next_global_offset;
            if (page_next < previous || page_next > request.end) throw std::runtime_error("Invalid continuation cursor");
            const bool needs_backoff = page_next == previous || info->transaction_pending || info->error;
            if (prefetch && !needs_backoff && !stopping && page_next < a.end && Clock::now() < deadline) {
                prefetch->submit(fetch_request(page_next));
                prefetch_submitted = true;
            }
            for (size_t i = 0; i < count; ++i) {
                const auto &record = values[i];
                collect(record.global_offset, record.physical_partition, record.physical_offset, record.payload, record.len, received_at);
            }
            next = page_next; // Also advance empty read_committed/aborted pages.
            if (next - previous >= static_cast<int64_t>(count)) skipped += next - previous - count;
            if (info->has_snapshot) committed = info->committed_global_end_offset;
            bool terminal_error = false;
            if (info->error) {
                ++broker_errors;
                const std::string message = info->error_string ? info->error_string : rd_kafka_err2str(info->error);
                errors << received_at << ",broker," << info->error << ',' << next << ',' << csv_quote(message) << '\n';
                // The valid prefix above was delivered once. Retry only from next.
                terminal_error = !retryable(info->error) || ++consecutive_errors >= a.max_errors;
                if (terminal_error) failure = message;
            } else {
                if (!count) ++empty;
                consecutive_errors = 0;
            }
            result.page.reset();
            if (count) {
                const auto processed_at = Clock::now();
                page_process.add(ms_between(process_started, processed_at));
                if (previous_page_processed_at) consume_cycle.add(ms_between(*previous_page_processed_at, processed_at));
                previous_page_processed_at = processed_at;
            }
            if (terminal_error) break;
            report(false);
            rd_kafka_poll(kafka.get(), 0);
            if (needs_backoff) backoff();
        }
    } catch (const std::exception &error) { failure = error.what(); }
    const auto finish = Clock::now();
    if (prefetch) prefetch->stop();
    const double elapsed = std::max(0.000001, std::chrono::duration<double>(finish - start).count());
    const bool completed = a.bounded && next >= a.end;
    std::string status = "SUCCESS";
    if (stopping) { status = "INTERRUPTED"; failure = "consumer_interrupted"; }
    else if (!failure.empty() || (a.bounded && !completed) || invalid || negative || order_errors) {
        status = "FAILED";
        if (failure.empty()) failure = a.bounded && !completed ? "target range incomplete at deadline" : "invalid payload/latency/order samples";
    }
    report(true);
    throughput.close();
    if (a.latency_log) latency.close();
    errors.close();
    if (!throughput || (a.latency_log && !latency) || !errors) throw std::runtime_error("Metric output flush failed");
    const auto finished_at = wall_ms();
    prefetch.reset(); // Join before accessing the shared topic UUID or destroying the Kafka handle.
    json summary = {{"status", status}, {"reason", failure}, {"topic", a.topic},
        {"topic_mode", a.topic_mode}, {"prefetch", a.prefetch}, {"offset_scope", single ? "partition" : "global"},
        {"partition", single ? json(0) : json(nullptr)},
        {"start_offset", a.start}, {"end_offset", a.bounded ? json(a.end) : json(nullptr)}, {"next_offset", next},
        {"topic_id", expected ? json(rd_kafka_Uuid_base64str(expected.get())) : json(nullptr)},
        {"started_at", started_at}, {"finished_at", finished_at}, {"elapsed_s", elapsed},
        {"batch_size", a.batch_size}, {"max_batches", single ? json(nullptr) : json(a.max_batches)}, {"max_bytes", a.max_bytes},
        {"start_global_offset", single ? json(nullptr) : json(a.start)},
        {"end_global_offset", a.bounded && !single ? json(a.end) : json(nullptr)},
        {"next_global_offset", single ? json(nullptr) : json(next)}, {"completed_range", completed},
        {"committed_global_end_offset", single ? json(nullptr) : json(committed)},
        {"consumed_records", records}, {"payload_bytes", bytes}, {"throughput_records_per_sec", records / elapsed},
        {"throughput_payload_bytes_per_sec", bytes / elapsed}, {"fetch_calls", fetches}, {"empty_pages", empty},
        {"local_errors", local_errors}, {"broker_errors", broker_errors}, {"fetch_time_ms", fetch_time_ms},
        {"fetch_call_mean_ms", fetches ? json(fetch_time_ms / fetches) : json(nullptr)},
        {"max_page_records", max_page_records}, {"skipped_global_positions", single ? json(nullptr) : json(skipped)},
        {"skipped_partition_positions", single ? json(skipped) : json(nullptr)}, {"partition_eof_events", eof_events},
        {"invalid_payload_records", invalid}, {"negative_latency_records", negative}, {"order_errors", order_errors},
        {"latency", e2e.summary()}};
    write_json(out / "summary.json", summary);
    std::cout << summary.dump() << '\n';
    return status == "SUCCESS" ? 0 : status == "INTERRUPTED" ? 130 : 1;
}
} // namespace

int main(int argc, char **argv) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") { usage(); return 0; }
    }
    std::signal(SIGINT, interrupt);
    std::signal(SIGTERM, interrupt);
    try { return run(parse(argc, argv)); }
    catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 2; }
}
