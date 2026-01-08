#pragma once

#include <iomanip>
#include <getopt.h>
#include <vector>
#include <sstream>
#include <string>

namespace {

constexpr int OPTION_PANE_WIDTH = 35;
constexpr int LONG_OPTIONS_VAL_OFFSET = 257;

enum class LongOnlyOptionVals {
    CLIENT_CNT = LONG_OPTIONS_VAL_OFFSET,
    SERVICE_CNT,
    NOISE_STDDEV,
    FOR_CREATION,
    NEED_FLUSH,
    IS_SYNC,
    SCRAPABLE,
    TAG_RECORD,
    READ_TAGGED_ONLY,
    SAMPLE_LOG,
    TOPIC_CNT_PER_CLIENT,
    INTERVAL_NOISE_STDDEV,
    INTERVAL_NOISE_STDDEV_RATE,
    INTERVAL_BTW_TOPIC,
    INTERVAL_BTW_TOPIC_NOISE_STDDEV,
    IGNORE_RESPONSE,
    WARMUP_CNT,
    WARMUP_TOPIC,
    START_BARRIER_DELAY,
    MONITORING_BATCH_SIZE,
    SERVICE_RUNNER_POOL_SIZE,
    SHARE_PRODUCER,
};

}

namespace common {
namespace util {

constexpr int CUSTOM_LONG_OPTIONS_VALUE_OFFSET = 1000;

class OptionWrapper {
private:
    const char* name;
    int has_arg;
    int* flag;
    int val;
    const char* description;

public:
    constexpr OptionWrapper(const char* name, int has_arg, int* flag, int val, const char* desc)
        : name(name), has_arg(has_arg), flag(flag), val(val), description(desc) {}

    constexpr struct option get_option() const {
        return {name, has_arg, flag, val};
    }

    constexpr bool has_short_option() const {
        return (val >= 0 && val < 256);
    }

    constexpr const char* get_name() const {
        return name;
    }

    constexpr int get_has_arg() const {
        return has_arg;
    }

    constexpr int get_val() const {
        return val;
    }

    constexpr int* get_flag() const {
        return flag;
    }

    constexpr const char* get_description() const {
        return description;
    }
};

constexpr OptionWrapper HELP_OPTION = {"help", no_argument, nullptr, 'h', "Show this help message"};

constexpr OptionWrapper VERBOSE_OPTION = {"verbose", no_argument, nullptr, 'v', "Enable verbose logging"};

constexpr OptionWrapper BROKER_OPTION = {"broker", required_argument, nullptr, 'b', "Broker address"};

constexpr OptionWrapper PREFIX_OPTION = {"prefix", required_argument, nullptr, 'p', "Prefix for topic, client, and etc."};

constexpr OptionWrapper ROUND_CNT_OPTION = {"round_cnt", required_argument, nullptr, 'n', "Number of rounds of each producer and service"};
constexpr OptionWrapper MSG_CNT_PER_TOPIC_OPTION = {"msg_cnt_per_topic", required_argument, nullptr, 'n', "Number of messages per topic"};

constexpr OptionWrapper INTERVAL_OPTION = {"interval", required_argument, nullptr, 'i', "Produce interval (ms)"};

constexpr OptionWrapper MSG_SIZE_OPTION = {"msg_size", required_argument, nullptr, 'm', "Message size in bytes"};

constexpr OptionWrapper RUNNING_TIME_OPTION = {"running_time", required_argument, nullptr, 'r', "Running time (ms)"};

constexpr OptionWrapper OUTDIR_OPTION = {"outdir", required_argument, nullptr, 'o', "Output directory"};


constexpr OptionWrapper CLIENT_CNT_OPTION = {"client_cnt", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::CLIENT_CNT), "Number of clients"};
constexpr OptionWrapper SERVICE_CNT_OPTION = {"service_cnt", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::SERVICE_CNT), "Number of services"};
constexpr OptionWrapper TOPIC_CNT_PER_CLIENT_OPTION = {"topic_cnt_per_client", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::TOPIC_CNT_PER_CLIENT), "Number of topics per client"};

constexpr OptionWrapper NOISE_STDDEV_OPTION = {"noise_stddev", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::NOISE_STDDEV), "Noise standard deviation"};
constexpr OptionWrapper INTERVAL_NOISE_STDDEV_OPTION = {"interval_noise_stddev", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::INTERVAL_NOISE_STDDEV), "Interval noise standard deviation"};
constexpr OptionWrapper INTERVAL_NOISE_STDDEV_RATE_OPTION = {"interval_noise_stddev_rate", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::INTERVAL_NOISE_STDDEV_RATE), "Interval noise standard deviation rate (0~100)"};
constexpr OptionWrapper INTERVAL_BTW_TOPIC_OPTION = {"interval_btw_topic", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::INTERVAL_BTW_TOPIC), "Interval between topics"};
constexpr OptionWrapper INTERVAL_BTW_TOPIC_NOISE_STDDEV_OPTION = {"interval_btw_topic_noise_stddev", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::INTERVAL_BTW_TOPIC_NOISE_STDDEV), "Interval between topics noise standard deviation"};

constexpr OptionWrapper FOR_CREATION_OPTION = {"for_creation", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::FOR_CREATION), "Enable creation mode"};
constexpr OptionWrapper NEED_FLUSH_OPTION = {"need_flush", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::NEED_FLUSH), "Enable flush after producing messages"};
constexpr OptionWrapper IGNORE_RESPONSE_OPTION = {"ignore_response", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::IGNORE_RESPONSE), "Ignore response"};
constexpr OptionWrapper IS_SYNC_OPTION = {"is_sync", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::IS_SYNC), "Enable synchronous mode"};
constexpr OptionWrapper SCRAPABLE_OPTION = {"scrapable", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::SCRAPABLE), "Enable scrapable mode"};
constexpr OptionWrapper TAG_RECORD_OPTION = {"tag_record", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::TAG_RECORD), "Enable record tagging"};
constexpr OptionWrapper READ_TAGGED_ONLY_OPTION = {"read_tagged_only", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::READ_TAGGED_ONLY), "Read tagged records only"};
constexpr OptionWrapper SAMPLE_LOG_OPTION = {"sample_log", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::SAMPLE_LOG), "Enable log sampling"};

constexpr OptionWrapper WARMUP_CNT_OPTION = {"warmup_cnt", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::WARMUP_CNT), "Number of messages for warmup"};
constexpr OptionWrapper WARMUP_TOPIC_OPTION = {"warmup_topic", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::WARMUP_TOPIC), "Warmup topic"};

constexpr OptionWrapper START_BARRIER_DELAY_OPTION = {"start_barrier_delay", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::START_BARRIER_DELAY), "Start barrier delay (ms)"};
constexpr OptionWrapper MONITORING_BATCH_SIZE_OPTION = {"monitoring_batch_size", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::MONITORING_BATCH_SIZE), "Monitoring batch size"};

constexpr OptionWrapper SERVICE_RUNNER_POOL_SIZE_OPTION = {"service_runner_pool_size", required_argument, nullptr, static_cast<int>(LongOnlyOptionVals::SERVICE_RUNNER_POOL_SIZE), "Service runner pool size"};

constexpr OptionWrapper SHARE_PRODUCER_OPTION = {"share_producer", no_argument, nullptr, static_cast<int>(LongOnlyOptionVals::SHARE_PRODUCER), "Enable sharing producer"};



inline std::vector<struct option> make_long_opts(const std::vector<OptionWrapper>& opts) {
    std::vector<struct option> long_opts;
    for (const auto& opt : opts) {
        long_opts.push_back(opt.get_option());
    }
    long_opts.push_back({nullptr, 0, nullptr, 0});
    return long_opts;
}

inline std::string make_short_opts(const std::vector<OptionWrapper>& opts) {
    if (opts.size() == 0) return "";

    std::stringstream ss;
    for (const auto& opt : opts) {
        if (!opt.has_short_option()) continue;
        ss << opt.get_val();
        if (opt.get_has_arg() == required_argument) ss << ':';
        else if (opt.get_has_arg() == optional_argument) ss << "::";
    }
    return ss.str();
}

inline std::string make_help_message(const std::vector<OptionWrapper>& opts) {
    std::stringstream ss;
    ss << "Usage: [options]";
    for (const auto& opt : opts) {
        std::stringstream line_ss;
        line_ss << " --" << opt.get_name();
        if (opt.has_short_option()) line_ss << ", -" << static_cast<char>(opt.get_val());
        if (opt.get_has_arg() == required_argument) line_ss << " <arg>";
        else if (opt.get_has_arg() == optional_argument) line_ss << " [<arg>]";
        ss << "\n" << std::left << std::setw(OPTION_PANE_WIDTH) << line_ss.str() << opt.get_description();
    }
    return ss.str();
}

}
}