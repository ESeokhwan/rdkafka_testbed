#pragma once

#include "monitor/stat_sum_monitor_message_adaptor.h"

#include <libmoniq/latency_monitor_log.h>

namespace common {
namespace monitor {

class StatSumMonitorLog: public moniq::JsonBasedLatencyMonitorLog {
private:
    common::monitor::IStatSumMonitorMessageAdaptor *message_adaptor;
    std::optional<std::string> extracted_service_;

public:
    StatSumMonitorLog(common::monitor::IStatSumMonitorMessageAdaptor *message_adaptor, const std::string& raw_data, const std::string& status, double responded_at);
    ~StatSumMonitorLog() = default;

    std::vector<std::string> get_headers() const override;
    std::vector<std::string> get_values() const override;
    void preprocess() override;

    virtual std::string get_service() const;
    double get_latency() const override;
};

}
}