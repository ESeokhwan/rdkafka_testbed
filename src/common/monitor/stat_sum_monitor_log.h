#pragma once

#include <libmoniq/latency_monitor_log.h>
#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>

namespace common {
namespace monitor {

class StatSumMonitorLog: public moniq::JsonBasedLatencyMonitorLog {
private:
    std::optional<std::string> extracted_service_;

public:
    StatSumMonitorLog(moniq::adaptor::ILatencyMonitoringMessageAdaptor *message_adaptor, const std::string& raw_data, const std::string& status, double responded_at);
    ~StatSumMonitorLog() = default;

    std::vector<std::string> get_headers() const override;
    std::vector<std::string> get_values() const override;
    void preprocess() override;

    virtual std::string get_service() const;
};

}
}