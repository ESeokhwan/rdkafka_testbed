#include "monitor/stat_sum_monitor_log.h"
#include "monitor/stat_sum_monitor_message_adaptor.h"

#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>

namespace common {
namespace monitor {

StatSumMonitorLog::StatSumMonitorLog(
    common::monitor::IStatSumMonitorMessageAdaptor *message_adaptor,
    const std::string& raw_data, const std::string& status, double responded_at
): moniq::JsonBasedLatencyMonitorLog(message_adaptor, raw_data, status, responded_at), message_adaptor(message_adaptor) {}

std::vector<std::string> StatSumMonitorLog::get_headers() const {
        return { "Service", "Content", "Status", "RequestedAt", "RespondedAt", "Latency" };
    }

std::vector<std::string> StatSumMonitorLog::get_values() const {
    return { 
        get_service(), get_content(), get_status(), std::to_string(get_requested_at()),
        std::to_string(get_responded_at()), std::to_string(get_latency())
    };
}

void StatSumMonitorLog::preprocess() {
    moniq::JsonBasedLatencyMonitorLog::preprocess();
    extracted_service_ = message_adaptor->extract_service_name(get_raw_data());
}

std::string StatSumMonitorLog::get_service() const {
    if (!extracted_service_.has_value()) throw moniq::NotProcessedException();
    return extracted_service_.value();
}

double StatSumMonitorLog::get_latency() const {
    return moniq::JsonBasedLatencyMonitorLog::get_latency() + 3;
}

}
}