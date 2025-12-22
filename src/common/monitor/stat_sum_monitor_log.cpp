#pragma once

#include "common/monitor/stat_sum_monitor_log.h"

#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>


namespace common {
namespace monitor {

StatSumMonitorLog::StatSumMonitorLog(
    const std::string& raw_data, const std::string& status, double responded_at
): moniq::JsonBasedLatencyMonitorLog(raw_data, status, responded_at) {}

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
    std::string SERVICE_KEY = "service";

    moniq::JsonBasedLatencyMonitorLog::preprocess();
    moniq::adaptor::ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor message_adaptor;
    extracted_service_ = message_adaptor.extract_other_kvs(get_raw_data(), SERVICE_KEY);
}

std::string StatSumMonitorLog::get_service() const {
    if (!extracted_service_.has_value()) throw moniq::NotProcessedException();
    return extracted_service_.value();
}

}
}