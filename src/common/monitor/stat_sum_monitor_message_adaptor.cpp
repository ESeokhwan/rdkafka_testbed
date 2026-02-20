#include "monitor/stat_sum_monitor_message_adaptor.h"

namespace {

const std::string SERVICE_KEY = "service";

}

namespace common {
namespace monitor {

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id) {
    return root_adaptor.generate(message_id);
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, int64_t requested_at) {
    return root_adaptor.generate(message_id, requested_at);
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, std::string service_name) {
    return root_adaptor.generate(message_id, { { SERVICE_KEY, service_name } });
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, int64_t requested_at, std::string service_name) {
    return root_adaptor.generate(message_id, requested_at, { { SERVICE_KEY, service_name } });
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::extract_content_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_content(message);
}

int64_t ExtractOnlyStatSumMonitorMessageAdaptor::extract_requested_at_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_requested_at(message);
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::extract_service_name_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_other_kvs(message, SERVICE_KEY);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id) {
    return root_adaptor.generate(message_id);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, int64_t requested_at) {
    return root_adaptor.generate(message_id, requested_at);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, std::string service_name) {
    return root_adaptor.generate(message_id, { { SERVICE_KEY, service_name } });
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, int64_t requested_at, std::string service_name) {
    return root_adaptor.generate(message_id, requested_at, { { SERVICE_KEY, service_name } });
}

std::string StatSumMonitorMessageGenerator::extract_content_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_content(message);
}

int64_t StatSumMonitorMessageGenerator::extract_requested_at_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_requested_at(message);
}

std::string StatSumMonitorMessageGenerator::extract_service_name_with_root_adaptor(const std::string& message) const {
    return root_adaptor.extract_other_kvs(message, SERVICE_KEY);
}

}
}