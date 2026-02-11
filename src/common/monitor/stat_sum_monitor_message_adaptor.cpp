#include "monitor/stat_sum_monitor_message_adaptor.h"

namespace {

const std::string SERVICE_KEY = "service";

}

namespace common {
namespace monitor {

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id) {
    throw moniq::ImproperUsageException();
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, int64_t requested_at) {
    throw moniq::ImproperUsageException();
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, std::string service_name) {
    throw moniq::ImproperUsageException();
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::generate_with_root_adaptor(std::string message_id, int64_t requested_at, std::string service_name) {
    throw moniq::ImproperUsageException();
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::extract_content_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_content(message);
    return root_adaptor.extract_content(message.substr(0, splitPos));
}

int64_t ExtractOnlyStatSumMonitorMessageAdaptor::extract_requested_at_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_requested_at(message);
    return root_adaptor.extract_requested_at(message.substr(0, splitPos));
}

std::string ExtractOnlyStatSumMonitorMessageAdaptor::extract_service_name_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_other_kvs(message, SERVICE_KEY);
    return root_adaptor.extract_other_kvs(message.substr(0, splitPos), SERVICE_KEY);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id) {
    return root_adaptor.generate(message_id);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, int64_t requested_at) {
    return root_adaptor.generate(message_id, requested_at);
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, std::string service_name) {
    return root_adaptor.generate(message_id, { { service_name, "" }, { SERVICE_KEY, service_name } });
}

std::string StatSumMonitorMessageGenerator::generate_with_root_adaptor(std::string message_id, int64_t requested_at, std::string service_name) {
    return root_adaptor.generate(message_id, requested_at, { { service_name, "" }, { SERVICE_KEY, service_name } });
}

std::string StatSumMonitorMessageGenerator::extract_content_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_content(message);
    return root_adaptor.extract_content(message.substr(0, splitPos));
}

int64_t StatSumMonitorMessageGenerator::extract_requested_at_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_requested_at(message);
    return root_adaptor.extract_requested_at(message.substr(0, splitPos));
}

std::string StatSumMonitorMessageGenerator::extract_service_name_with_root_adaptor(const std::string& message) const {
    size_t splitPos = message.find(0x02);
    if (splitPos == std::string::npos) return root_adaptor.extract_other_kvs(message, SERVICE_KEY);
    return root_adaptor.extract_other_kvs(message.substr(0, splitPos), SERVICE_KEY);
}

}
}