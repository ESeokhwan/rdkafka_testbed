#pragma once

#include <libmoniq/adaptor/latency_monitoring_message_adaptor.h>

namespace common {
namespace monitor {

class IStatSumMonitorMessageAdaptor: public moniq::adaptor::ILatencyMonitoringMessageAdaptor {
public:
    using ILatencyMonitoringMessageAdaptor::generate;

    virtual std::string generate(std::string message_id, std::string service_name) = 0;

    virtual std::string generate(std::string message_id, double requested_at, std::string service_name) = 0;

    virtual std::string extract_service_name(const std::string& message) const = 0;
};

class StatSumMonitorMessageAdaptor: public IStatSumMonitorMessageAdaptor {
protected:
    virtual std::string generate_with_root_adaptor(std::string message_id) = 0;
    virtual std::string generate_with_root_adaptor(std::string message_id, std::string service_name) = 0;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at) = 0;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at, std::string service_name) = 0;
    virtual std::string extract_content_with_root_adaptor(const std::string& message) const = 0;
    virtual double extract_requested_at_with_root_adaptor(const std::string& message) const = 0;
    virtual std::string extract_service_name_with_root_adaptor(const std::string& message) const = 0;

public:
    StatSumMonitorMessageAdaptor() = default;

    virtual ~StatSumMonitorMessageAdaptor() = default;

    std::string generate(std::string message_id) override {
        return generate_with_root_adaptor(message_id);
    }

    std::string generate(std::string messageId, double requested_at) override {
        return generate_with_root_adaptor(messageId, requested_at);
    }

    virtual std::string generate(std::string message_id, std::string service_name) override {
        return generate_with_root_adaptor(message_id, service_name);
    }

    virtual std::string generate(std::string message_id, double requested_at, std::string service_name) override {
        return generate_with_root_adaptor(message_id, requested_at, service_name);
    }

    std::string extract_content(const std::string& message) const override {
        return extract_content_with_root_adaptor(message);
    }

    double extract_requested_at(const std::string& message) const override {
        return extract_requested_at_with_root_adaptor(message);
    }

    virtual std::string extract_service_name(const std::string& message) const override {
        return extract_service_name_with_root_adaptor(message);
    }
};

class ExtractOnlyStatSumMonitorMessageAdaptor: public StatSumMonitorMessageAdaptor {
private:
    moniq::adaptor::ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor root_adaptor;

protected:
    virtual std::string generate_with_root_adaptor(std::string message_id) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, std::string service_name) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at, std::string service_name) override;
    virtual std::string extract_content_with_root_adaptor(const std::string& message) const override;
    virtual double extract_requested_at_with_root_adaptor(const std::string& message) const override;
    virtual std::string extract_service_name_with_root_adaptor(const std::string& message) const override;

public:
    ExtractOnlyStatSumMonitorMessageAdaptor() = default;

    virtual ~ExtractOnlyStatSumMonitorMessageAdaptor() = default;
};

class StatSumMonitorMessageGenerator: public StatSumMonitorMessageAdaptor {
private:
    moniq::adaptor::JsonBasedLatencyMonitoringMessageGenerator root_adaptor;

protected:
    virtual std::string generate_with_root_adaptor(std::string message_id) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, std::string service_name) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at) override;
    virtual std::string generate_with_root_adaptor(std::string message_id, double requested_at, std::string service_name) override;
    virtual std::string extract_content_with_root_adaptor(const std::string& message) const override;
    virtual double extract_requested_at_with_root_adaptor(const std::string& message) const override;
    virtual std::string extract_service_name_with_root_adaptor(const std::string& message) const override;

public:
    StatSumMonitorMessageGenerator(int payload_size, int pre_indices_size):
        root_adaptor(payload_size, pre_indices_size) {}

    virtual ~StatSumMonitorMessageGenerator() = default;
};

}
}