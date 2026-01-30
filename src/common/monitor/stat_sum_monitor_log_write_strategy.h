#pragma once

#include <libmoniq/writer/write_strategy/monitor_log_write_strategy.h>

#include <map>
#include <iostream>

namespace common {
namespace monitor {

typedef struct ServiceInfo {
    std::string name;
    double threshold;
    std::ostream *latency_ostream;
    std::ostream *per_sec_ostream;
    std::ostream *statistic_ostream;

    ServiceInfo()
        : name(""), threshold(0.0), latency_ostream(&std::cout), per_sec_ostream(&std::cout), statistic_ostream(&std::cout) {}

    ServiceInfo(std::string n, double t, std::ostream *os1, std::ostream *os2, std::ostream *os3)
        : name(n), threshold(t), latency_ostream(os1), per_sec_ostream(os2), statistic_ostream(os3) {}
} ServiceInfo;


typedef struct ProcessedLog {
    std::string content;
    std::string status;
    double requested_at;
    double responded_at;
    double latency;
} ProcessedLog;


typedef struct Statistics {
    std::size_t record_cnt;
    double epoch_size;
    double reliability;
    double avg_latency;
    double p90_latency;
    double p99_latency;
} Statistics;


class StatSumPerSecMonitorLogWriteStrategy : public moniq::writer::IMonitorLogWriteStrategy {
private:
    std::map<std::string, ServiceInfo> services_;
    std::map<std::string, std::vector<ProcessedLog>> processed_logs_map_;
    double epoch_size_ms_;

public:
    StatSumPerSecMonitorLogWriteStrategy(std::vector<ServiceInfo> services, double epoch_size_ms);

    ~StatSumPerSecMonitorLogWriteStrategy() = default;

    void write(std::unique_ptr<moniq::IMonitorLog> log_pq);

    bool commit();
};

}
}