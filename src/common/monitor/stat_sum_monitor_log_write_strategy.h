#pragma once

#include <libmoniq/writer/write_strategy/monitor_log_write_strategy.h>

#include <map>

namespace common {
namespace monitor {

typedef struct ServiceInfo {
    std::string name;
    double threshold;
    std::ostream &latency_ostream;
    std::ostream &per_sec_ostream;
    std::ostream &statistic_ostream;
} ServiceInfo;


typedef struct ProcessedLog {
    std::string content;
    std::string status;
    double requested_at;
    double responded_at;
    double latency;
} ProcessedLog;


typedef struct Statistics {
    int record_cnt;
    double reliability;
    double avg_latency;
    double p90_latency;
    double p99_latency;
} Statistics;


class StatSumPerSecMonitorLogWriteStrategy : public moniq::writer::IMonitorLogWriteStrategy {
private:
    std::map<std::string, ServiceInfo> services_;
    std::map<std::string, std::vector<ProcessedLog>> processed_logs_map_;

public:
    StatSumPerSecMonitorLogWriteStrategy(std::vector<ServiceInfo> services);

    ~StatSumPerSecMonitorLogWriteStrategy() = default;

    void write(std::unique_ptr<moniq::IMonitorLog> log_pq);

    bool commit();
};

}
}