#pragma once

#include "common/monitor/stat_sum_monitor_log_write_strategy.h"
#include "common/monitor/stat_sum_monitor_log.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

using namespace moniq;

namespace common {
namespace monitor {

Statistics calc_statistics(const std::vector<ProcessedLog> &logs, double threshold);
double calc_reliability(const std::vector<ProcessedLog> &logs, double threshold);
double calc_avg_latency(const std::vector<ProcessedLog> &logs);
double calc_latency_percentile(const std::vector<double> &sorted_latencies, double percentile);
std::vector<double> make_sorted_latencies(const std::vector<ProcessedLog> &logs);
std::map<int64_t, std::vector<ProcessedLog>> devide_logs_by_epoch_sec(const std::vector<ProcessedLog> &logs);


StatSumPerSecMonitorLogWriteStrategy::StatSumPerSecMonitorLogWriteStrategy(std::vector<ServiceInfo> services) {
    for (auto service: services) {
        services_.insert({service.name, service});
        processed_logs_map_.insert({service.name, {}});
    }
}

void StatSumPerSecMonitorLogWriteStrategy::write(std::unique_ptr<moniq::IMonitorLog> log_pq) {
    StatSumMonitorLog *converted = dynamic_cast<StatSumMonitorLog *>(log_pq.get());
    if (converted == nullptr) return;

    if (processed_logs_map_.find(converted->get_service()) == processed_logs_map_.end()) return;
    processed_logs_map_[converted->get_service()].push_back({
        converted->get_content(),
        converted->get_status(),
        converted->get_requested_at(),
        converted->get_responded_at(),
        converted->get_latency()
    });
}

bool StatSumPerSecMonitorLogWriteStrategy::commit() {
    for (const auto& [service_name, processed_logs]: processed_logs_map_) {
        ServiceInfo service = services_[service_name];
        Statistics stat = calc_statistics(processed_logs, service.threshold);

        service.latency_ostream << "Content,Status,RequestedAt,respondedAt,Latency\n";
        for (const auto& log: processed_logs) {
            print_latency_log(service.latency_ostream, log);
        }

        service.statistic_ostream << "[" << service_name << "] "
            << "Total=" << processed_logs.size()
            << " AvgGap=" << std::fixed << std::setprecision(2) << stat.avg_latency << "ms"
            << " P90=" << stat.p90_latency << "ms"
            << " P99=" << stat.p99_latency << "ms"
            << " Reliability=" << std::fixed << std::setprecision(2) << stat.reliability << "%"
            << std::endl;

        service.per_sec_ostream << "SecondEpoch,Total,Rel(%),AvgLatency,P90Latency,P99Latency\n";
        std::map<int64_t, std::vector<ProcessedLog>> logs_map_by_sec = devide_logs_by_epoch_sec(processed_logs);
        for (const auto& [sec_epoch, logs]: logs_map_by_sec) {
            Statistics stat = calc_statistics(logs, service.threshold);
            service.per_sec_ostream << sec_epoch << ",";
            print_stat_log(service.per_sec_ostream, stat);
        }
    }
}

std::ostream& print_latency_log(std::ostream &ostream, const ProcessedLog &log) {
    return ostream << log.content << "," << log.status << "," 
        << log.requested_at << "," << log.responded_at << "," << log.latency << "\n";
}

std::ostream& print_stat_log(std::ostream &ostream, const Statistics &log) {
    return ostream << log.reliability << "," << log.avg_latency 
        << "," << log.p90_latency << "," << log.p99_latency << "\n";
}

// Statistics calculating functions.
Statistics calc_statistics(const std::vector<ProcessedLog> &logs, double threshold) {
    double reliability = calc_reliability(logs, threshold);
    double avg_latency = calc_avg_latency(logs);

    std::vector<double> sorted_latencies = make_sorted_latencies(logs);
    double p90 = calc_latency_percentile(sorted_latencies, 0.90);
    double p99 = calc_latency_percentile(sorted_latencies, 0.99);
    return {reliability, avg_latency, p90, p99};
}

double calc_reliability(const std::vector<ProcessedLog> &logs, double threshold) {
    int record_cnt = logs.size();
    int success_record_cnt = 0;

    for (auto log: logs) {
        if (log.latency <= threshold) success_record_cnt += 1;
    }
    return (record_cnt > 0) ? (double(success_record_cnt) / double(record_cnt)): 0.0;
}

double calc_avg_latency(const std::vector<ProcessedLog> &logs) {
    double avg_latency = 0.0;
    for (auto log: logs) {
        avg_latency += log.latency;
    }
    return (logs.size() > 0) ? (avg_latency / double(logs.size())): 0.0;
}

double calc_latency_percentile(const std::vector<double> &sorted_latencies, double percentile) {
    size_t idx = size_t(sorted_latencies.size() * percentile);
    if (idx < sorted_latencies.size()) return sorted_latencies[idx];
    return 0.0;
}

std::vector<double> make_sorted_latencies(const std::vector<ProcessedLog> &logs) {
    std::vector<double> latencies;
    for (auto log: logs) {
        latencies.push_back(log.latency);
    }
    std::sort(latencies.begin(), latencies.end());
    return latencies;
}

std::map<int64_t, std::vector<ProcessedLog>> devide_logs_by_epoch_sec(const std::vector<ProcessedLog> &logs) {
    std::map<int64_t, std::vector<ProcessedLog>> result;
    for (auto log: logs) {
        int64_t sec_epoch = int64_t(log.responded_at / 1000);
        if (result.find(sec_epoch) == result.end()) {
            result.insert({sec_epoch, std::vector<ProcessedLog>()});
        }
        result[sec_epoch].push_back(log);
    }
    return result;
}

}
}