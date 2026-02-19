#pragma once

#include "service.h"
#include "util/noise_util.h"

#include <queue>
#include <chrono>
#include <latch>
#include <condition_variable>

namespace common {

class ServicesRunner : public std::enable_shared_from_this<ServicesRunner> {
private:
    struct ScheduleEntry {
        std::chrono::steady_clock::time_point scheduled_time;
        std::shared_ptr<IService> service;

        bool operator>(const ScheduleEntry& other) const {
            return scheduled_time > other.scheduled_time;
        }
    };

    std::vector<std::shared_ptr<IService>> services;
    std::shared_ptr<IService> warmup_service;
    double interval;
    util::Noises noises;

    std::latch *start_signal;

    std::priority_queue<ScheduleEntry, std::vector<ScheduleEntry>, std::greater<ScheduleEntry>> schedule_queue;
    std::mutex queue_mutex;

    std::condition_variable close_cv;
    std::atomic<bool> is_closed{false};

    void warmup();
    void init_first_schedules();
    void run_schedules();

public:
    ServicesRunner(std::vector<std::shared_ptr<IService>> svcs, 
                   std::shared_ptr<IService> warmup_svc,
                   double interval, double stddev, double max_noise, 
                   std::mt19937& rng, std::latch *start_sig);
    virtual ~ServicesRunner();

    void run();
    void close();
};

}