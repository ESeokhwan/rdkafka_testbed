#pragma once

#include "service.h"
#include "util/noise_util.h"

#include <queue>
#include <chrono>
#include <latch>
#include <boost/asio.hpp>

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
    int interval;
    util::Noises noises;

    std::latch& start_signal;
    std::unique_ptr<std::latch> completion_signal;

    boost::asio::io_context io_context;
    boost::asio::steady_timer scheduler_timer;
    boost::asio::thread_pool pool;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;

    std::priority_queue<ScheduleEntry, std::vector<ScheduleEntry>, std::greater<ScheduleEntry>> schedule_queue;
    std::mutex queue_mutex;

    std::atomic<int> current_service_idx{0};
    std::atomic<bool> is_closed{false};

    void warmup();
    void init_first_schedules();
    void start_next_task();
    void process_task(ScheduleEntry entry);

public:
    ServicesRunner(std::vector<std::shared_ptr<IService>> svcs, 
                   std::shared_ptr<IService> warmup_svc,
                   int inter, double stddev, int max_noise, 
                   std::mt19937& rng, std::latch& start_sig, int pool_size);

    void run();
    void close();
};

}