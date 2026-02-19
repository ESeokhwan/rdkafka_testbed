#include "service_runner.h"
#include <chrono>

namespace {

std::chrono::steady_clock::time_point calc_next_tick(
    std::chrono::steady_clock::time_point cur_tick, double interval
) {
    auto duration = std::chrono::duration<double, std::milli>(interval);
    return (cur_tick + std::chrono::duration_cast<std::chrono::steady_clock::duration>(duration));
}

std::chrono::steady_clock::time_point calc_next_tick(double interval) {
    return calc_next_tick(std::chrono::steady_clock::now(), interval);
}

}

namespace common {

ServicesRunner::ServicesRunner(
    std::vector<std::shared_ptr<IService>> svcs, 
    std::shared_ptr<IService> warmup_svc,
    double interval, double stddev, double max_noise, 
    std::mt19937& rng, std::latch *start_sig
): services(svcs), warmup_service(warmup_svc), interval(interval),
   start_signal(start_sig)
{
    if (interval < 0) {
        this->noises = util::empty_noises();
    } else {
        this->noises = util::generate_noises(
            stddev,
            max_noise,
            std::min(svcs.size(), util::MAX_NOISE_LIST_LENGTH),
            rng
        );
    }
}

ServicesRunner::~ServicesRunner() {
    close();
}

void ServicesRunner::run() {
    if (is_closed.load()) return;
    warmup();

    start_signal->wait();
    if (is_closed.load()) return;

    init_first_schedules();
    run_schedules();

    close();
}

void ServicesRunner::close() {
    if (is_closed.exchange(true)) return;

    close_cv.notify_all();
    for (auto& svc : services) svc->close();
}

void ServicesRunner::warmup() {
    if (!warmup_service) return;
    while (warmup_service->has_more()) {
        warmup_service->reserve();
        warmup_service->work();
    }
    warmup_service->close_scheduled();
    warmup_service->close();
}

void ServicesRunner::init_first_schedules() {
    std::lock_guard<std::mutex> lock(queue_mutex);
    auto cur_time = std::chrono::steady_clock::now();
    for (auto& svc : services) {
        auto next = calc_next_tick(cur_time, svc->cur_interval());
        schedule_queue.push({next, svc});
        if (interval != -1) break;
    }
}

void ServicesRunner::run_schedules() {
    std::unique_lock<std::mutex> lock(queue_mutex);
    while (!schedule_queue.empty() && !is_closed.load()) {
        ScheduleEntry cur_task = schedule_queue.top();

        close_cv.wait_until(lock, cur_task.scheduled_time);
        if (is_closed.load()) break;
        if (std::chrono::steady_clock::now() < cur_task.scheduled_time) continue;
        schedule_queue.pop();
        lock.unlock();

        bool has_more = cur_task.service->reserve();
        cur_task.service->work();

        if (has_more) {
            auto next_time = calc_next_tick(cur_task.service->cur_interval());
            lock.lock();
            schedule_queue.push({next_time, cur_task.service});
            lock.unlock();
        }
        if (cur_task.service->is_done()) cur_task.service->close_scheduled();

        lock.lock();
    }
}

}