#include "service_runner.h"
#include <chrono>
#include <stdexcept>

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
    if (!has_run.load()) {
        close_warmup();
        close_services();
    }
}

void ServicesRunner::run() {
    if (has_run.exchange(true)) {
        throw std::logic_error("ServicesRunner::run() must only be called once.");
    }

    try {
        if (!is_closed.load()) warmup();

        if (!is_closed.load()) start_signal->wait();
        if (!is_closed.load()) {
            init_first_schedules();
            run_schedules();
        }
    } catch (...) {
        close();
        close_warmup();
        close_services();
        throw;
    }

    close();
    close_warmup();
    close_services();
}

void ServicesRunner::close() {
    if (is_closed.exchange(true)) return;

    close_cv.notify_all();
}

void ServicesRunner::warmup() {
    if (!warmup_service) return;

    while (!is_closed.load() && warmup_service->has_more()) {
        warmup_service->reserve();
        warmup_service->work();
    }

    close_warmup();
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

        if (has_more && !is_closed.load()) {
            auto next_time = calc_next_tick(cur_task.service->cur_interval());
            lock.lock();
            schedule_queue.push({next_time, cur_task.service});
            lock.unlock();
        }
        if (cur_task.service->is_done()) cur_task.service->close_scheduled();

        lock.lock();
    }
}

void ServicesRunner::close_warmup() {
    std::call_once(warmup_close_flag, [this]() {
        if (!warmup_service) return;
        warmup_service->close_scheduled();
        warmup_service->close();
    });
}

void ServicesRunner::close_services() {
    std::call_once(services_close_flag, [this]() {
        for (const auto& service : services) service->close();
    });
}

}
