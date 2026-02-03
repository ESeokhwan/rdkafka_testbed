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
    std::mt19937& rng, std::latch *start_sig, int pool_size
): services(svcs), warmup_service(warmup_svc), interval(interval),
   start_signal(start_sig), scheduler_timer(io_context),
   pool(pool_size),
   work_guard(boost::asio::make_work_guard(io_context))
{
    completion_signal = std::make_unique<std::latch>(services.size());

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
    if (io_thread.joinable()) {
        close();
        throw std::runtime_error("Invalid function call. Don't call run() twice.");
    }
    warmup();

    start_signal->wait();
    if (completion_signal->try_wait()) return;

    init_first_schedules();
    io_thread = std::thread([this]() { io_context.run(); });
    start_next_task();

    completion_signal->wait();
    close();
}

void ServicesRunner::close() {
    if (is_closed.exchange(true)) return;

    io_context.stop();
    pool.stop();
    pool.join();
    if (io_thread.joinable()) io_thread.join();

    for (auto& svc : services) svc->close();
    while (!completion_signal->try_wait()) {
        completion_signal->count_down();
    }
}

void ServicesRunner::warmup() {
    if (!warmup_service) return;
    while (warmup_service->has_more() && !completion_signal->try_wait()) {
        warmup_service->reserve();
        warmup_service->work();
    }
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

void ServicesRunner::start_next_task() {
    std::lock_guard<std::mutex> lock(queue_mutex);
    if (schedule_queue.empty() || is_closed) return;

    auto entry = schedule_queue.top();
    schedule_queue.pop();

    scheduler_timer.expires_at(entry.scheduled_time);
    scheduler_timer.async_wait([this, entry](const boost::system::error_code& ec) {
        if (!ec) {
            boost::asio::post(pool, [this, entry]() {
                process_task(entry);
            });
        }
    });
}

void ServicesRunner::process_task(ScheduleEntry entry) {
    if (completion_signal->try_wait()) return;

    bool has_more = entry.service->reserve();
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        if (has_more) {
            auto next_time = calc_next_tick(entry.service->cur_interval());
            schedule_queue.push({next_time, entry.service});
        } else if (interval != -1 && current_service_idx < services.size() - 1) {
            int noise = noises.next();
            auto next_time = calc_next_tick(interval + noise);
            current_service_idx += 1;
            schedule_queue.push({next_time, services[current_service_idx]});
        }
    }

    start_next_task();

    entry.service->work();
    if (entry.service->is_done()) {
        if (!entry.service->close_scheduled()) {
            completion_signal->count_down();
        }
    }
}

}