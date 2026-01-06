#include "service_runner.h"

namespace common {

ServicesRunner::ServicesRunner(
    std::vector<std::shared_ptr<IService>> svcs, 
    std::shared_ptr<IService> warmup_svc,
    int inter, double stddev, int max_noise, 
    std::mt19937& rng, std::latch& start_sig, int pool_size
): services(svcs), warmup_service(warmup_svc), interval(inter),
   start_signal(start_sig), scheduler_timer(io_context),
   pool(pool_size),
   work_guard(boost::asio::make_work_guard(io_context))
{
    completion_signal = std::make_unique<std::latch>(services.size());

    if (inter == -1) {
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

void ServicesRunner::run() {
    warmup();

    start_signal.wait();
    if (completion_signal->try_wait()) return;

    init_first_schedules();
    std::thread io_thread([this]() { io_context.run(); });
    start_next_task();

    completion_signal->wait();
    close();

    if (io_thread.joinable()) io_thread.join();
}

void ServicesRunner::close() {
    if (is_closed.exchange(true)) return;

    io_context.stop();
    pool.stop();
    pool.join();

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
    auto curTime = std::chrono::steady_clock::now();
    for (auto& svc : services) {
        auto next = curTime + std::chrono::milliseconds(svc->cur_interval());
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

    bool hasMore = entry.service->reserve();
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        if (hasMore) {
            auto nextTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(entry.service->cur_interval());
            schedule_queue.push({nextTime, entry.service});
        } else if (interval != -1 && current_service_idx < services.size() - 1) {
            int noise = noises.next();
            auto nextTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(interval + noise);
            current_service_idx += 1;
            schedule_queue.push({nextTime, services[current_service_idx]});
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