#include "throughput_service_runner.h"

#include <deque>
#include <stdexcept>
#include <utility>

namespace common {

ThroughputServicesRunner::ThroughputServicesRunner(
    std::vector<std::shared_ptr<IService>> svcs,
    std::shared_ptr<IService> warmup_svc,
    std::size_t batch_size,
    std::latch* start_sig
): services(std::move(svcs)),
   warmup_service(std::move(warmup_svc)),
   batch_size(batch_size),
   start_signal(start_sig)
{
    if (batch_size == 0) {
        throw std::invalid_argument("ThroughputServicesRunner batch_size must be greater than zero.");
    }
    if (start_signal == nullptr) {
        throw std::invalid_argument("ThroughputServicesRunner start_signal must not be null.");
    }
    for (const auto& service : services) {
        if (service == nullptr) {
            throw std::invalid_argument("ThroughputServicesRunner services must not contain null.");
        }
    }
}

ThroughputServicesRunner::~ThroughputServicesRunner() {
    close();
    if (!has_run.load()) {
        close_warmup();
        close_services();
    }
}

void ThroughputServicesRunner::run() {
    if (has_run.exchange(true)) {
        throw std::logic_error("ThroughputServicesRunner::run() must only be called once.");
    }
    try {
        if (!is_closed.load()) warmup();

        if (!is_closed.load()) start_signal->wait();
        if (!is_closed.load()) run_round_robin();
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

void ThroughputServicesRunner::close() {
    is_closed.store(true);
}

void ThroughputServicesRunner::warmup() {
    if (!warmup_service) return;

    while (!is_closed.load()) {
        if (is_closed.load() || !warmup_service->has_more()) break;

        warmup_service->reserve();
        warmup_service->work();
    }

    close_warmup();
}

void ThroughputServicesRunner::run_round_robin() {
    std::deque<std::shared_ptr<IService>> active_services;
    if (is_closed.load()) return;

    for (const auto& service : services) {
        if (service->has_more()) active_services.push_back(service);
        else service->close_scheduled();
    }

    while (!active_services.empty() && !is_closed.load()) {
        auto service = std::move(active_services.front());
        active_services.pop_front();
        bool has_more = true;

        for (std::size_t batch_idx = 0;
             batch_idx < batch_size && has_more && !is_closed.load();
             ++batch_idx) {
            if (!service->has_more()) {
                has_more = false;
                service->close_scheduled();
                break;
            }

            has_more = service->reserve();
            service->work();
            if (!has_more && service->is_done()) service->close_scheduled();
        }

        if (has_more && !is_closed.load()) {
            active_services.push_back(std::move(service));
        }
    }
}

void ThroughputServicesRunner::close_warmup() {
    std::call_once(warmup_close_flag, [this]() {
        if (!warmup_service) return;
        warmup_service->close_scheduled();
        warmup_service->close();
    });
}

void ThroughputServicesRunner::close_services() {
    std::call_once(services_close_flag, [this]() {
        for (const auto& service : services) service->close();
    });
}

}
