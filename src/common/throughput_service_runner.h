#pragma once

#include "service.h"

#include <atomic>
#include <cstddef>
#include <latch>
#include <memory>
#include <mutex>
#include <vector>

namespace common {

/**
 * Runs non-throttled services in round-robin order.
 *
 * Each service may perform at most batch_size work items during one turn. A
 * service that still has work remaining is then moved to the back of the
 * active-service queue. Unlike ServicesRunner, this runner does not calculate
 * timestamps or insert individual work items into a priority queue.
 *
 * close() only requests that the runner stop. The thread executing run()
 * closes the services after the current work() call returns. The caller must
 * join the runner thread before destroying this object or its services.
 */
class ThroughputServicesRunner {
private:
    std::vector<std::shared_ptr<IService>> services;
    std::shared_ptr<IService> warmup_service;
    std::size_t batch_size;
    std::latch* start_signal;

    std::atomic<bool> is_closed{false};
    std::atomic<bool> has_run{false};

    std::once_flag warmup_close_flag;
    std::once_flag services_close_flag;

    void warmup();
    void run_round_robin();
    void close_warmup();
    void close_services();

public:
    ThroughputServicesRunner(
        std::vector<std::shared_ptr<IService>> svcs,
        std::shared_ptr<IService> warmup_svc,
        std::size_t batch_size,
        std::latch* start_sig
    );
    ~ThroughputServicesRunner();

    ThroughputServicesRunner(const ThroughputServicesRunner&) = delete;
    ThroughputServicesRunner& operator=(const ThroughputServicesRunner&) = delete;
    ThroughputServicesRunner(ThroughputServicesRunner&&) = delete;
    ThroughputServicesRunner& operator=(ThroughputServicesRunner&&) = delete;

    void run();
    void close();
};

}
