#pragma once

#include <csignal>
#include <latch>
#include <memory>
#include <thread>

#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>

class AbstractApplication {
protected:
    std::shared_ptr<moniq::MonitorQueue> monitor_queue;
    std::shared_ptr<moniq::writer::MonitorLogWriter> writer;
    std::unique_ptr<std::thread> writer_thread;
    std::latch start_signal;

public:
    AbstractApplication(
        std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
        std::shared_ptr<moniq::writer::MonitorLogWriter> &writer);
    AbstractApplication(bool scrapable, int monitoring_batch_size, int monitoring_timeout);
    virtual ~AbstractApplication() =default;

    virtual void run() = 0;
    void cleanup() {
        cleanup_main();
        cleanup_monitor();
    }
protected:
    void start_barrier(int delay);

    virtual void cleanup_main() = 0;
    void cleanup_monitor();
};