#pragma once

#include <csignal>
#include <latch>
#include <memory>
#include <thread>

#include <libmoniq/monitor_queue.h>
#include <libmoniq/writer/monitor_log_writer.h>

class AbstractApplication {
protected:
    moniq::MonitorQueue *monitor_queue;
    moniq::writer::MonitorLogWriter *writer;
    std::unique_ptr<std::thread> writer_thread;
    std::latch start_signal;

private:
    const bool need_to_remove_monitor_instances;

public:
    AbstractApplication(moniq::MonitorQueue *mq, moniq::writer::MonitorLogWriter *mw);
    // TODO
    // AbstractApplication(int init_monitoring_batch_size);
    virtual ~AbstractApplication();

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