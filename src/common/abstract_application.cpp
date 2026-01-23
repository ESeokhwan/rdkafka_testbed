#include "abstract_application.h"

#include <libmoniq/writer/write_strategy/console_monitor_log_write_strategy.h>

#include <iostream>
#include <memory>

AbstractApplication::AbstractApplication(
    std::shared_ptr<moniq::MonitorQueue> &monitor_queue,
    std::shared_ptr<moniq::writer::MonitorLogWriter> &writer
): monitor_queue(monitor_queue), writer(writer), start_signal(1) {
    this->writer_thread = std::make_unique<std::thread>(&moniq::writer::MonitorLogWriter::run, writer);
}

AbstractApplication::AbstractApplication(
    bool scrapable, int monitoring_batch_size, int monitoring_timeout
): start_signal(1) {
    this->monitor_queue = std::make_shared<moniq::MonitorQueue>();
    this->writer = std::make_shared<moniq::writer::MonitorLogWriter>(
        monitor_queue,
        std::make_shared<moniq::writer::ConsoleMonitorLogWriteStrategy>(scrapable),
        monitoring_batch_size,
        monitoring_timeout
    );
    this->writer_thread = std::make_unique<std::thread>(&moniq::writer::MonitorLogWriter::run, writer);
}


void AbstractApplication::start_barrier(int delay) {
    std::cout << "Start threads in " << delay << "seconds...\n";
    std::this_thread::sleep_for(std::chrono::seconds(delay));
    std::cout << "Allez!\n";
    start_signal.count_down();
}

void AbstractApplication::cleanup_monitor() {
    writer->graceful_shutdown();
    if (writer_thread && writer_thread->joinable()) writer_thread->join();
}