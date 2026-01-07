#include "abstract_application.h"

#include <iostream>

AbstractApplication::AbstractApplication(
    moniq::MonitorQueue *monitor_queue,
    moniq::writer::MonitorLogWriter *writer
): monitor_queue(monitor_queue), writer(writer),
    start_signal(1), need_to_remove_monitor_instances(false) {
    this->writer_thread = std::make_unique<std::thread>(&moniq::writer::MonitorLogWriter::run, writer);
}

/* TODO
AbstractApplication(
    int init_monitoring_batch_size
): start_signal(1), need_to_remove_monitor_instances(true) {
    this->monitor_queue = new moniq::MonitorQueue();
    this->writer = new moniq::writer::MonitorLogWriter()
    this->writer_thread = std::make_unique<std::thread>(&moniq::writer::MonitorLogWriter::run, writer);
}
*/

void AbstractApplication::start_barrier(int delay) {
    std::cout << "En Garde...\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    std::cout << "Allez!\n";
    start_signal.count_down();
}

void AbstractApplication::cleanup_monitor() {
    writer->graceful_shutdown();
    if (writer_thread && writer_thread->joinable()) writer_thread->join();
    if (need_to_remove_monitor_instances) {
        delete monitor_queue;
        delete writer;
    }
}