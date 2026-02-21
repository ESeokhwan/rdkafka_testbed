#pragma once

#include <string>
#include <memory>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

namespace common {
namespace consumer {

std::unique_ptr<RdKafka::KafkaConsumer> create_consumer(RdKafka::Conf* conf);

bool subscribe_topics(
    RdKafka::KafkaConsumer *consumer,
    std::vector<std::string> &topics
);

RdKafka::Message * consume_message(
    RdKafka::KafkaConsumer* consumer,
    int poll_time
);

}
}