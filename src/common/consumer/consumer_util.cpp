#include "common/consumer/consumer_util.h"

#include <iostream>

namespace common {
namespace consumer {

std::unique_ptr<RdKafka::KafkaConsumer> create_consumer(RdKafka::Conf* conf) {
    std::string errstr;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer(
        RdKafka::KafkaConsumer::create(conf, errstr)
    );
    if (!consumer) {
        std::cerr << "Failed to create Consumer instance" << std::endl;
        std::cerr << errstr << std::endl;
        return nullptr;
    }
    return consumer;
}

bool subscribe_topics(RdKafka::KafkaConsumer *consumer, std::vector<std::string> &topics) {
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err) {
        std::cerr << "Failed to subscribe to " << topics.size() << " topics: " << RdKafka::err2str(err) << std::endl;
        return false;
    }
    return true;
}

std::optional<std::string> consume_message(
    RdKafka::KafkaConsumer* consumer,
    int poll_time
) {
    RdKafka::Message *msg = consumer->consume(poll_time);
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
        if (msg->err() != RdKafka::ERR__TIMED_OUT) {
            std::cerr << "Consume Error: " << msg->errstr() << std::endl;
        }
        delete msg;
        return std::nullopt;
    }

    std::string output = std::string(static_cast<const char*>(msg->payload()));
    delete msg;
    return output;
}

}
}