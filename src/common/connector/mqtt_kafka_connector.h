#pragma once

#include <atomic>
#include <string>

#include <mosquitto.h>
#include <librdkafka/rdkafkacpp.h>


class MqttKafkaConnector {
private:
    RdKafka::Producer *kafka_producer;
    mosquitto *mqtt_client;

    std::string id;
    std::string source_topic;
    std::string sink_topic;
    RdKafka::Topic *sink_topic_obj; // to reduce warmup effect on produce

    std::atomic<int> total_send_cnt{0};
    std::atomic<int> fail_send_cnt{0};
    std::atomic<int> success_send_cnt{0};

    static void on_connect(struct mosquitto *mosq, void *obj, int rc);
    static void on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg);

public:
    MqttKafkaConnector(
        std::string kafka_broker,
        std::string mqtt_broker,
        std::string id,
        std::string source_topic,
        std::string sink_topic
    );

    ~MqttKafkaConnector();

    std::string get_id();
    std::string get_source_topic();
    std::string get_sink_topic();
    int get_total_send_cnt();
    int get_fail_send_cnt();
    int get_success_send_cnt();
};