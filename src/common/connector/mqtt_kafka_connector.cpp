#include "connector/mqtt_kafka_connector.h"

#include <stdexcept>
#include <string>

#include <mosquitto.h>
#include <librdkafka/rdkafkacpp.h>

namespace {

RdKafka::Producer *create_kafka_producer(std::string brokers, std::string client_id);
RdKafka::Topic *create_kafka_topic(RdKafka::Producer *producer, std::string topic);
mosquitto *create_mosq_client(
    std::string broker,
    std::string client_id,
    void *obj,
    void (*on_connect)(struct mosquitto *, void *, int),
    void (*on_message)(struct mosquitto *, void *, const struct mosquitto_message *)
);
void flush_producer(RdKafka::Producer* producer);

}

MqttKafkaConnector::MqttKafkaConnector(
    std::string kafka_broker,
    std::string mqtt_broker,
    std::string id,
    std::string source_topic,
    std::string sink_topic
): id(id), source_topic(source_topic), sink_topic(sink_topic) {
    kafka_producer = create_kafka_producer(kafka_broker, id + "_connector");
    sink_topic_obj = create_kafka_topic(kafka_producer, sink_topic);
    mqtt_client = create_mosq_client(mqtt_broker, id + "_connector", this, on_connect, on_message);
}

MqttKafkaConnector::~MqttKafkaConnector() {
    mosquitto_disconnect(mqtt_client);
    mosquitto_loop_stop(mqtt_client, false);
    mosquitto_destroy(mqtt_client);

    flush_producer(kafka_producer);
    delete sink_topic_obj;
    delete kafka_producer;
}

void MqttKafkaConnector::on_connect(struct mosquitto *mosq, void *obj, int rc) {
    MqttKafkaConnector *self = static_cast<MqttKafkaConnector *>(obj);
    if (rc == 0) {
        mosquitto_subscribe(mosq, nullptr, self->source_topic.c_str(), 0);
    } else {
        throw std::runtime_error("Could not connect to Broker");
    }
}

void MqttKafkaConnector::on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg) {
    MqttKafkaConnector *self = static_cast<MqttKafkaConnector *>(obj);
    const char *data = static_cast<const char *>(msg->payload);

    self->total_send_cnt += 1;
    int rc = self->kafka_producer->produce(
        self->sink_topic_obj, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
        const_cast<void *>(static_cast<const void *>(data)),
        strlen(data), nullptr, nullptr
    );
    self->kafka_producer->poll(0);

    if (rc == RdKafka::ERR_NO_ERROR) {
        self->success_send_cnt += 1;
    } else {
        self->fail_send_cnt += 1;
    }
}

std::string MqttKafkaConnector::get_id() {
    return id;
}

std::string MqttKafkaConnector::get_source_topic() {
    return source_topic;
}

std::string MqttKafkaConnector::get_sink_topic() {
    return sink_topic;
}

int MqttKafkaConnector::get_total_send_cnt() {
    return total_send_cnt.load();
}

int MqttKafkaConnector::get_fail_send_cnt() {
    return fail_send_cnt.load();
}

int MqttKafkaConnector::get_success_send_cnt() {
    return success_send_cnt.load();
}

namespace {

RdKafka::Producer *create_kafka_producer(std::string brokers, std::string client_id) {
    std::string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("client.id", client_id, errstr);
    conf->set("acks", "0", errstr);
    conf->set("linger.ms", "0", errstr);
    conf->set("batch.size", "1", errstr);
    conf->set("batch.num.messages", "1", errstr);
    conf->set("socket.nagle.disable", "true", errstr);
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        throw std::runtime_error(errstr);
    }
    return producer;
}

RdKafka::Topic *create_kafka_topic(RdKafka::Producer *producer, std::string topic) {
    std::string errstr;
    RdKafka::Topic *topic_obj = RdKafka::Topic::create(producer, topic, nullptr, errstr);
    if (!topic_obj) {
        throw std::runtime_error(errstr);
    }
    return topic_obj;
}

mosquitto *create_mosq_client(
    std::string broker,
    std::string client_id,
    void *obj,
    void (*on_connect)(struct mosquitto *, void *, int),
    void (*on_message)(struct mosquitto *, void *, const struct mosquitto_message *)
) {
    mosquitto_lib_init();
    mosquitto *mosq = mosquitto_new(client_id.c_str(), true, obj);
    if (!mosq) {
        throw std::runtime_error("Failed to create Mosquitto instance");
    }
    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);

    std::string host;
    int port;
    try {
        host = broker.substr(0, broker.find(':'));
        port = std::stoi(broker.substr(broker.find(':') + 1));
    } catch (...) {
        throw std::runtime_error("Invalid broker format");
    }

    int ret = mosquitto_connect(mosq, host.c_str(), port, 120);
    if (ret) {
        mosquitto_destroy(mosq);
        throw std::runtime_error("Could not connect to Broker");
    }

    mosquitto_loop_start(mosq);
    return mosq;
}

void flush_producer(RdKafka::Producer* producer) {
    while (producer->outq_len() > 0) {
        producer->poll(0);
    }
}

}