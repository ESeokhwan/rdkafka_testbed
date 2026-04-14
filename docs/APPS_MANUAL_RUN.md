# Running Applications

All executable applications are placed in the `bin/` directory after building the project. Each application is configured through command-line arguments. You can get a full list of options for any application by running it with the `--help` or `-h` flag.

For example:
```bash
./bin/<application_name> --help
```

The following sections provide detailed descriptions of the options available for key applications in this project.

## `v2x_expr_consumer`

This application acts as a Kafka consumer for a V2X experiment. It can be used as a **Measurement Consumer** (to measure latency/reliability) or a **Load Consumer** (to generate background load).

To run the consumer:
```bash
./bin/v2x_expr_consumer --broker <your_kafka_broker> --topic-prefix <topic_prefix> [OPTIONS]
```

| Option                  | Description                                            | Default      |
| ----------------------- | ------------------------------------------------------ | ------------ |
| `--help`, `-h`          | Show the help message.                                 |              |
| `--broker`, `-b`        | Kafka broker address.                                  | (required)   |
| `--group-prefix`        | Prefix for the consumer group ID.                      | ""           |
| `--topic-prefix`        | Prefix for the topics to subscribe to.                 | ""           |
| `--client-cnt`          | Number of consumer clients to create.                  | 1            |
| `--start-idx`           | Starting index for client IDs.                         | 0            |
| `--running-time`, `-r`  | Total running time for the test in seconds.            | 10           |
| `--start-barrier-delay` | Delay in seconds before consumers start fetching.      | 2            |
| `--scrapable`           | Enable scrapable monitoring mode.                      | off          |
| `--no-log`              | Disable monitoring logging. Turn it on for Load Consumers. | off          |
| `--read-tagged-only`    | Enable log sampling (read only tagged messages).       | off          |
| `--monitoring-epoch-size`| Epoch size in ms for calculating statistics.          | 1000.0       |
| `--outdir`, `-o`        | Directory to save output log files.                    | ""           |
| `--out-prefix`          | Prefix for output log file names.                      | ""           |
| `--verbose`, `-v`       | Enable verbose logging.                                | off          |

## `v2x_expr_mqtt_producer`

This is a multi-threaded MQTT producer that simulates multiple vehicles, each running multiple V2X services.

To run the producer:
```bash
./bin/v2x_expr_mqtt_producer --broker <your_mqtt_broker> --topic-prefix <topic_prefix> [OPTIONS]
```

| Option                         | Description                                                    | Default    |
| ------------------------------ | -------------------------------------------------------------- | ---------- |
| `--help`, `-h`                 | Show the help message.                                         |            |
| `--broker`, `-b`               | MQTT broker address.                                           | (required) |
| `--client-prefix`              | Prefix for the client ID.                                      | ""         |
| `--topic-prefix`               | Prefix for the topics to publish to.                           | ""         |
| `--client-cnt`                 | Number of producer clients to create.                          | 1          |
| `--start-idx`                  | Starting index for client IDs.                                 | 0          |
| `--running-time`, `-r`         | Total running time for the test in seconds.                    | 10         |
| `--interval-noise-stddev-rate` | Interval noise standard deviation rate (0-1.0).                | 0.0        |
| `--warmup-cnt`                 | Number of messages for warmup.                                 | 0          |
| `--warmup-topic`               | Topic for warmup messages.                                     | "warmup"   |
| `--start-barrier-delay`        | Delay in seconds before producers start publishing.            | 2          |
| `--client-spread-time`         | Total time to spread client starts in milliseconds.            | 100        |
| `--client-spread-interval`     | Interval between client starts in milliseconds.                | 5          |
| `--monitoring-batch-size`      | The number of messages to wait before logging monitoring data. | -1 (inf.)  |
| `--scrapable`                  | Enable scrapable monitoring mode.                              | off        |
| `--verbose`, `-v`              | Enable verbose logging.                                        | off        |

## `v2x_expr_mqtt_producer_light`

A lightweight version of the MQTT producer, optimized for high-density client simulation with lower resource overhead.

To run the light producer:
```bash
./bin/v2x_expr_mqtt_producer_light --broker <your_mqtt_broker> --client-cnt <count> [OPTIONS]
```

| Option                         | Description                                                    | Default    |
| ------------------------------ | -------------------------------------------------------------- | ---------- |
| `--help`, `-h`                 | Show the help message.                                         |            |
| `--broker`, `-b`               | MQTT broker address.                                           | (required) |
| `--client-prefix`              | Prefix for the client ID.                                      | ""         |
| `--topic-prefix`               | Prefix for the topics to publish to.                           | ""         |
| `--client-cnt`                 | Number of producer clients to create.                          | 1          |
| `--start-idx`                  | Starting index for client IDs.                                 | 0          |
| `--wakeup-interval`            | Internal wakeup interval in ms for checking publish timing.    | 5          |
| `--running-time`, `-r`         | Total running time for the test in seconds.                    | 10         |
| `--start-barrier-delay`        | Delay in seconds before producers start publishing.            | 2          |
| `--client-spread-time`         | Total time to spread client starts in milliseconds.            | 100        |
| `--client-spread-interval`     | Interval between client starts in milliseconds.                | 5          |
| `--no-log`                     | Disable monitoring logging.                                    | off        |
| `--scrapable`                  | Enable scrapable monitoring mode.                              | off        |
| `--verbose`, `-v`              | Enable verbose logging.                                        | off        |

## `v2x_expr_mqtt_kafka_connector`

A bridge application that forwards messages from MQTT topics to corresponding Kafka topics.

To run the connector:
```bash
./bin/v2x_expr_mqtt_kafka_connector --kafka-broker <kafka_addr> --mqtt-broker <mqtt_addr> [OPTIONS]
```

| Option                  | Description                                            | Default      |
| ----------------------- | ------------------------------------------------------ | ------------ |
| `--help`, `-h`          | Show the help message.                                 |              |
| `--kafka-broker`        | Kafka broker address.                                  | (required)   |
| `--mqtt-broker`         | MQTT broker address.                                   | (required)   |
| `--running-time`        | Total running time in seconds.                         | 10           |
| `--scrapable`           | Enable scrapable monitoring output (JSON).             | off          |
| `--no-log`              | Disable connection stats logging.                      | off          |
| `--verbose`             | Enable verbose logging.                                | off          |
