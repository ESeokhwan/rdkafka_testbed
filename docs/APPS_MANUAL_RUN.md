# Running Applications

All executable applications are placed in the `bin/` directory after building the project. Each application is configured through command-line arguments. You can get a full list of options for any application by running it with the `--help` or `-h` flag.

For example:
```bash
./bin/<application_name> --help
```

The following sections provide detailed descriptions of the options available for key applications in this project.

## `v2x_expr_consumer`

This application acts as a Kafka consumer for a V2X (Vehicle-to-Everything) experiment. It creates a specified number of consumer clients, each subscribing to a set of predefined V2X topics (e.g., `S10Hz-Info`, `S30Hz`). Its main purpose is to receive messages from these topics and pass them to a monitoring queue to measure statistics like latency. It can be configured to run for a specific duration, and its output logs can be saved to a directory.

To run the V2X experiment consumer, you can use the following command:

```bash
./bin/v2x_expr_consumer --broker <your_kafka_broker> --topic-prefix <topic_prefix> [OPTIONS]
```

| Option                  | Description                                            | Default      |
| ----------------------- | ------------------------------------------------------ | ------------ |
| `--help`, `-h`          | Show the help message.                                 |              |
| `--broker`, `-b`        | Kafka broker address.                                  | (required)   |
| `--group_prefix`        | Prefix for the consumer group ID.                      | ""           |
| `--topic_prefix`        | Prefix for the topics to subscribe to.                 | ""           |
| `--client_cnt`          | Number of consumer clients to create.                  | 1            |
| `--start_idx`           | Starting index for client IDs.                         | 0            |
| `--running_time`, `-r`  | Total running time for the test in seconds.            | 10           |
| `--start_barrier_delay` | Delay in seconds before consumers start fetching.      | 2            |
| `--scrapable`           | Enable scrapable monitoring.                           | off          |
| `--read_tagged_only`    | Enable log sampling.                                   | off          |
| `--outdir`, `-o`        | Directory to save output log files.                    | ""           |
| `--out_prefix`          | Prefix for output log file names.                      | ""           |
| `--verbose`, `-v`       | Enable verbose logging.                                | off          |

## `v2x_expr_mqtt_producer`

This application is a multi-threaded MQTT producer designed for a V2X experiment. It simulates multiple vehicles (clients), where each vehicle runs several services that publish messages to different MQTT topics at various frequencies (e.g., 50Hz, 30Hz, 10Hz). The application is used to generate a realistic load on an MQTT broker. It supports configurable message intervals, message sizes, and running duration. It also includes a warmup phase to prepare the system before the actual test run.

To run the MQTT producer, you can use the following command:

```bash
./bin/v2x_expr_mqtt_producer --broker <your_mqtt_broker> --topic-prefix <topic_prefix> [OPTIONS]
```

| Option                         | Description                                                    | Default    |
| ------------------------------ | -------------------------------------------------------------- | ---------- |
| `--help`, `-h`                 | Show the help message.                                         |            |
| `--broker`, `-b`               | MQTT broker address.                                           | (required) |
| `--client_prefix`              | Prefix for the client ID.                                      | ""         |
| `--topic_prefix`               | Prefix for the topics to publish to.                           | ""         |
| `--client_cnt`                 | Number of producer clients to create.                          | 1          |
| `--running_time`, `-r`         | Total running time for the test in seconds.                    | 10         |
| `--interval_noise_stddev_rate` | Interval noise standard deviation rate (0-100).                | 0          |
| `--warmup_cnt`                 | Number of messages for warmup.                                 | 100        |
| `--warmup_topic`               | Topic for warmup messages.                                     | "warmup"   |
| `--start_barrier_delay`        | Delay in seconds before producers start publishing.            | 2          |
| `--monitoring_batch_size`      | The number of messages to wait before logging monitoring data. | 100        |
| `--scrapable`                  | Enable scrapable monitoring.                                   | off        |
| `--outdir`, `-o`               | Directory to save output log files.                            | ""         |
| `--verbose`, `-v`              | Enable verbose logging.                                        | off        |