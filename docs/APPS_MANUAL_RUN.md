# Running Applications

All executable applications are placed in the `bin/` directory after building the project. Each application is configured through command-line arguments. You can get a full list of options for any application by running it with the `--help` or `-h` flag.

For example:
```bash
./bin/<application_name> --help
```

The following sections provide detailed descriptions of the options available for key applications in this project.

## `v2x_expr_consumer` Options

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

## `v2x_expr_mqtt_producer` Options

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

For other applications, you can typically find usage instructions by running them with a `--help` flag if implemented.