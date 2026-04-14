# Script Documentation

## Overview

This document provides detailed information about the experiment orchestration scripts located in the `script/` directory. These scripts are used to automate the execution of the static and dynamic V2X experiments described in the main `README.md`.

### Configuration Precedence

The scripts use a three-tiered configuration system, applied in the following order of precedence:

1.  **Command-line arguments**: Highest precedence. Any option passed directly to the script will override other settings.
2.  **Configuration file**: A configuration file (specified with the `--config` option) can be used to set values. These values override the script's defaults.
3.  **Default values**: The scripts contain default values for all options, which are used if no other configuration is provided.

It is recommended to use a configuration file for fixed settings that do not change between runs, such as broker addresses and component paths.

## Common Configuration Options

The following options are common to both `static_v2x_expr.sh` and `dynamic_v2x_expr.sh` (and their `_light` variants).

### Broker and Script Settings
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--config <path>` | - | Path to a configuration file containing `KEY="VALUE"` pairs. | `""` |
| `--kafka-broker <host:port>` | `KAFKA_BROKER` | Address of the Kafka broker. | `127.0.0.1:9092` |
| `--mqtt-broker <host:port>` | `MQTT_BROKER` | Address of the MQTT broker. | `127.0.0.1:1883` |
| `--kafka-bin-path <path>` | `KAFKA_BIN_PATH` | Path to Kafka binary directory (containing `kafka-consumer-groups.sh`). | `./kafka/bin` |
| `--common-script-root <path>`| `COMMON_SCRIPT_ROOT` | Root directory where common helper scripts are located. | `.` |
| `--terminate-timeout <sec>` | `TERMINATE_TIMEOUT` | Timeout in seconds to wait before force-killing background processes. | `60` |
| `-v`, `--verbose` | `VERBOSE` | Enable verbose output. | `0` |

### Connector Settings
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--connector-host <user@host>` | `CONNECTOR_HOST` | Remote host for executing the Connector. | `""` |
| `--connector-root <path>` | `CONNECTOR_ROOT` | Root directory where the Connector is located. | `./connector` |
| `--connector-exec <path>` | `CONNECTOR_EXEC` | Path to the Connector executable. | `{connector-root}/bin/v2x_expr_mqtt_kafka_connector` |

### Load Consumer Settings (Load Generation)
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--load-consumer-host <host>` | `LOAD_CONSUMER_HOST` | Remote host for Load Consumers. | `""` |
| `--load-consumer-root <path>` | `LOAD_CONSUMER_ROOT` | Root directory for Load Consumers. | `./client` |
| `--load-consumer-exec <path>` | `LOAD_CONSUMER_EXEC` | Path to the Load Consumer executable. | `{load-consumer-root}/bin/v2x_expr_consumer` |
| `--load-consumer-poll-timeout <ms>` | `LOAD_CONSUMER_POLL_TIMEOUT` | Poll timeout in milliseconds for Load Consumer. | `100` |

### Measurement Consumer Settings (Metric Gathering)
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--measure-consumer-host <host>`| `MEASURE_CONSUMER_HOST`| Remote host for Measurement Consumers. | `""` |
| `--measure-consumer-root <path>`| `MEASURE_CONSUMER_ROOT`| Root directory for Measurement Consumers. | `./client` |
| `--measure-consumer-exec <path>`| `MEASURE_CONSUMER_EXEC`| Path to the Measurement Consumer executable. | `{measure-consumer-root}/bin/v2x_expr_consumer` |
| `--measure-consumer-poll-timeout <ms>` | `MEASURE_CONSUMER_POLL_TIMEOUT` | Poll timeout in milliseconds for Measure Consumer. | `0` |

### Producer Settings
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--producer-host <host>` | `PRODUCER_HOST` | Remote host for Producers. | `""` |
| `--producer-root <path>` | `PRODUCER_ROOT` | Root directory for Producers. | `./client` |
| `--producer-exec <path>` | `PRODUCER_EXEC` | Path to the Producer executable. | `{producer-root}/bin/v2x_expr_mqtt_producer` |

### Experiment Behavior Options
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--interval-noise-rate <f>` | `INTERVAL_NOISE_RATE` | Std. dev. of noise for producer interval (Non-light scripts). | `0.0` |
| `--producer-wakeup-interval <ms>`| `PRODUCER_WAKEUP_INTERVAL`| Wakeup interval for light producer (Light scripts only). | `5` |
| `--producer-spread-time <ms>`| `PRODUCER_SPREAD_TIME`| Total time to spread producer starts. | `100` |
| `--producer-spread-interval <ms>`| `PRODUCER_SPREAD_INTERVAL`| Interval between producer starts. | `5` |
| `--monitoring-epoch-size <ms>` | `MONITORING_EPOCH_SIZE` | Epoch size for calculating statistics. | `1000.0` |

## `static_v2x_expr.sh` / `static_v2x_expr_light.sh`

These scripts run the static V2X experiment. They iterate through a list of fixed client counts (`--num-car`), running a complete test for each count.

### Script-Specific Options
| Option | Config Key | Description | Default |
|---|---|---|---|
| `-d`, `--duration <sec>` | `DURATION` | The duration in seconds for each test run. | `100` |
| `--num-car <n1,n2,...>` | `NUM_CAR` | A comma-separated list of car counts to test. | `(10)` |

## `dynamic_v2x_expr.sh` / `dynamic_v2x_expr_light.sh`

These scripts run the dynamic V2X experiment, simulating scaling up and down.

### Script-Specific Options
| Option | Config Key | Description | Default |
|---|---|---|---|
| `--step-cars <n1,n2,...>` | `STEP_CARS` | A comma-separated list of car counts for scaling steps. | `(10,20...120)` |
| `--step-interval <sec>` | `STEP_INTERVAL`| Interval between scaling steps. | `20` |
| `--final-hold <sec>` | `FINAL_HOLD` | Duration to hold at peak load. | `20` |
