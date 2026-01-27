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

The following options are common to both `static_v2x_expr.sh` and `dynamic_v2x_expr.sh`.

| Option | Config Key | Description | Default |
|---|---|---|---|
| `--config <path>` | - | Path to a configuration file containing `KEY="VALUE"` pairs. | `""` |
| `--kafka-broker <host:port>` | `KAFKA_BROKER` | Address of the Kafka broker. | `127.0.0.1:9092` |
| `--mqtt-broker <host:port>` | `MQTT_BROKER` | Address of the MQTT broker. | `127.0.0.1:1883` |
| `--common-script-root <path>`| `COMMON_SCRIPT_ROOT` | Root directory where common helper scripts (`delete_consumer_group.sh`, etc.) are located. | `.` |
| `--connect-host <user@host>` | `CONNECT_HOST` | Remote host (`user@ip`) for executing the Connector application. If empty, runs locally. | `""` |
| `--connect-root <path>` | `CONNECT_ROOT` | Root directory on the remote/local host where the Connector application is located. | `./connect` |
| `--connect-out <path>` | `CONNECT_OUT` | Output directory for Connector logs. | `{connect-root}/out` |
| `--connect-temp <path>` | `CONNECT_TEMP` | Temporary directory for Connector files (e.g., PID files). | `{connect-root}/temp` |
| `--connect-exec <path>` | `CONNECT_EXEC` | Path to the Connector executable. | `{connect-root}/bin/v2x_expr_mqtt_kafka_connector` |
| `--r-client-host <user@host>`| `R_CLIENT_HOST` | Remote host (`user@ip`) for executing the Metric Consumer clients. If empty, runs locally. | `""` |
| `--r-client-root <path>` | `R_CLIENT_ROOT` | Root directory on the remote/local host where the remote client applications are located. | `client` |
| `--r-client-out <path>` | `R_CLIENT_OUT` | Output directory for remote client logs. | `{r-client-root}/out` |
| `--r-client-temp <path>` | `R_CLIENT_TEMP` | Temporary directory for remote client files (e.g., PID files). | `{r-client-root}/temp` |
| `--r-consumer-exec <name>` | `R_CONSUMER_EXEC` | Executable name for the Metric Consumer (remote). | `{r-client-root}/bin/v2x_expr_consumer` |
| `--client-root <path>` | `CLIENT_ROOT` | Root directory where local client applications are located. | `./client` |
| `--client-out <path>` | `CLIENT_OUT` | Output directory for local client logs. | `{client-root}/out` |
| `--client-temp <path>` | `CLIENT_TEMP` | Temporary directory for local client files (e.g., PID files). | `{client-root}/temp` |
| `--consumer-exec <path>` | `CONSUMER_EXEC` | Path to the Stress Consumer executable (local). | `{client-root}/bin/v2x_expr_consumer` |
| `--producer-exec <path>` | `PRODUCER_EXEC` | Path to the Producer executable (local). | `{client-root}/bin/v2x_expr_mqtt_producer` |
| `--terminate-timeout <sec>` | `TERMINATE_TIMEOUT` | Timeout in seconds to wait before force-killing background processes during cleanup. | `60` |
| `--interval-noise-rate <f>` | `INTERVAL_NOISE_RATE` | Standard deviation of noise to add to the producer's message interval, as a rate of the interval. | `0.0` |
| `-v`, `--verbose` | `VERBOSE` | Enable verbose output, printing all configuration values and script actions. | `0` (off) |
| `-h`, `--help` | `HELP` | Display the help message for the script and exit. | N/A |

## `mqtt_static_v2x_expr.sh`

This script runs the static V2X experiment. It iterates through a list of fixed client counts (`--num-car`), running a complete test for each count for a specified duration.

### Script-Specific Options

| Option | Config Key | Description | Default |
|---|---|---|---|
| `-d`, `--duration <sec>` | `DURATION` | The duration in seconds for each individual test run. | `100` |
| `--num-car <n1,n2,...>` | `NUM_CAR` | A comma-separated string of client (car) counts to test. The script will perform a full run for each number in the list. | `(10)` |

## `dynamic_v2x_expr.sh`

This script (named `dynamic_v2x_expr.sh` in the filesystem) runs the dynamic V2X experiment. It simulates a changing number of clients over time by incrementally scaling the number of producers and consumers up to a maximum value and then scaling them back down.

### Script-Specific Options

| Option | Config Key | Description | Default |
|---|---|---|---|
| `--step-cars <n1,n2,...>` | `STEP_CARS` | A comma-separated string of total client counts for each scaling step. The load will increase to match these numbers sequentially. | `(10,20,30,40,50,60,70,80,90,100,110,120)` |
| `--step-interval <sec>` | `STEP_INTERVAL`| The interval in seconds to wait between each scaling step (both up and down). | `20` |
| `--final-hold <sec>` | `FINAL_HOLD` | The duration in seconds to hold the test at the peak client count before starting to scale down. | `20` |
