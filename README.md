# Testbed for `Kafka-Boost`

This project is a C++ testbed for `librdkafka` and `mosquitto`, primarily serving as a research platform for the paper `Kafka-Boost: An Adaptive Service Boosting Data Streaming Platform for V2X In Edge`. It is designed for performance testing and experimentation with Kafka producers and consumers, particularly in a V2X (Vehicle-to-Everything) context. It utilizes a custom version of `librdkafka` that simulates 5G edge network behaviors. The project uses C++20, CMake for building, and vcpkg for dependency management.

## Prerequisites

*   A C++20 compatible compiler (e.g., GCC 10+, Clang 12+)
*   CMake (version 3.30 or later)
*   Git

## Project Structure

```
├── CMakeLists.txt                       # Main CMake build script
├── vcpkg.json                           # vcpkg dependencies
├── config/                              # Configuration files
├── libmoniq/                            # Submodule for monitoring
├── librdkafka/                          # Submodule for custom librdkafka (5G edge network simulation)
├── script/                              # Scripts
│   ├── static_v2x_expr.sh               # Script for the experiments corresponding to Figure 9~11
│   ├── static_v2x_expr_light.sh         # Script for the experiments corresponding to Figure 9~11 with light version producer.
│   ├── dynamic_v2x_expr.sh              # Script for the experiments corresponding to Figure 12~13
│   ├── dynamic_v2x_expr_light.sh        # Script for the experiments corresponding to Figure 12~13 with light version producer.
│   └── ...                              # Helper scripts for experiment automation
└── src/
    ├── apps/                            # Main application executables
    │   ├── v2x_expr_consumer.cpp
    │   ├── v2x_expr_mqtt_producer.cpp
    │   ├── v2x_expr_mqtt_producer_light.cpp
    │   ├── v2x_expr_mqtt_kafka_connector.cpp
    │   ├── basic_mqtt_producers_test.cpp
    │   └── basic_producers_test.cpp
    └── common/                          # Common code shared across applications

```

You can find more details for each application [here](docs/APPS_MANUAL_RUN.md)

## Building the Project

1.  **Clone the repository with submodules:**

    ```bash
    git clone --recurse-submodules https://github.com/ESeokhwan/rdkafka_testbed.git
    cd rdkafka_testbed
    ```

2.  **Bootstrap vcpkg:**

    **Linux/macOS:**
    ```bash
    ./vcpkg/bootstrap-vcpkg.sh
    ```
    **Windows:**
    ```cmd
    .\vcpkg\bootstrap-vcpkg.bat
    ```

3.  **Configure and build with CMake:**

    To build, run the following commands. Then, the executables of applications will be placed in the `bin/` directory.
    ```bash
    cmake --preset <preset-name>
    cmake --build --preset <preset-name>
    ```
    Once built, the application executables will be placed in the `bin/` directory.
    > **Note:** This project supports various CMake presets, including `linux-release`, `win-64-release`, `macos-release`, and `macos-arm64-release`. Please refer to [`CMakePresets.json`](CMakePresets.json) for the full list of available presets.

## The Experimental Setup

The experimental setup is distributed across two server instances to isolate broker and client workloads, as illustrated below.

![expr_overview](docs/assets/experiments_replicating_overview.png)

### Instance 1: Broker and Measurement Consumers
This instance hosts the core data pipeline and metric-gathering clients:

*   **Kafka Broker**: The central message bus in the edge server for the V2X data.
*   **Mosquitto Broker**: To support robust ingestion from many clients over unreliable links, this broker acts as an ingestion point, receiving data from each client and buffering it for the connector.
*   **Custom Connector**: A bridge that forwards messages from the Mosquitto broker to the Kafka broker.
*   **Measurement Consumer Clients (x4)**: Four dedicated consumer clients connect to the Kafka broker to measure end-to-end latency and reliability for different service types (`sensor info. sharing`, `info. sharing`, `platooning-lower`, and `platooning-lowest`). A 6ms artificial delay is added to simulate network round-trip time, as these consumers run on the same instance as the brokers.

### Instance 2: Producer and Load Consumers
This instance generates the workload for the system:

*   **Producer Clients (N)**: A variable number of clients that generate and send data to the Mosquitto broker on Instance 1.
*   **Load Consumer Clients (N-4)**: These clients connect to the Kafka broker to generate additional consumer load on the edge server, helping to simulate high system load on the edge server.

### Component Implementation

The source code for the clients and the connector can be found in the `src/apps/` directory:

*   **Producer Client**: `src/apps/v2x_expr_mqtt_producer.cpp` (or `..._light.cpp`)
*   **Measurement/Load Consumer Client**: `src/apps/v2x_expr_consumer.cpp`
*   **Custom Connector**: `src/apps/v2x_expr_mqtt_kafka_connector.cpp`


## Static V2X Experiments (Figures 9-11)

The static V2X experiments measure key performance metrics with a fixed number of V2X clients within the coverage area.

This experiment is orchestrated by the `script/static_v2x_expr.sh` script (or `_light.sh`). This script automates setting up the test environment, launching the clients and connector, and running the experiment. It allows for configuring various settings, such as the number of clients, the test duration for each run and more. You can find more details in the [script documentation](docs/SCRIPT_DOCUMENTATION.md)

### Script Usage

A typical command to run the script is shown below.

```bash
./script/static_v2x_expr.sh --config config/your.config --duration 120 --num-car 10,20,30
```

> It is recommended to use a configuration file for fixed settings, such as broker addresses and component root paths. You can view all available options in the [script documentation](docs/SCRIPT_DOCUMENTATION.md).

#### Key Options
- `--duration`, `-d`: The duration in seconds for each individual test run.
- `--num-car`: A comma-separated string of client (car) counts to test.
- `--interval-noise-rate`: (Non-light) Noise rate for producer interval.
- `--producer-wakeup-interval`: (Light only) Wakeup interval for producer (ms).
- `--producer-spread-time`: Total time to spread client starts (ms).
- `--producer-spread-interval`: Interval between client starts (ms).
- `--monitoring-epoch-size`: Epoch size in milliseconds for calculating statistics.

### Execution Flow

The script iterates through a predefined list of client numbers (e.g., 10, 20, 40... clients) and performs the following steps for each number:

1.  **Start Connector**: Launches the custom connector application.
2.  **Clear Consumer Groups**: Deletes any existing Kafka consumer groups from previous runs.
3.  **Start Measurement Consumers**: Runs the 6 dedicated measurement consumer clients.
4.  **Start Load Consumers**: Runs a number of load consumer clients to match the number of producers.
5.  **Verify Consumers**: Checks to ensure all consumer clients have successfully connected.
6.  **Run Producers**: Starts the producer clients, generating the V2X message load.
7.  **Clean Up**: Terminates the connector and all consumer clients.
8.  **Output Results**: Prints the results (e.g., end-to-end latency) from the measurement consumers.


## Dynamic V2X Experiments (Figures 12-13)

The dynamic V2X experiments measure performance when the number of V2X clients varies over time.

This experiment is orchestrated by the `script/dynamic_v2x_expr.sh` script (or `_light.sh`). It automates setting up the test environment and launching the clients/connector.

### Script Usage

A typical command to run the script is shown below:

```bash
./script/dynamic_v2x_expr.sh --config config/your.config --step-interval 20 --final-hold 20 --step-cars 10,20,30,40,50
```

> It is recommended to use a configuration file for fixed settings, such as broker addresses and component root paths. You can view all available options in the [script documentation](docs/SCRIPT_DOCUMENTATION.md).

#### Key Options
- `--step-cars`: A comma-separated string of total client counts for each scaling step.
- `--step-interval`: The interval in seconds to wait between each scaling step.
- `--final-hold`: The duration in seconds to hold the test at the peak client count.
- `--interval-noise-rate`: (Non-light) Noise rate for producer interval.
- `--producer-wakeup-interval`: (Light only) Wakeup interval for producer (ms).
- `--producer-spread-time`: Total time to spread client starts (ms).
- `--producer-spread-interval`: Interval between client starts (ms).
- `--monitoring-epoch-size`: Epoch size in milliseconds for calculating statistics.

### Execution Flow

The script executes a single dynamic scaling test that progressively increases the load to a maximum point and then decreases it. It iterates through a list of target client numbers (e.g., 10, 20... 120 cars) and performs the following steps:


1.  **Start Connector**: Launches the custom connector application.
2.  **Clear Consumer Groups**: Deletes existing Kafka consumer groups.
3.  **Start Measurement Consumers**: Runs the 6 dedicated measurement consumer clients continuously.
4.  **Incremental Scale Up**: Iterates through the step list, launching new batches of load consumers and producers.
5.  **Stable Hold**: Runs all clients for a defined `final-hold` period.
6.  **Incremental Scale Down**: Sequentially terminates the producers and load consumers for each batch.
7.  **Final Clean Up**: Once all load-generating clients have been stopped, the script terminates the connector and the mesurement consumers.


## Citations

If you use this testbed in your projects, please consider citing the following:

```bib
// TODO
```
