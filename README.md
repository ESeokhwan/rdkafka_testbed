# Testbed for `Kafka-Boost`

This project is a C++ testbed for `librdkafka` and `mosquitto`, primarily serving as a research platform for the paper `Kafka-Boost: An Adaptive Service Boosting Data Streaming Platform for V2X In Edge`. It is designed for performance testing and experimentation with Kafka producers and consumers, particularly in a V2X (Vehicle-to-Everything) context. It utilizes C++20, CMake for building, and vcpkg for dependency management.

## Prerequisites

*   A C++20 compatible compiler (e.g., GCC 10+, Clang 12+)
*   CMake (version 3.14 or later)
*   Git

## Project Structure

```
├── CMakeLists.txt                       # Main CMake build script
├── vcpkg.json                           # vcpkg dependencies
├── config/                              # Configuration files
├── libmoniq/                            # Submodule for monitoring
├── scripts/                             # Scripts
│   ├── mqtt_static_v2x_test.sh            # Script for the experiments of Figure 9~11
│   └── mqtt_dynamic_v2x_test.sh           # Script for the experiments of Figure 12~13
└── src/
    ├── apps/                            # Main application executables
    │   ├── v2x_expr_consumer.cpp
    │   └── v2x_expr_mqtt_producer.cpp
    └── common/                          # Common code shared across applications
```

You can view more details for each application in [here](docs/APPS_MANUAL_RUN.md)

## Building the Project

1.  **Clone the repository with submodules:**

    ```bash
    git clone --recurse-submodules https://github.com/ESeokhwan/rdkafka_testbed.git
    cd rdkafka_testbed
    ```

2.  **Bootstrap vcpkg:**

    ```bash
    ./vcpkg/bootstrap-vcpkg.sh
    ```

3.  **Configure and build with CMake:**

    This project uses a CMake preset. To build, run the following commands:
    </br>for Linux:
    ```bash
    cmake --preset=linux-release
    cmake --build --preset=linux-release
    ```
    for Windows:
    ```bash
    cmake --preset=windows-release
    cmake --build --preset=windows-release
    ```

    The executables will be placed in the `bin/` directory.

## Replicating the Experiments

The experimental setup is distributed across two server instances to isolate broker and client workloads, as illustrated below.

![expr_overview](docs/assets/experiments_replicating_overview.png)

### Architecture Overview

#### Instance 1: Broker and Metric Consumers
This instance hosts the core data pipeline and metric-gathering clients:

*   **Kafka Broker**: The central message bus in the edge server for the V2X data.
*   **Mosquitto Broker**: To support robust ingestion from many clients over the unreliable links, this broker receives data from each client and forwards it to the Kafka broker.
*   **Custom Connector**: A bridge that forwards messages from the Mosquitto broker to the Kafka broker.
*   **Metric Consumer Clients (x4)**: Four dedicated consumer clients connect to the Kafka broker to measure end-to-end latency and reliability for different service types (`sensor info. sharing`, `info. sharing`, `platooning-lower`, and `platooning-lowest`). A 6ms delay is added to simulate network round-trip time, as these consumers run on the same instance as the brokers.

#### Instance 2: Producer and Stress Clients
This instance generates the workload for the system:

*   **Producer Clients (N)**: A variable number of clients that generate and send data to the Mosquitto broker on Instance 1.
*   **Stress Consumer Clients (N-4)**: These clients connect to the Kafka broker to generate additional consumer load on the edge server, helping to simulate a busy network.

### Component Implementation

The source code for the clients and the connector can be found in the `src/apps/` directory:

*   **Producer Client**: `src/apps/v2x_expr_mqtt_producer.cpp`
*   **Metric Consumer Client**: `src/apps/v2x_expr_consumer.cpp`
*   **Custom Connector**: `src/apps/v2x_expr_custom_connector.cpp`


### Static V2X Experiments (Figure 9~11)

The static V2X experiments are orchestrated by the `script/mqtt_static_v2x_test.sh` script. This script automates setting up the test environment, launching the clients and connector, and running the experiment. It allows for configuring various settings, such as the number of clients and the test duration.

#### Script Usage

A typical command to run the script is shown below.

```bash
./script/mqtt_static_v2x_test.sh --config config/your.config --duration 120 --num-car 10,20,30
```

#### Configuration

This script has three levels of configuration, in order of precedence:
1.  **Command-line arguments** (e.g., `--duration 120`): Highest precedence.
2.  **Configuration file** (e.g., `config/example.config`): Values defined here override the defaults.
3.  **Default values** in the script: Lowest precedence.

It is recommended to use a configuration file for fixed settings, such as broker addresses and component root paths. You can view all available options in [here](docs/TODO.md).

#### Execution Flow

The script iterates through a predefined list of client numbers (e.g., 10, 20, 40... clients) and performs the following steps for each number:

1.  **Start Connector**: Launches the custom connector application, which bridges the Mosquitto and Kafka brokers.
2.  **Clear Consumer Groups**: Deletes any existing Kafka consumer groups from previous runs to ensure a clean start.
3.  **Start Metric Consumers**: Runs the 4 dedicated metric consumer clients that connect to the Kafka broker. These clients are responsible for measuring latency and reliability.
4.  **Start Stress Consumers**: Runs a number of stress consumer clients making the current number of consumers same with the producers. These connect to the Kafka broker to generate background load.
5.  **Verify Consumers**: Waits a moment and checks to ensure all consumer clients have successfully connected to their respective groups.
6.  **Run Producers**: Starts the producer clients, which generate the V2X message load for the duration of the test.
7.  **Clean Up**: Once the producers have finished, the script terminates the connector and all consumer clients.
8.  **Output Results**: Fetches and prints the log file from the metric consumers, which contains the results (e.g., end-to-end latency) for the completed run.

After completing the run for one client number, the script waits for a brief period before starting the next run with an increased number of clients.


### dynamic v2x experiments (Figure 12~13)
TODO


## References

If you use Docling in your projects, please consider citing the following:

```bib
// TODO
```