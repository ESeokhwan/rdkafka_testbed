#!/bin/bash

# Precedence order:
# 1. Command-line arguments (highest)
# 2. Configuration file values
# 3. Default values in this script (lowest)

# --- Default values for options ---
KAFKA_BROKER="127.0.0.1:9092"
MQTT_BROKER="127.0.0.1:1883"

KAFKA_BIN_PATH="kafka/bin"
COMMON_SCRIPT_ROOT="."

REMOTE_USER="user"
REMOTE_IP="127.0.0.1"

CONNECTOR_HOST=""
CONNECTOR_ROOT="connector"
CONNECTOR_OUT=""
CONNECTOR_TEMP=""
CONNECTOR_EXEC=""

LOAD_CONSUMER_HOST=""
LOAD_CONSUMER_ROOT="client"
LOAD_CONSUMER_OUT=""
LOAD_CONSUMER_TEMP=""
LOAD_CONSUMER_EXEC=""

MEASURE_CONSUMER_HOST=""
MEASURE_CONSUMER_ROOT="client"
MEASURE_CONSUMER_OUT=""
MEASURE_CONSUMER_TEMP=""
MEASURE_CONSUMER_EXEC=""

PRODUCER_HOST=""
PRODUCER_ROOT="client"
PRODUCER_OUT=""
PRODUCER_TEMP=""
PRODUCER_EXEC=""

TERMINATE_TIMEOUT=60

DURATION=100

NUM_CAR=(10)

INTERVAL_NOISE_RATE=0.0
PRODUCER_SPREAD_TIME=100
PRODUCER_SPREAD_INTERVAL=5

MONITORING_EPOCH_SIZE=1000.0

VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o d:vh --longoptions \
    "config:, verbose, help, kafka-broker:, mqtt-broker:, kafka-bin-path:, common-script-root:, \
    connector-host:, connector-root:, connector-out:, connector-temp:, connector-exec:, \
    load-consumer-host:, load-consumer-root:, load-consumer-out:, load-consumer-temp:, \
    load-consumer-exec:, measure-consumer-host:, measure-consumer-root:, measure-consumer-out:,\
    measure-consumer-temp:, measure-consumer-exec:, producer-host:, producer-root:, producer-out:, \
    producer-temp:, producer-exec:, duration:, num-car:, interval-noise-rate:, \
    producer-spread-time:, producer-spread-interval:, monitoring-epoch-size:, terminate-timeout:" \
    -n 'myscript' -- "$@" \
)

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_KAFKA_BROKER=""
CL_MQTT_BROKER=""
CL_KAFKA_BIN_PATH=""
CL_COMMON_SCRIPT_ROOT=""
CL_CONNECTOR_HOST=""
CL_CONNECTOR_ROOT=""
CL_CONNECTOR_OUT=""
CL_CONNECTOR_TEMP=""
CL_CONNECTOR_EXEC=""
CL_LOAD_CONSUMER_HOST=""
CL_LOAD_CONSUMER_ROOT=""
CL_LOAD_CONSUMER_OUT=""
CL_LOAD_CONSUMER_TEMP=""
CL_LOAD_CONSUMER_EXEC=""
CL_MEASURE_CONSUMER_HOST=""
CL_MEASURE_CONSUMER_ROOT=""
CL_MEASURE_CONSUMER_OUT=""
CL_MEASURE_CONSUMER_TEMP=""
CL_MEASURE_CONSUMER_EXEC=""
CL_PRODUCER_HOST=""
CL_PRODUCER_ROOT=""
CL_PRODUCER_OUT=""
CL_PRODUCER_TEMP=""
CL_PRODUCER_EXEC=""
CL_TERMINATE_TIMEOUT=""
CL_DURATION=""
CL_NUM_CAR=()
CL_INTERVAL_NOISE_RATE=""
CL_PRODUCER_SPREAD_TIME=""
CL_PRODUCER_SPREAD_INTERVAL=""
CL_MONITORING_EPOCH_SIZE=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --kafka-broker) CL_KAFKA_BROKER="$2" ; shift 2;;
        --mqtt-broker) CL_MQTT_BROKER="$2" ; shift 2 ;;
        --kafka-bin-path) CL_KAFKA_BIN_PATH="$2" ; shift 2 ;;
        --common-script-root) CL_COMMON_SCRIPT_ROOT="$2" ; shift 2 ;;
        --connector-host) CL_CONNECTOR_HOST="$2" ; shift 2 ;;
        --connector-root) CL_CONNECTOR_ROOT="$2" ; shift 2 ;;
        --connector-out) CL_CONNECTOR_OUT="$2" ; shift 2 ;;
        --connector-temp) CL_CONNECTOR_TEMP="$2" ; shift 2 ;;
        --connector-exec) CL_CONNECTOR_EXEC="$2" ; shift 2 ;;
        --load-consumer-host) CL_LOAD_CONSUMER_HOST="$2" ; shift 2 ;;
        --load-consumer-root) CL_LOAD_CONSUMER_ROOT="$2" ; shift 2 ;;
        --load-consumer-out) CL_LOAD_CONSUMER_OUT="$2" ; shift 2 ;;
        --load-consumer-temp) CL_LOAD_CONSUMER_TEMP="$2" ; shift 2 ;;
        --load-consumer-exec) CL_LOAD_CONSUMER_EXEC="$2" ; shift 2 ;;
        --measure-consumer-host) CL_MEASURE_CONSUMER_HOST="$2" ; shift 2 ;;
        --measure-consumer-root) CL_MEASURE_CONSUMER_ROOT="$2" ; shift 2 ;;
        --measure-consumer-out) CL_MEASURE_CONSUMER_OUT="$2" ; shift 2 ;;
        --measure-consumer-temp) CL_MEASURE_CONSUMER_TEMP="$2" ; shift 2 ;;
        --measure-consumer-exec) CL_MEASURE_CONSUMER_EXEC="$2" ; shift 2 ;;
        --producer-host) CL_PRODUCER_HOST="$2" ; shift 2 ;;
        --producer-root) CL_PRODUCER_ROOT="$2" ; shift 2 ;;
        --producer-out) CL_PRODUCER_OUT="$2" ; shift 2 ;;
        --producer-temp) CL_PRODUCER_TEMP="$2" ; shift 2 ;;
        --producer-exec) CL_PRODUCER_EXEC="$2" ; shift 2 ;;
        --terminate-timeout) CL_TERMINATE_TIMEOUT="$2" ; shift 2 ;;
        -d|--duration) CL_DURATION="$2" ; shift 2 ;;
        --num-car) IFS=',' read -r -a CL_NUM_CAR <<< "$2" ; shift 2 ;;
        --interval-noise-rate) CL_INTERVAL_NOISE_RATE="$2" ; shift 2 ;;
        --producer-spread-time) CL_PRODUCER_SPREAD_TIME="$2" ; shift 2 ;;
        --producer-spread-interval) CL_PRODUCER_SPREAD_INTERVAL="$2" ; shift 2 ;;
        --monitoring-epoch-size) CL_MONITORING_EPOCH_SIZE="$2" ; shift 2 ;;
        -v|--verbose) CL_VERBOSE=1 ; shift ;;
        -h|--help) HELP=1 ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

# --- Help Message ---
if [ "$HELP" -eq 1 ]; then
    echo "Usage: $(basename "$0") [OPTIONS] [POSITIONAL_ARG1] [POSITIONAL_ARG2...]"
    echo ""
    echo "This script orchestrates a static V2X performance experiment."
    echo "It iterates through a list of fixed client counts, running a complete test iteration for each."
    echo ""
    echo "Options:"
    echo "      --config <path>                      Path to a configuration file. (e.g., key=\"value\" pairs)"
    echo "      --broker <host:port>                 Kafka broker address. (Default: 127.0.0.1:9092)"
    echo "      --mqtt-broker <host:port>            MQTT broker address. (Default: 127.0.0.1:1883)"
    echo "      --kafka-bin-path <path>              Path to Kafka binary directory. (Default: ./kafka/bin)"
    echo "      --common-script-root <path>          Root directory where common script are located. (Default: .)"
    echo "      --connector-host <user@host>         Remote host for Connector execution. (Default: empty string for local)"
    echo "      --connector-root <path>              Root directory on remote host where Connector is located. (Default: ./connector)"
    echo "      --connector-out <path>               Output root directory for Connector logs. (Default: {connector-root}/out)"
    echo "      --connector-temp <path>              Temporary root directory for Connector files. (Default: {connector-root}/temp)"
    echo "      --connector-exec <path>              Executable path for Connector. (Default: {connector-root}/bin/v2x_expr_mqtt_kafka_connector)"
    echo "      --load-consumer-host <user@host>     Remote host for Load Consumers execution. (Default: empty string for local)"
    echo "      --load-consumer-root <path>          Root directory of Load Consumers (Default: client)"
    echo "      --load-consumer-out <path>           Output root directory for Load Consumer logs. (Default: {load-consumer-root}/out)"
    echo "      --load-consumer-temp <path>          Temporary root directory for Load Consumer files. (Default: {load-consumer-root}/temp)"
    echo "      --load-consumer-exec <name>          Executable name for Load Consumer. (Default: {load-consumer-root}/bin/v2x_expr_consumer)"
    echo "      --measure-consumer-host <user@host>  Remote host for Measurement Consumers execution. (Default: empty string for local)"
    echo "      --measure-consumer-root <path>       Root directory of Measurement Consumers (Default: client)"
    echo "      --measure-consumer-out <path>        Output root directory for Measurement Consumer logs. (Default: {measure-consumer-root}/out)"
    echo "      --measure-consumer-temp <path>       Temporary root directory for Measurement Consumer files. (Default: {measure-consumer-root}/temp)"
    echo "      --measure-consumer-exec <name>       Executable name for Measurement Consumer. (Default: {measure-consumer-root}/bin/v2x_expr_consumer)"
    echo "      --producer-host <user@host>          Remote host for Producer execution. (Default: empty string for local)"
    echo "      --producer-root <path>               Root directory of Producer (Default: client)"
    echo "      --producer-out <path>                Output root directory for Producer logs. (Default: {producer-root}/out)"
    echo "      --producer-temp <path>               Temporary root directory for Producer files. (Default: {producer-root}/temp)"
    echo "      --producer-exec <name>               Executable name for Producer. (Default: {producer-root}/bin/v2x_expr_mqtt_producer)"
    echo "      --terminate-timeout <seconds>        Timeout second to wait before force killing (Default: 60)."
    echo "  -d, --duration <seconds>                 Duration for the test run. (Default: 100)"
    echo "      --num-car <num1,num2,...>            Comma-separated list of car counts for the test. (Default: (10))"
    echo "      --interval-noise-rate <f>            Standard deviation of noise to add to produce interval (Default: 0.0)"
    echo "      --producer-spread-time <ms>          Time in milliseconds to spread producer clients during startup. (Default: 100)"
    echo "      --producer-spread-interval <ms>      Interval in milliseconds between each producer client startup. (Default: 5)"
    echo "      --monitoring-epoch-size <f>          Epoch size in milli seconds of calculating throughput, reliability, and more. (Default: 1000.0)"
    echo "  -v, --verbose                            Enable verbose output. (Config key: VERBOSE=1)"
    echo "  -h, --help                               Display this help message and exit."
    echo ""
    echo "Positional Arguments:"
    echo "  Any arguments not preceded by an option flag will be treated as positional arguments."
    echo "  Example: $(basename "$0") --config my.conf -f input.txt arg1 arg2"
    exit 0
fi

# --- Load configuration file ---
# If a config file is provided, source it. This will set the variables.
# The config file should contain KEY="VALUE" pairs, e.g., COUNT=10
if [ -n "$CONFIG_FILE" ]; then
    if [ -f "$CONFIG_FILE" ]; then
        echo "Loading configuration from '$CONFIG_FILE'"
        source "$CONFIG_FILE"
    else
        echo "Error: Config file not found at '$CONFIG_FILE'" >&2
        exit 1
    fi
fi

# --- Apply command-line arguments to override config/defaults ---
# If the temporary variable is not empty, it means it was set on the command line.
if [ -n "$CL_KAFKA_BROKER" ]; then
    KAFKA_BROKER="$CL_KAFKA_BROKER"
fi
if [ -n "$CL_MQTT_BROKER" ]; then
    MQTT_BROKER="$CL_MQTT_BROKER"
fi
if [ -n "$CL_KAFKA_BIN_PATH" ]; then
    KAFKA_BIN_PATH="$CL_KAFKA_BIN_PATH"
fi
if [ -n "$CL_COMMON_SCRIPT_ROOT" ]; then
    COMMON_SCRIPT_ROOT="$CL_COMMON_SCRIPT_ROOT"
fi
if [ -n "$CL_CONNECTOR_HOST" ]; then
    CONNECTOR_HOST="$CL_CONNECTOR_HOST"
fi
if [ -n "$CL_CONNECTOR_ROOT" ]; then
    CONNECTOR_ROOT="$CL_CONNECTOR_ROOT"
fi
if [ -n "$CL_CONNECTOR_OUT" ]; then
    CONNECTOR_OUT="$CL_CONNECTOR_OUT"
fi
if [ -n "$CL_CONNECTOR_TEMP" ]; then
    CONNECTOR_TEMP="$CL_CONNECTOR_TEMP"
fi
if [ -n "$CL_CONNECTOR_EXEC" ]; then
    CONNECTOR_EXEC="$CL_CONNECTOR_EXEC"
fi
if [ -n "$CL_LOAD_CONSUMER_HOST" ]; then
    LOAD_CONSUMER_HOST="$CL_LOAD_CONSUMER_HOST"
fi
if [ -n "$CL_LOAD_CONSUMER_ROOT" ]; then
    LOAD_CONSUMER_ROOT="$CL_LOAD_CONSUMER_ROOT"
fi
if [ -n "$CL_LOAD_CONSUMER_OUT" ]; then
    LOAD_CONSUMER_OUT="$CL_LOAD_CONSUMER_OUT"
fi
if [ -n "$CL_LOAD_CONSUMER_TEMP" ]; then
    LOAD_CONSUMER_TEMP="$CL_LOAD_CONSUMER_TEMP"
fi
if [ -n "$CL_LOAD_CONSUMER_EXEC" ]; then
    LOAD_CONSUMER_EXEC="$CL_LOAD_CONSUMER_EXEC"
fi
if [ -n "$CL_MEASURE_CONSUMER_HOST" ]; then
    MEASURE_CONSUMER_HOST="$CL_MEASURE_CONSUMER_HOST"
fi
if [ -n "$CL_MEASURE_CONSUMER_ROOT" ]; then
    MEASURE_CONSUMER_ROOT="$CL_MEASURE_CONSUMER_ROOT"
fi
if [ -n "$CL_MEASURE_CONSUMER_OUT" ]; then
    MEASURE_CONSUMER_OUT="$CL_MEASURE_CONSUMER_OUT"
fi
if [ -n "$CL_MEASURE_CONSUMER_TEMP" ]; then
    MEASURE_CONSUMER_TEMP="$CL_MEASURE_CONSUMER_TEMP"
fi
if [ -n "$CL_MEASURE_CONSUMER_EXEC" ]; then
    MEASURE_CONSUMER_EXEC="$CL_MEASURE_CONSUMER_EXEC"
fi
if [ -n "$CL_PRODUCER_HOST" ]; then
    PRODUCER_HOST="$CL_PRODUCER_HOST"
fi
if [ -n "$CL_PRODUCER_ROOT" ]; then
    PRODUCER_ROOT="$CL_PRODUCER_ROOT"
fi
if [ -n "$CL_PRODUCER_OUT" ]; then
    PRODUCER_OUT="$CL_PRODUCER_OUT"
fi
if [ -n "$CL_PRODUCER_TEMP" ]; then
    PRODUCER_TEMP="$CL_PRODUCER_TEMP"
fi
if [ -n "$CL_PRODUCER_EXEC" ]; then
    PRODUCER_EXEC="$CL_PRODUCER_EXEC"
fi
if [ -n "$CL_TERMINATE_TIMEOUT" ]; then
    TERMINATE_TIMEOUT="$CL_TERMINATE_TIMEOUT"
fi
if [ -n "$CL_DURATION" ]; then
    DURATION="$CL_DURATION"
fi
if [ ${#CL_NUM_CAR[@]} -gt 0 ]; then
    NUM_CAR=("${CL_NUM_CAR[@]}")
fi
if [ -n "$CL_INTERVAL_NOISE_RATE" ]; then
    INTERVAL_NOISE_RATE="$CL_INTERVAL_NOISE_RATE"
fi
if [ -n "$CL_PRODUCER_SPREAD_TIME" ]; then
    PRODUCER_SPREAD_TIME="$CL_PRODUCER_SPREAD_TIME"
fi
if [ -n "$CL_PRODUCER_SPREAD_INTERVAL" ]; then
    PRODUCER_SPREAD_INTERVAL="$CL_PRODUCER_SPREAD_INTERVAL"
fi
if [ -n "$CL_MONITORING_EPOCH_SIZE" ]; then
    MONITORING_EPOCH_SIZE="$CL_MONITORING_EPOCH_SIZE"
fi
if [ -n "$CL_VERBOSE" ]; then
    VERBOSE="$CL_VERBOSE"
fi

# --- post-setup-defaults ---
if [ -z "$CONNECTOR_OUT" ]; then
    CONNECTOR_OUT=${CONNECTOR_ROOT}/out
fi
if [ -z "$CONNECTOR_TEMP" ]; then
    CONNECTOR_TEMP=${CONNECTOR_ROOT}/temp
fi
if [ -z "$CONNECTOR_EXEC" ]; then
    CONNECTOR_EXEC=${CONNECTOR_ROOT}/bin/v2x_expr_mqtt_kafka_connector
fi
if [ -z "$LOAD_CONSUMER_OUT" ]; then
    LOAD_CONSUMER_OUT=${LOAD_CONSUMER_ROOT}/out
fi
if [ -z "$LOAD_CONSUMER_TEMP" ]; then
    LOAD_CONSUMER_TEMP=${LOAD_CONSUMER_ROOT}/temp
fi
if [ -z "$LOAD_CONSUMER_EXEC" ]; then
    LOAD_CONSUMER_EXEC=${LOAD_CONSUMER_ROOT}/bin/v2x_expr_consumer
fi
if [ -z "$MEASURE_CONSUMER_OUT" ]; then
    MEASURE_CONSUMER_OUT=${MEASURE_CONSUMER_ROOT}/out
fi
if [ -z "$MEASURE_CONSUMER_TEMP" ]; then
    MEASURE_CONSUMER_TEMP=${MEASURE_CONSUMER_ROOT}/temp
fi
if [ -z "$MEASURE_CONSUMER_EXEC" ]; then
    MEASURE_CONSUMER_EXEC=${MEASURE_CONSUMER_ROOT}/bin/v2x_expr_consumer
fi
if [ -z "$PRODUCER_OUT" ]; then
    PRODUCER_OUT=${PRODUCER_ROOT}/out
fi
if [ -z "$PRODUCER_TEMP" ]; then
    PRODUCER_TEMP=${PRODUCER_ROOT}/temp
fi
if [ -z "$PRODUCER_EXEC" ]; then
    PRODUCER_EXEC=${PRODUCER_ROOT}/bin/v2x_expr_mqtt_producer
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Kafka Broker:           $KAFKA_BROKER"
    echo "MQTT Broker:            $MQTT_BROKER"
    echo "Kafka Bin Path:         $KAFKA_BIN_PATH"
    echo "Common Script Root:     $COMMON_SCRIPT_ROOT"
    echo "Connector Host:         $CONNECTOR_HOST"
    echo "Connector Root:         $CONNECTOR_ROOT"
    echo "Connector Out:          $CONNECTOR_OUT"
    echo "Connector Temp:         $CONNECTOR_TEMP"
    echo "Connector Exec:         $CONNECTOR_EXEC"
    echo "Load Consumer Host:     $LOAD_CONSUMER_HOST"
    echo "Load Consumer Root:     $LOAD_CONSUMER_ROOT"
    echo "Load Consumer Out:      $LOAD_CONSUMER_OUT"
    echo "Load Consumer Temp:     $LOAD_CONSUMER_TEMP"
    echo "Load Consumer Exec:     $LOAD_CONSUMER_EXEC"
    echo "Measure Consumer Host:  $MEASURE_CONSUMER_HOST"
    echo "Measure Consumer Root:  $MEASURE_CONSUMER_ROOT"
    echo "Measure Consumer Out:   $MEASURE_CONSUMER_OUT"
    echo "Measure Consumer Temp:  $MEASURE_CONSUMER_TEMP"
    echo "Measure Consumer Exec:  $MEASURE_CONSUMER_EXEC"
    echo "Producer Host:          $PRODUCER_HOST"
    echo "Producer Root:          $PRODUCER_ROOT"
    echo "Producer Out:           $PRODUCER_OUT"
    echo "Producer Temp:          $PRODUCER_TEMP"
    echo "Producer Exec:          $PRODUCER_EXEC"
    echo "Terminate Timeout:      $TERMINATE_TIMEOUT"
    echo "Duration:               $DURATION"
    echo "Num Car:                $NUM_CAR"
    echo "Interval Noise Rate:    $INTERVAL_NOISE_RATE"
    echo "Producer Spread Start Time (ms): $PRODUCER_SPREAD_TIME"
    echo "Producer Spread Interval (ms):   $PRODUCER_SPREAD_INTERVAL"
    echo "Monitoring Epoch Size:  $MONITORING_EPOCH_SIZE"
    echo "Verbose Mode:           $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ $# -gt 0 ]; then
        echo "Positional Arguments: $@"
    fi
fi

# clean up functions
clean_up_connector() {
    IDENTIFIER=$1
    CONNECTOR_COMMAND="$CONNECTOR_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $CONNECTOR_TEMP --timeout $TERMINATE_TIMEOUT $VERBOSE_TAG"
    if [ -z "$CONNECTOR_HOST" ]; then
        eval $CONNECTOR_COMMAND
    else
        ssh $CONNECTOR_HOST $CONNECTOR_COMMAND
    fi
}

clean_up_measure_consumer() {
    IDENTIFIER=$1
    MEASURE_CONSUMER_COMMAND="$MEASURE_CONSUMER_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $MEASURE_CONSUMER_TEMP --timeout $TERMINATE_TIMEOUT $VERBOSE_TAG"
    if [ -z "$MEASURE_CONSUMER_HOST" ]; then
        eval $MEASURE_CONSUMER_COMMAND
    else
        ssh $MEASURE_CONSUMER_HOST $MEASURE_CONSUMER_COMMAND
    fi
}

clean_up_load_consumer() {
    IDENTIFIER=$1
    LOAD_CONSUMER_COMMAND="$LOAD_CONSUMER_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $LOAD_CONSUMER_TEMP --timeout $TERMINATE_TIMEOUT $VERBOSE_TAG"
    if [ -z "$LOAD_CONSUMER_HOST" ]; then
        eval $LOAD_CONSUMER_COMMAND
    else
        ssh $LOAD_CONSUMER_HOST $LOAD_CONSUMER_COMMAND
    fi
}

# trap handler
trap_handler() {
    echo "[TRAP] Ctrl+C detected! Cleaning up..."

    clean_up_load_consumer $LOAD_CONSUMER_ID
    clean_up_measure_consumer $MEASURE_CONSUMER_ID
    clean_up_connector $CONNECTOR_ID
    exit 1
}
trap trap_handler SIGINT

# script's main logic
echo "------------------------------------------------"
echo "🚀 Starting test script"
echo "------------------------------------------------"

SERVICE_CNT=6
INF_DURATION=$((60 * 60 * 10)) # 10 hours
VERBOSE_TAG=""
if [ $VERBOSE -eq 1 ]; then
    VERBOSE_TAG="--verbose"
fi

for CAR_NUM in "${NUM_CAR[@]}"; do
    CURRENT_CAR_NUM=$CAR_NUM

    echo "=================================================="
    echo "Starting experiment: NUM_CAR=$CURRENT_CAR_NUM"

    echo "--------------------------------------------------"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[1/9] Executing Connector ($TIMESTAMP)"
    CONNECTOR_ID="Connector_${CURRENT_CAR_NUM}"
    CONNECTOR_COMMAND="$CONNECTOR_ROOT/script/run-on-bg.sh --id $CONNECTOR_ID \
        --out-dir $CONNECTOR_OUT --temp-dir $CONNECTOR_TEMP $VERBOSE_TAG \
        --exec-path $CONNECTOR_EXEC -- \
            --kafka-broker $KAFKA_BROKER --mqtt-broker $MQTT_BROKER \
            --running-time $INF_DURATION"
    if [ -z "$CONNECTOR_HOST" ]; then
        eval $CONNECTOR_COMMAND
    else
        ssh $CONNECTOR_HOST $CONNECTOR_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[2/9] Deleting Consumer Groups ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "r_group_" --count $SERVICE_CNT $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[3/9] Executing Consumer for measurement ($TIMESTAMP)"
    MEASURE_CONSUMER_ID="MeasuerConsumer_${CURRENT_CAR_NUM}"
    MEASURE_CONSUMER_COMMAND="$MEASURE_CONSUMER_ROOT/script/run-on-bg.sh --id $MEASURE_CONSUMER_ID \
        --out-dir $MEASURE_CONSUMER_OUT --temp-dir $MEASURE_CONSUMER_TEMP $VERBOSE_TAG \
        --exec-path $MEASURE_CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group-prefix r_group_ \
            --client-cnt -1 --running-time $INF_DURATION \
            --outdir $MEASURE_CONSUMER_OUT --out-prefix \"${CURRENT_CAR_NUM}C_${TIMESTAMP}\" \
            --monitoring-epoch-size $MONITORING_EPOCH_SIZE $VERBOSE_TAG"
    if [ -z "$MEASURE_CONSUMER_HOST" ]; then
        eval $MEASURE_CONSUMER_COMMAND
    else
        ssh $MEASURE_CONSUMER_HOST $MEASURE_CONSUMER_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[4/9] Executing Consumer for load ($TIMESTAMP)"
    LOAD_CONSUMER_ID="LoadConsumer_${CURRENT_CAR_NUM}"
    LOAD_CONSUMER_COMMAND="$LOAD_CONSUMER_ROOT/script/run-on-bg.sh --id $LOAD_CONSUMER_ID \
        --out-dir $LOAD_CONSUMER_OUT --temp-dir $LOAD_CONSUMER_TEMP $VERBOSE_TAG \
        --exec-path $LOAD_CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group-prefix group_ \
            --client-cnt $CURRENT_CAR_NUM --running-time $INF_DURATION \
            --outdir $LOAD_CONSUMER_OUT --out-prefix \"${CURRENT_CAR_NUM}C_\" --no-log \
            --monitoring-epoch-size $MONITORING_EPOCH_SIZE $VERBOSE_TAG"
    if [ -z "$LOAD_CONSUMER_HOST" ]; then
        eval $LOAD_CONSUMER_COMMAND
    else
        ssh $LOAD_CONSUMER_HOST $LOAD_CONSUMER_COMMAND
    fi
    echo "--------------------------------------------------"

    GAURD_TIME=3
    echo "Waiting for ${GAURD_TIME} seconds before next operation..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[5/9] Checking Consumer Groups connection ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "r_group_" --count $SERVICE_CNT $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[6/9] Executing Producer ($TIMESTAMP)"
    PRODUCER_COMMAND="$PRODUCER_EXEC --broker $MQTT_BROKER --client-cnt $CURRENT_CAR_NUM \
        --running-time $DURATION --interval-noise-stddev-rate $INTERVAL_NOISE_RATE \
        --client-spread-time $PRODUCER_SPREAD_TIME \
        --client-spread-interval $PRODUCER_SPREAD_INTERVAL \
        $VERBOSE_TAG"
    if [ -z "$PRODUCER_HOST" ]; then
        eval $PRODUCER_COMMAND
    else
        ssh $PRODUCER_HOST $PRODUCER_COMMAND
    fi
    echo "--------------------------------------------------"

    GAURD_TIME=35
    echo "Waiting for ${GAURD_TIME} seconds before terminating consumers..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[7/9] Terminating Consumer + Connector ($TIMESTAMP)"
    clean_up_load_consumer $LOAD_CONSUMER_ID
    clean_up_measure_consumer $MEASURE_CONSUMER_ID
    clean_up_connector $CONNECTOR_ID
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[8/9] Deleting Consumer Groups ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "r_group_" --count $SERVICE_CNT $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[9/9] Printing results ($TIMESTAMP)"

    {
        if [ -z "$MEASURE_CONSUMER_HOST" ]; then
            cat $MEASURE_CONSUMER_OUT/$MEASURE_CONSUMER_ID.log
        else
            ssh $MEASURE_CONSUMER_HOST "cat $MEASURE_CONSUMER_OUT/$MEASURE_CONSUMER_ID.log"
        fi
    }
    echo "--------------------------------------------------"
    echo "=================================================="
done

echo "--------------------------------------------------"
echo "✅ All experiments completed!"
echo "--------------------------------------------------"
