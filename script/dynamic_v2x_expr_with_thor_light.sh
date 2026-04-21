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
LOAD_CONSUMER_POLL_TIMEOUT=100

MEASURE_CONSUMER_HOST=""
MEASURE_CONSUMER_ROOT="client"
MEASURE_CONSUMER_OUT=""
MEASURE_CONSUMER_TEMP=""
MEASURE_CONSUMER_EXEC=""
MEASURE_CONSUMER_POLL_TIMEOUT=0

PRODUCER_HOST=""
PRODUCER_ROOT="client"
PRODUCER_OUT=""
PRODUCER_TEMP=""
PRODUCER_EXEC=""

TERMINATE_TIMEOUT=60

START_GUARD_TIME=3
END_GUARD_TIME=35

STEP_CARS=(10 20 30 40 50 60 70 80 90 100 110 120)
STEP_INTERVAL=20
FINAL_HOLD=20

PRODUCER_WAKEUP_INTERVAL=5
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
    load-consumer-exec:, load-consumer-poll-timeout:, measure-consumer-host:, measure-consumer-root:, \
    measure-consumer-out:, measure-consumer-temp:, measure-consumer-exec:, measure-consumer-poll-timeout:, \
    producer-host:, producer-root:, producer-out:, \
    producer-temp:, producer-exec:, step-cars:, step-interval:, final-hold:, \
    producer-wakeup-interval:, producer-spread-time:, producer-spread-interval:, \
    monitoring-epoch-size:, terminate-timeout:, start-guard-time:, end-guard-time:" \
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
CL_LOAD_CONSUMER_POLL_TIMEOUT=""
CL_MEASURE_CONSUMER_HOST=""
CL_MEASURE_CONSUMER_ROOT=""
CL_MEASURE_CONSUMER_OUT=""
CL_MEASURE_CONSUMER_TEMP=""
CL_MEASURE_CONSUMER_EXEC=""
CL_MEASURE_CONSUMER_POLL_TIMEOUT=""
CL_PRODUCER_HOST=""
CL_PRODUCER_ROOT=""
CL_PRODUCER_OUT=""
CL_PRODUCER_TEMP=""
CL_PRODUCER_EXEC=""
CL_TERMINATE_TIMEOUT=""
CL_STEP_CARS=()
CL_STEP_INTERVAL=""
CL_FINAL_HOLD=""
CL_PRODUCER_WAKEUP_INTERVAL=""
CL_PRODUCER_SPREAD_TIME=""
CL_PRODUCER_SPREAD_INTERVAL=""
CL_MONITORING_EPOCH_SIZE=""
CL_START_GUARD_TIME=""
CL_END_GUARD_TIME=""
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
        --load-consumer-poll-timeout) CL_LOAD_CONSUMER_POLL_TIMEOUT="$2" ; shift 2 ;;
        --measure-consumer-host) CL_MEASURE_CONSUMER_HOST="$2" ; shift 2 ;;
        --measure-consumer-root) CL_MEASURE_CONSUMER_ROOT="$2" ; shift 2 ;;
        --measure-consumer-out) CL_MEASURE_CONSUMER_OUT="$2" ; shift 2 ;;
        --measure-consumer-temp) CL_MEASURE_CONSUMER_TEMP="$2" ; shift 2 ;;
        --measure-consumer-exec) CL_MEASURE_CONSUMER_EXEC="$2" ; shift 2 ;;
        --measure-consumer-poll-timeout) CL_MEASURE_CONSUMER_POLL_TIMEOUT="$2" ; shift 2 ;;
        --producer-host) CL_PRODUCER_HOST="$2" ; shift 2 ;;
        --producer-root) CL_PRODUCER_ROOT="$2" ; shift 2 ;;
        --producer-out) CL_PRODUCER_OUT="$2" ; shift 2 ;;
        --producer-temp) CL_PRODUCER_TEMP="$2" ; shift 2 ;;
        --producer-exec) CL_PRODUCER_EXEC="$2" ; shift 2 ;;
        --terminate-timeout) CL_TERMINATE_TIMEOUT="$2" ; shift 2 ;;
        --step-cars) IFS=',' read -r -a CL_STEP_CARS <<< "$2" ; shift 2 ;;
        --step-interval) CL_STEP_INTERVAL="$2" ; shift 2 ;;
        --final-hold) CL_FINAL_HOLD="$2" ; shift 2 ;;
        --producer-wakeup-interval) CL_PRODUCER_WAKEUP_INTERVAL="$2" ; shift 2 ;;
        --producer-spread-time) CL_PRODUCER_SPREAD_TIME="$2" ; shift 2 ;;
        --producer-spread-interval) CL_PRODUCER_SPREAD_INTERVAL="$2" ; shift 2 ;;
        --monitoring-epoch-size) CL_MONITORING_EPOCH_SIZE="$2" ; shift 2 ;;
        --start-guard-time) CL_START_GUARD_TIME="$2" ; shift 2 ;;
        --end-guard-time) CL_END_GUARD_TIME="$2" ; shift 2 ;;
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
    echo "This script orchestrates a dynamic V2X performance experiment with Thor (Lightweight)."
    echo "It simulates time-varying traffic load by scaling producers and consumers up and down at specified intervals."
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
    echo "      --load-consumer-poll-timeout <ms>    Poll timeout in milliseconds for Load Consumer. (Default: 100)"
    echo "      --measure-consumer-host <user@host>  Remote host for Measurement Consumers execution. (Default: empty string for local)"
    echo "      --measure-consumer-root <path>       Root directory of Measurement Consumers (Default: client)"
    echo "      --measure-consumer-out <path>        Output root directory for Measurement Consumer logs. (Default: {measure-consumer-root}/out)"
    echo "      --measure-consumer-temp <path>       Temporary root directory for Measurement Consumer files. (Default: {measure-consumer-root}/temp)"
    echo "      --measure-consumer-exec <name>       Executable name for Measurement Consumer. (Default: {measure-consumer-root}/bin/v2x_expr_consumer)"
    echo "      --measure-consumer-poll-timeout <ms> Poll timeout in milliseconds for Measure Consumer. (Default: 0)"
    echo "      --producer-host <user@host>          Remote host for Producer execution. (Default: empty string for local)"
    echo "      --producer-root <path>               Root directory of Producer (Default: client)"
    echo "      --producer-out <path>                Output root directory for Producer logs. (Default: {producer-root}/out)"
    echo "      --producer-temp <path>               Temporary root directory for Producer files. (Default: {producer-root}/temp)"
    echo "      --producer-exec <name>               Executable name for Producer. (Default: {producer-root}/bin/v2x_expr_with_thor_mqtt_producer_light)"
    echo "      --terminate-timeout <seconds>        Timeout second to wait before force killing (Default: 60)."
    echo "      --step-cars <num1,num2,...>          Comma-separated list of total car counts for each scaling step. (Default: (10,20,30,40,50,60,70,80,90,100,110,120))"
    echo "      --step-interval <seconds>            Interval in seconds between each scaling step. (Default: 20)"
    echo "      --final-hold <seconds>               Hold time in seconds after reaching final scale before termination. (Default: 20)"
    echo "      --producer-wakeup-interval <ms>      Interval in milliseconds for producer wakeup to check whether produce or not. (Default: 5)"
    echo "      --producer-spread-time <ms>          Time in milliseconds to spread producer clients during startup. (Default: 100)"
    echo "      --producer-spread-interval <ms>      Interval in milliseconds between each producer client startup. (Default: 5)"
    echo "      --monitoring-epoch-size <f>          Epoch size in milli seconds of calculating throughput, reliability, and more. (Default: 1000.0)"
    echo "      --start-guard-time <seconds>         Guard time in seconds before starting producer. (Default: 3)"
    echo "      --end-guard-time <seconds>           Guard time in seconds before terminating consumers. (Default: 35)"
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
if [ -n "$CL_LOAD_CONSUMER_POLL_TIMEOUT" ]; then
    LOAD_CONSUMER_POLL_TIMEOUT="$CL_LOAD_CONSUMER_POLL_TIMEOUT"
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
if [ -n "$CL_MEASURE_CONSUMER_POLL_TIMEOUT" ]; then
    MEASURE_CONSUMER_POLL_TIMEOUT="$CL_MEASURE_CONSUMER_POLL_TIMEOUT"
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
if [ ${#CL_STEP_CARS[@]} -gt 0 ]; then
    STEP_CARS=("${CL_STEP_CARS[@]}")
fi
if [ -n "$CL_STEP_INTERVAL" ]; then
    STEP_INTERVAL="$CL_STEP_INTERVAL"
fi
if [ -n "$CL_FINAL_HOLD" ]; then
    FINAL_HOLD="$CL_FINAL_HOLD"
fi
if [ -n "$CL_PRODUCER_WAKEUP_INTERVAL" ]; then
    PRODUCER_WAKEUP_INTERVAL="$CL_PRODUCER_WAKEUP_INTERVAL"
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
if [ -n "$CL_START_GUARD_TIME" ]; then
    START_GUARD_TIME="$CL_START_GUARD_TIME"
fi
if [ -n "$CL_END_GUARD_TIME" ]; then
    END_GUARD_TIME="$CL_END_GUARD_TIME"
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
    PRODUCER_EXEC=${PRODUCER_ROOT}/bin/v2x_expr_with_thor_mqtt_producer_light
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
    echo "Load Consumer Poll Timeout: $LOAD_CONSUMER_POLL_TIMEOUT"
    echo "Measure Consumer Host:  $MEASURE_CONSUMER_HOST"
    echo "Measure Consumer Root:  $MEASURE_CONSUMER_ROOT"
    echo "Measure Consumer Out:   $MEASURE_CONSUMER_OUT"
    echo "Measure Consumer Temp:  $MEASURE_CONSUMER_TEMP"
    echo "Measure Consumer Exec:  $MEASURE_CONSUMER_EXEC"
    echo "Measure Consumer Poll Timeout: $MEASURE_CONSUMER_POLL_TIMEOUT"
    echo "Producer Host:          $PRODUCER_HOST"
    echo "Producer Root:          $PRODUCER_ROOT"
    echo "Producer Out:           $PRODUCER_OUT"
    echo "Producer Temp:          $PRODUCER_TEMP"
    echo "Producer Exec:          $PRODUCER_EXEC"
    echo "Terminate Timeout:      $TERMINATE_TIMEOUT"
    echo "Step Cars:              $STEP_CARS"
    echo "Step Interval:          $STEP_INTERVAL"
    echo "Final Hold:             $FINAL_HOLD"
    echo "Producer Wakeup Interval (ms):   $PRODUCER_WAKEUP_INTERVAL"
    echo "Producer Spread Start Time (ms): $PRODUCER_SPREAD_TIME"
    echo "Producer Spread Interval (ms):   $PRODUCER_SPREAD_INTERVAL"
    echo "Monitoring Epoch Size:  $MONITORING_EPOCH_SIZE"
    echo "Start Guard Time:       $START_GUARD_TIME"
    echo "End Guard Time:         $END_GUARD_TIME"
    echo "Verbose Mode:           $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ $# -gt 0 ]; then
        echo "Positional Arguments: $@"
    fi
fi

INF_DURATION=$((60 * 60 * 10)) # 10 hours
VERBOSE_TAG=""
if [ $VERBOSE -eq 1 ]; then
    VERBOSE_TAG="--verbose"
fi

start_load_consumers() {
    local from=$1
    local to=$2

    local load_consumer_id="LoadConsumer_${from}_${to}"
    local load_consumer_cmd="$LOAD_CONSUMER_ROOT/script/run-on-bg.sh --id $load_consumer_id \
        --out-dir $LOAD_CONSUMER_OUT --temp-dir $LOAD_CONSUMER_TEMP $VERBOSE_TAG \
        --exec-path $LOAD_CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group-prefix group_ \
            --client-cnt $((to-from)) --start-idx $from --running-time $INF_DURATION \
            --poll-timeout $LOAD_CONSUMER_POLL_TIMEOUT \
            --outdir $LOAD_CONSUMER_OUT --out-prefix \"${from}_${to}C_\" --no-log \
            --monitoring-epoch-size $MONITORING_EPOCH_SIZE \
            --start-barrier-delay 1 $VERBOSE_TAG"
    if [ -z "$LOAD_CONSUMER_HOST" ]; then
        eval $load_consumer_cmd
    else
        ssh $LOAD_CONSUMER_HOST $load_consumer_cmd
    fi
    LOAD_CONSUMER_IDS+=("$load_consumer_id")
}

start_producers() {
    local from=$1
    local to=$2

    local producer_id="Producer_${from}_${to}"
    local producer_cmd="$PRODUCER_ROOT/script/run-on-bg.sh --id $producer_id \
        --out-dir $PRODUCER_OUT --temp-dir $PRODUCER_TEMP $VERBOSE_TAG \
        --exec-path $PRODUCER_EXEC -- \
            --broker $MQTT_BROKER --client-cnt $((to-from)) --start-idx $from \
            --running-time $INF_DURATION --start-barrier-delay 1 \
            --wakeup-interval $PRODUCER_WAKEUP_INTERVAL \
            --client-spread-time $PRODUCER_SPREAD_TIME \
            --client-spread-interval $PRODUCER_SPREAD_INTERVAL \
            --no-log $VERBOSE_TAG"
    if [ -z "$PRODUCER_HOST" ]; then
        eval $producer_cmd
    else
        ssh $PRODUCER_HOST $producer_cmd
    fi
    PRODUCER_IDS+=("$producer_id")
}

# clean up functions
clean_up_connector() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$CONNECTOR_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $CONNECTOR_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$CONNECTOR_HOST" ] && [ -n "$is_async" ]; then
        ssh "$CONNECTOR_HOST" "$cmd" $args &
    elif [ -n "$CONNECTOR_HOST" ]; then
        ssh "$CONNECTOR_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

clean_up_producer() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$PRODUCER_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $PRODUCER_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$PRODUCER_HOST" ] && [ -n "$is_async" ]; then
        ssh "$PRODUCER_HOST" "$cmd" $args &
    elif [ -n "$PRODUCER_HOST" ]; then
        ssh "$PRODUCER_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

clean_up_measure_consumer() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$MEASURE_CONSUMER_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $MEASURE_CONSUMER_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$MEASURE_CONSUMER_HOST" ] && [ -n "$is_async" ]; then
        ssh "$MEASURE_CONSUMER_HOST" "$cmd" $args &
    elif [ -n "$MEASURE_CONSUMER_HOST" ]; then
        ssh "$MEASURE_CONSUMER_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

clean_up_load_consumer() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$LOAD_CONSUMER_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $LOAD_CONSUMER_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$LOAD_CONSUMER_HOST" ] && [ -n "$is_async" ]; then
        ssh "$LOAD_CONSUMER_HOST" "$cmd" $args &
    elif [ -n "$LOAD_CONSUMER_HOST" ]; then
        ssh "$LOAD_CONSUMER_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

# trap handler
trap_handler() {
    echo "[TRAP] Ctrl+C detected! Cleaning up..."

    for PRODUCER_ID in "${PRODUCER_IDS[@]}"; do
        clean_up_producer $PRODUCER_ID $TERMINATE_TIMEOUT
    done
    for LOAD_CONSUMER_ID in "${LOAD_CONSUMER_IDS[@]}"; do
        clean_up_load_consumer $LOAD_CONSUMER_ID $TERMINATE_TIMEOUT
    done
    clean_up_measure_consumer $MEASURE_CONSUMER_ID $TERMINATE_TIMEOUT
    clean_up_connector $CONNECTOR_ID $TERMINATE_TIMEOUT
    exit 1
}
trap trap_handler SIGINT

# script's main logic
echo "------------------------------------------------"
echo "🚀 Starting test script"
echo "------------------------------------------------"

LOAD_CONSUMER_IDS=()
PRODUCER_IDS=()

NUM_STEPS=${#STEP_CARS[@]}
MAX_CAR_CNT=${STEP_CARS[$((NUM_STEPS-1))]}
SERVICE_CNT=6

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[1/9] Executing Connector ($TIMESTAMP)"
CONNECTOR_ID="Connector_Dynamic"
CONNECTOR_COMMAND="$CONNECTOR_ROOT/script/run-on-bg.sh --id $CONNECTOR_ID \
    --out-dir $CONNECTOR_OUT --temp-dir $CONNECTOR_TEMP $VERBOSE_TAG\
    --exec-path $CONNECTOR_EXEC $MAX_CAR_CNT $INF_DURATION 8"
if [ -z "$CONNECTOR_HOST" ]; then
    eval $CONNECTOR_COMMAND
else
    ssh $CONNECTOR_HOST $CONNECTOR_COMMAND
fi
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[2/9] Deleting Consumer Groups ($TIMESTAMP)"
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "group_" --start-idx 1 --count $MAX_CAR_CNT $VERBOSE_TAG
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "r_group_" --start-idx 1 --count $SERVICE_CNT $VERBOSE_TAG
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[3/9] Executing Consumer for measurement ($TIMESTAMP)"
MEASURE_CONSUMER_ID="MeasureConsumer_Dynamic"
MEASURE_CONSUMER_COMMAND="$MEASURE_CONSUMER_ROOT/script/run-on-bg.sh --id $MEASURE_CONSUMER_ID \
    --out-dir $MEASURE_CONSUMER_OUT --temp-dir $MEASURE_CONSUMER_TEMP $VERBOSE_TAG \
    --exec-path $MEASURE_CONSUMER_EXEC -- \
        --broker $KAFKA_BROKER --group-prefix r_group_ \
        --client-cnt -1 --start-idx 1 --running-time $INF_DURATION \
        --poll-timeout $MEASURE_CONSUMER_POLL_TIMEOUT \
        --outdir $MEASURE_CONSUMER_OUT --out-prefix \"Dynamic_${TIMESTAMP}\" \
        --monitoring-epoch-size $MONITORING_EPOCH_SIZE $VERBOSE_TAG"
if [ -z "$MEASURE_CONSUMER_HOST" ]; then
    eval $MEASURE_CONSUMER_COMMAND
else
    ssh $MEASURE_CONSUMER_HOST $MEASURE_CONSUMER_COMMAND
fi
echo "--------------------------------------------------"

GUARD_TIME=$START_GUARD_TIME
echo "Waiting for ${GUARD_TIME} seconds before execute producers..."
sleep $GUARD_TIME
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[4/9] Starting scale up steps ($TIMESTAMP)"
PREV_CAR_CNT=1
for (( step=0; step<$NUM_STEPS; step++ )); do
    CUR_CAR_NUM=$((STEP_CARS[$step] + 1))
    C_START_IDX=$((PREV_CAR_CNT))
    if [ $step -eq 0 ]; then
        C_START_IDX=$((SERVICE_CNT + 1))
    else
        sleep $STEP_INTERVAL
    fi
    echo "  - Increasing car count to ${CUR_CAR_NUM}"
    start_load_consumers $C_START_IDX $CUR_CAR_NUM
    start_producers $PREV_CAR_CNT $CUR_CAR_NUM
    PREV_CAR_CNT=$CUR_CAR_NUM
done
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[5/9] Starting final hold time ($TIMESTAMP): ${FINAL_HOLD}s"
sleep $((FINAL_HOLD + 1))
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[6/9] Starting sequential termination ($TIMESTAMP)"
for (( step=0; step<$NUM_STEPS; step++ )); do
    CUR_CAR_NUM=${STEP_CARS[$step]}
    CUR_REMAIN_CAR_NUM=$((MAX_CAR_CNT - CUR_CAR_NUM))
    echo "  - Decreasing car count to ${CUR_REMAIN_CAR_NUM}"
    clean_up_producer "${PRODUCER_IDS[$step]}" $TERMINATE_TIMEOUT "async"
    clean_up_load_consumer "${LOAD_CONSUMER_IDS[$step]}" $TERMINATE_TIMEOUT "async"
    sleep $STEP_INTERVAL
done
echo "--------------------------------------------------"

GUARD_TIME=$END_GUARD_TIME
echo "Waiting for ${GUARD_TIME} seconds before terminating consumers..."
sleep $GUARD_TIME
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[7/9] Terminating Connector and measurement Consumer ($TIMESTAMP)"
clean_up_connector $CONNECTOR_ID $TERMINATE_TIMEOUT
clean_up_measure_consumer $MEASURE_CONSUMER_ID $TERMINATE_TIMEOUT
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[8/9] Deleting Consumer Groups ($TIMESTAMP)"
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "group_" --start-idx 1 --count $MAX_CAR_CNT $VERBOSE_TAG
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --kafka-bin-path $KAFKA_BIN_PATH --broker $KAFKA_BROKER --prefix "r_group_" --start-idx 1 --count $SERVICE_CNT $VERBOSE_TAG
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

echo "--------------------------------------------------"
echo "✅ All experiments completed!"
echo "--------------------------------------------------"
