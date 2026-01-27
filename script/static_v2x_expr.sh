#!/bin/bash

# Precedence order:
# 1. Command-line arguments (highest)
# 2. Configuration file values
# 3. Default values in this script (lowest)

# --- Default values for options ---
KAFKA_BROKER="127.0.0.1:9092"
MQTT_BROKER="127.0.0.1:1883"

COMMON_SCRIPT_ROOT="."

REMOTE_USER="user"
REMOTE_IP="127.0.0.1"

CONNECTOR_HOST=""
CONNECTOR_ROOT="./connector"
CONNECTOR_OUT=""
CONNECTOR_TEMP=""

CONNECTOR_EXEC=""

R_CLIENT_HOST=""
R_CLIENT_ROOT="client"
R_CLIENT_OUT=""
R_CLIENT_TEMP=""

R_CONSUMER_EXEC=""

CLIENT_ROOT="./client"
CLIENT_OUT=""
CLIENT_TEMP=""

CONSUMER_EXEC=""
PRODUCER_EXEC=""

TERMINATE_TIMEOUT=60

DURATION=100

NUM_CAR=(10)

INTERVAL_NOISE_RATE=0.0

VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o d:vh --longoptions \
    "config:, verbose, help, kafka-broker:, mqtt-broker:, common-script-root:, \
    connector-host:, connector-root:, connector-out:, connector-temp:, connector-exec:, \
    r-client-host:, r-client-root:, r-client-out:, r-client-temp:, r-consumer-exec:, \
    client-root:, client-out:, client-temp:, consumer-exec:, producer-exec:, duration:, \
    num-car:, interval-noise-rate:, terminate-timeout:" \
    -n 'myscript' -- "$@" \
)

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_KAFKA_BROKER=""
CL_MQTT_BROKER=""
CL_COMMON_SCRIPT_ROOT=""
CL_CONNECTOR_HOST=""
CL_CONNECTOR_ROOT=""
CL_CONNECTOR_OUT=""
CL_CONNECTOR_TEMP=""
CL_CONNECTOR_EXEC=""
CL_R_CLIENT_HOST=""
CL_R_CLIENT_ROOT=""
CL_R_CLIENT_OUT=""
CL_R_CLIENT_TEMP=""
CL_R_CONSUMER_EXEC=""
CL_CLIENT_ROOT=""
CL_CLIENT_OUT=""
CL_CLIENT_TEMP=""
CL_CONSUMER_EXEC=""
CL_PRODUCER_EXEC=""
CL_TERMINATE_TIMEOUT=""
CL_DURATION=""
CL_NUM_CAR=()
CL_INTERVAL_NOISE_RATE=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --kafka-broker) CL_KAFKA_BROKER="$2" ; shift 2;;
        --mqtt-broker) CL_MQTT_BROKER="$2" ; shift 2 ;;
        --common-script-root) CL_COMMON_SCRIPT_ROOT="$2" ; shift 2 ;;
        --connector-host) CL_CONNECTOR_HOST="$2" ; shift 2 ;;
        --connector-root) CL_CONNECTOR_ROOT="$2" ; shift 2 ;;
        --connector-out) CL_CONNECTOR_OUT="$2" ; shift 2 ;;
        --connector-temp) CL_CONNECTOR_TEMP="$2" ; shift 2 ;;
        --connector-exec) CL_CONNECTOR_EXEC="$2" ; shift 2 ;;
        --r-client-host) CL_R_CLIENT_HOST="$2" ; shift 2 ;;
        --r-client-root) CL_R_CLIENT_ROOT="$2" ; shift 2 ;;
        --r-client-out) CL_R_CLIENT_OUT="$2" ; shift 2 ;;
        --r-client-temp) CL_R_CLIENT_TEMP="$2" ; shift 2 ;;
        --r-consumer-exec) CL_R_CONSUMER_EXEC="$2" ; shift 2 ;;
        --client-root) CL_CLIENT_ROOT="$2" ; shift 2 ;;
        --client-out) CL_CLIENT_OUT="$2" ; shift 2 ;;
        --client-temp) CL_CLIENT_TEMP="$2" ; shift 2 ;;
        --consumer-exec) CL_CONSUMER_EXEC="$2" ; shift 2 ;;
        --producer-exec) CL_PRODUCER_EXEC="$2" ; shift 2 ;;
        --terminate-timeout) CL_TERMINATE_TIMEOUT="$2" ; shift 2 ;;
        -d|--duration) CL_DURATION="$2" ; shift 2 ;;
        --num-car) IFS=',' read -r -a CL_NUM_CAR <<< "$2" ; shift 2 ;;
        --interval-noise-rate) CL_INTERVAL_NOISE_RATE="$2" ; shift 2 ;;
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
    echo "TODO: "
    echo ""
    echo "Options:"
    echo "      --config <path>               Path to a configuration file. (e.g., key=\"value\" pairs)"
    echo "      --broker <host:port>          Kafka broker address. (Default: 127.0.0.1:9092)"
    echo "      --mqtt-broker <host:port>     MQTT broker address. (Default: 127.0.0.1:1883)"
    echo "      --common-script-root <path>   Root directory where common script are located. (Default: .)"
    echo "      --connector-host <user@host>  Remote host for Connector execution. (Default: empty string for local)"
    echo "      --connector-root <path>       Root directory on remote host where Connector is located. (Default: ./connector)"
    echo "      --connector-out <path>        Output root directory for Connector logs. (Default: {connector-root}/out)"
    echo "      --connector-temp <path>       Temporary root directory for Connector files. (Default: {connector-root}/temp)"
    echo "      --connector-exec <path>       Executable path for Connector. (Default: {connector-root}/bin/v2x_expr_mqtt_kafka_connector)"
    echo "      --r-client-host <user@host>   Remote host for Remote Clients execution. (Default: empty string for local)"
    echo "      --r-client-root <path>        Root directory on remote host where Remote Client is located. (Default: client)"
    echo "      --r-client-out <path>         Output root directory for Remote Client logs. (Default: {r-client-root}/out)"
    echo "      --r-client-temp <path>        Temporary root directory for Remote Client files. (Default: {r-client-root}/temp)"
    echo "      --r-consumer-exec <name>      Executable name for Remote Consumer. (Default: {r-client-root}/bin/v2x_expr_consumer)"
    echo "      --client-root <path>          Root directory where Client is located. (Default: ./client)"
    echo "      --client-out <path>           Output root directory for Client logs. (Default: {client-root}/out)"
    echo "      --client-temp <path>          Temporary root directory for Client files. (Default: {client-root}/temp)"
    echo "      --consumer-exec <path>        Executable path for Local Consumer. (Default: {client-root}/bin/v2x_expr_consumer)"
    echo "      --producer-exec <path>        Executable path for Producer. (Default: {client-root}/bin/v2x_expr_mqtt_producer)"
    echo "      --terminate-timeout <seconds> Timeout second to wait before force killing (Default: 60)."
    echo "  -d, --duration <seconds>          Duration for the test run. (Default: 100)"
    echo "      --num-car <num1,num2,...>     Comma-separated list of car counts for the test. (Default: (10))"
    echo "      --interval-noise-rate <f>     Standard deviation of noise to add to produce interval (Default: 0.0)"
    echo "  -v, --verbose                     Enable verbose output. (Config key: VERBOSE=1)"
    echo "  -h, --help                        Display this help message and exit."
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
if [ -n "$CL_R_CLIENT_HOST" ]; then
    R_CLIENT_HOST="$CL_R_CLIENT_HOST"
fi
if [ -n "$CL_R_CLIENT_ROOT" ]; then
    R_CLIENT_ROOT="$CL_R_CLIENT_ROOT"
fi
if [ -n "$CL_R_CLIENT_OUT" ]; then
    R_CLIENT_OUT="$CL_R_CLIENT_OUT"
fi
if [ -n "$CL_R_CLIENT_TEMP" ]; then
    R_CLIENT_TEMP="$CL_R_CLIENT_TEMP"
fi
if [ -n "$CL_R_CONSUMER_EXEC" ]; then
    R_CONSUMER_EXEC="$CL_R_CONSUMER_EXEC"
fi
if [ -n "$CL_CLIENT_ROOT" ]; then
    CLIENT_ROOT="$CL_CLIENT_ROOT"
fi
if [ -n "$CL_CLIENT_OUT" ]; then
    CLIENT_OUT="$CL_CLIENT_OUT"
fi
if [ -n "$CL_CLIENT_TEMP" ]; then
    CLIENT_TEMP="$CL_CLIENT_TEMP"
fi
if [ -n "$CL_CONSUMER_EXEC" ]; then
    CONSUMER_EXEC="$CL_CONSUMER_EXEC"
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
if [ -z "$R_CLIENT_OUT" ]; then
    R_CLIENT_OUT=${R_CLIENT_ROOT}/out
fi
if [ -z "$R_CLIENT_TEMP" ]; then
    R_CLIENT_TEMP=${R_CLIENT_ROOT}/temp
fi
if [ -z "$R_CONSUMER_EXEC" ]; then
    R_CONSUMER_EXEC=${R_CLIENT_ROOT}/bin/v2x_expr_consumer
fi
if [ -z "$CLIENT_OUT" ]; then
    CLIENT_OUT=${CLIENT_ROOT}/out
fi
if [ -z "$CLIENT_TEMP" ]; then
    CLIENT_TEMP=${CLIENT_ROOT}/temp
fi
if [ -z "$CONSUMER_EXEC" ]; then
    CONSUMER_EXEC=${CLIENT_ROOT}/bin/v2x_expr_consumer
fi
if [ -z "$PRODUCER_EXEC" ]; then
    PRODUCER_EXEC=${CLIENT_ROOT}/bin/v2x_expr_mqtt_producer
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Kafka Broker:           $KAFKA_BROKER"
    echo "MQTT Broker:            $MQTT_BROKER"
    echo "Common Script Root:     $COMMON_SCRIPT_ROOT"
    echo "Connector Host:         $CONNECTOR_HOST"
    echo "Connector Root:         $CONNECTOR_ROOT"
    echo "Connector Out:          $CONNECTOR_OUT"
    echo "Connector Temp:         $CONNECTOR_TEMP"
    echo "Connector Exec:         $CONNECTOR_EXEC"
    echo "Remote Client Host:     $R_CLIENT_HOST"
    echo "Remote Client Root:     $R_CLIENT_ROOT"
    echo "Remote Client Out:      $R_CLIENT_OUT"
    echo "Remote Client Temp:     $R_CLIENT_TEMP"
    echo "Remote Consumer Exec:   $R_CONSUMER_EXEC"
    echo "Client Root:            $CLIENT_ROOT"
    echo "Client Out:             $CLIENT_OUT"
    echo "Client Temp:            $CLIENT_TEMP"
    echo "Consumer Exec:          $CONSUMER_EXEC"
    echo "Producer Exec:          $PRODUCER_EXEC"
    echo "Terminate Timeout:      $TERMINATE_TIMEOUT"
    echo "Duration:               $DURATION"
    echo "Num Car:                $NUM_CAR"
    echo "Interval Noise Rate:    $INTERVAL_NOISE_RATE"
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
    CONNECTOR_COMMAND="$CONNECTOR_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $CONNECTOR_TEMP --timeout $TERMINATE_TIMEOUT VERBOSE_TAG"
    if [ $CONNECTOR_HOST == "" ]; then
        $CONNECTOR_COMMAND
    else
        ssh $CONNECTOR_HOST $CONNECTOR_COMMAND
    fi
}

clean_up_consumer() {
    IDENTIFIER=$1
    $CLIENT_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $CLIENT_TEMP --timeout $TERMINATE_TIMEOUT VERBOSE_TAG
}

clean_up_r_consumer() {
    IDENTIFIER=$1
    R_CONSUMER_COMMAND="$R_CLIENT_ROOT/script/terminate-on-bg.sh --id $IDENTIFIER --temp-dir $R_CLIENT_TEMP --timeout $TERMINATE_TIMEOUT VERBOSE_TAG"
    if [ $R_CLIENT_HOST == "" ]; then
        $R_CONSUMER_COMMAND
    else
        ssh $R_CLIENT_HOST $R_CONSUMER_COMMAND
    fi
}

# trap handler
trap_handler() {
    echo "[TRAP] Ctrl+C detected! Cleaning up..."

    clean_up_consumer $CONSUMER_ID
    clean_up_r_consumer $R_CONSUMER_ID
    clean_up_connector $CONNECTOR_ID
    exit 1
}
trap trap_handler SIGINT

# script's main logic
echo "------------------------------------------------"
echo "🚀 Starting test script"
echo "------------------------------------------------"

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
    echo "[1/7] Executing Connector ($TIMESTAMP)"
    CONNECTOR_ID="Connector_${CURRENT_CAR_NUM}"
    CONNECTOR_COMMAND="$CONNECTOR_ROOT/script/run-on-bg.sh --id $CONNECTOR_ID \
        --out-dir $CONNECTOR_OUT --temp-dir $CONNECTOR_TEMP $VERBOSE_TAG\
        --exec-path $CONNECTOR_EXEC -- \
            --kafka-broker $KAFKA_BROKER --mqtt-broker $MQTT_BROKER \
            --running-time $INF_DURATION"
    if [ $CONNECTOR_HOST == "" ]; then
        $CONNECTOR_COMMAND
    else
        ssh $CONNECTOR_HOST $CONNECTOR_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[2/7] Deleting Consumer Groups ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "r_group_" --count 4 $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[3/7] Executing Consumer (Remote) for measurement ($TIMESTAMP)"
    R_CONSUMER_ID="RemoteConsumer_${CURRENT_CAR_NUM}"
    R_CONSUMER_COMMAND="$R_CLIENT_ROOT/script/run-on-bg.sh --id $R_CONSUMER_ID \
        --out-dir $R_CLIENT_OUT --temp-dir $R_CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $R_CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group-prefix 'r_group_' \
            --client-cnt -1 --running-time $INF_DURATION \
            --outdir $R_CLIENT_OUT --out-prefix '${CURRENT_CAR_NUM}C_' $VERBOSE_TAG"
    if [ $R_CLIENT_HOST == "" ]; then
        $R_CONSUMER_COMMAND
    else
        ssh $R_CLIENT_HOST $R_CONSUMER_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[4/7] Executing Consumer (Local) for load ($TIMESTAMP)"
    CONSUMER_ID="Consumer_${CURRENT_CAR_NUM}"
    $CLIENT_ROOT/script/run-on-bg.sh --id $CONSUMER_ID \
        --out-dir $CLIENT_OUT --temp-dir $CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group-prefix "group_" \
            --client-cnt $CURRENT_CAR_NUM --running-time $INF_DURATION \
            --outdir $CLIENT_OUT --out-prefix "${CURRENT_CAR_NUM}C_" --no-log $VERBOSE_TAG
    echo "--------------------------------------------------"

    GAURD_TIME=3
    echo "Waiting for ${GAURD_TIME} seconds before next operation..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[5/7] Checking Consumer Groups connection ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "r_group_" --count 4 $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[6/7] Executing Producer ($TIMESTAMP)"
    $PRODUCER_EXEC --broker $MQTT_BROKER --client-cnt $CURRENT_CAR_NUM \
        --running-time $DURATION --interval-noise-stddev-rate $INTERVAL_NOISE_RATE $VERBOSE_TAG
    echo "--------------------------------------------------"

    GAURD_TIME=3
    echo "Waiting for ${GAURD_TIME} seconds before next operation..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[7/7] Terminating Consumer + Connector ($TIMESTAMP)"
    clean_up_consumer $CONSUMER_ID
    clean_up_r_consumer $R_CONSUMER_ID
    clean_up_connector $CONNECTOR_ID
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[8/8] Printing results ($TIMESTAMP)"

    {
        ssh $R_CLIENT_HOST "cat $R_CLIENT_OUT/$R_CONSUMER_ID.log"
    }
    echo "--------------------------------------------------"
    echo "=================================================="
done

echo "--------------------------------------------------"
echo "✅ All experiments completed!"
echo "--------------------------------------------------"
