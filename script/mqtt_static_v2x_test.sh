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

CONNECT_HOST=""
CONNECT_ROOT="./connect"
CONNECT_OUT=""
CONNECT_TEMP=""

CONNECT_EXEC=""

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

DURATION=100

# NUM_CAR=(10 10 20 40 60 80 100 120 130 140 150)
NUM_CAR=(10)

VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o d:vh --longoptions \
    "config:, verbose, help, kafka-broker:, mqtt-broker:, common-script-root:, \
    connect-host:, connect-root:, connect-out:, connect-temp:, connect-exec:, \
    r-client-host:, r-client-root:, r-client-out:, r-client-temp:, r-consumer-exec:, \
    client-root:, client-out:, client-temp:, consumer-exec:, producer-exec:, duration:" \
    -n 'myscript' -- "$@" \
)

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_KAFKA_BROKER=""
CL_MQTT_BROKER=""
CL_COMMON_SCRIPT_ROOT=""
CL_CONNECT_HOST=""
CL_CONNECT_ROOT=""
CL_CONNECT_OUT=""
CL_CONNECT_TEMP=""
CL_CONNECT_EXEC=""
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
CL_DURATION=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --kafka-broker) CL_KAFKA_BROKER="$2" ; shift 2;;
        --mqtt-broker) CL_MQTT_BROKER="$2" ; shift 2 ;;
        --common-script-root) CL_COMMON_SCRIPT_ROOT="$2" ; shift 2 ;;
        --connect-host) CL_CONNECT_HOST="$2" ; shift 2 ;;
        --connect-root) CL_CONNECT_ROOT="$2" ; shift 2 ;;
        --connect-out) CL_CONNECT_OUT="$2" ; shift 2 ;;
        --connect-temp) CL_CONNECT_TEMP="$2" ; shift 2 ;;
        --connect-exec) CL_CONNECT_EXEC="$2" ; shift 2 ;;
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
        -d|--duration) CL_DURATION="$2" ; shift 2 ;;
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
    echo "      --connect-host <user@host>    Remote host for Connect execution. (Default: empty string for local)"
    echo "      --connect-root <path>         Root directory on remote host where Connect is located. (Default: ./connect)"
    echo "      --connect-out <path>          Output rootectory for Connect logs. (Default: {connect_root}/out)"
    echo "      --connect-temp <path>         Temporary rootectory for Connect files. (Default: {connect_root}/temp)"
    echo "      --connect-exec <path>         Executable path for Connect. (Default: {connect_root}/bin/Connect)"
    echo "      --r-client-host <user@host>   Remote host for Remote Clients execution. (Default: empty string for local)"
    echo "      --r-client-root <path>        Root directory on remote host where Remote Client is located. (Default: client)"
    echo "      --r-client-out <path>         Output rootectory for Remote Client logs. (Default: {r_client_root}/out)"
    echo "      --r-client-temp <path>        Temporary rootectory for Remote Client files. (Default: {r_client_root}/temp)"
    echo "      --r-consumer-exec <name>      Executable name for Remote Consumer. (Default: {r_client_root}/bin/vehicle)"
    echo "      --client-root <path>          Root directory where Client is located. (Default: ./client)"
    echo "      --client-out <path>           Output rootectory for Client logs. (Default: {client_root}/out)"
    echo "      --client-temp <path>          Temporary rootectory for Client files. (Default: {client_root}/temp)"
    echo "      --consumer-exec <path>        Executable path for Local Consumer. (Default: {client_root}/bin/vehicle)"
    echo "      --producer-exec <path>        Executable path for Producer. (Default: {client_root}/bin/producer)"
    echo "  -d, --duration <seconds>          Duration for the test run. (Default: 100)"
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
if [ -n "$CL_CONNECT_HOST" ]; then
    CONNECT_HOST="$CL_CONNECT_HOST"
fi
if [ -n "$CL_CONNECT_ROOT" ]; then
    CONNECT_ROOT="$CL_CONNECT_ROOT"
fi
if [ -n "$CL_CONNECT_OUT" ]; then
    CONNECT_OUT="$CL_CONNECT_OUT"
fi
if [ -n "$CL_CONNECT_TEMP" ]; then
    CONNECT_TEMP="$CL_CONNECT_TEMP"
fi
if [ -n "$CL_CONNECT_EXEC" ]; then
    CONNECT_EXEC="$CL_CONNECT_EXEC"
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
if [ -n "$CL_DURATION" ]; then
    DURATION="$CL_DURATION"
fi
if [ -n "$CL_VERBOSE" ]; then
    VERBOSE="$CL_VERBOSE"
fi

# --- post-setup-defaults ---
if [ -z "$CONNECT_OUT" ]; then
  CONNECT_OUT=${CONNECT_ROOT}/out
fi
if [ -z "$CONNECT_TEMP" ]; then
    CONNECT_TEMP=${CONNECT_ROOT}/temp
fi
if [ -z "$CONNECT_EXEC" ]; then
    CONNECT_EXEC=${CONNECT_ROOT}/bin/Connect
fi
if [ -z "$R_CLIENT_OUT" ]; then
    R_CLIENT_OUT=${R_CLIENT_ROOT}/out
fi
if [ -z "$R_CLIENT_TEMP" ]; then
    R_CLIENT_TEMP=${R_CLIENT_ROOT}/temp
fi
if [ -z "$R_CONSUMER_EXEC" ]; then
    R_CONSUMER_EXEC=${R_CLIENT_ROOT}/bin/Vehicle
fi
if [ -z "$CLIENT_OUT" ]; then
    CLIENT_OUT=${CLIENT_ROOT}/out
fi
if [ -z "$CLIENT_TEMP" ]; then
    CLIENT_TEMP=${CLIENT_ROOT}/temp
fi
if [ -z "$CONSUMER_EXEC" ]; then
    CONSUMER_EXEC=${CLIENT_ROOT}/bin/Vehicle
fi
if [ -z "$PRODUCER_EXEC" ]; then
    PRODUCER_EXEC=${CLIENT_ROOT}/bin/Producer
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Kafka Broker:           $KAFKA_BROKER"
    echo "MQTT Broker:            $MQTT_BROKER"
    echo "Common Script Root:     $COMMON_SCRIPT_ROOT"
    echo "Connect Host:           $CONNECT_HOST"
    echo "Connect Root:           $CONNECT_ROOT"
    echo "Connect Out:            $CONNECT_OUT"
    echo "Connect Temp:           $CONNECT_TEMP"
    echo "Connect Exec:           $CONNECT_EXEC"
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
    echo "Duration:               $DURATION"
    echo "Verbose Mode:           $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ $# -gt 0 ]; then
        echo "Positional Arguments: $@"
    fi
fi

# clean up functions
clean_up_connect() {
    IDENTIFIER=$1
    CONNECT_COMMAND="$CONNECT_ROOT/script/run-on-bg.sh --id $IDENTIFIER --terminate --out-dir $CONNECT_OUT --temp-dir $CONNECT_TEMP $VERBOSE_TAG"
    if [ $CONNECT_HOST == "" ]; then
        $CONNECT_COMMAND
    else
        ssh $CONNECT_HOST $CONNECT_COMMAND
    fi
}

clean_up_consumer() {
    IDENTIFIER=$1
    $CLIENT_ROOT/script/run-on-bg.sh --id $IDENTIFIER --terminate --out-dir $CLIENT_OUT --temp-dir $CLIENT_TEMP $VERBOSE_TAG
}

clean_up_r_consumer() {
    IDENTIFIER=$1
    R_CONSUMER_COMMAND="$R_CLIENT_ROOT/script/run-on-bg.sh --id $IDENTIFIER --terminate --out-dir $R_CLIENT_OUT --temp-dir $R_CLIENT_TEMP $VERBOSE_TAG"
    if [ $R_CLIENT_HOST == "" ]; then
        $R_CONSUMER_COMMAND
    else 
        ssh $R_CLIENT_HOST $R_CONSUMER_COMMAND
    fi
}

# trap handler
trap_handler() {
    echo "[TRAP] Ctrl+C 감지! 정리 중..."

    sleep 10
    clean_up_consumer $CONSUMER_ID
    clean_up_r_consumer $R_CONSUMER_ID
    clean_up_connect $CONNECT_ID
    exit 1
}
trap trap_handler SIGINT

# script's main logic
echo "------------------------------------------------"
echo "🚀 start test script"
echo "------------------------------------------------"

INF_DURATION_MS=$((1000 * 60 * 60 * 10)) # 10 hours
VERBOSE_TAG=""
if [ $VERBOSE -eq 1 ]; then
    VERBOSE_TAG="--verbose"
fi

for CAR_NUM in "${NUM_CAR[@]}"; do
    CURRENT_CAR_NUM=$CAR_NUM

    echo "=================================================="
    echo "실험 시작: NUM_CAR=$CURRENT_CAR_NUM"

    echo "--------------------------------------------------"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[1/7] Connect 실행 ($TIMESTAMP)"
    CONNECT_ID="Connect_${CURRENT_CAR_NUM}"
    CONNECT_COMMAND="$CONNECT_ROOT/script/run-on-bg.sh --id $CONNECT_ID \
        --out-dir $CONNECT_OUT --temp-dir $CONNECT_TEMP $VERBOSE_TAG\
        --exec-path $CONNECT_EXEC 1"
    if [ $CONNECT_HOST == "" ]; then
        $CONNECT_COMMAND
    else 
        ssh $CONNECT_HOST $CONNECT_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[2/7] Consumer Groups 삭제 ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "r_group_" --count 4 $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[3/7] 측정을 위한 Consumer (Remote) 실행 ($TIMESTAMP)"
    R_CONSUMER_ID="RemoteConsumer_${CURRENT_CAR_NUM}"
    R_CONSUMER_COMMAND="$R_CLIENT_ROOT/script/run-on-bg.sh --id $R_CONSUMER_ID \
        --out-dir $R_CLIENT_OUT --temp-dir $R_CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $R_CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group_prefix 'r_group_' \
            --client_cnt -1 --running_time $INF_DURATION_MS \
            --outdir $R_CLIENT_OUT $VERBOSE_TAG"
    if [ $R_CLIENT_HOST == "" ]; then
       $R_CONSUMER_COMMAND
    else 
        ssh $R_CLIENT_HOST $R_CONSUMER_COMMAND
    fi
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[4/7] 부하를 위한 Consumer (Local) 실행 ($TIMESTAMP)"
    CONSUMER_ID="Consumer_${CURRENT_CAR_NUM}"
    $CLIENT_ROOT/script/run-on-bg.sh --id $CONSUMER_ID \
        --out-dir $CLIENT_OUT --temp-dir $CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group_prefix 'group_' \
            --client_cnt $CURRENT_CAR_NUM --running_time $INF_DURATION_MS \
            --outdir $CLIENT_OUT $VERBOSE_TAG
    echo "--------------------------------------------------"

    GAURD_TIME=5
    echo "${GAURD_TIME}초 대기 후 다음 작업 실행..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[5/7] Consumer Groups 연결 확인 ($TIMESTAMP)"
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "group_" --count $CURRENT_CAR_NUM $VERBOSE_TAG
    $COMMON_SCRIPT_ROOT/script/check_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "r_group_" --count 4 $VERBOSE_TAG
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[6/7] Producer 실행 ($TIMESTAMP)"
    DURATION_MS=$((DURATION * 1000))
    $PRODUCER_EXEC --broker $MQTT_BROKER --client_cnt $CURRENT_CAR_NUM \
        --running_time $DURATION_MS $VERBOSE_TAG
    echo "--------------------------------------------------"

    GAURD_TIME=5
    echo "${GAURD_TIME}초 대기 후 다음 작업 실행..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[7/7] Consumer + Connect 종료 ($TIMESTAMP)"
    clean_up_consumer $CONSUMER_ID
    clean_up_r_consumer $R_CONSUMER_ID
    clean_up_connect $CONNECT_ID
    echo "--------------------------------------------------"

    GAURD_TIME=30
    echo "${GAURD_TIME}초 대기 후 다음 작업 실행..."
    sleep $GAURD_TIME
    echo "--------------------------------------------------"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "[8/8] 결과 출력 ($TIMESTAMP)"

    {
        ssh $R_CLIENT_HOST "cat $R_CLIENT_OUT/$R_CONSUMER_ID.log"
    }
    echo "--------------------------------------------------"
    echo "=================================================="
done

echo "--------------------------------------------------"
echo "✅ 모든 실험 완료!"
echo "--------------------------------------------------"
