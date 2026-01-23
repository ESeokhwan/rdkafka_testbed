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

TERMINATE_TIMEOUT=60

STEP_CARS=(10 20 30 40 50 60 70 80 90 100 110 120)
STEP_INTERVAL=5
FINAL_HOLD=20

INTERVAL_NOISE_RATE=0

VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o d:vh --longoptions \
    "config:, verbose, help, kafka-broker:, mqtt-broker:, common-script-root:, \
    connect-host:, connect-root:, connect-out:, connect-temp:, connect-exec:, \
    r-client-host:, r-client-root:, r-client-out:, r-client-temp:, r-consumer-exec:, \
    client-root:, client-out:, client-temp:, consumer-exec:, producer-exec:, step-cars:, \
    step-interval:, final-hold:, interval-noise-rate:, terminate-timeout:" \
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
CL_TERMINATE_TIMEOUT=""
CL_STEP_CARS=()
CL_STEP_INTERVAL=""
CL_FINAL_HOLD=""
CL_INTERVAL_NOISE_RATE=""
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
        --terminate-timeout) CL_TERMINATE_TIMEOUT="$2" ; shift 2 ;;
        --step-cars) IFS=',' read -r -a CL_STEP_CARS <<< "$2" ; shift 2 ;;
        --step-interval) CL_STEP_INTERVAL="$2" ; shift 2 ;;
        --final-hold) CL_FINAL_HOLD="$2" ; shift 2 ;;
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
    echo "      --connect-host <user@host>    Remote host for Connect execution. (Default: empty string for local)"
    echo "      --connect-root <path>         Root directory on remote host where Connect is located. (Default: ./connect)"
    echo "      --connect-out <path>          Output root directory for Connect logs. (Default: {connect_root}/out)"
    echo "      --connect-temp <path>         Temporary root directory for Connect files. (Default: {connect_root}/temp)"
    echo "      --connect-exec <path>         Executable path for Connect. (Default: {connect_root}/bin/Connect)"
    echo "      --r-client-host <user@host>   Remote host for Remote Clients execution. (Default: empty string for local)"
    echo "      --r-client-root <path>        Root directory on remote host where Remote Client is located. (Default: client)"
    echo "      --r-client-out <path>         Output root directory for Remote Client logs. (Default: {r_client_root}/out)"
    echo "      --r-client-temp <path>        Temporary root directory for Remote Client files. (Default: {r_client_root}/temp)"
    echo "      --r-consumer-exec <name>      Executable name for Remote Consumer. (Default: {r_client_root}/bin/vehicle)"
    echo "      --client-root <path>          Root directory where Client is located. (Default: ./client)"
    echo "      --client-out <path>           Output root directory for Client logs. (Default: {client_root}/out)"
    echo "      --client-temp <path>          Temporary root directory for Client files. (Default: {client_root}/temp)"
    echo "      --consumer-exec <path>        Executable path for Local Consumer. (Default: {client_root}/bin/vehicle)"
    echo "      --producer-exec <path>        Executable path for Producer. (Default: {client_root}/bin/producer)"
    echo "      --terminate-timeout <seconds> Timeout second to wait before force killing (Default: 60)."
    echo "      --step-cars <num1,num2,...>   Comma-separated list of total car counts for each scaling step. (Default: (10,20,30,40,50,60,70,80,90,100,110,120))"
    echo "      --step-interval <seconds>     Interval in seconds between each scaling step. (Default: 5)"
    echo "      --final-hold <seconds>        Hold time in seconds after reaching final scale before termination. (Default: 20)"
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
if [ -n "$CL_INTERVAL_NOISE_RATE" ]; then
    INTERVAL_NOISE_RATE="$CL_INTERVAL_NOISE_RATE"
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
    echo "Terminate Timeout:      $TERMINATE_TIMEOUT"
    echo "Step Cars:              $STEP_CARS"
    echo "Step Interval:          $STEP_INTERVAL"
    echo "Final Hold:             $FINAL_HOLD"
    echo "Interval Noise Rate:    $INTERVAL_NOISE_RATE"
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

    local load_consumer_id="Consumer_${from}_${to}"
    $CLIENT_ROOT/script/run-on-bg.sh --id $load_consumer_id \
        --out-dir $CLIENT_OUT --temp-dir $CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $CONSUMER_EXEC -- \
            --broker $KAFKA_BROKER --group_prefix "group_" \
            --client_cnt $((to-from)) --start_idx $from --running_time $INF_DURATION \
            --outdir $CLIENT_OUT --out_prefix "${from}_${to}C_" --no_log \
            --start_barrier_delay 1 $VERBOSE_TAG
    LOAD_CONSUMER_IDS+=("$load_consumer_id")
}

start_producers() {
    local from=$1
    local to=$2

    local producer_id="Producer_${from}_${to}"
    $CLIENT_ROOT/script/run-on-bg.sh --id $producer_id \
        --out-dir $CLIENT_OUT --temp-dir $CLIENT_TEMP $VERBOSE_TAG \
        --exec-path $PRODUCER_EXEC -- \
            --broker $MQTT_BROKER --client_cnt $((to-from)) --start_idx $from \
            --running_time $INF_DURATION --start_barrier_delay 1 \
            --interval_noise_stddev_rate $INTERVAL_NOISE_RATE $VERBOSE_TAG
    PRODUCER_IDS+=("$producer_id")
}

# clean up functions
clean_up_connect() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$CONNECT_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $CONNECT_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$CONNECT_HOST" ] && [ -n "$is_async" ]; then
        ssh "$CONNECT_HOST" "$cmd" $args &
    elif [ -n "$CONNECT_HOST" ]; then
        ssh "$CONNECT_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

clean_up_client() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$CLIENT_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $CLIENT_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

clean_up_r_client() {
    local id=$1
    local timeout=$2
    local is_async=$3

    local cmd="$R_CLIENT_ROOT/script/terminate-on-bg.sh"
    local args="--id $id --temp-dir $R_CLIENT_TEMP --timeout $timeout $VERBOSE_TAG"

    if [ -n "$R_CLIENT_HOST" ] && [ -n "$is_async" ]; then
        ssh "$R_CLIENT_HOST" "$cmd" $args &
    elif [ -n "$R_CLIENT_HOST" ]; then
        ssh "$R_CLIENT_HOST" "$cmd" $args
    elif [ -n "$is_async" ]; then
        "$cmd" $args &
    else
        "$cmd" $args
    fi
}

# trap handler
trap_handler() {
    echo "[TRAP] Ctrl+C 감지! 정리 중..."

    for PRODUCER_ID in "${PRODUCER_IDS[@]}"; do
        clean_up_client $PRODUCER_ID $TERMINATE_TIMEOUT
    done
    for LOAD_CONSUMER_ID in "${LOAD_CONSUMER_IDS[@]}"; do
        clean_up_client $LOAD_CONSUMER_ID $TERMINATE_TIMEOUT
    done
    clean_up_r_client $R_CONSUMER_ID $TERMINATE_TIMEOUT
    clean_up_connect $CONNECT_ID $TERMINATE_TIMEOUT
    exit 1
}
trap trap_handler SIGINT

# script's main logic
echo "------------------------------------------------"
echo "🚀 start test script"
echo "------------------------------------------------"

LOAD_CONSUMER_IDS=()
PRODUCER_IDS=()

NUM_STEPS=${#STEP_CARS[@]}
MAX_CAR_CNT=${STEP_CARS[$((NUM_STEPS-1))]}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[1/7] Connect 실행 ($TIMESTAMP)"
CONNECT_ID="Connect_Dynamic"
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
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "group_" --count $MAX_CAR_CNT $VERBOSE_TAG
$COMMON_SCRIPT_ROOT/script/delete_consumer_group.sh --config $COMMON_SCRIPT_ROOT/config/common.config --broker $KAFKA_BROKER --prefix "r_group_" --count 4 $VERBOSE_TAG
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[3/7] 측정을 위한 Consumer (Remote) 실행 ($TIMESTAMP)"
R_CONSUMER_ID="RemoteConsumer_Dynamic"
R_CONSUMER_COMMAND="$R_CLIENT_ROOT/script/run-on-bg.sh --id $R_CONSUMER_ID \
    --out-dir $R_CLIENT_OUT --temp-dir $R_CLIENT_TEMP $VERBOSE_TAG \
    --exec-path $R_CONSUMER_EXEC -- \
        --broker $KAFKA_BROKER --group_prefix 'r_group_' \
        --client_cnt -1 --running_time $INF_DURATION \
        --outdir $R_CLIENT_OUT --out_prefix 'Dynamic_' $VERBOSE_TAG"
if [ $R_CLIENT_HOST == "" ]; then
    $R_CONSUMER_COMMAND
else
    ssh $R_CLIENT_HOST $R_CONSUMER_COMMAND
fi
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[4/7] 스케일 단계 시작 ($TIMESTAMP)"
PREV_CAR_CNT=0
for (( step=0; step<$NUM_STEPS; step++ )); do
    CUR_CAR_NUM=${STEP_CARS[$step]}
    C_START_IDX=$((PREV_CAR_CNT))
    if [ $step -eq 0 ]; then
        C_START_IDX=4
    else
        sleep $STEP_INTERVAL
    fi
    echo "  - 차량 수 ${CUR_CAR_NUM}대로 증가"
    start_load_consumers $C_START_IDX $CUR_CAR_NUM
    start_producers $PREV_CAR_CNT $CUR_CAR_NUM
    PREV_CAR_CNT=$CUR_CAR_NUM
done
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[5/7] 최종 대기 시간 시작 ($TIMESTAMP): ${FINAL_HOLD}s"
sleep $((FINAL_HOLD + 1))
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[6/7] 순차적 종료 시작 ($TIMESTAMP)"
for (( step=0; step<$NUM_STEPS; step++ )); do
    CUR_CAR_NUM=${STEP_CARS[$step]}
    CUR_REMAIN_CAR_NUM=$((MAX_CAR_CNT - CUR_CAR_NUM))
    echo "  - 차량 수 ${CUR_REMAIN_CAR_NUM}대로 감소"
    clean_up_client "${PRODUCER_IDS[$step]}" $TERMINATE_TIMEOUT "async"
    clean_up_client "${LOAD_CONSUMER_IDS[$step]}" $TERMINATE_TIMEOUT "async"
    sleep $STEP_INTERVAL
done
echo "--------------------------------------------------"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "[7/7] Connect 및 측정용 Consumer 종료 ($TIMESTAMP)"
clean_up_connect $CONNECT_ID $TERMINATE_TIMEOUT
clean_up_r_client $R_CONSUMER_ID $TERMINATE_TIMEOUT
echo "--------------------------------------------------"

