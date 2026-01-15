#!/bin/bash

# Precedence order:
# 1. Command-line arguments (highest)
# 2. Configuration file values
# 3. Default values in this script (lowest)

# --- Default values for options ---
KAFKA_BIN_PATH="."
BROKER="127.0.0.1:9092"
PREFIX="group-"
START_IDX=0
COUNT=1
VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o b:p:c:vh --longoptions kafka-bin-path:,broker:,prefix:,start-idx:,count:,verbose,help,config: -n 'myscript' -- "$@")
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_KAFKA_BIN_PATH=""
CL_BROKER=""
CL_PREFIX=""
CL_START_IDX=""
CL_COUNT=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --kafka-bin-path) CL_KAFKA_BIN_PATH="$2" ; shift 2;;
        -b|--broker) CL_BROKER="$2" ; shift 2;;
        -p|--prefix) CL_PREFIX="$2" ; shift 2;;
        --start-idx) CL_START_IDX="$2" ; shift 2;;
        -c|--count) CL_COUNT="$2" ; shift 2 ;;
        -v|--verbose) CL_VERBOSE=1 ; shift ;;
        -h|--help) HELP=1 ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

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
if [ -n "$CL_KAFKA_BIN_PATH" ]; then
    KAFKA_BIN_PATH="$CL_KAFKA_BIN_PATH"
fi
if [ -n "$CL_BROKER" ]; then
    BROKER="$CL_BROKER"
fi
if [ -n "$CL_PREFIX" ]; then
    PREFIX="$CL_PREFIX"
fi
if [ -n "$CL_START_IDX" ]; then
    START_IDX="$CL_START_IDX"
fi
if [ -n "$CL_COUNT" ]; then
    COUNT="$CL_COUNT"
fi
if [ -n "$CL_VERBOSE" ]; then
    VERBOSE="$CL_VERBOSE"
fi

# --- Help Message ---
if [ "$HELP" -eq 1 ]; then
    echo "Usage: $(basename "$0") [OPTIONS] [POSITIONAL_ARG1] [POSITIONAL_ARG2...]"
    echo ""
    echo "This is a script for checking if kafka consumer groups successfully connected."
    echo ""
    echo "Options:"
    echo "      --config <path>               Path to a configuration file. (e.g., key=\"value\" pairs)"
    echo "      --kafka-bin-path <path>       Directory where Kafka command-line tools are located. (Default: .)"
    echo "  -b, --broker <host:port>          Kafka broker address. (Default: 127.0.0.1:9092)"
    echo "  -p, --prefix <prefix>             Prefix for consumer group names. (Default: group-)"
    echo "      --start-idx <number>          Starting index for consumer group names. (Default: 0)"
    echo "  -c, --count <number>              Number of consumer groups to check. (Default: 1)"
    echo "  -v, --verbose                     Enable verbose output. (Config key: VERBOSE=1)"
    echo "  -h, --help                        Display this help message and exit."
    echo ""
    echo "Positional Arguments:"
    echo "  Any arguments not preceded by an option flag will be treated as positional arguments."
    echo "  Example: $(basename "$0") --config my.conf -f input.txt arg1 arg2"
    exit 0
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Kafka Bin:          $KAFKA_BIN_PATH"
    echo "Broker:             $BROKER"
    echo "Prefix:             $PREFIX"
    echo "Start Index:        $START_IDX"
    echo "Count:              $COUNT"
    echo "Verbose Mode:       $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ $# -gt 0 ]; then
        echo "Positional Arguments: $@"
    fi
fi

# script's main logic
echo "[CG_CHECKER] Fetching active consumer groups..."
EXISTING_GROUPS=$($KAFKA_BIN_PATH/kafka-consumer-groups.sh --bootstrap-server $BROKER --list 2>&1)

ALL_CONNECTED=1
for ((i=START_IDX; i<START_IDX+COUNT; ++i)); do
    group="$PREFIX$i"
    if !echo "$EXISTING_GROUPS" | grep -q "^$group$"; then
        if [ $VERBOSE -eq 1 ]; then
            echo "[CG_CHECKER] $group has not connected yet."
        fi
        ALL_CONNECTED=0
        break
    fi
    elif [ $VERBOSE -eq 1 ]; then
        echo "[CG_CHECKER] $group is connected."
    fi
done

if [[ $ALL_CONNECTED -eq 1 ]]; then
    echo "[CG_CHECKER] ✅ All $COUNT Consumer Groups are connected."
    exit 0
else
    echo "[CG_CHECKER] ❌ Some Consumer Groups failed to connect." >&2
    exit 1
fi