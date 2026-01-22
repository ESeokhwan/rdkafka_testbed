#!/bin/bash

# Precedence order:
# 1. Command-line arguments (highest)
# 2. Configuration file values
# 3. Default values in this script (lowest)

# --- Default values for options ---
ID="run-on-bg"
TEMP_DIR="temp"
TIMEOUT=-1
VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o o:vh --longoptions \
    config:,verbose,help,id:,temp-dir:timeout: \
    -n 'myscript' -- "$@" \
)

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_ID=""
CL_TEMP_DIR=""
CL_TIMEOUT=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --id) CL_ID="$2" ; shift 2 ;;
        --temp-dir) CL_TEMP_DIR="$2" ; shift 2 ;;
        --timeout) CL_TIMEOUT="$2" ; shift 2 ;;
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
    echo "This script terminates the running process executed by run-on-bg.sh on background."
    echo ""
    echo "Options:"
    echo "      --config <path>               Path to a configuration file. (e.g., key=\"value\" pairs)"
    echo "      --id <identifier>             Identifier for the run instance."
    echo "      --temp-dir <path>             Directory where temporary files will be stored. (Default: temp)"
    echo "      --timeout <timeout>           Maximum time in seconds to wait before force killing. (Default: -1)"
    echo "  -v, --verbose                     Enable verbose output. (Config key: VERBOSE=1)"
    echo "  -h, --help                        Display this help message and exit."
    echo ""
    echo "Positional Arguments:"
    echo "  Any arguments not preceded by an option flag will be treated as positional arguments."
    echo "  Example: $(basename "$0") --config my.conf arg1 arg2"
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
if [ -n "$CL_ID" ]; then
    ID="$CL_ID"
fi
if [ -n "$CL_TEMP_DIR" ]; then
    TEMP_DIR="$CL_TEMP_DIR"
fi
if [ -n "$CL_TIMEOUT" ]; then
    TIMEOUT="$CL_TIMEOUT"
fi
if [ -n "$CL_VERBOSE" ]; then
    VERBOSE="$CL_VERBOSE"
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Id:                 $ID"
    echo "Temp Dir:           $TEMP_DIR"
    echo "Timeout:            $TIMEOUT"
    echo "Verbose Mode:       $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ $# -gt 0 ]; then
        echo "Positional Arguments: $@"
    fi
fi

# --- Termination Logic ---
RUNNING_PID=$(cat $TEMP_DIR/$ID.pid 2>/dev/null)
if [[ -n "$RUNNING_PID" ]]; then
    echo "[Terminate_On_BG] Kill process using pid file (PID: $RUNNING_PID)"
    kill -SIGTERM $RUNNING_PID
else
    echo "[Terminate_On_BG] No process pid found."
    exit 1
fi

if [ "$TIMEOUT" -lt 0 ]; then
    exit 0
fi

# --- Timeout Logic ---
for i in $(seq 1 $TIMEOUT); do
    if ! kill -0 $RUNNING_PID > /dev/null 2>&1; then
        echo "[Terminate_On_BG] Process $RUNNING_PID stopped gracefully."
        exit 0
    fi
    sleep 1
done

echo "[Terminate_On_BG] Timeout reached! Force killing process $RUNNING_PID..."
kill -9 $RUNNING_PID
exit 0