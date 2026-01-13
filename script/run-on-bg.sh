#!/bin/bash

# Precedence order:
# 1. Command-line arguments (highest)
# 2. Configuration file values
# 3. Default values in this script (lowest)

# --- Default values for options ---
ID="run-on-bg"
EXEC_PATH=""
OUT_DIR="out"
TEMP_DIR="temp"
TERMINATE=0
TERMINATE_ALL=0
VERBOSE=0
HELP=0
CONFIG_FILE=""

# --- Argument Parsing ---
TEMP=$(getopt -o o:vh --longoptions \
    config:,verbose,help,id:,exec-path:,out-dir:,temp-dir:,service-num:,car-num:,terminate,terminate-all \
    -n 'myscript' -- "$@" \
)

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

# Temporary variables to store command-line arguments
CL_ID=""
CL_EXEC_PATH=""
CL_OUT_DIR=""
CL_TEMP_DIR=""
CL_TERMINATE=""
CL_TERMINATE_ALL=""
CL_VERBOSE=""

# Process arguments and store them in temporary variables
while true ; do
    case "$1" in
        --config) CONFIG_FILE="$2" ; shift 2 ;;
        --id) CL_ID="$2" ; shift 2 ;;
        --exec-path) CL_EXEC_PATH="$2" ; shift 2 ;;
        --out-dir) CL_OUT_DIR="$2" ; shift 2 ;;
        --temp-dir) CL_TEMP_DIR="$2" ; shift 2 ;;
        --terminate) CL_TERMINATE=1 ; shift ;;
        --terminate-all) CL_TERMINATE_ALL=1 ; shift ;;
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
    echo "This script runs the executable file on background with specified service and car numbers."
    echo ""
    echo "Options:"
    echo "      --config <path>               Path to a configuration file. (e.g., key=\"value\" pairs)"
    echo "      --id <identifier>             Identifier for the run instance."
    echo "      --exec-path <path>            Path to the executable to run in background."
    echo "  -o, --out-dir <path>              Directory where output logs will be stored. (Default: out)"
    echo "      --temp-dir <path>             Directory where temporary files will be stored. (Default: temp)"
    echo "      --terminate                   Terminate the specific process for the given id."
    echo "      --terminate-all               Terminate all running processes for the executable and arguments."
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
if [ -n "$CL_EXEC_PATH" ]; then
    EXEC_PATH="$CL_EXEC_PATH"
fi
if [ -n "$CL_OUT_DIR" ]; then
    OUT_DIR="$CL_OUT_DIR"
fi
if [ -n "$CL_TEMP_DIR" ]; then
    TEMP_DIR="$CL_TEMP_DIR"
fi
if [ -n "$CL_TERMINATE" ]; then
    TERMINATE="$CL_TERMINATE"
fi
if [ -n "$CL_TERMINATE_ALL" ]; then
    TERMINATE_ALL="$CL_TERMINATE_ALL"
fi
if [ -n "$CL_VERBOSE" ]; then
    VERBOSE="$CL_VERBOSE"
fi

# --- Script Logic ---
if [ "$VERBOSE" -eq 1 ]; then
    echo "--- Script Configuration ---"
    echo "Execution Target:   $EXEC_PATH"
    echo "Output Dir:         $OUT_DIR"
    echo "Temp Dir:           $TEMP_DIR"
    echo "Terminate Flag:     $TERMINATE"
    echo "Terminate All Flag: $TERMINATE_ALL"
    echo "Verbose Mode:       $VERBOSE"
    echo "--------------------------"

    # Handle positional arguments. After the getopt loop, "$@" contains the remaining positional arguments.
    if [ -n "$@" ]; then
        echo "Positional Arguments:"
        for arg in "$@"; do
            echo "  - $arg"
        done
    fi
fi

EXEC_COMMAND="$EXEC_PATH $@"
# --- Termination Logic ---
if [ "$TERMINATE_ALL" -eq 1 ]; then
    while true; do
        RUNNING_PID=$(pgrep -f "^$EXEC_COMMAND" | head -1)
        if [[ -n "$RUNNING_PID" ]]; then
            echo "[Run_On_BG] Kill process (PID: $RUNNING_PID)"
            kill -SIGTERM $RUNNING_PID || kill -9 $RUNNING_PID
            sleep 1
        else
            echo "[Run_On_BG] No more processes found"
            break
        fi
    done
    exit 0
fi

if [ "$TERMINATE" -eq 1 ]; then
    RUNNING_PID=$(cat $TEMP_DIR/$ID.pid 2>/dev/null)
    if [[ -n "$RUNNING_PID" ]]; then
        echo "[Run_On_BG] Kill process using pid file (PID: $RUNNING_PID)"
        kill -SIGTERM $RUNNING_PID || kill -9 $RUNNING_PID
    else
        echo "[Run_On_BG] No process pid found."
    fi
    exit 0
fi

# --- script's main logic ---
mkdir -p $OUT_DIR
mkdir -p $TEMP_DIR
echo "===== Program(${ID}) starts at \$(date) =====" >> $OUT_DIR/$ID.log
nohup $EXEC_COMMAND >> $OUT_DIR/$ID.log 2>&1 & sleep 2
pgrep -f "^$EXEC_COMMAND" | head -1 > $TEMP_DIR/$ID.pid
