#!/bin/bash
# Keep running after an individual experiment fails. Do not use `set -e`.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd) || exit 2
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd) || exit 2

BROKER=166.104.110.13:9092
PARTITIONS=16
CLIENTS_CSV=16,32,64,128,160,192
MODES_CSV=normal,gst
INTERVAL_MS=3
MESSAGES_PER_CLIENT=20000
MESSAGE_SIZE=1000
REPETITIONS=3
REPLICATION_FACTOR=1
START_BARRIER_DELAY=2
DRAIN_WAIT=20
COOLDOWN=60
PRODUCER="$PROJECT_ROOT/bin/single_topic_throughput_expr_producer"
KAFKA_BIN="$PROJECT_ROOT/../kafka_sandbox/bin"
OUTPUT_ROOT="$PROJECT_ROOT/out/gst_fixed_partition"
NORMAL_TOPIC=""
GST_TOPIC=""
SINGLE_TOPIC=test_single_topic
CREATE_TOPICS=0
DELETE_TOPICS_AFTER=0
CLEANUP_ONLY=0
TOPIC_DELETE_TIMEOUT=120
DRY_RUN=0
RUN_HOOK=""
RECORD_PREFIX=""
SAMPLE_LOG=1
READY_TOPICS=()
ACTIVE_COMMAND_PID=""
CLEANUP_WATCHDOG_PID=""
CLEANUP_TIMED_OUT=0
CLEANUP_INTERRUPTED=0
CLEANUP_SIGNAL_EXIT=0

usage() {
    cat <<'EOF'
Usage: run_gst_fixed_partition_expr.sh [options]

Hold topic partition count P and the per-client interval fixed; increase client count C.
The existing producer app is used unchanged, asynchronously with acks=0.

  --broker HOST:PORT          Bootstrap broker (default: 166.104.110.13:9092)
  --partitions P              Fixed topic partition count for normal and GST (16)
  --clients C1,C2,...          Increasing client counts (16,32,64,128,160,192)
  --interval MS               Fixed per-client nominal interval (3)
  --messages-per-client N     Attempts per client; total --msg-cnt is C*N (20000)
  --message-size BYTES        Producer payload size (1000)
  --repetitions N             Repeat the complete client-count sweep (3)
  --modes normal,gst,single   Selected comparisons (default: normal,gst)
  --normal-topic NAME         Default: test_topic_p<P>
  --gst-topic NAME            Default: test_gs_topic_p<P>
  --single-topic NAME         Default: test_single_topic (one physical partition)
  --create-topics             Create missing topics without changing existing ones
  --delete-topics-after       Delete validated experiment topics when this sweep exits
  --cleanup-only              Verify and delete existing selected topics; no measurements
  --topic-delete-timeout SEC  Per-topic deletion/list-confirmation timeout (120)
  --replication-factor N      Required source-topic replication factor (1)
  --producer PATH             Default: <project>/bin/single_topic_throughput_expr_producer
  --kafka-bin DIR             Default: <project>/../kafka_sandbox/bin
  --output-root DIR           Default: <project>/out/gst_fixed_partition
  --start-barrier-delay SEC   Producer startup barrier delay (2)
  --drain-wait SEC            Wait after producer exit before broker rollout (20)
  --cooldown SEC              Wait after rollout before the next run (60)
  --run-hook PATH             Optional before_produce/after_rollout callback
  --record-prefix NAME        Optional record/client ID prefix; topic stays unchanged
  --no-sample-log             Tag/log all clients (default: sample client 0)
  --dry-run                   Print configuration and commands; no server access
  -h, --help                  Show this help

Topics must match P, replication factor and GST mode. --create-topics uses
--if-not-exists and does not change an existing topic with different settings.
Single mode uses P=1. Producers send to the first min(C, P) partitions.
Interval noise is fixed to zero. Runtime is count-based, not a fixed duration.
Actual rates and sustainable capacity are not inferred from the settings.

Each run resets broker metrics, runs the producer, then rolls out broker logs.
SUCCESS/FAILED/SKIPPED, command exit codes and output are saved per run.
SUCCESS means commands completed; it does not certify delivery or capacity.
Any failed/skipped run yields exit 1 only after all remaining runs are tried.
Topic cleanup is opt-in, also deletes matching pre-existing experiment topics,
and uses Bash plus the existing Kafka CLI. Results and measurement files are preserved.
Cleanup waits for metadata removal; physical deletion is asynchronous. The
shared __global_sequence_index is retained and cannot be a cleanup target.
EOF
}

die() { printf 'Error: %s\n' "$*" >&2; exit 2; }

while (( $# > 0 )); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --create-topics) CREATE_TOPICS=1; shift; continue ;;
        --delete-topics-after) DELETE_TOPICS_AFTER=1; shift; continue ;;
        --cleanup-only) CLEANUP_ONLY=1; DELETE_TOPICS_AFTER=1; shift; continue ;;
        --dry-run) DRY_RUN=1; shift; continue ;;
        --no-sample-log) SAMPLE_LOG=0; shift; continue ;;
    esac
    (( $# >= 2 )) || die "Missing value for $1"
    case "$1" in
        --broker) BROKER=$2 ;;
        --partitions) PARTITIONS=$2 ;;
        --clients) CLIENTS_CSV=$2 ;;
        --interval) INTERVAL_MS=$2 ;;
        --messages-per-client) MESSAGES_PER_CLIENT=$2 ;;
        --message-size) MESSAGE_SIZE=$2 ;;
        --repetitions) REPETITIONS=$2 ;;
        --modes) MODES_CSV=$2 ;;
        --normal-topic) NORMAL_TOPIC=$2 ;;
        --gst-topic) GST_TOPIC=$2 ;;
        --single-topic) SINGLE_TOPIC=$2 ;;
        --replication-factor) REPLICATION_FACTOR=$2 ;;
        --producer) PRODUCER=$2 ;;
        --kafka-bin) KAFKA_BIN=$2 ;;
        --output-root) OUTPUT_ROOT=$2 ;;
        --start-barrier-delay) START_BARRIER_DELAY=$2 ;;
        --drain-wait) DRAIN_WAIT=$2 ;;
        --cooldown) COOLDOWN=$2 ;;
        --run-hook) RUN_HOOK=$2 ;;
        --record-prefix) RECORD_PREFIX=$2 ;;
        --topic-delete-timeout) TOPIC_DELETE_TIMEOUT=$2 ;;
        *) die "Unknown option: $1" ;;
    esac
    shift 2
done

(( ! CLEANUP_ONLY || ! CREATE_TOPICS )) || die "--cleanup-only cannot be combined with --create-topics"

positive_int() {
    [[ "$2" =~ ^[1-9][0-9]*$ ]] && (( ${#2} <= 10 )) && (( $2 <= 2147483647 )) || die "$1 must be an integer in 1..2147483647"
}
nonnegative_int() {
    [[ "$2" == 0 ]] || positive_int "$1" "$2"
}

positive_int partitions "$PARTITIONS"
positive_int messages-per-client "$MESSAGES_PER_CLIENT"
positive_int message-size "$MESSAGE_SIZE"
positive_int repetitions "$REPETITIONS"
positive_int replication-factor "$REPLICATION_FACTOR"
(( REPLICATION_FACTOR <= 32767 )) || die "replication-factor must be <= 32767"
nonnegative_int start-barrier-delay "$START_BARRIER_DELAY"
nonnegative_int drain-wait "$DRAIN_WAIT"
nonnegative_int cooldown "$COOLDOWN"
[[ "$TOPIC_DELETE_TIMEOUT" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] && \
    awk -v n="$TOPIC_DELETE_TIMEOUT" 'BEGIN {exit !(n > 0 && n < 1e9)}' || die "topic-delete-timeout must be positive and < 1e9 seconds"
[[ "$INTERVAL_MS" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] && \
    awk -v n="$INTERVAL_MS" 'BEGIN {exit !(n > 0 && n < 1e9)}' || die "interval must be a positive number of milliseconds"
[[ -n "$BROKER" && -n "$OUTPUT_ROOT" ]] || die "broker and output-root must not be empty"
[[ -z "$RECORD_PREFIX" || "$RECORD_PREFIX" =~ ^[a-zA-Z0-9._-]+$ ]] || die "Invalid record-prefix: $RECORD_PREFIX"
[[ "$CLIENTS_CSV" =~ ^[0-9]+(,[0-9]+)*$ ]] || die "clients must be comma-separated positive integers"
[[ "$MODES_CSV" =~ ^[a-z]+(,[a-z]+)*$ ]] || die "modes must be comma-separated names"
IFS=',' read -r -a CLIENT_COUNTS <<< "$CLIENTS_CSV"
IFS=',' read -r -a MODES <<< "$MODES_CSV"
NORMAL_TOPIC=${NORMAL_TOPIC:-test_topic_p$PARTITIONS}
GST_TOPIC=${GST_TOPIC:-test_gs_topic_p$PARTITIONS}
TOPICS_COMMAND="$KAFKA_BIN/kafka-topics.sh"
EXTENSION_COMMAND="$KAFKA_BIN/kafka-broker-extension.sh"

seen_modes=,
seen_topics=,
for mode in "${MODES[@]}"; do
    case "$mode" in
        normal) topic=$NORMAL_TOPIC ;;
        gst) topic=$GST_TOPIC ;;
        single) topic=$SINGLE_TOPIC ;;
        *) die "Unknown mode: $mode" ;;
    esac
    [[ "$seen_modes" != *",$mode,"* ]] || die "Duplicate mode: $mode"
    [[ "$topic" =~ ^[a-zA-Z0-9._-]+$ ]] && [[ "$topic" != . && "$topic" != .. ]] || die "Invalid topic name: $topic"
    (( ! DELETE_TOPICS_AFTER )) || [[ "$topic" != __global_sequence_index ]] || die "The shared GST index cannot be a cleanup target"
    [[ "$seen_topics" != *",$topic,"* ]] || die "Selected modes must use different topics"
    seen_modes="$seen_modes$mode,"
    seen_topics="$seen_topics$topic,"
done
previous_clients=0
for clients in "${CLIENT_COUNTS[@]}"; do
    positive_int clients "$clients"
    (( clients > previous_clients )) || die "clients must be strictly increasing"
    (( clients <= 2147483647 / MESSAGES_PER_CLIENT )) || die "C * messages-per-client exceeds the producer's --msg-cnt range"
    previous_clients=$clients
done

NOMINAL_CLIENT_RPS=$(awk -v ms="$INTERVAL_MS" 'BEGIN {printf "%.6f", 1000/ms}')
NOMINAL_PRODUCE_S=$(awk -v n="$MESSAGES_PER_CLIENT" -v ms="$INTERVAL_MS" 'BEGIN {printf "%.6f", n*ms/1000}')
TOTAL_RUNS=$((REPETITIONS * ${#CLIENT_COUNTS[@]} * ${#MODES[@]}))
if (( CLEANUP_ONLY )); then
    echo "Cleanup only: verify and delete selected existing topics (P=$PARTITIONS modes=$MODES_CSV)."
else
    printf 'P=%s C=%s modes=%s interval_ms=%s nominal_client_rps=%s messages_per_client=%s repetitions=%s runs=%s\n' \
        "$PARTITIONS" "$CLIENTS_CSV" "$MODES_CSV" "$INTERVAL_MS" "$NOMINAL_CLIENT_RPS" "$MESSAGES_PER_CLIENT" "$REPETITIONS" "$TOTAL_RUNS"
    echo "Nominal production time per run: $NOMINAL_PRODUCE_S s (count-based; actual time may differ)."
fi

if (( ! DRY_RUN )); then
    [[ -x "$TOPICS_COMMAND" ]] || die "Topic command not found: $TOPICS_COMMAND"
    if (( ! CLEANUP_ONLY )); then
        [[ -x "$PRODUCER" ]] || die "Producer executable not found: $PRODUCER"
        [[ -x "$EXTENSION_COMMAND" ]] || die "Broker measurement extension not found: $EXTENSION_COMMAND"
        [[ -z "$RUN_HOOK" || -x "$RUN_HOOK" ]] || die "Run hook not executable: $RUN_HOOK"
    fi
    open_files=$(ulimit -Sn)
    if [[ "$open_files" != unlimited ]] && (( open_files < 65536 )); then
        ulimit -Sn 65536 || echo "Could not raise the open-file limit; keeping $(ulimit -Sn)." >&2
    fi
    mkdir -p "$OUTPUT_ROOT" || die "Cannot create output-root"
    RESULTS_DIR=$(mktemp -d "$OUTPUT_ROOT/$(date -u +%Y%m%dT%H%M%SZ).XXXXXX") || die "Cannot create results directory"
    SUMMARY="$RESULTS_DIR/summary.tsv"
    printf 'run_id\trepetition\tmode\ttopic\tpartitions\tclients\tinterval_ms\tnominal_client_rps\tnominal_total_rps\ttotal_msg_attempts\tnominal_produce_s\tstatus\tproducer_exit\treason\tstarted_at\tfinished_at\tlog_dir\n' > "$SUMMARY" || die "Cannot write summary"
    {
        printf 'broker\t%s\nproducer\t%s\nkafka_bin\t%s\n' "$BROKER" "$PRODUCER" "$KAFKA_BIN"
        printf 'partitions\t%s\nclients\t%s\nmodes\t%s\n' "$PARTITIONS" "$CLIENTS_CSV" "$MODES_CSV"
        printf 'interval_ms\t%s\ninterval_noise_stddev\t0\nacks\t0\n' "$INTERVAL_MS"
        printf 'messages_per_client\t%s\npayload_bytes\t%s\nrepetitions\t%s\n' "$MESSAGES_PER_CLIENT" "$MESSAGE_SIZE" "$REPETITIONS"
        printf 'normal_topic\t%s\ngst_topic\t%s\nsingle_topic\t%s\n' "$NORMAL_TOPIC" "$GST_TOPIC" "$SINGLE_TOPIC"
        printf 'replication_factor\t%s\ncreate_topics\t%s\nstart_barrier_delay\t%s\ndrain_wait\t%s\ncooldown\t%s\n' \
            "$REPLICATION_FACTOR" "$CREATE_TOPICS" "$START_BARRIER_DELAY" "$DRAIN_WAIT" "$COOLDOWN"
        printf 'open_files_soft_limit\t%s\n' "$(ulimit -Sn)"
        printf 'run_hook\t%s\n' "$RUN_HOOK"
        printf 'record_prefix\t%s\nsample_log\t%s\n' "$RECORD_PREFIX" "$SAMPLE_LOG"
        printf 'delete_topics_after\t%s\ntopic_delete_timeout\t%s\n' "$DELETE_TOPICS_AFTER" "$TOPIC_DELETE_TIMEOUT"
        printf 'cleanup_only\t%s\n' "$CLEANUP_ONLY"
    } > "$RESULTS_DIR/config.tsv" || die "Cannot write config"
    cp "${BASH_SOURCE[0]}" "$RESULTS_DIR/script.sh" || die "Cannot save script snapshot"
    echo "Results: $RESULTS_DIR"
fi

print_command() { printf 'COMMAND: '; printf '%q ' "$@"; printf '\n'; }
utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

stop_active_command() {
    [[ -n "$ACTIVE_COMMAND_PID" ]] || return 0
    local command_pid=$ACTIVE_COMMAND_PID attempt
    # Each logged command and its tee pipeline share a dedicated job group.
    kill -TERM -- "-$command_pid" 2>/dev/null || :
    for (( attempt=0; attempt<50; attempt++ )); do
        kill -0 -- "-$command_pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -KILL -- "-$command_pid" 2>/dev/null || :
    wait "$command_pid" 2>/dev/null || :
    ACTIVE_COMMAND_PID=""
}

handle_signal() {
    trap '' INT TERM
    echo "Interrupted; stopping the active command before topic cleanup." >&2
    stop_active_command
    exit "$1"
}

run_logged() {
    local log_file=$1 command_exit
    shift
    if (( DRY_RUN )); then print_command "$@"; return 0; fi
    (( ! CLEANUP_TIMED_OUT && ! CLEANUP_INTERRUPTED )) || return 124
    print_command "$@" >> "$log_file" || return 1
    # Job control gives this command a group; the nested pipeline stays in it.
    set -m
    (
        set +m
        trap - INT TERM USR1
        "$@" 2>&1 | tee -a "$log_file"
        pipeline_status=("${PIPESTATUS[@]}")
        printf 'EXIT_CODE: %s\n' "${pipeline_status[0]}" >> "$log_file" || exit 1
        if (( pipeline_status[0] != 0 )); then exit "${pipeline_status[0]}"; fi
        exit "${pipeline_status[1]}"
    ) &
    ACTIVE_COMMAND_PID=$!
    set +m
    if (( CLEANUP_TIMED_OUT || CLEANUP_INTERRUPTED )); then
        stop_active_command
        return 124
    fi
    wait "$ACTIVE_COMMAND_PID" 2>/dev/null
    command_exit=$?
    ACTIVE_COMMAND_PID=""
    return "$command_exit"
}

prepare_topic() {
    local mode=$1 topic=$2 partitions=$3 run_dir=$4
    local actual_partitions actual_rf configs
    TOPIC_FAILURE=""
    if (( CREATE_TOPICS )); then
        local command=("$TOPICS_COMMAND" --create --if-not-exists --topic "$topic" --bootstrap-server "$BROKER" --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" --config cleanup.policy=delete)
        if [[ "$mode" == gst ]]; then command+=(--config global.sequence.enabled=true); fi
        if ! run_logged "$run_dir/create_topic.log" "${command[@]}"; then TOPIC_FAILURE=topic_creation_failed; return 1; fi
    fi
    if ! run_logged "$run_dir/topic.log" "$TOPICS_COMMAND" --describe --topic "$topic" --bootstrap-server "$BROKER"; then
        TOPIC_FAILURE=topic_describe_failed; return 1
    fi
    (( DRY_RUN )) && return 0
    actual_partitions=$(awk -v topic="$topic" '$1=="Topic:" && $2==topic && /PartitionCount:/ {for (i=1; i<NF; i++) if ($i=="PartitionCount:") {print $(i+1); exit}}' "$run_dir/topic.log")
    actual_rf=$(awk -v topic="$topic" '$1=="Topic:" && $2==topic && /PartitionCount:/ {for (i=1; i<NF; i++) if ($i=="ReplicationFactor:") {print $(i+1); exit}}' "$run_dir/topic.log")
    [[ "$actual_partitions" == "$partitions" ]] || { TOPIC_FAILURE="partition_count_expected_${partitions}_got_${actual_partitions:-unknown}"; return 1; }
    [[ "$actual_rf" == "$REPLICATION_FACTOR" ]] || { TOPIC_FAILURE="replication_factor_expected_${REPLICATION_FACTOR}_got_${actual_rf:-unknown}"; return 1; }
    configs=$(awk -v topic="$topic" '$1=="Topic:" && $2==topic && /PartitionCount:/ {sub(/^.*Configs: /, ""); print; exit}' "$run_dir/topic.log")
    case ",$configs," in
        *,global.sequence.enabled=true,*)
            [[ "$mode" == gst ]] || { TOPIC_FAILURE=unexpected_gst_enabled; return 1; } ;;
        *) [[ "$mode" != gst ]] || { TOPIC_FAILURE=gst_not_enabled; return 1; } ;;
    esac
    # Only verified source topics become cleanup targets, even on interruption.
    printf '%s\t%s\t%s\n' "$mode" "$topic" "$partitions" > "$run_dir/topic_ready.tsv" || {
        TOPIC_FAILURE=topic_ready_write_failed; return 1;
    }
    local ready
    for ready in "${READY_TOPICS[@]}"; do [[ "$ready" == "$topic" ]] && return 0; done
    READY_TOPICS+=("$topic")
}

stop_cleanup_watchdog() {
    if [[ -n "$CLEANUP_WATCHDOG_PID" ]]; then
        kill -TERM "$CLEANUP_WATCHDOG_PID" 2>/dev/null || :
        wait "$CLEANUP_WATCHDOG_PID" 2>/dev/null || :
        CLEANUP_WATCHDOG_PID=""
    fi
}

start_cleanup_watchdog() {
    CLEANUP_TIMED_OUT=0
    (
        timer_pid=""
        trap '[[ -z "$timer_pid" ]] || kill "$timer_pid" 2>/dev/null; wait "$timer_pid" 2>/dev/null; exit 0' INT TERM
        sleep "$TOPIC_DELETE_TIMEOUT" &
        timer_pid=$!
        wait "$timer_pid" || exit 0
        kill -USR1 "$$" 2>/dev/null
    ) &
    CLEANUP_WATCHDOG_PID=$!
}

topic_delete_timeout() {
    CLEANUP_TIMED_OUT=1
    stop_active_command
}

interrupt_cleanup() {
    trap '' INT TERM
    CLEANUP_INTERRUPTED=1
    CLEANUP_SIGNAL_EXIT=$1
    stop_active_command
    stop_cleanup_watchdog
}

cleanup_topics() {
    (( DELETE_TOPICS_AFTER )) || return 0
    local mode topic ready prepared status reason started_at log_file list_file command_exit
    local cleanup_exit=0 report="${RESULTS_DIR:-dry-run}/topic_cleanup.tsv"
    if (( ! DRY_RUN )); then
        printf 'topic\tstatus\treason\tstarted_at\tfinished_at\tlog_file\n' > "$report" || return 1
    fi
    trap topic_delete_timeout USR1
    trap 'interrupt_cleanup 130' INT
    trap 'interrupt_cleanup 143' TERM
    for mode in "${MODES[@]}"; do
        case "$mode" in
            normal) topic=$NORMAL_TOPIC ;;
            gst) topic=$GST_TOPIC ;;
            single) topic=$SINGLE_TOPIC ;;
        esac
        if (( DRY_RUN )); then
            echo "After this sweep, delete $topic if its preparation checks passed:"
            print_command "$TOPICS_COMMAND" --bootstrap-server "$BROKER" --delete --if-exists --topic "^${topic//./\\.}$"
            print_command "$TOPICS_COMMAND" --bootstrap-server "$BROKER" --list
            continue
        fi
        prepared=0
        for ready in "${READY_TOPICS[@]}"; do [[ "$ready" == "$topic" ]] && prepared=1; done
        status=SKIPPED_NOT_PREPARED reason=topic_never_passed_preparation_checks
        started_at=$(utc_now)
        log_file="$RESULTS_DIR/topic_cleanup_$topic.log"
        list_file="$RESULTS_DIR/.topic_list_$topic.tmp"
        if (( prepared )); then
            status=ERROR reason=""
            echo "Deleting experiment topic: $topic"
            start_cleanup_watchdog
            # Kafka interprets --topic as a regex. Dots are the only regex
            # metacharacters allowed by the topic-name validation above.
            run_logged "$log_file" "$TOPICS_COMMAND" --bootstrap-server "$BROKER" --delete --if-exists --topic "^${topic//./\\.}$"
            command_exit=$?
            if (( command_exit != 0 )); then
                reason="delete_exit_$command_exit"
            else
                while (( ! CLEANUP_TIMED_OUT && ! CLEANUP_INTERRUPTED )); do
                    run_logged "$log_file" "$TOPICS_COMMAND" --bootstrap-server "$BROKER" --list > "$list_file"
                    command_exit=$?
                    if (( command_exit != 0 )); then reason="list_exit_$command_exit"; break; fi
                    if ! grep -Fxq -- "$topic" "$list_file"; then status=DELETED; break; fi
                    run_logged "$log_file" sleep 1
                done
            fi
            stop_cleanup_watchdog
            if (( CLEANUP_INTERRUPTED )); then
                status=INTERRUPTED reason=controller_interrupted_during_cleanup
            elif (( CLEANUP_TIMED_OUT )); then
                status=ERROR reason=topic_delete_timeout
            fi
            rm -f "$list_file"
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$topic" "$status" "$reason" "$started_at" "$(utc_now)" "$log_file" >> "$report" || cleanup_exit=1
        if [[ "$status" == ERROR ]]; then
            echo "Topic cleanup failed: $topic ($reason); see $log_file" >&2
            cleanup_exit=1
        fi
        (( CLEANUP_INTERRUPTED )) && break
    done
    stop_cleanup_watchdog
    trap '' USR1
    if (( CLEANUP_INTERRUPTED )); then return "$CLEANUP_SIGNAL_EXIT"; fi
    return "$cleanup_exit"
}

finish() {
    local exit_code=$?
    trap - EXIT
    stop_active_command
    cleanup_topics
    local cleanup_exit=$?
    if (( exit_code == 0 && cleanup_exit != 0 )); then exit_code=$cleanup_exit; fi
    exit "$exit_code"
}

execute_rollout() {
    local log_file=$1 command_exit
    ROLLOUT_FAILURES=""
    run_logged "$log_file" "$EXTENSION_COMMAND" --bootstrap-server "$BROKER" --target produce-request-throughput-rollout --operation flush-and-rollout-with-reset
    command_exit=$?
    if (( command_exit != 0 )); then ROLLOUT_FAILURES="throughput_rollout_exit_$command_exit"; fi
    run_logged "$log_file" "$EXTENSION_COMMAND" --bootstrap-server "$BROKER" --target monitor-log-rollout --operation flush-and-rollout
    command_exit=$?
    if (( command_exit != 0 )); then ROLLOUT_FAILURES="${ROLLOUT_FAILURES:+$ROLLOUT_FAILURES; }monitor_rollout_exit_$command_exit"; fi
    [[ -z "$ROLLOUT_FAILURES" ]]
}

run_hook() {
    local phase=$1 run_dir=$2
    [[ -z "$RUN_HOOK" ]] && return 0
    run_logged "$run_dir/${phase}_hook.log" "$RUN_HOOK" "$phase" "$run_dir"
}

SUCCESS_COUNT=0
FAILED_COUNT=0
SKIPPED_COUNT=0
run_experiment() {
    local repetition=$1 clients=$2 mode=$3
    local topic partitions=$PARTITIONS run_id run_dir started_at nominal_total_rps
    local total_msgs=$((clients * MESSAGES_PER_CLIENT)) status=SKIPPED producer_exit=NA reason=""
    case "$mode" in
        normal) topic=$NORMAL_TOPIC ;;
        gst) topic=$GST_TOPIC ;;
        single) topic=$SINGLE_TOPIC; partitions=1 ;;
    esac
    run_id="repeat_${repetition}_clients_${clients}_${mode}"
    run_dir="${RESULTS_DIR:-dry-run}/$run_id"
    started_at=$(utc_now)
    nominal_total_rps=$(awk -v c="$clients" -v ms="$INTERVAL_MS" 'BEGIN {printf "%.6f", 1000*c/ms}')
    printf '\n=== %s: P=%s C=%s nominal_total_rps=%s attempts=%s ===\n' "$run_id" "$partitions" "$clients" "$nominal_total_rps" "$total_msgs"
    if (( ! DRY_RUN )) && ! mkdir -p "$run_dir"; then
        reason=log_directory_failed
    elif ! prepare_topic "$mode" "$topic" "$partitions" "$run_dir"; then
        reason=$TOPIC_FAILURE
    elif ! execute_rollout "$run_dir/prepare_metrics.log"; then
        reason="metrics_not_ready: $ROLLOUT_FAILURES"
    elif ! run_hook before_produce "$run_dir"; then
        reason=before_produce_hook_failed
    else
        local producer_partitions=$(( clients < partitions ? clients : partitions ))
        local producer_command=("$PRODUCER" --broker "$BROKER" --prefix "${RECORD_PREFIX:-$topic}" --partition-cnt "$producer_partitions" --client-cnt "$clients" --interval "$INTERVAL_MS" --interval-noise-stddev 0 --msg-cnt "$total_msgs" --msg-size "$MESSAGE_SIZE" --start-barrier-delay "$START_BARRIER_DELAY" --ignore-response)
        if (( SAMPLE_LOG )); then producer_command+=(--sample-log); fi
        producer_command+=(--tag-record --scrapable --verbose)
        if [[ -n "$RECORD_PREFIX" ]]; then producer_command+=(--topic-name "$topic"); fi
        run_logged "$run_dir/producer.log" "${producer_command[@]}"
        producer_exit=$?
        status=SUCCESS
        if (( producer_exit != 0 )); then status=FAILED; reason="producer_exit_$producer_exit"; fi
        if (( ! DRY_RUN )); then sleep "$DRAIN_WAIT"; fi
        if ! execute_rollout "$run_dir/rollout.log"; then
            status=FAILED
            reason="${reason:+$reason; }$ROLLOUT_FAILURES"
        fi
        if ! run_hook after_rollout "$run_dir"; then
            status=FAILED
            reason="${reason:+$reason; }after_rollout_hook_failed"
        fi
        if (( ! DRY_RUN )); then sleep "$COOLDOWN"; fi
    fi
    if (( DRY_RUN )); then return 0; fi
    case "$status" in
        SUCCESS) SUCCESS_COUNT=$((SUCCESS_COUNT + 1)) ;;
        FAILED) FAILED_COUNT=$((FAILED_COUNT + 1)) ;;
        SKIPPED) SKIPPED_COUNT=$((SKIPPED_COUNT + 1)) ;;
    esac
    local row=("$run_id" "$repetition" "$mode" "$topic" "$partitions" "$clients" "$INTERVAL_MS" "$NOMINAL_CLIENT_RPS" "$nominal_total_rps" "$total_msgs" "$NOMINAL_PRODUCE_S" "$status" "$producer_exit" "$reason" "$started_at" "$(utc_now)" "$run_dir")
    (IFS=$'\t'; printf '%s\n' "${row[*]}") >> "$SUMMARY" || die "Cannot append summary"
    echo "=== $run_id: $status ${reason:+($reason)} ==="
}

trap finish EXIT
trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM

if (( CLEANUP_ONLY )); then
    cleanup_prepare_failed=0
    for mode in "${MODES[@]}"; do
        cleanup_partitions=$PARTITIONS
        case "$mode" in
            normal) topic=$NORMAL_TOPIC ;;
            gst) topic=$GST_TOPIC ;;
            single) topic=$SINGLE_TOPIC; cleanup_partitions=1 ;;
        esac
        run_dir="${RESULTS_DIR:-dry-run}/cleanup_$mode"
        if (( ! DRY_RUN )) && ! mkdir -p "$run_dir"; then
            cleanup_prepare_failed=1
        elif ! prepare_topic "$mode" "$topic" "$cleanup_partitions" "$run_dir"; then
            echo "Topic cleanup preparation failed: $topic ($TOPIC_FAILURE)" >&2
            cleanup_prepare_failed=1
        fi
    done
    exit "$cleanup_prepare_failed"
fi

for (( repetition=1; repetition<=REPETITIONS; repetition++ )); do
    for clients in "${CLIENT_COUNTS[@]}"; do
        for mode in "${MODES[@]}"; do
            run_experiment "$repetition" "$clients" "$mode"
        done
    done
done
if (( DRY_RUN )); then echo "Dry run complete; no broker/producer commands executed or results written."; exit 0; fi
printf '\nCompleted: success=%d failed=%d skipped=%d\nSummary: %s\n' "$SUCCESS_COUNT" "$FAILED_COUNT" "$SKIPPED_COUNT" "$SUMMARY"
if (( FAILED_COUNT + SKIPPED_COUNT > 0 )); then exit 1; fi
