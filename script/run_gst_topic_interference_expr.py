#!/usr/bin/env python3
"""Compare a constant B workload alone and alongside three A configurations."""

import argparse
import csv
import datetime
import json
import math
import os
from pathlib import Path
import re
import resource
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
CASES = ("baseline", "single", "normal", "gst")


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def positive_int(value):
    if not re.fullmatch(r"[1-9][0-9]*", value) or int(value) > 2147483647:
        raise argparse.ArgumentTypeError("expected an integer in 1..2147483647")
    return int(value)


def seconds(value):
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    return number


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Both workloads use the unchanged producer app with acks=0 and zero interval noise. "
        "Missing B-specific metrics remain null; global Produce latency is not B latency.",
    )
    parser.add_argument("--broker", default="166.104.110.13:9092")
    parser.add_argument("--partitions", type=positive_int, default=16, help="A normal/GST physical and active P*")
    parser.add_argument("--a-clients", type=positive_int, default=160)
    parser.add_argument("--a-interval", type=seconds, default=3, help="A per-client interval, milliseconds")
    parser.add_argument("--b-partitions", type=positive_int, default=1)
    parser.add_argument("--b-clients", type=positive_int, default=1)
    parser.add_argument("--b-interval", type=seconds, default=10, help="B per-client interval, milliseconds")
    parser.add_argument("--message-size", type=positive_int, default=1000, help="payload bytes for both workloads")
    parser.add_argument("--duration", type=seconds, default=60, help="measurement window in seconds, controlled by this script")
    parser.add_argument("--warmup", type=seconds, default=30, help="warmup after both processes have been launched")
    parser.add_argument("--repetitions", type=positive_int, default=3)
    parser.add_argument("--cases", default=",".join(CASES), help="comma-separated subset/order: baseline,single,normal,gst")
    parser.add_argument("--a-single-topic", default="impact_a_single")
    parser.add_argument("--a-normal-topic", help="default: impact_a_normal_p<P*>")
    parser.add_argument("--a-gst-topic", help="default: impact_a_gst_p<P*>")
    parser.add_argument("--b-topic", default="impact_b")
    parser.add_argument("--create-topics", action="store_true", help="create missing topics; never delete existing data")
    parser.add_argument("--replication-factor", type=positive_int, default=1, help="required source-topic replication factor")
    parser.add_argument("--producer", type=Path, default=ROOT / "bin/single_topic_throughput_expr_producer")
    parser.add_argument("--kafka-bin", type=Path, default=ROOT.parent / "kafka_sandbox/bin")
    parser.add_argument("--metrics-hook", type=Path, help="optional executable called with start/end; see docs/GST_TOPIC_INTERFERENCE.md")
    parser.add_argument("--output-root", type=Path, default=ROOT / "out/gst_topic_interference")
    parser.add_argument("--start-barrier-delay", type=int, default=2, help="internal app barrier, not a cross-process barrier")
    parser.add_argument("--command-timeout", type=seconds, default=30)
    parser.add_argument("--stop-grace", type=seconds, default=30, help="TERM grace period before KILL")
    parser.add_argument("--drain-wait", type=seconds, default=20, help="post-stop wait before a separate tail rollout")
    parser.add_argument("--cooldown", type=seconds, default=60)
    parser.add_argument("--dry-run", action="store_true", help="print the plan; no broker/producer calls or output files")
    args = parser.parse_args()
    args.cases = args.cases.split(",")
    if len(set(args.cases)) != len(args.cases) or any(case not in CASES for case in args.cases):
        parser.error("cases must contain distinct names from baseline,single,normal,gst")
    if any(value <= 0 for value in (args.a_interval, args.b_interval, args.duration, args.command_timeout)):
        parser.error("intervals, duration and command-timeout must be positive")
    if args.start_barrier_delay < 0 or args.start_barrier_delay > 2147483647:
        parser.error("start-barrier-delay must be in 0..2147483647")
    if args.replication_factor > 32767:
        parser.error("replication-factor must be <= 32767")
    required_a_clients = args.partitions if any(c in args.cases for c in ("normal", "gst")) else 1
    if args.a_clients < required_a_clients or args.b_clients < args.b_partitions:
        parser.error("each selected workload needs clients >= active partitions to preserve the per-client rate")
    args.a_normal_topic = args.a_normal_topic or "impact_a_normal_p{}".format(args.partitions)
    args.a_gst_topic = args.a_gst_topic or "impact_a_gst_p{}".format(args.partitions)
    topics = [args.b_topic] + [getattr(args, "a_{}_topic".format(case)) for case in args.cases if case != "baseline"]
    if len(set(topics)) != len(topics) or any(not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", t) or t in (".", "..") for t in topics):
        parser.error("selected A/B topics must have distinct valid Kafka names")
    if not args.broker:
        parser.error("broker must not be empty")
    # The app remains count-based. Budget for warmup, measurement, collection and startup.
    budget_s = args.warmup + args.duration + args.start_barrier_delay + 6 * args.command_timeout + 60
    for role in ("a", "b"):
        estimate = budget_s * 1000 / getattr(args, role + "_interval")
        if not math.isfinite(estimate) or estimate > 2147483647 // getattr(args, role + "_clients"):
            parser.error("{} attempt budget exceeds the app's --msg-cnt range; adjust the profile".format(role.upper()))
        count = max(1, math.ceil(estimate))
        setattr(args, role + "_messages_per_client", count)
    for name in ("producer", "kafka_bin", "output_root", "metrics_hook"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.resolve())
    return args


class RunError(Exception):
    pass


class Cancelled(BaseException):
    def __init__(self, signum):
        self.signum = signum


class Experiment:
    def __init__(self, args):
        self.args = args
        self.active = set()
        self.results = []
        self.cancelled = False
        self.output = None

    def a_topic(self, case):
        return None if case == "baseline" else getattr(self.args, "a_{}_topic".format(case))

    def producer_command(self, role, case):
        a = self.args
        topic = a.b_topic if role == "b" else self.a_topic(case)
        partitions = a.b_partitions if role == "b" else 1 if case == "single" else a.partitions
        clients = getattr(a, role + "_clients")
        return [str(a.producer), "--broker", a.broker, "--topic-name", topic,
                "--prefix", "probe_B" if role == "b" else "load_A",
                "--partition-cnt", str(partitions), "--client-cnt", str(clients),
                "--interval", str(getattr(a, role + "_interval")), "--interval-noise-stddev", "0",
                "--msg-cnt", str(clients * getattr(a, role + "_messages_per_client")),
                "--msg-size", str(a.message_size), "--start-barrier-delay", str(a.start_barrier_delay),
                "--ignore-response", "--sample-log", "--tag-record", "--scrapable", "--verbose"]

    def event(self, directory, phase, **fields):
        record = dict(phase=phase, utc=utc_now(), monotonic_ns=time.monotonic_ns(), **fields)
        with (directory / "events.jsonl").open("a") as output:
            output.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def stop(self, processes):
        processes = list(processes)
        forced = set()
        for process in processes:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + self.args.stop_grace
        while any(p.poll() is None for p in processes) and time.monotonic() < deadline:
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        for process in processes:
            if process.poll() is None:
                forced.add(process.pid)
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait()
            self.active.discard(process)
        return forced

    def command(self, command, directory, label, env=None, json_output=False):
        self.event(directory, "command_start", label=label, argv=command)
        stdout_path = directory / (label + (".json" if json_output else ".log"))
        stderr_path = directory / (label + ".stderr.log")
        before = time.monotonic_ns()
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                                       start_new_session=True, env=env)
            self.active.add(process)
            try:
                code = process.wait(timeout=self.args.command_timeout)
            except subprocess.TimeoutExpired:
                self.stop([process])
                code = 124
            except BaseException:
                self.stop([process])
                raise
            finally:
                self.active.discard(process)
        after = time.monotonic_ns()
        self.event(directory, "command_end", label=label, exit_code=code)
        if code:
            raise RunError("{}:exit_{}".format(label, code))
        return stdout_path, (before + after) // 2

    def rollout(self, directory, phase):
        failures = []
        for target, operation in (("produce-request-throughput-rollout", "flush-and-rollout-with-reset"),
                                  ("monitor-log-rollout", "flush-and-rollout")):
            try:
                self.command([str(self.args.kafka_bin / "kafka-broker-extension.sh"),
                              "--bootstrap-server", self.args.broker, "--target", target, "--operation", operation],
                             directory, phase + "_" + target)
            except (RunError, OSError) as error:
                failures.append(str(error))
        if failures:
            raise RunError("; ".join(failures))

    def prepare_topic(self, directory, topic, partitions, gst):
        a = self.args
        command = [str(a.kafka_bin / "kafka-topics.sh"), "--bootstrap-server", a.broker, "--topic", topic]
        label = "topic_B" if topic == a.b_topic else "topic_A"
        if a.create_topics:
            create = command + ["--create", "--if-not-exists", "--partitions", str(partitions),
                                "--replication-factor", str(a.replication_factor), "--config", "cleanup.policy=delete"]
            if gst:
                create += ["--config", "global.sequence.enabled=true"]
            self.command(create, directory, label + "_create")
        path, _ = self.command(command + ["--describe"], directory, label + "_describe")
        header = next((line for line in path.read_text(errors="replace").splitlines()
                       if line.split()[:2] == ["Topic:", topic] and "PartitionCount:" in line), "")
        count = re.search(r"\bPartitionCount:\s*(\d+)", header)
        rf = re.search(r"\bReplicationFactor:\s*(\d+)", header)
        configs = header.split("Configs:", 1)[-1].strip().split(",")
        if not count or int(count.group(1)) != partitions:
            raise RunError("{}:partition_count_mismatch_expected_{}".format(topic, partitions))
        if not rf or int(rf.group(1)) != a.replication_factor:
            raise RunError("{}:replication_factor_mismatch".format(topic))
        if ("global.sequence.enabled=true" in configs) != gst:
            raise RunError("{}:gst_setting_mismatch".format(topic))

    def check_live(self, roles):
        for role, process in roles.items():
            code = process.poll()
            if code is not None:
                raise RunError("{}_exited_before_collection_finished:exit_{}".format(role, code))

    def wait_live(self, seconds_to_wait, roles):
        deadline = time.monotonic() + seconds_to_wait
        while time.monotonic() < deadline:
            self.check_live(roles)
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        self.check_live(roles)

    def hook(self, directory, phase, case, roles):
        if self.args.metrics_hook is None:
            return None
        env = dict(os.environ, GST_EXPR_BROKER=self.args.broker, GST_EXPR_B_TOPIC=self.args.b_topic,
                   GST_EXPR_RUN_ID=directory.name, GST_EXPR_RUN_DIR=str(directory), GST_EXPR_CASE=case,
                   GST_EXPR_A_TOPIC=self.a_topic(case) or "", GST_EXPR_A_PREFIX="load_A", GST_EXPR_B_PREFIX="probe_B")
        path, midpoint = self.command([str(self.args.metrics_hook), phase], directory, "metrics_" + phase,
                                      env=env, json_output=True)
        self.check_live(roles)
        try:
            data = json.loads(path.read_text())
        except (ValueError, OSError) as error:
            raise RunError("metrics_{}:invalid_json:{}".format(phase, error))
        if not isinstance(data, dict) or data.get("topic") != self.args.b_topic:
            raise RunError("metrics_{}:expected_topic_{}".format(phase, self.args.b_topic))
        return data, midpoint

    def b_metrics(self, start, end):
        result = dict(b_metrics_status="not_configured", b_store_records=None, b_counter_window_s=None,
                      b_store_rps=None, b_latency_p95_ms=None, b_latency_p99_ms=None, b_latency_kind=None)
        if start is None:
            return result
        first, first_time = start
        last, last_time = end
        result["b_metrics_status"] = "partial"
        counts = [data.get("store_success_total") for data in (first, last)]
        if any(count is not None for count in counts):
            if any(type(count) is not int or not 0 <= count <= 2**64 - 1 for count in counts) or counts[1] < counts[0]:
                raise RunError("metrics:invalid_or_reset_store_counter")
            timestamps = [data.get("timestamp_ns") for data in (first, last)]
            if any(stamp is not None for stamp in timestamps):
                if any(type(stamp) is not int or not 0 <= stamp <= 2**64 - 1 for stamp in timestamps):
                    raise RunError("metrics:timestamp_ns_required_in_both_snapshots")
                first_time, last_time = timestamps
                result["b_counter_clock"] = "hook_timestamp_ns"
            else:
                result["b_counter_clock"] = "controller_command_midpoint"
            elapsed = (last_time - first_time) / 1e9
            if elapsed <= 0:
                raise RunError("metrics:nonpositive_counter_window")
            result.update(b_store_records=counts[1] - counts[0], b_counter_window_s=elapsed,
                          b_store_rps=(counts[1] - counts[0]) / elapsed)
        for percentile in (95, 99):
            key = "latency_p{}_ms".format(percentile)
            value = last.get(key)
            if value is not None:
                try:
                    valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
                except OverflowError:
                    valid = False
                if not valid:
                    raise RunError("metrics:invalid_{}".format(key))
                if (last.get("latency_scope") != "topic" or last.get("latency_window") != "measurement"
                        or not isinstance(last.get("latency_kind"), str) or not last["latency_kind"]):
                    raise RunError("metrics:B_latency_requires_topic_scope_measurement_window_and_kind")
                result["b_" + key] = value
                result["b_latency_kind"] = last["latency_kind"]
        if result["b_store_rps"] is not None and result["b_latency_p99_ms"] is not None:
            result["b_metrics_status"] = "collected"
        return result

    def run_case(self, repetition, case):
        a = self.args
        run_id = "repeat_{}_{}".format(repetition, case)
        directory = self.output / run_id
        directory.mkdir()
        result = dict(run_id=run_id, repetition=repetition, case=case, status="SUCCESS", reason="",
                      a_topic=self.a_topic(case), a_partitions=None if case == "baseline" else 1 if case == "single" else a.partitions,
                      a_clients=0 if case == "baseline" else a.a_clients,
                      a_interval_ms=a.a_interval, a_nominal_rps=0 if case == "baseline" else a.a_clients * 1000 / a.a_interval,
                      b_topic=a.b_topic, b_partitions=a.b_partitions, b_clients=a.b_clients,
                      b_interval_ms=a.b_interval, b_nominal_rps=a.b_clients * 1000 / a.b_interval,
                      warmup_s=a.warmup, requested_measurement_s=a.duration, measured_s=None,
                      measurement_start_utc=None, measurement_end_utc=None, a_exit=None, b_exit=None,
                      started_at=utc_now(), log_dir=str(directory), forced_kill_roles=[])
        result.update(self.b_metrics(None, None))
        if a.metrics_hook is not None:
            result["b_metrics_status"] = "not_collected"
        roles, streams = {}, []
        print("=== {}: B={}, A={} ===".format(run_id, a.b_topic, self.a_topic(case) or "off"), flush=True)
        try:
            self.prepare_topic(directory, a.b_topic, a.b_partitions, False)
            if case != "baseline":
                self.prepare_topic(directory, self.a_topic(case), result["a_partitions"], case == "gst")
            self.rollout(directory, "prepare")
            for role in (("b",) if case == "baseline" else ("b", "a")):
                command = self.producer_command(role, case)
                stream = (directory / (role + "_producer.log")).open("w")
                streams.append(stream)
                self.event(directory, "producer_start", role=role, argv=command)
                roles[role] = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                               stdin=subprocess.DEVNULL, start_new_session=True)
                self.active.add(roles[role])
            self.event(directory, "warmup_start")
            self.wait_live(a.warmup, roles)
            self.rollout(directory, "measurement_start")
            self.check_live(roles)
            first = self.hook(directory, "start", case, roles)
            beginning = self.event(directory, "measurement_start")
            result["measurement_start_utc"] = beginning["utc"]
            print("{}: measuring {} s".format(run_id, a.duration), flush=True)
            self.wait_live(a.duration, roles)
            ending = self.event(directory, "measurement_end")
            result["measurement_end_utc"] = ending["utc"]
            result["measured_s"] = (ending["monotonic_ns"] - beginning["monotonic_ns"]) / 1e9
            last = self.hook(directory, "end", case, roles)
            self.rollout(directory, "measurement_end")
            self.check_live(roles)
            result.update(self.b_metrics(first, last))
        except (RunError, OSError) as error:
            result.update(status="FAILED" if roles else "SKIPPED", reason=str(error))
        except Cancelled:
            result.update(status="INTERRUPTED", reason="controller_interrupted")
            raise
        finally:
            # Signal both workloads before waiting: neither can bleed into the next case.
            self.event(directory, "producer_stop_requested")
            forced = self.stop(roles.values())
            for role, process in roles.items():
                result[role + "_exit"] = process.returncode
                if process.pid in forced:
                    result["forced_kill_roles"].append(role)
                elif process.returncode not in (0, -signal.SIGTERM):
                    result["reason"] += "; {}_exit_{}".format(role, process.returncode)
                    if result["status"] == "SUCCESS":
                        result["status"] = "FAILED"
            if forced:
                result["reason"] += "; producer_stop_grace_exceeded"
                if result["status"] == "SUCCESS":
                    result["status"] = "FAILED"
            for stream in streams:
                stream.close()
            self.event(directory, "producers_stopped", a_exit=result["a_exit"], b_exit=result["b_exit"])
            if roles and not self.cancelled:
                time.sleep(a.drain_wait)
                try:
                    self.rollout(directory, "post_stop")
                except (RunError, OSError) as error:
                    result["status"] = "FAILED"
                    result["reason"] += "; post_stop: " + str(error)
            result["finished_at"] = utc_now()
            result["reason"] = result["reason"].strip("; ")
            self.results.append(result)
            self.write_results()
        print("=== {}: {} {} ===".format(run_id, result["status"], result["reason"]), flush=True)
        if roles:
            time.sleep(a.cooldown)

    def write_results(self):
        baselines = {r["repetition"]: r for r in self.results if r["case"] == "baseline" and r["status"] == "SUCCESS"}
        for result in self.results:
            baseline = baselines.get(result["repetition"])
            result.update(b_baseline_run_id=baseline["run_id"] if baseline else None,
                          b_store_ratio_to_baseline=None, b_latency_p99_delta_ms=None)
            if baseline and result["status"] == "SUCCESS":
                if baseline["b_store_rps"] and result["b_store_rps"] is not None:
                    result["b_store_ratio_to_baseline"] = result["b_store_rps"] / baseline["b_store_rps"]
                if (baseline["b_latency_p99_ms"] is not None and result["b_latency_p99_ms"] is not None
                        and baseline["b_latency_kind"] == result["b_latency_kind"]):
                    result["b_latency_p99_delta_ms"] = result["b_latency_p99_ms"] - baseline["b_latency_p99_ms"]
            (Path(result["log_dir"]) / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        fields = list(dict.fromkeys(key for result in self.results for key in result))
        if fields:
            with (self.output / "summary.tsv").open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=fields, delimiter="\t")
                writer.writeheader()
                writer.writerows(self.results)

    def run(self):
        a = self.args
        if a.dry_run:
            print("DRY RUN: no broker/producer calls or output files")
            print("cases={} repetitions={} warmup={}s measurement={}s acks=0".format(a.cases, a.repetitions, a.warmup, a.duration))
            for case in a.cases:
                print("\nCASE {}: B partitions={}, A partitions={}".format(case, a.b_partitions, "off" if case == "baseline" else 1 if case == "single" else a.partitions))
                for role in (("b",) if case == "baseline" else ("b", "a")):
                    print("{}: {}".format(role.upper(), " ".join(shlex.quote(part) for part in self.producer_command(role, case))))
            print("Topic creation={} (if missing only). Metrics hook={}.".format(a.create_topics, a.metrics_hook or "none; B metrics remain null"))
            return 0
        for executable in (a.producer, a.kafka_bin / "kafka-topics.sh", a.kafka_bin / "kafka-broker-extension.sh", a.metrics_hook):
            if executable is not None and (not executable.is_file() or not os.access(str(executable), os.X_OK)):
                raise RunError("executable not found: {}".format(executable))
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 65536 if hard == resource.RLIM_INFINITY else min(65536, hard)
        if soft != resource.RLIM_INFINITY and soft < target:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            except OSError as error:
                print("Could not raise open-file limit: {}".format(error), file=sys.stderr)
        a.output_root.mkdir(parents=True, exist_ok=True)
        self.output = Path(tempfile.mkdtemp(prefix=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ."), dir=str(a.output_root)))
        config = {key: str(value) if isinstance(value, Path) else value for key, value in vars(a).items()}
        config.update(acks=0, interval_noise_stddev=0, generator_host=socket.gethostname(),
                      open_files_soft_limit=resource.getrlimit(resource.RLIMIT_NOFILE)[0],
                      b_latency_note="Requires B-specific broker metrics; global Produce latency is not B latency")
        (self.output / "config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
        (self.output / "script.py").write_bytes(Path(__file__).read_bytes())
        print("Results: {}".format(self.output), flush=True)
        for repetition in range(1, a.repetitions + 1):
            for case in a.cases:
                self.run_case(repetition, case)
        counts = {status: sum(r["status"] == status for r in self.results) for status in ("SUCCESS", "FAILED", "SKIPPED")}
        print("Completed: {}\nSummary: {}".format(counts, self.output / "summary.tsv"), flush=True)
        return 1 if any(r["status"] != "SUCCESS" for r in self.results) else 0


def main():
    experiment = Experiment(parse_args())

    def cancel(signum, frame):
        experiment.cancelled = True
        raise Cancelled(signum)

    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    try:
        return experiment.run()
    except Cancelled as error:
        print("Interrupted; stopping owned child process groups.", file=sys.stderr)
        return 128 + error.signum
    except (RunError, OSError) as error:
        print("Error: {}".format(error), file=sys.stderr)
        return 2
    finally:
        experiment.stop(experiment.active)


if __name__ == "__main__":
    sys.exit(main())
