#!/usr/bin/env python3
"""Measure tagged topic B beside untagged load A using the fixed-partition runner."""

import argparse
import csv
import datetime
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time

from gst_interference_logs import DataError, capture, snapshot


ROOT = Path(__file__).resolve().parent.parent
CASES = ("baseline", "single", "normal", "gst")


class RunError(Exception):
    pass


class CleanupError(RunError):
    pass


class Cancelled(BaseException):
    pass


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def positive_int(value):
    if not value.isdecimal() or not 0 < int(value) <= 2147483647:
        raise argparse.ArgumentTypeError("expected an integer in 1..2147483647")
    return int(value)


def seconds(value):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    return result


def client_counts(value):
    counts = [positive_int(part.strip()) for part in value.split(",")]
    if any(previous >= current for previous, current in zip(counts, counts[1:])):
        raise argparse.ArgumentTypeError("A client counts must be distinct and strictly increasing")
    return counts


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, epilog="B always uses one client and one ordinary partition. No warmup or B metrics collector is required.")
    p.add_argument("--broker", default="166.104.110.13:9092")
    p.add_argument("--partitions", type=positive_int, default=16, help="A normal/GST partition count")
    p.add_argument("--a-clients", type=client_counts, default="160", help="increasing A client counts, e.g. 16,32,64,128,160")
    p.add_argument("--a-interval", type=seconds, default=3, help="milliseconds per A client")
    p.add_argument("--b-interval", type=seconds, default=10, help="milliseconds per B client")
    p.add_argument("--b-messages-per-client", type=positive_int, default=6000, help="B attempts; default nominal production time is 60 seconds")
    p.add_argument("--message-size", type=positive_int, default=1000)
    p.add_argument("--repetitions", type=positive_int, default=3)
    p.add_argument("--cases", default=",".join(CASES))
    p.add_argument("--a-single-topic", default="impact_a_single")
    p.add_argument("--a-normal-topic", help="default: impact_a_normal_p<P>")
    p.add_argument("--a-gst-topic", help="default: impact_a_gst_p<P>")
    p.add_argument("--b-topic", default="impact_b")
    p.add_argument("--replication-factor", type=positive_int, default=1)
    p.add_argument("--create-topics", action="store_true", default=True,
                   help="compatibility flag: missing experiment topics are always created/recreated")
    p.add_argument("--delete-topics-after", action="store_true", default=True,
                   help="compatibility flag: delete A/B topics after all cases/repetitions at each A client count")
    p.add_argument("--topic-delete-timeout", type=seconds, default=120)
    p.add_argument("--producer", type=Path, default=ROOT / "bin/single_topic_throughput_expr_producer")
    p.add_argument("--runner", type=Path, default=ROOT / "script/run_gst_fixed_partition_expr.sh")
    p.add_argument("--kafka-bin", type=Path, default=ROOT.parent / "kafka_sandbox/bin")
    p.add_argument("--broker-log-dir", type=Path, default=Path("broker"))
    p.add_argument("--throughput-glob", help="optional throughput filename glob, including the unnumbered file")
    p.add_argument("--latency-glob", help="optional latency filename glob; default detects headers in .csv/.log")
    p.add_argument("--csv-wait", type=seconds, default=10)
    p.add_argument("--csv-stable", type=seconds, default=0.5)
    p.add_argument("--start-barrier-delay", type=int, default=2, help="existing producer startup barrier, not an added warmup")
    p.add_argument("--drain-wait", type=int, default=20)
    p.add_argument("--cooldown", type=seconds, default=60)
    p.add_argument("--command-timeout", type=seconds, default=30)
    p.add_argument("--run-timeout", type=seconds, help="complete B runner time limit, excluding the outer cooldown")
    p.add_argument("--stop-grace", type=seconds, default=10)
    p.add_argument("--output-root", type=Path, default=ROOT / "out/gst_interference")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    a.cases = a.cases.split(",")
    if len(set(a.cases)) != len(a.cases) or any(case not in CASES for case in a.cases):
        p.error("cases must be distinct names from baseline,single,normal,gst")
    if any(not 0 < value < 1e9 for value in (a.a_interval, a.b_interval, a.command_timeout, a.topic_delete_timeout)):
        p.error("intervals/timeouts must be positive and < 1e9")
    if a.csv_stable > a.csv_wait or a.replication_factor > 32767:
        p.error("invalid CSV timing or replication factor")
    if not 0 <= a.start_barrier_delay <= 2147483647 or not 0 <= a.drain_wait <= 2147483647:
        p.error("startup/drain waits must be in 0..2147483647")
    a.a_normal_topic = a.a_normal_topic or "impact_a_normal_p{}".format(a.partitions)
    a.a_gst_topic = a.a_gst_topic or "impact_a_gst_p{}".format(a.partitions)
    topics = [a.b_topic] + [getattr(a, "a_" + case + "_topic") for case in a.cases if case != "baseline"]
    if len(set(topics)) != len(topics) or any(not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic) or topic in (".", "..", "__global_sequence_index") for topic in topics):
        p.error("experiment topics must be distinct valid source-topic names")
    for name in ("throughput_glob", "latency_glob"):
        glob = getattr(a, name)
        if glob is not None and (not glob or Path(glob).is_absolute() or ".." in Path(glob).parts):
            p.error("log globs must stay inside broker-log-dir")
    for name in ("producer", "runner", "kafka_bin", "broker_log_dir", "output_root"):
        setattr(a, name, getattr(a, name).resolve())
    if a.output_root == a.broker_log_dir or a.output_root in a.broker_log_dir.parents or a.broker_log_dir in a.output_root.parents:
        p.error("broker-log-dir and output-root must be separate directory trees")
    if not a.broker:
        p.error("broker must not be empty")
    nominal = a.b_messages_per_client * a.b_interval / 1000
    if not math.isfinite(nominal):
        p.error("nominal B duration must be finite")
    if a.run_timeout is None:
        a.run_timeout = max(300, nominal + a.start_barrier_delay + a.drain_wait + a.csv_wait + 4 * a.command_timeout + 60)
    if a.run_timeout <= 0:
        p.error("run-timeout must be positive")
    budget = (a.run_timeout + a.command_timeout + 60) * 1000 / a.a_interval
    if not math.isfinite(budget) or budget > 2147483647 // max(a.a_clients):
        p.error("A attempt budget exceeds the producer's count limit")
    a.a_messages_per_client = max(1, math.ceil(budget))
    return a


class Experiment:
    def __init__(self, args):
        self.args, self.output = args, None
        self.a_process = self.b_process = self.command_process = None
        self.results = []
        self.cancelled = False
        self.finalizing = False
        self.cleaning = False
        self.pending_topics = {}
        self.cleanups = []

    def a_topic(self, case):
        return None if case == "baseline" else getattr(self.args, "a_" + case + "_topic")

    def a_partitions(self, case):
        return 1 if case == "single" else self.args.partitions

    def a_command(self, case, prefix, clients):
        a = self.args
        producer_partitions = min(clients, self.a_partitions(case))
        return [str(a.producer), "--broker", a.broker, "--topic-name", self.a_topic(case),
                "--prefix", "load_A_" + prefix, "--partition-cnt", str(producer_partitions),
                "--client-cnt", str(clients), "--interval", str(a.a_interval),
                "--interval-noise-stddev", "0", "--msg-cnt", str(clients * a.a_messages_per_client),
                "--msg-size", str(a.message_size), "--start-barrier-delay", str(a.start_barrier_delay),
                "--ignore-response", "--sample-log"]

    def b_command(self, directory, prefix):
        a = self.args
        command = ["/bin/bash", str(a.runner), "--broker", a.broker, "--partitions", "1", "--clients", "1",
                   "--modes", "normal", "--normal-topic", a.b_topic, "--interval", str(a.b_interval),
                   "--messages-per-client", str(a.b_messages_per_client), "--message-size", str(a.message_size),
                   "--repetitions", "1", "--replication-factor", str(a.replication_factor),
                   "--producer", str(a.producer), "--kafka-bin", str(a.kafka_bin),
                   "--output-root", str(directory / "runner"), "--record-prefix", prefix, "--no-sample-log",
                   "--start-barrier-delay", str(a.start_barrier_delay), "--drain-wait", str(a.drain_wait),
                   "--cooldown", "0", "--run-hook", str(Path(__file__).resolve())]
        if a.create_topics:
            command.append("--create-topics")
        return command

    def event(self, directory, event, **values):
        with (directory / "events.jsonl").open("a") as output:
            output.write(json.dumps(dict(event=event, utc=utc_now(), **values)) + "\n")

    def stop(self, process, grace=None):
        if process is None:
            return False
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        forced = False
        try:
            process.wait(timeout=self.args.stop_grace if grace is None else grace)
        except subprocess.TimeoutExpired:
            forced = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        return forced

    def stop_all(self):
        self.stop(self.command_process)
        self.stop(self.b_process, self.args.stop_grace + 6)
        self.stop(self.a_process)
        self.command_process = self.b_process = self.a_process = None

    def command(self, command, directory, label, timeout=None):
        (directory / (label + "_command.json")).write_text(json.dumps(command, indent=2) + "\n")
        with (directory / (label + ".log")).open("w") as output:
            self.command_process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = self.command_process.wait(timeout=timeout or self.args.command_timeout)
            except subprocess.TimeoutExpired:
                raise RunError(label + " timed out")
            finally:
                self.stop(self.command_process)
                self.command_process = None
        if code:
            raise RunError("{} exited {}".format(label, code))

    def prepare_a(self, case, directory):
        a, topic, partitions = self.args, self.a_topic(case), self.a_partitions(case)
        base = [str(a.kafka_bin / "kafka-topics.sh"), "--bootstrap-server", a.broker, "--topic", topic]
        if a.create_topics:
            command = base + ["--create", "--if-not-exists", "--partitions", str(partitions),
                              "--replication-factor", str(a.replication_factor), "--config", "cleanup.policy=delete"]
            if case == "gst":
                command += ["--config", "global.sequence.enabled=true"]
            self.command(command, directory, "topic_a_create")
        self.command(base + ["--describe"], directory, "topic_a_describe")
        lines = (directory / "topic_a_describe.log").read_text().splitlines()
        line = next((line for line in lines if line.split()[:2] == ["Topic:", topic] and "PartitionCount:" in line), "")
        count = re.search(r"\bPartitionCount:\s*(\d+)", line)
        rf = re.search(r"\bReplicationFactor:\s*(\d+)", line)
        configs = line.split("Configs:", 1)[-1].strip().split(",")
        if not count or int(count.group(1)) != partitions or not rf or int(rf.group(1)) != a.replication_factor:
            raise RunError("A topic partition/replication mismatch")
        if ("global.sequence.enabled=true" in configs) != (case == "gst"):
            raise RunError("A topic GST setting mismatch")

    def cleanup_commands(self, directory, topics=None):
        a = self.args
        # Pack compatible source topics into the runner's normal/gst/single
        # slots. A-single and A-normal can require separate normal slots.
        groups = []
        for topic, (mode, partitions) in (self.pending_topics if topics is None else topics).items():
            group = next((group for group in groups if mode not in group["modes"]
                          and (mode == "single" or group["partitions"] == partitions)), None)
            if group is None:
                group = dict(partitions=partitions, modes={})
                groups.append(group)
            group["modes"][mode] = topic
        commands = []
        for index, group in enumerate(groups):
            command = ["/bin/bash", str(a.runner), "--cleanup-only", "--broker", a.broker,
                       "--partitions", str(group["partitions"]), "--replication-factor", str(a.replication_factor),
                       "--kafka-bin", str(a.kafka_bin), "--topic-delete-timeout", str(a.topic_delete_timeout),
                       "--output-root", str(directory / ("runner_" + str(index))), "--modes", ",".join(group["modes"])]
            for mode, topic in group["modes"].items():
                command += ["--" + mode + "-topic", topic]
            commands.append((command, list(group["modes"].values())))
        return commands

    def cleanup(self, command, targets, directory, label):
        try:
            self.command(command, directory, label, timeout=len(targets) * self.args.topic_delete_timeout + 60)
        except RunError as error:
            # A failed preparation or an interrupted deletion can leave some
            # targets already absent. Confirm absence using a successful list,
            # rather than treating any describe/connection failure as deletion.
            self.command([str(self.args.kafka_bin / "kafka-topics.sh"), "--bootstrap-server", self.args.broker, "--list"],
                         directory, label + "_verify")
            listed = {line.strip() for line in (directory / (label + "_verify.log")).read_text().splitlines()}
            remaining = set(targets).intersection(listed)
            if remaining:
                raise RunError("{}; topics still present: {}".format(error, ",".join(sorted(remaining))))

    def finish_clients(self, clients):
        self.finalizing = True
        directory = self.output / "clients_{}_cleanup".format(clients)
        result = dict(load_clients=clients, status="SUCCESS", reason="", started_at=utc_now(),
                      targets=list(self.pending_topics), log_dir=str(directory))
        try:
            self.stop_all()
            directory.mkdir()
            print("=== A clients={}: all cases/repetitions finished; cleaning topics ===".format(clients), flush=True)
            self.cleaning = True
            errors = []
            for index, (command, targets) in enumerate(self.cleanup_commands(directory)):
                label = "cleanup_" + str(index)
                try:
                    self.cleanup(command, targets, directory, label)
                except Cancelled:
                    result["interrupted"] = True
                    # Finish deletion after an interruption. The signal handler
                    # suppresses subsequent interrupts while this retry runs.
                    try:
                        self.cleanup(command, targets, directory, label + "_retry")
                    except (RunError, OSError) as error:
                        errors.append(str(error))
                except (RunError, OSError) as error:
                    errors.append(str(error))
            if errors:
                result.update(status="FAILED", reason="; ".join(errors))
        except (RunError, OSError) as error:
            result.update(status="FAILED", reason=str(error))
        finally:
            self.cleaning = False
            result["finished_at"] = utc_now()
            self.cleanups.append(result)
            for row in self.results:
                if row["load_clients"] == clients:
                    row.update(cleanup_status=result["status"], cleanup_log_dir=str(directory))
                    if result["reason"]:
                        row["cleanup_reason"] = result["reason"]
            self.write_results()
            self.finalizing = False
        if self.cancelled:
            raise Cancelled()
        if result["status"] == "FAILED":
            raise CleanupError("Topic cleanup failed; stopping before the next A client count. See " + str(directory))
        # Put the final cooldown after deletion so asynchronous broker log-file
        # removal has a settling interval before the next client count.
        time.sleep(self.args.cooldown)

    def run_case(self, clients, repetition, case, cooldown=True):
        a = self.args
        run_id = "repeat_{}_{}".format(repetition, case)
        if len(a.a_clients) > 1:
            run_id = "clients_{}_".format(clients) + run_id
        directory = self.output / run_id
        directory.mkdir()
        prefix = "probe_B_" + self.output.name.replace(".", "_") + "_" + run_id
        result = dict(run_id=run_id, repetition=repetition, case=case, load_clients=clients, status="SUCCESS", reason="",
                      a_topic=self.a_topic(case), a_partitions=None if case == "baseline" else self.a_partitions(case),
                      a_clients=0 if case == "baseline" else clients, a_interval_ms=a.a_interval,
                      b_topic=a.b_topic, b_clients=1, b_partitions=1, b_interval_ms=a.b_interval,
                      b_messages=a.b_messages_per_client, b_record_prefix=prefix, b_scope="all_B_clients",
                      a_exit=None, a_forced_kill=False, b_runner_exit=None, cleanup_status="PENDING", started_at=utc_now(), log_dir=str(directory))
        started = False
        print("=== {}: A={} B={} ===".format(run_id, self.a_topic(case) or "off", a.b_topic), flush=True)
        try:
            if case != "baseline":
                self.pending_topics[self.a_topic(case)] = ("gst" if case == "gst" else "normal", self.a_partitions(case))
                self.prepare_a(case, directory)
                command = self.a_command(case, prefix, clients)
                self.event(directory, "a_start", command=command)
                # A remains untagged; its client log output is discarded.
                self.a_process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                                  stderr=subprocess.DEVNULL, start_new_session=True)
                started = True
            command = self.b_command(directory, prefix)
            self.event(directory, "b_runner_start", command=command)
            env = dict(os.environ, GST_INTERFERENCE_LOG_DIR=str(a.broker_log_dir), GST_INTERFERENCE_PREFIX=prefix,
                       GST_INTERFERENCE_THROUGHPUT_GLOB=a.throughput_glob or "", GST_INTERFERENCE_LATENCY_GLOB=a.latency_glob or "",
                       GST_INTERFERENCE_CSV_WAIT=str(a.csv_wait), GST_INTERFERENCE_CSV_STABLE=str(a.csv_stable),
                       GST_INTERFERENCE_CASE=case)
            with (directory / "runner.log").open("w") as output:
                self.pending_topics[a.b_topic] = ("single", 1)
                self.b_process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env=env, start_new_session=True)
                started = True
                deadline = time.monotonic() + a.run_timeout
                while self.b_process.poll() is None:
                    if self.a_process is not None and self.a_process.poll() is not None:
                        raise RunError("A exited before B collection finished: {}".format(self.a_process.returncode))
                    if time.monotonic() >= deadline:
                        raise RunError("B runner timed out")
                    time.sleep(0.05)
                result["b_runner_exit"] = self.b_process.returncode
            if self.a_process is not None and self.a_process.poll() is not None:
                raise RunError("A exited before B collection finished: {}".format(self.a_process.returncode))
            summaries = list((directory / "runner").glob("*/summary.tsv"))
            if len(summaries) != 1:
                raise RunError("missing B runner summary; see runner.log")
            with summaries[0].open() as source:
                runs = list(csv.DictReader(source, delimiter="\t"))
            if len(runs) != 1:
                raise RunError("expected one B measurement")
            metrics = Path(runs[0]["log_dir"]) / "b_metrics.json"
            if metrics.exists():
                result.update(json.loads(metrics.read_text()))
            if result["b_runner_exit"] or runs[0]["status"] != "SUCCESS":
                raise RunError("B measurement {}: {}".format(runs[0]["status"], runs[0]["reason"]))
            if not metrics.exists():
                raise RunError("B metrics were not collected")
        except (RunError, DataError, OSError, ValueError, csv.Error) as error:
            result.update(status="FAILED" if started else "SKIPPED", reason=str(error))
        except Cancelled:
            result.update(status="INTERRUPTED", reason="controller_interrupted")
            raise
        finally:
            self.finalizing = True
            self.event(directory, "stop_requested")
            self.stop(self.b_process, a.stop_grace + 6)
            if self.b_process is not None:
                result["b_runner_exit"] = self.b_process.returncode
            if self.a_process is not None:
                forced = self.stop(self.a_process)
                result["a_exit"] = self.a_process.returncode
                # A has already covered B collection. Forced shutdown afterwards
                # does not invalidate those measurements; retain it as metadata.
                result["a_forced_kill"] = forced
            self.b_process = self.a_process = None
            self.event(directory, "producers_stopped")
            result["finished_at"] = utc_now()
            self.results.append(result)
            self.write_results()
            self.finalizing = False
            if self.cancelled:
                raise Cancelled()
        print("  {} B_rps={} IO_p99_ms={}".format(result["status"], result.get("b_store_rps"), result.get("b_io_latency_p99_ms")), flush=True)
        if started and cooldown:
            time.sleep(a.cooldown)

    def write_results(self):
        baselines = {(r["load_clients"], r["repetition"]): r for r in self.results if r["case"] == "baseline" and r["status"] == "SUCCESS"}
        for result in self.results:
            baseline = baselines.get((result["load_clients"], result["repetition"]))
            result.update(b_baseline_run_id=baseline["run_id"] if baseline else None,
                          b_store_ratio_to_baseline=None, b_store_drop_pct=None)
            for stage in ("io", "network"):
                result["b_{}_latency_p99_delta_ms".format(stage)] = None
            if baseline and result["status"] == "SUCCESS":
                if baseline["b_store_rps"]:
                    ratio = result["b_store_rps"] / baseline["b_store_rps"]
                    result.update(b_store_ratio_to_baseline=ratio, b_store_drop_pct=(1 - ratio) * 100)
                for stage in ("io", "network"):
                    key = "b_{}_latency_p99_ms".format(stage)
                    result["b_{}_latency_p99_delta_ms".format(stage)] = result[key] - baseline[key]
            (Path(result["log_dir"]) / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        self.write_table(self.output / "summary.tsv", self.results)
        (self.output / "summary.json").write_text(json.dumps(self.results, indent=2) + "\n")
        aggregates = []
        metrics = ("b_store_rps", "b_store_ratio_to_baseline", "b_store_drop_pct",
                   "b_io_latency_mean_ms", "b_io_latency_p95_ms", "b_io_latency_p99_ms", "b_io_latency_p99_delta_ms",
                   "b_network_latency_mean_ms", "b_network_latency_p95_ms", "b_network_latency_p99_ms", "b_network_latency_p99_delta_ms")
        for clients, case in ((clients, case) for clients in self.args.a_clients for case in self.args.cases):
            rows = [r for r in self.results if r["case"] == case and r["load_clients"] == clients]
            row = dict(load_clients=clients, case=case, successful_runs=sum(r["status"] == "SUCCESS" for r in rows),
                       failed_or_skipped_runs=sum(r["status"] != "SUCCESS" for r in rows))
            for metric in metrics:
                values = [r[metric] for r in rows if r["status"] == "SUCCESS" and r.get(metric) is not None]
                row[metric + "_n"] = len(values)
                row[metric + "_mean"] = statistics.mean(values) if values else None
                row[metric + "_stddev"] = statistics.stdev(values) if len(values) > 1 else None
            aggregates.append(row)
        self.write_table(self.output / "conditions.tsv", aggregates)
        (self.output / "conditions.json").write_text(json.dumps(aggregates, indent=2) + "\n")
        self.write_table(self.output / "client_cleanup.tsv", self.cleanups)
        (self.output / "client_cleanup.json").write_text(json.dumps(self.cleanups, indent=2) + "\n")

    @staticmethod
    def write_table(path, rows):
        fields = list(dict.fromkeys(key for row in rows for key in row))
        with path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

    def run(self):
        a = self.args
        if a.dry_run:
            print("B: one tagged client/partition; A: untagged; no warmup; clean topics after all cases/repetitions at each A client count")
            topics = {a.b_topic: ("single", 1)}
            topics.update({self.a_topic(case): ("gst" if case == "gst" else "normal", self.a_partitions(case))
                           for case in a.cases if case != "baseline"})
            for clients in a.a_clients:
                print("\nA clients={}: repeat cases {} x{}, then cleanup".format(clients, a.cases, a.repetitions))
                for case in a.cases:
                    print("CASE " + case)
                    if case != "baseline":
                        print("A: " + " ".join(shlex.quote(arg) for arg in self.a_command(case, "dry_run", clients)))
                    print("B runner: " + " ".join(shlex.quote(arg) for arg in self.b_command(a.output_root / "dry-run", "probe_B_dry_run")))
                for command, _ in self.cleanup_commands(a.output_root / "dry-run-cleanup", topics):
                    print("Cleanup after all repetitions: " + " ".join(shlex.quote(arg) for arg in command))
            return 0
        for executable in (a.producer, a.runner, a.kafka_bin / "kafka-topics.sh", a.kafka_bin / "kafka-broker-extension.sh", Path(__file__).resolve()):
            if not executable.is_file() or not os.access(str(executable), os.X_OK):
                raise RunError("executable not found: {}".format(executable))
        if not a.broker_log_dir.is_dir():
            raise RunError("broker-log-dir must be locally readable or mounted")
        a.output_root.mkdir(parents=True, exist_ok=True)
        self.output = Path(tempfile.mkdtemp(prefix=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ."), dir=str(a.output_root)))
        config = {key: str(value) if isinstance(value, Path) else value for key, value in vars(a).items()}
        config.update(generator_host=socket.gethostname(), a_tagged=False, b_tagged=True, b_clients=1, warmup_s=0, acks=0)
        (self.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        for name in (Path(__file__).name, "gst_interference_logs.py"):
            (self.output / name).write_bytes(Path(__file__).with_name(name).read_bytes())
        print("Results: {}".format(self.output), flush=True)
        for clients in a.a_clients:
            self.pending_topics = {}
            try:
                for repetition in range(1, a.repetitions + 1):
                    for index, case in enumerate(a.cases):
                        last = repetition == a.repetitions and index == len(a.cases) - 1
                        self.run_case(clients, repetition, case, cooldown=not last)
            finally:
                self.finish_clients(clients)
        return 1 if any(r["status"] != "SUCCESS" or r["cleanup_status"] == "FAILED" for r in self.results) else 0


def hook(phase, directory):
    root = Path(os.environ["GST_INTERFERENCE_LOG_DIR"])
    throughput_glob = os.environ["GST_INTERFERENCE_THROUGHPUT_GLOB"] or None
    latency_glob = os.environ["GST_INTERFERENCE_LATENCY_GLOB"] or None
    if phase == "before_produce":
        snapshot(root, directory, throughput_glob, latency_glob)
    else:
        capture(root, directory, os.environ["GST_INTERFERENCE_PREFIX"],
                float(os.environ["GST_INTERFERENCE_CSV_WAIT"]), float(os.environ["GST_INTERFERENCE_CSV_STABLE"]),
                throughput_glob, latency_glob)


def main():
    if len(sys.argv) == 3 and sys.argv[1] in ("before_produce", "after_rollout"):
        try:
            hook(sys.argv[1], Path(sys.argv[2]))
            return 0
        except (DataError, OSError, ValueError, csv.Error) as error:
            print("Broker log capture failed: {}".format(error), file=sys.stderr)
            return 1
    experiment = Experiment(parse_args())

    def cancel(signum, frame):
        if not experiment.cancelled:
            experiment.cancelled = True
            if not experiment.finalizing or experiment.cleaning:
                raise Cancelled()

    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    try:
        return experiment.run()
    except Cancelled:
        print("Interrupted; owned A/B processes stopped and topic cleanup attempted.", file=sys.stderr)
        return 130
    except CleanupError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (RunError, OSError) as error:
        print("Error: {}".format(error), file=sys.stderr)
        return 2
    finally:
        experiment.stop_all()


if __name__ == "__main__":
    sys.exit(main())
