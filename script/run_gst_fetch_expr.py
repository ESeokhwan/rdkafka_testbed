#!/usr/bin/env python3
"""Measure GST or ordinary single-partition Fetch throughput and E2E latency."""

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
import statistics
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent


class RunError(Exception):
    pass


class Cancelled(BaseException):
    pass


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def configure_fd_limit(clients, bootstrap_servers):
    # Each producer has an internal broker (one wake-up pipe) and each
    # bootstrap broker needs another pipe and a connection socket.
    brokers = len(set(address.strip() for address in bootstrap_servers.split(",") if address.strip()))
    minimum = 3 + (clients + 1) * (2 + 3 * max(1, brokers))
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    unlimited = resource.RLIM_INFINITY
    if hard != unlimited and hard < minimum:
        raise RunError(f"hard open-file limit {hard} is too low for {clients} producer clients "
                       f"(estimated minimum {minimum}); raise the hard nofile limit before running")
    target = max(65536, minimum)
    if hard != unlimited:
        target = min(target, hard)
    if soft != unlimited and soft < target:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (ValueError, OSError) as error:
            # Some systems impose a separate per-process ceiling even with an
            # unlimited hard rlimit. Try the workload estimate in that case.
            if soft < minimum:
                try:
                    resource.setrlimit(resource.RLIMIT_NOFILE, (minimum, hard))
                except (ValueError, OSError) as fallback_error:
                    raise RunError(f"could not raise soft open-file limit from {soft} to {minimum}: "
                                   f"{fallback_error}") from fallback_error
            print(f"Could not apply preferred open-file limit {target}: {error}", file=sys.stderr)
    effective, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    return dict(soft_before=soft, soft=effective, hard=hard, estimated_minimum=minimum)


def positive(value):
    if not value.isdecimal() or not 0 < int(value) <= 2147483647:
        raise argparse.ArgumentTypeError("expected an integer in 1..2147483647")
    return int(value)


def nonnegative(value):
    if value == "0":
        return 0
    return positive(value)


def seconds(value):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    return result


def batch_sizes(value):
    values = [positive(part.strip()) for part in value.split(",")]
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("batch sizes must be distinct")
    return values


def client_counts(value):
    values = [positive(part.strip()) for part in value.split(",")]
    if any(previous >= current for previous, current in zip(values, values[1:])):
        raise argparse.ArgumentTypeError("client counts must be distinct and strictly increasing")
    return values


def topic_modes(value):
    values = [part.strip() for part in value.split(",")]
    if any(mode not in ("gst", "single") for mode in values) or len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("topic modes must be distinct names from gst,single")
    return values


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--broker", default="166.104.110.13:9092")
    p.add_argument("--mode", choices=("live", "backlog"), default="live",
                   help="live: produce/fetch together; backlog: produce first, then measure drain")
    p.add_argument("--topic-modes", "--topic-mode", type=topic_modes, default="gst",
                   help="ordered topic modes, e.g. gst,single; single always uses partition 0")
    p.add_argument("--partitions", type=positive, default=16, help="GST source partitions; single mode always uses 1")
    p.add_argument("--clients", type=client_counts, default="16",
                   help="increasing producer client counts, e.g. 16,32,64,128")
    p.add_argument("--interval", type=seconds, default=3, help="producer interval in ms")
    p.add_argument("--messages-per-client", type=positive, default=20000)
    p.add_argument("--message-size", type=positive, default=1000)
    p.add_argument("--batch-sizes", "--batch-size", type=batch_sizes, default="1000",
                   help="GST position range / single consumer delivery cap, e.g. 100,500,1000")
    p.add_argument("--max-batches", type=positive, default=1000, help="API98 production-batch limit")
    p.add_argument("--max-bytes", type=positive, default=1048576, help="API98 soft encoded-byte limit")
    p.add_argument("--isolation-level", type=int, choices=(0, 1), default=0)
    p.add_argument("--fetch-timeout-ms", type=positive, default=10000)
    p.add_argument("--poll-interval-ms", type=nonnegative, default=1)
    p.add_argument("--metrics-interval-ms", type=positive, default=1000)
    p.add_argument("--prefetch", choices=("on", "off"), default="off",
                   help="overlap GST page processing with one next Fetch (default: off)")
    p.add_argument("--repetitions", type=positive, default=1)
    p.add_argument("--warmup-records-per-partition", type=nonnegative, default=0,
                   help="warmup records per active source partition")
    p.add_argument("--warmup-interval-ms", type=positive, default=1,
                   help="interval between warmup records on each active partition in ms (default: 1)")
    p.add_argument("--warmup-timeout", type=positive, default=30, help="warmup deadline in seconds")
    p.add_argument("--start-barrier-delay", type=nonnegative, default=2)
    p.add_argument("--prepare-producers", choices=("on", "off"), default="off",
                   help="prepare each measurement producer's connections before production (default: off)")
    p.add_argument("--prepare-timeout-ms", type=positive, default=30000,
                   help="overall producer preparation timeout in milliseconds")
    p.add_argument("--producer-timeout", type=seconds)
    p.add_argument("--catchup-timeout", type=seconds, default=60, help="wait after producer exit for the finite target")
    p.add_argument("--command-timeout", type=seconds, default=30)
    p.add_argument("--stop-grace", type=seconds, default=10)
    p.add_argument("--topic-delete-timeout", type=seconds, default=120)
    p.add_argument("--cleanup-after", choices=("clients", "run"), default="clients",
                   help="delete topics after all topic modes/batches/repetitions at each client count (default), or each run")
    p.add_argument("--cooldown", type=seconds, default=60, help="wait after topic deletion")
    p.add_argument("--replication-factor", type=positive, default=1)
    p.add_argument("--topic-prefix", default="gst_fetch")
    p.add_argument("--producer", type=Path, default=ROOT / "bin/single_topic_throughput_expr_producer")
    p.add_argument("--consumer", type=Path, default=ROOT / "bin/gst_fetch_expr_consumer")
    p.add_argument("--cleanup-runner", type=Path, default=ROOT / "script/run_gst_fixed_partition_expr.sh")
    p.add_argument("--kafka-bin", type=Path, default=ROOT.parent / "kafka_sandbox/bin")
    p.add_argument("--output-root", type=Path, default=ROOT / "out/gst_fetch")
    p.add_argument("--no-latency-log", action="store_true", help="keep exact consumer summary/histogram, skip per-record CSV")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    if not 0 < a.interval < 1e9:
        p.error("interval must be positive and < 1e9 ms")
    if a.warmup_interval_ms > 2147483647:
        p.error("warmup interval exceeds the supported millisecond range")
    if a.warmup_timeout > 2147483 or a.warmup_records_per_partition > 2147483647:
        p.error("warmup timeout or record count exceeds the supported range")
    if a.prepare_timeout_ms > 3600000:
        p.error("prepare-timeout-ms must be in 1..3600000")
    if max(a.clients) > 2147483647 // a.messages_per_client:
        p.error("total producer attempt count exceeds the existing app limit")
    if a.max_batches > 1000 or a.max_bytes > 16646144 or a.replication_factor > 32767:
        p.error("invalid API98 batch/byte limit or replication factor")
    if "single" in a.topic_modes and a.max_bytes < 1000:
        p.error("single mode needs max-bytes >= 1000")
    if a.prefetch == "on" and "gst" not in a.topic_modes:
        p.error("prefetch requires GST mode")
    if not a.broker or not re.fullmatch(r"[A-Za-z0-9._-]{1,120}", a.topic_prefix) or a.topic_prefix in (".", ".."):
        p.error("invalid broker or topic prefix")
    if any(value <= 0 for value in (a.command_timeout, a.catchup_timeout, a.topic_delete_timeout)):
        p.error("command/catchup/delete timeouts must be positive")
    nominal = a.messages_per_client * a.interval / 1000
    if a.producer_timeout is None:
        a.producer_timeout = max(300, nominal * 3 + a.start_barrier_delay + 60)
    if a.producer_timeout <= 0 or not math.isfinite(a.producer_timeout + a.catchup_timeout):
        p.error("invalid producer/consumer time budget")
    for name in ("producer", "consumer", "cleanup_runner", "kafka_bin", "output_root"):
        setattr(a, name, getattr(a, name).resolve())
    return a


class Experiment:
    def __init__(self, args):
        self.args, self.output = args, None
        self.producer = self.consumer = self.command_process = None
        self.cancelled = self.finalizing = self.cleaning = False
        self.results = []
        self.pending_topics = {}
        self.cleanups = []

    def partitions_for(self, topic_mode):
        return self.args.partitions if topic_mode == "gst" else 1

    def stop(self, process, grace=None):
        if process is None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=self.args.stop_grace if grace is None else grace)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()

    def stop_all(self):
        self.stop(self.command_process, self.args.stop_grace + 6)
        self.stop(self.producer)
        self.stop(self.consumer, self.args.fetch_timeout_ms / 1000 + 2)
        self.command_process = self.producer = self.consumer = None

    def command(self, argv, directory, label, timeout=None):
        (directory / (label + "_command.json")).write_text(json.dumps(argv, indent=2) + "\n")
        with (directory / (label + ".log")).open("w") as output:
            self.command_process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = self.command_process.wait(timeout=timeout or self.args.command_timeout)
            except subprocess.TimeoutExpired:
                raise RunError(label + " timed out")
            finally:
                self.stop(self.command_process, self.args.stop_grace + 6)
                self.command_process = None
        if code:
            raise RunError("{} exited {}".format(label, code))

    def producer_process_timeout(self):
        a = self.args
        return a.producer_timeout + (a.prepare_timeout_ms / 1000 if a.prepare_producers == "on" else 0)

    def warmup_command(self, topic, clients, topic_mode):
        a = self.args
        partitions = min(clients, self.partitions_for(topic_mode))
        return [str(ROOT / "bin/gst_fetch_expr_warmup"), a.broker, topic, topic_mode,
                str(partitions), str(a.warmup_records_per_partition), str(a.warmup_timeout * 1000),
                str(a.message_size), str(a.warmup_interval_ms)]

    def commands(self, directory, topic, size, clients, topic_mode, start_offset=0):
        a = self.args
        producer_partitions = min(clients, self.partitions_for(topic_mode))
        producer = [str(a.producer), "--broker", a.broker, "--topic-name", topic, "--prefix", topic,
                    "--partition-cnt", str(producer_partitions), "--client-cnt", str(clients),
                    "--interval", str(a.interval), "--interval-noise-stddev", "0",
                    "--msg-cnt", str(clients * a.messages_per_client), "--msg-size", str(a.message_size),
                    "--start-barrier-delay", str(a.start_barrier_delay), "--ignore-response", "--sample-log"]
        if a.prepare_producers == "on":
            producer.extend(("--prepare-producers", "on", "--prepare-timeout-ms", str(a.prepare_timeout_ms)))
        consumer = [str(a.consumer), "--broker", a.broker, "--topic", topic, "--topic-mode", topic_mode, "--start-offset", str(start_offset),
                    "--end-offset", str(start_offset + clients * a.messages_per_client), "--batch-size", str(size),
                    "--max-batches", str(a.max_batches), "--max-bytes", str(a.max_bytes),
                    "--isolation-level", str(a.isolation_level), "--fetch-timeout-ms", str(a.fetch_timeout_ms),
                    "--poll-interval-ms", str(a.poll_interval_ms), "--metrics-interval-ms", str(a.metrics_interval_ms),
                    "--duration", str(a.command_timeout + self.producer_process_timeout() + a.catchup_timeout + 10),
                    "--output-dir", str(directory / "consumer"), "--ready-file", str(directory / "consumer_ready.json")]
        if a.no_latency_log:
            consumer.append("--no-latency-log")
        if topic_mode == "gst" and a.prefetch == "on":
            consumer.extend(("--prefetch", "on"))
        return producer, consumer

    def cleanup(self, directory, topic, topic_mode):
        a = self.args
        topic_option = "--gst-topic" if topic_mode == "gst" else "--single-topic"
        command = ["/bin/bash", str(a.cleanup_runner), "--cleanup-only", "--broker", a.broker,
                   "--partitions", str(self.partitions_for(topic_mode)), "--modes", topic_mode, topic_option, topic,
                   "--replication-factor", str(a.replication_factor), "--kafka-bin", str(a.kafka_bin),
                   "--topic-delete-timeout", str(a.topic_delete_timeout), "--output-root", str(directory / "cleanup")]
        try:
            self.command(command, directory, "cleanup", a.topic_delete_timeout + a.command_timeout + 60)
        except RunError as error:
            self.command([str(a.kafka_bin / "kafka-topics.sh"), "--bootstrap-server", a.broker, "--list"], directory, "cleanup_verify")
            if topic in {line.strip() for line in (directory / "cleanup_verify.log").read_text().splitlines()}:
                raise error

    def cleanup_pending(self, clients):
        if not self.pending_topics:
            return
        self.finalizing = True
        self.stop_all()
        result = dict(producer_clients=clients, scope=self.args.cleanup_after, status="SUCCESS",
                      targets=list(self.pending_topics), started_at=now(), deleted_topics=[], failed_topics=[])
        print("=== clients={}: cleaning {} experiment topics ===".format(clients, len(self.pending_topics)), flush=True)
        self.cleaning = True
        try:
            for topic, (directory, row) in list(self.pending_topics.items()):
                try:
                    try:
                        self.cleanup(directory, topic, row["topic_mode"])
                    except Cancelled:
                        result["cleanup_interrupted"] = True
                        self.cleanup(directory, topic, row["topic_mode"])
                    row["cleanup_status"] = "SUCCESS"
                    result["deleted_topics"].append(topic)
                except (RunError, OSError) as error:
                    row.update(cleanup_status="FAILED", cleanup_reason=str(error))
                    result["status"] = "FAILED"
                    result["failed_topics"].append(topic)
                row["cleanup_finished_at"] = now()
                row["cleanup_log_dir"] = str(directory)
                del self.pending_topics[topic]
        finally:
            self.cleaning = False
            result["finished_at"] = now()
            self.cleanups.append(result)
            # Finish all deletion attempts before persisting reports.
            try:
                for row in self.results:
                    if row["producer_clients"] == clients:
                        (Path(row["log_dir"]) / "result.json").write_text(json.dumps(row, indent=2) + "\n")
                self.write_results()
            finally:
                self.finalizing = False
        if self.cancelled:
            raise Cancelled()
        if result["status"] != "SUCCESS":
            raise RunError("topic cleanup failed; no further production will start: " + ", ".join(result["failed_topics"]))

    def run_case(self, clients, topic_mode, size, repetition):
        a = self.args
        partitions = self.partitions_for(topic_mode)
        case_id = "batch_{}_repeat_{}".format(size, repetition)
        run_id = "clients_{}_{}_{}".format(clients, topic_mode, case_id)
        directory = self.output / "clients_{}".format(clients) / topic_mode / case_id
        directory.mkdir()
        topic = "{}_p{}_{}_{}".format(a.topic_prefix, partitions, run_id, self.output.name.split(".")[-1])
        result = dict(run_id=run_id, topic_mode=topic_mode, batch_size=size, repetition=repetition, topic=topic, partitions=partitions,
                      producer_clients=clients, producer_interval_ms=a.interval,
                      producer_attempts=clients * a.messages_per_client, message_size=a.message_size,
                      max_batches=a.max_batches if topic_mode == "gst" else None,
                      max_bytes=a.max_bytes, status="SUCCESS", reason="",
                      producer_exit=None, consumer_exit=None, cleanup_status="NOT_CREATED", started_at=now(), log_dir=str(directory))
        streams = []
        print("=== {}: {} {} Fetch, P={}, topic={} ===".format(run_id, a.mode, topic_mode, partitions, topic), flush=True)
        try:
            create = [str(a.kafka_bin / "kafka-topics.sh"), "--bootstrap-server", a.broker, "--create", "--topic", topic,
                      "--partitions", str(partitions), "--replication-factor", str(a.replication_factor),
                      "--config", "cleanup.policy=delete", "--config", "global.sequence.enabled=" + str(topic_mode == "gst").lower()]
            self.command(create, directory, "create")
            self.pending_topics[topic] = (directory, result)
            result["cleanup_status"] = "PENDING"
            start_offset = 0
            if a.warmup_records_per_partition:
                self.command(self.warmup_command(topic, clients, topic_mode), directory, "warmup", a.warmup_timeout + 5)
                lines = (directory / "warmup.log").read_text().splitlines()
                if not lines or not lines[-1].isdigit():
                    raise RunError("missing warmup boundary; see warmup.log")
                start_offset = int(lines[-1])
                expected = min(clients, partitions) * a.warmup_records_per_partition
                if start_offset != expected:
                    raise RunError("unexpected warmup boundary; see warmup.log")
                result["warmup_records"] = start_offset
                result["measurement_start_offset"] = start_offset
            producer, consumer = self.commands(directory, topic, size, clients, topic_mode, start_offset)
            (directory / "producer_command.json").write_text(json.dumps(producer, indent=2) + "\n")
            (directory / "consumer_command.json").write_text(json.dumps(consumer, indent=2) + "\n")
            def start_consumer():
                consumer_log = (directory / "consumer.log").open("w")
                streams.append(consumer_log)
                self.consumer = subprocess.Popen(consumer, stdout=consumer_log, stderr=subprocess.STDOUT, start_new_session=True)
                deadline = time.monotonic() + a.command_timeout
                while not (directory / "consumer_ready.json").exists():
                    if self.consumer.poll() is not None:
                        raise RunError("consumer exited during initialization; see consumer.log")
                    if time.monotonic() >= deadline:
                        raise RunError("consumer initialization timed out")
                    time.sleep(.02)

            if a.mode == "live":
                start_consumer()
            producer_log = (directory / "producer.log").open("w")
            streams.append(producer_log)
            self.producer = subprocess.Popen(producer, stdout=producer_log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + self.producer_process_timeout()
            while self.producer.poll() is None:
                if self.consumer is not None and self.consumer.poll() not in (None, 0):
                    raise RunError("consumer failed while producer was active")
                if time.monotonic() >= deadline:
                    raise RunError("producer timed out")
                time.sleep(.05)
            if self.producer.returncode:
                raise RunError("producer exited {}; see producer.log".format(self.producer.returncode))
            if a.mode == "backlog":
                start_consumer()
            try:
                self.consumer.wait(timeout=a.catchup_timeout)
            except subprocess.TimeoutExpired:
                raise RunError("consumer did not reach the target within catchup-timeout")
            if self.consumer.returncode:
                raise RunError("consumer exited {}; inspect its summary.json".format(self.consumer.returncode))
            with (directory / "producer.log").open("rb") as log:
                if any(b"Too many open files" in line for line in log):
                    raise RunError("producer exhausted file descriptors; see producer.log")
        except (RunError, OSError, ValueError) as error:
            result.update(status="FAILED", reason=str(error))
        except Cancelled:
            result.update(status="INTERRUPTED", reason="controller_interrupted")
            raise
        finally:
            self.finalizing = True
            producer_process, consumer_process = self.producer, self.consumer
            self.stop_all()
            if producer_process is not None:
                result["producer_exit"] = producer_process.returncode
            if consumer_process is not None:
                result["consumer_exit"] = consumer_process.returncode
            for stream in streams:
                stream.close()
            summary = directory / "consumer/summary.json"
            if summary.exists():
                try:
                    metrics = json.loads(summary.read_text())
                    result["consumer_status"] = metrics["status"]
                    for name in ("completed_range", "consumed_records", "elapsed_s", "throughput_records_per_sec",
                                 "throughput_payload_bytes_per_sec", "fetch_calls", "empty_pages", "local_errors",
                                 "broker_errors", "invalid_payload_records", "negative_latency_records", "order_errors"):
                        result[name] = metrics[name]
                    for name, value in metrics["latency"].items():
                        result["e2e_latency_" + name] = value
                    if result["status"] == "SUCCESS" and (metrics["status"] != "SUCCESS" or not metrics["completed_range"]):
                        result.update(status="FAILED", reason="consumer did not complete a valid target range")
                except (OSError, ValueError, KeyError, TypeError) as error:
                    result.update(status="FAILED", reason="invalid consumer summary: " + str(error))
            elif result["status"] == "SUCCESS":
                result.update(status="FAILED", reason="consumer summary missing")
            result["finished_at"] = now()
            self.results.append(result)
            (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            self.write_results()
            self.finalizing = False
            if self.cancelled:
                raise Cancelled()
        print("  {} records={} throughput={} E2E_p99_ms={}".format(result["status"], result.get("consumed_records"),
              result.get("throughput_records_per_sec"), result.get("e2e_latency_p99_ms")), flush=True)

    @staticmethod
    def table(path, rows):
        fields = list(dict.fromkeys(key for row in rows for key in row))
        with path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

    def write_results(self):
        self.table(self.output / "summary.tsv", self.results)
        (self.output / "summary.json").write_text(json.dumps(self.results, indent=2) + "\n")
        self.table(self.output / "client_cleanup.tsv", self.cleanups)
        (self.output / "client_cleanup.json").write_text(json.dumps(self.cleanups, indent=2) + "\n")
        groups = []
        for clients in self.args.clients:
            for topic_mode in self.args.topic_modes:
                for size in self.args.batch_sizes:
                    rows = [row for row in self.results if row["producer_clients"] == clients
                            and row["topic_mode"] == topic_mode and row["batch_size"] == size]
                    group = dict(topic_mode=topic_mode, partitions=self.partitions_for(topic_mode),
                                 producer_clients=clients, batch_size=size,
                                 successful_runs=sum(row["status"] == "SUCCESS" for row in rows),
                                 failed_runs=sum(row["status"] != "SUCCESS" for row in rows))
                    for metric in ("throughput_records_per_sec", "throughput_payload_bytes_per_sec",
                                   "e2e_latency_mean_ms", "e2e_latency_p95_ms", "e2e_latency_p99_ms"):
                        values = [row[metric] for row in rows if row["status"] == "SUCCESS" and row.get(metric) is not None]
                        group[metric + "_n"] = len(values)
                        group[metric + "_mean"] = statistics.mean(values) if values else None
                        group[metric + "_stddev"] = statistics.stdev(values) if len(values) > 1 else None
                    groups.append(group)
        self.table(self.output / "batches.tsv", groups)
        (self.output / "batches.json").write_text(json.dumps(groups, indent=2) + "\n")

    def run(self):
        a = self.args
        if a.dry_run:
            for clients in a.clients:
                print("\nclients={}: all topic modes/batch sizes/repetitions, cleanup-after={}".format(clients, a.cleanup_after))
                for topic_mode in a.topic_modes:
                    partitions = self.partitions_for(topic_mode)
                    for size in a.batch_sizes:
                        topic = "{}_{}_p{}_clients_{}_batch_{}_repeat_1_dry".format(a.topic_prefix, topic_mode, partitions, clients, size)
                        planned_start = min(clients, partitions) * a.warmup_records_per_partition
                        producer, consumer = self.commands(a.output_root / "dry-run", topic, size, clients, topic_mode, planned_start)
                        stages = "consumer ready -> producer -> catch up" if a.mode == "live" else "producer -> consumer drain"
                        if planned_start:
                            stages = "warmup -> confirm committed start -> " + stages
                        print("topic_mode={} P={} batch_size={} repetitions={}: create fresh topic -> {}".format(topic_mode, partitions, size, a.repetitions, stages))
                        if planned_start:
                            print("WARMUP: " + " ".join(shlex.quote(arg) for arg in self.warmup_command(topic, clients, topic_mode)))
                        print("CONSUMER: " + " ".join(shlex.quote(arg) for arg in consumer))
                        print("PRODUCER: " + " ".join(shlex.quote(arg) for arg in producer))
                        if a.cleanup_after == "run":
                            print("  delete this run's topic -> cooldown {}s".format(a.cooldown))
                if a.cleanup_after == "clients":
                    print("delete all topics for clients={} -> cooldown {}s -> next client count".format(clients, a.cooldown))
            return 0
        paths = [a.producer, a.consumer, a.cleanup_runner, a.kafka_bin / "kafka-topics.sh"]
        if a.warmup_records_per_partition:
            paths.append(ROOT / "bin/gst_fetch_expr_warmup")
        for path in paths:
            if not path.is_file() or not os.access(str(path), os.X_OK):
                raise RunError("executable missing; build the local GST-enabled targets: " + str(path))
        fd_limit = configure_fd_limit(max(a.clients), a.broker)
        a.output_root.mkdir(parents=True, exist_ok=True)
        self.output = Path(tempfile.mkdtemp(prefix=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ."), dir=str(a.output_root)))
        config = {key: str(value) if isinstance(value, Path) else value for key, value in vars(a).items()}
        config.update(open_files=fd_limit, generator_host=socket.gethostname(), measurement=a.mode + " " + ",".join(a.topic_modes) + " fetch", producer_acks=0, consumer_count=1,
                      topic_partitions={mode: self.partitions_for(mode) for mode in a.topic_modes},
                      message_generator="FastJsonBasedLatencyMonitoringMessageGenerator", throughput_clock="consumer steady elapsed time")
        (self.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        (self.output / "script.py").write_bytes(Path(__file__).read_bytes())
        print("Results: " + str(self.output), flush=True)
        for clients in a.clients:
            (self.output / "clients_{}".format(clients)).mkdir()
            try:
                for topic_mode in a.topic_modes:
                    (self.output / "clients_{}".format(clients) / topic_mode).mkdir()
                    for size in a.batch_sizes:
                        for repetition in range(1, a.repetitions + 1):
                            try:
                                self.run_case(clients, topic_mode, size, repetition)
                            finally:
                                if a.cleanup_after == "run":
                                    self.cleanup_pending(clients)
                            if a.cleanup_after == "run":
                                time.sleep(a.cooldown)
            finally:
                self.cleanup_pending(clients)
            if a.cleanup_after == "clients":
                time.sleep(a.cooldown)
        return 1 if any(row["status"] != "SUCCESS" for row in self.results) else 0


def main():
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
        print("Interrupted; producer/consumer stopped and topic cleanup attempted.", file=sys.stderr)
        return 130
    except (RunError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1 if experiment.output else 2
    finally:
        experiment.stop_all()


if __name__ == "__main__":
    sys.exit(main())
