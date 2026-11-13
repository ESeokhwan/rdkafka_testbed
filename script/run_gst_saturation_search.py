#!/usr/bin/env python3
"""Search the first 10-client grid point below the chosen throughput ratio."""

import argparse
import csv
import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
STEP = 10
COLUMNS = ("measured_at", "measurement_duration_ms", "success_processed_req_cnt",
           "failed_processed_req_cnt", "throughput_req_per_sec")


class SearchError(Exception):
    pass


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def number(value, name):
    try:
        result = Decimal(value.strip())
    except (InvalidOperation, AttributeError):
        raise SearchError("invalid {}: {!r}".format(name, value))
    if not result.is_finite() or result < 0:
        raise SearchError("invalid {}: {!r}".format(name, value))
    return result


def read_samples(data, name):
    text = data.decode("utf-8-sig")
    first = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = "\t" if "\t" in first else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = next((row for row in reader if any(cell.strip() for cell in row)), [])
    header = [cell.strip() for cell in header]
    if not set(COLUMNS).issubset(header):
        return None  # Another kind of monitor CSV; do not aggregate it.
    if len(header) != len(set(header)):
        raise SearchError("{}: duplicate column names".format(name))
    indices = {key: header.index(key) for key in COLUMNS}
    result = dict(success=0, failed=0, active_ms=Decimal(0), total_ms=Decimal(0), rows=0, zero_rows=0)
    for line, row in enumerate(reader, 2):
        if not any(cell.strip() for cell in row) or [cell.strip() for cell in row] == header:
            continue
        try:
            duration = number(row[indices["measurement_duration_ms"]], "measurement_duration_ms")
            success = number(row[indices["success_processed_req_cnt"]], "success_processed_req_cnt")
            failed = number(row[indices["failed_processed_req_cnt"]], "failed_processed_req_cnt")
        except (IndexError, SearchError) as error:
            raise SearchError("{}:{}: {}".format(name, line, error))
        if success != success.to_integral_value() or failed != failed.to_integral_value():
            raise SearchError("{}:{}: counts must be integers".format(name, line))
        if success and not duration:
            raise SearchError("{}:{}: positive count with zero duration".format(name, line))
        result["success"] += int(success)
        result["failed"] += int(failed)
        result["total_ms"] += duration
        result["rows"] += 1
        if success:
            result["active_ms"] += duration
        else:
            result["zero_rows"] += 1
    return result


def summarize(samples):
    if not samples:
        raise SearchError("no CSV with the required throughput columns")
    success = sum(s["success"] for s in samples)
    active_ms = sum((s["active_ms"] for s in samples), Decimal(0))
    if not active_ms:
        raise SearchError("no nonzero-success samples; Excel denominator is zero")
    average = Decimal(success) * 1000 / active_ms
    if not math.isfinite(float(average)):
        raise SearchError("throughput does not fit a finite number")
    return dict(success_count_sum=success, failed_count_sum=sum(s["failed"] for s in samples),
                active_duration_ms=str(active_ms), total_duration_ms=str(sum((s["total_ms"] for s in samples), Decimal(0))),
                rows=sum(s["rows"] for s in samples), zero_success_rows=sum(s["zero_rows"] for s in samples),
                average_throughput_req_per_sec=float(average), average_throughput_decimal=str(average))


def signature(path):
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def log_order(root, path):
    relative = path.relative_to(root)
    match = re.fullmatch(r"(.*?)(?:\.(\d+))?\.(csv|log)", relative.name, re.IGNORECASE)
    if match:
        base, index, extension = match.groups()
        return (str(relative.parent), base, index is not None, int(index or 0), extension.lower(), relative.name)
    return (str(relative.parent), relative.name, False, 0, "", relative.name)


def log_files(root, pattern):
    paths = {path for glob in ((pattern,) if pattern else ("*.csv", "*.log"))
             for path in root.glob(glob) if path.is_file()}
    selected = []
    for path in sorted(paths, key=lambda path: log_order(root, path)):
        try:
            # Ignore unrelated active logs before snapshot/stability checks.
            with path.open(encoding="utf-8-sig", errors="replace") as source:
                first = next((line for line in source if line.strip()), "")
            header = next(csv.reader([first], delimiter="\t" if "\t" in first else ","))
            if set(COLUMNS).issubset(cell.strip() for cell in header):
                selected.append(path)
        except (FileNotFoundError, StopIteration, csv.Error):
            continue
    return selected


def capture(phase, directory):
    """Called by the existing sweep at exact boundaries, not by a poll of app logs."""
    root = Path(os.environ["GST_SATURATION_LOG_DIR"])
    pattern = os.environ["GST_SATURATION_CSV_GLOB"] or None
    wait = float(os.environ["GST_SATURATION_CSV_WAIT"])
    stable = float(os.environ["GST_SATURATION_CSV_STABLE"])
    before_path = directory / "throughput_before.json"
    if phase == "before_produce":
        before = {}
        for path in log_files(root, pattern):
            start = signature(path)
            data = path.read_bytes()
            if signature(path) != start:
                raise SearchError("CSV changed during the before snapshot: {}".format(path))
            before[str(path.relative_to(root))] = dict(signature=start, sha256=hashlib.sha256(data).hexdigest(),
                complete_offset=len(data) if data.endswith(b"\n") else data.rfind(b"\n") + 1)
        before_path.write_text(json.dumps(before, indent=2) + "\n")
        return
    before = json.loads(before_path.read_text())
    before_identity = {tuple(record["signature"][:2]): record for record in before.values()}
    deadline = time.monotonic() + wait
    previous = None
    stable_since = time.monotonic()
    last_error = "no new or changed throughput CSV after production"
    while True:
        paths = log_files(root, pattern)
        changed = [(path, signature(path)) for path in paths
                   if str(path.relative_to(root)) not in before or signature(path) != before[str(path.relative_to(root))]["signature"]]
        current = [(str(path), stat) for path, stat in changed]
        now = time.monotonic()
        if current != previous:
            previous, stable_since = current, now
        if changed and now - stable_since >= stable:
            try:
                samples, archives, seen = [], [], set()
                for path, stat in changed:
                    identity = tuple(stat[:2])
                    if identity in seen:
                        continue
                    seen.add(identity)
                    data = path.read_bytes()
                    if signature(path) != stat:
                        raise SearchError("CSV is still being written: {}".format(path))
                    old = before_identity.get(identity)
                    original_name = before.get(str(path.relative_to(root)))
                    renamed = old and (not original_name or tuple(original_name["signature"][:2]) != identity)
                    if renamed and hashlib.sha256(data).hexdigest() == old["sha256"]:
                        continue  # Existing content renamed by rollover, not a new sample.
                    kind = "new_or_rewritten"
                    if old and old["signature"][:2] == stat[:2] and len(data) > old["signature"][2]:
                        old_size = old["signature"][2]
                        if old_size and hashlib.sha256(data[:old_size]).hexdigest() == old["sha256"]:
                            header = data.splitlines()[0]
                            appended = data[old_size:]
                            if old["complete_offset"] != old_size:
                                # A row begun before this trial is not a new sample.
                                appended = appended.partition(b"\n")[2]
                            data = header + b"\n" + appended
                            kind = "appended_rows"
                    sample = read_samples(data, str(path))
                    if sample is not None:
                        samples.append(sample)
                        archives.append((path, data, kind))
                stats = summarize(samples)
                archive_dir = directory / "throughput_csv"
                archive_dir.mkdir(exist_ok=True)
                records = []
                for index, (path, data, kind) in enumerate(archives):
                    destination = archive_dir / ("{:03d}_".format(index) + path.name)
                    destination.write_bytes(data)
                    records.append(dict(source=str(path), archive=str(destination), capture_kind=kind))
                stats["files"] = records
                (directory / "throughput.json").write_text(json.dumps(stats, indent=2) + "\n")
                print(json.dumps(stats))
                return
            except (SearchError, OSError, UnicodeError, csv.Error) as error:
                last_error = str(error)
        if now >= deadline:
            raise SearchError(last_error)
        time.sleep(min(0.1, max(0, deadline - now)))


def positive_int(value):
    if not value.isdecimal() or int(value) < 1 or int(value) > 2147483647:
        raise argparse.ArgumentTypeError("expected an integer in 1..2147483647")
    return int(value)


def finite_float(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    return number


def ceil_grid(value):
    return math.ceil(value / STEP) * STEP


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--partitions", default="1,4,8,16,32", help="comma-separated physical partition counts")
    p.add_argument("--modes", default="normal,gst", help="normal,gst,single; single P=1 is measured only once")
    p.add_argument("--start-clients", type=positive_int, default=10)
    p.add_argument("--max-clients", type=positive_int, default=1000)
    p.add_argument("--growth-factor", type=finite_float, default=2)
    p.add_argument("--min-ratio", type=finite_float, default=0.9, help="saturated when measured/theoretical < this ratio (0.9)")
    p.add_argument("--max-probes", type=positive_int, default=32, help="maximum unique client counts per mode/P")
    p.add_argument("--broker-log-dir", type=Path, default=Path("broker"), help="locally readable/mounted broker log directory (./broker)")
    p.add_argument("--csv-glob", help="optional throughput filename glob; default includes unnumbered and numbered .csv/.log files")
    p.add_argument("--csv-wait", type=finite_float, default=10)
    p.add_argument("--csv-stable", type=finite_float, default=0.5)
    p.add_argument("--broker", default="166.104.110.13:9092")
    p.add_argument("--interval", type=finite_float, default=3, help="fixed nominal interval per client, milliseconds")
    p.add_argument("--messages-per-client", type=positive_int, default=20000)
    p.add_argument("--message-size", type=positive_int, default=1000)
    p.add_argument("--repetitions", type=positive_int, default=1, help="runs per tested client count; default minimizes experiments")
    p.add_argument("--replication-factor", type=positive_int, default=1)
    p.add_argument("--start-barrier-delay", type=int, default=2)
    p.add_argument("--drain-wait", type=int, default=20)
    p.add_argument("--cooldown", type=int, default=60)
    p.add_argument("--producer", type=Path, default=ROOT / "bin/single_topic_throughput_expr_producer")
    p.add_argument("--kafka-bin", type=Path, default=ROOT.parent / "kafka_sandbox/bin")
    p.add_argument("--runner", type=Path, default=ROOT / "script/run_gst_fixed_partition_expr.sh")
    p.add_argument("--normal-topic-template", default="test_topic_p{p}")
    p.add_argument("--gst-topic-template", default="test_gs_topic_p{p}")
    p.add_argument("--single-topic", default="test_single_topic")
    p.add_argument("--create-topics", action="store_true")
    p.add_argument("--output-root", type=Path, default=ROOT / "out/gst_saturation")
    p.add_argument("--probe-timeout", type=finite_float, help="seconds per complete candidate, including repetitions/cooldown")
    p.add_argument("--dry-run", action="store_true", help="print the adaptive policy and initial commands; no experiments/files")
    p.add_argument("--analyze-csv", type=Path, nargs="+", help="only calculate the Excel-equivalent average of these files")
    a = p.parse_args()
    if a.analyze_csv:
        return a
    try:
        a.partitions = [positive_int(value) for value in a.partitions.split(",")]
    except argparse.ArgumentTypeError as error:
        p.error(str(error))
    a.modes = a.modes.split(",")
    if len(set(a.partitions)) != len(a.partitions) or len(set(a.modes)) != len(a.modes) or any(mode not in ("normal", "gst", "single") for mode in a.modes):
        p.error("partitions/modes must be distinct; modes are normal,gst,single")
    if not 0 < a.min_ratio <= 1 or a.growth_factor <= 1 or not 0 < a.interval < 1e9:
        p.error("need 0 < min-ratio <= 1, growth-factor > 1 and 0 < interval < 1e9")
    if (a.csv_stable > a.csv_wait or a.replication_factor > 32767
            or min(a.start_barrier_delay, a.drain_wait, a.cooldown) < 0
            or max(a.start_barrier_delay, a.drain_wait, a.cooldown) > 2147483647):
        p.error("invalid CSV timing, replication factor or negative wait")
    if a.probe_timeout is not None and a.probe_timeout <= 0:
        p.error("probe-timeout must be positive")
    if a.csv_glob == "" or a.csv_glob is not None and (Path(a.csv_glob).is_absolute() or ".." in Path(a.csv_glob).parts):
        p.error("csv-glob must stay within broker-log-dir")
    for name in ("broker_log_dir", "producer", "kafka_bin", "runner", "output_root"):
        setattr(a, name, getattr(a, name).resolve())
    if a.output_root == a.broker_log_dir or a.broker_log_dir in a.output_root.parents or a.output_root in a.broker_log_dir.parents:
        p.error("broker-log-dir and output-root must be separate directory trees")
    a.combinations = list(dict.fromkeys((mode, 1 if mode == "single" else partitions) for partitions in a.partitions for mode in a.modes))
    a.max_clients = a.max_clients // STEP * STEP
    for mode, partitions in a.combinations:
        if a.max_clients < ceil_grid(partitions):
            p.error("max-clients is below the minimum C>=P on the 10-client grid")
        try:
            topic = a.single_topic if mode == "single" else getattr(a, mode + "_topic_template").format(p=partitions)
        except (KeyError, ValueError, IndexError, AttributeError) as error:
            p.error("invalid topic template: {}".format(error))
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic) or topic in (".", ".."):
            p.error("topic template must produce a valid Kafka topic name")
    if a.max_clients > 2147483647 // a.messages_per_client:
        p.error("max-clients * messages-per-client exceeds the app's --msg-cnt range")
    if not math.isfinite(a.max_clients * 1000 / a.interval):
        p.error("theoretical throughput must be finite")
    if a.probe_timeout is None:
        a.probe_timeout = max(300, a.repetitions * (a.messages_per_client * a.interval / 1000 + a.start_barrier_delay + a.drain_wait + a.cooldown + a.csv_wait + 120))
    return a


class Search:
    def __init__(self, args):
        self.args, self.output, self.child = args, None, None
        self.probes, self.boundaries = [], []

    def command(self, mode, partitions, clients, directory):
        a = self.args
        topic_option = "--single-topic" if mode == "single" else "--{}-topic".format(mode)
        topic = a.single_topic if mode == "single" else getattr(a, mode + "_topic_template").format(p=partitions)
        command = ["/bin/bash", str(a.runner), "--broker", a.broker, "--partitions", str(partitions),
                   "--clients", str(clients), "--modes", mode, "--interval", str(a.interval),
                   "--messages-per-client", str(a.messages_per_client), "--message-size", str(a.message_size),
                   "--repetitions", str(a.repetitions), "--replication-factor", str(a.replication_factor),
                   "--start-barrier-delay", str(a.start_barrier_delay), "--drain-wait", str(a.drain_wait),
                   "--cooldown", str(a.cooldown), "--producer", str(a.producer), "--kafka-bin", str(a.kafka_bin),
                   "--output-root", str(directory / "runner"), topic_option, topic,
                   "--run-hook", str(Path(__file__).resolve())]
        if a.create_topics:
            command.append("--create-topics")
        return command

    def save(self):
        for name, rows in (("probes", self.probes), ("boundaries", self.boundaries)):
            (self.output / (name + ".json")).write_text(json.dumps(rows, indent=2) + "\n")
            if rows:
                fields = list(dict.fromkeys(key for row in rows for key in row))
                with (self.output / (name + ".tsv")).open("w", newline="") as output:
                    writer = csv.DictWriter(output, fieldnames=fields, delimiter="\t")
                    writer.writeheader()
                    writer.writerows(rows)

    def stop_child(self):
        if self.child is not None:
            try:
                os.killpg(self.child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            # The runner may exit before one of its pipeline children does.
            try:
                os.killpg(self.child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.child.wait()
            self.child = None

    def probe(self, mode, partitions, clients, phase, cache):
        if clients in cache:
            return cache[clients]
        if len(cache) >= self.args.max_probes:
            raise SearchError("probe budget reached")
        directory = self.output / "{}_p{}".format(mode, partitions) / "clients_{}".format(clients)
        directory.mkdir(parents=True)
        row = dict(mode=mode, partitions=partitions, clients=clients, search_phase=phase, status="ERROR",
                   theoretical_throughput_req_per_sec=clients * 1000 / self.args.interval,
                   average_throughput_req_per_sec=None, achieved_ratio=None, saturated=None,
                   started_at=utc_now(), log_dir=str(directory), reason="")
        self.probes.append(row)
        print("{} P={} C={} ({})".format(mode, partitions, clients, phase), flush=True)
        env = dict(os.environ, GST_SATURATION_LOG_DIR=str(self.args.broker_log_dir),
                   GST_SATURATION_CSV_GLOB=self.args.csv_glob or "", GST_SATURATION_CSV_WAIT=str(self.args.csv_wait),
                   GST_SATURATION_CSV_STABLE=str(self.args.csv_stable))
        try:
            command = self.command(mode, partitions, clients, directory)
            (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n")
            with (directory / "runner.log").open("w") as log:
                self.child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
                try:
                    code = self.child.wait(timeout=self.args.probe_timeout)
                except subprocess.TimeoutExpired:
                    self.stop_child()
                    raise SearchError("runner timed out")
                self.child = None
            summaries = list((directory / "runner").glob("*/summary.tsv"))
            if len(summaries) != 1:
                raise SearchError("expected exactly one runner summary; see runner.log")
            with summaries[0].open() as source:
                runs = list(csv.DictReader(source, delimiter="\t"))
            if code or len(runs) != self.args.repetitions or any(run["status"] != "SUCCESS" for run in runs):
                details = "; ".join(run.get("reason", "") for run in runs if run["status"] != "SUCCESS")
                raise SearchError("runner exit {}: {}".format(code, details or "incomplete repetitions"))
            samples, files = [], []
            for run in runs:
                captured = json.loads((Path(run["log_dir"]) / "throughput.json").read_text())
                for record in captured["files"]:
                    path = Path(record["archive"])
                    sample = read_samples(path.read_bytes(), str(path))
                    if sample is None:
                        raise SearchError("captured CSV schema changed")
                    samples.append(sample)
                    files.append(str(path))
            stats = summarize(samples)
            numerator = Decimal(stats["success_count_sum"]) * Decimal(str(self.args.interval))
            denominator = Decimal(stats["active_duration_ms"]) * clients
            ratio = numerator / denominator
            saturated = numerator < Decimal(str(self.args.min_ratio)) * denominator
            row.update(stats, achieved_ratio=float(ratio), saturated=saturated, status="SATURATED" if saturated else "NOT_SATURATED", csv_files=files)
            cache[clients] = row
            print("  average={:.3f}, theory={:.3f}, ratio={:.2%}: {}".format(
                row["average_throughput_req_per_sec"], row["theoretical_throughput_req_per_sec"], row["achieved_ratio"], row["status"]), flush=True)
            return row
        except (SearchError, OSError, ValueError, csv.Error) as error:
            row["reason"] = str(error)
            raise SearchError(str(error))
        except KeyboardInterrupt:
            row.update(status="INTERRUPTED", reason="controller_interrupted")
            raise
        finally:
            row["finished_at"] = utc_now()
            self.save()

    def search(self, mode, partitions):
        a = self.args
        minimum = ceil_grid(partitions)
        start = min(a.max_clients, max(minimum, ceil_grid(a.start_clients)))
        cache, low, high = {}, None, None
        boundary = dict(mode=mode, partitions=partitions, client_step=STEP, min_ratio=a.min_ratio,
                        status="ERROR", last_good_clients=None, first_saturated_clients=None,
                        boundary_gap_clients=None, probes=0, reason="")
        try:
            point = self.probe(mode, partitions, start, "coarse", cache)
            if point["saturated"]:
                high = start
                while high > minimum:
                    candidate = max(minimum, math.floor(high / a.growth_factor / STEP) * STEP)
                    candidate = min(high - STEP, candidate)
                    if self.probe(mode, partitions, candidate, "coarse_down", cache)["saturated"]:
                        high = candidate
                    else:
                        low = candidate
                        break
            else:
                low = start
                while low < a.max_clients:
                    candidate = min(a.max_clients, max(low + STEP, ceil_grid(min(float(a.max_clients), low * a.growth_factor))))
                    if self.probe(mode, partitions, candidate, "coarse", cache)["saturated"]:
                        high = candidate
                        break
                    low = candidate
            while low is not None and high is not None and high - low > STEP:
                candidate = ((low // STEP + high // STEP) // 2) * STEP
                if self.probe(mode, partitions, candidate, "binary", cache)["saturated"]:
                    high = candidate
                else:
                    low = candidate
            boundary.update(status="FOUND" if low is not None and high is not None else "AT_OR_BELOW_MINIMUM" if high is not None else "NOT_FOUND_WITHIN_LIMIT")
        except SearchError as error:
            boundary["reason"] = str(error)
        boundary.update(last_good_clients=low, first_saturated_clients=high,
                        boundary_gap_clients=high - low if high is not None and low is not None else None,
                        probes=len(cache), probe_attempts=sum(1 for r in self.probes if r["mode"] == mode and r["partitions"] == partitions),
                        scheduled_run_attempts=sum(1 for r in self.probes if r["mode"] == mode and r["partitions"] == partitions) * a.repetitions)
        for prefix, clients in (("last_good", low), ("first_saturated", high)):
            boundary[prefix + "_average_throughput"] = cache[clients]["average_throughput_req_per_sec"] if clients in cache else None
        self.boundaries.append(boundary)
        self.save()
        print("  boundary: {} last_good={} first_saturated={}".format(boundary["status"], low, high), flush=True)

    def run(self):
        a = self.args
        print("Formula: SUM(success)*1000 / SUM(duration_ms WHERE success<>0)")
        print("Saturated: average / (C*1000/interval_ms) < {}; grid={} clients".format(a.min_ratio, STEP))
        if a.dry_run:
            for mode, partitions in a.combinations:
                start = min(a.max_clients, max(ceil_grid(partitions), ceil_grid(a.start_clients)))
                print("{} P={}: start C={}, grow x{}, cap C={}; then binary search".format(mode, partitions, start, a.growth_factor, a.max_clients))
                print(" ".join(shlex.quote(part) for part in self.command(mode, partitions, start, a.output_root / "dry-run")))
            return 0
        for executable in (a.runner, a.producer, a.kafka_bin / "kafka-topics.sh", a.kafka_bin / "kafka-broker-extension.sh", Path(__file__).resolve()):
            if not executable.is_file() or not os.access(str(executable), os.X_OK):
                raise SearchError("executable not found: {}".format(executable))
        if not a.broker_log_dir.is_dir():
            raise SearchError("broker-log-dir is not a readable local/mounted directory: {}".format(a.broker_log_dir))
        a.output_root.mkdir(parents=True, exist_ok=True)
        self.output = Path(tempfile.mkdtemp(prefix=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ."), dir=str(a.output_root)))
        config = {key: str(value) if isinstance(value, Path) else value for key, value in vars(a).items()}
        (self.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        (self.output / "script.py").write_bytes(Path(__file__).read_bytes())
        print("Results: {}".format(self.output), flush=True)
        for mode, partitions in a.combinations:
            self.search(mode, partitions)
        return 1 if any(result["status"] == "ERROR" for result in self.boundaries) else 0


def main():
    if len(sys.argv) == 3 and sys.argv[1] in ("before_produce", "after_rollout"):
        try:
            capture(sys.argv[1], Path(sys.argv[2]))
            return 0
        except (SearchError, OSError, ValueError, KeyError, UnicodeError, csv.Error) as error:
            print("CSV capture failed: {}".format(error), file=sys.stderr)
            return 1
    args = parse_args()
    if args.analyze_csv:
        try:
            samples = [read_samples(path.read_bytes(), str(path)) for path in args.analyze_csv]
            if any(sample is None for sample in samples):
                raise SearchError("an input file lacks the required throughput columns")
            print(json.dumps(summarize(samples), indent=2))
            return 0
        except (SearchError, OSError, ValueError, UnicodeError, csv.Error) as error:
            print("CSV analysis failed: {}".format(error), file=sys.stderr)
            return 2
    search = Search(args)

    def interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        return search.run()
    except KeyboardInterrupt:
        print("Interrupted; stopping the current experiment group.", file=sys.stderr)
        return 130
    except (SearchError, OSError) as error:
        print("Error: {}".format(error), file=sys.stderr)
        return 2
    finally:
        search.stop_child()


if __name__ == "__main__":
    sys.exit(main())
