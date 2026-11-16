"""Collect per-run broker logs and analyze the tagged B records (stdlib only)."""

import csv
from decimal import Decimal
import hashlib
import io
import json
import math
from pathlib import Path
import time

from run_gst_saturation_search import (
    COLUMNS, SearchError as DataError, log_order, number, read_samples,
    signature, summarize,
)


LATENCY_COLUMNS = ("Content", "Status", "RequestedAt", "RespondedAt", "Latency")
STAGES = {"IO_COMMITED": "io", "NETWORK_PROCESSED": "network"}


def header(data):
    first = next((line for line in data.decode("utf-8-sig").splitlines() if line.strip()), "")
    delimiter = "\t" if "\t" in first else ","
    fields = next(csv.reader([first], delimiter=delimiter), [])
    return [field.strip() for field in fields], delimiter


def log_files(root, throughput_glob=None, latency_glob=None):
    selected = {}
    for kind, pattern, columns in (("throughput", throughput_glob, COLUMNS),
                                    ("latency", latency_glob, LATENCY_COLUMNS)):
        for glob in ((pattern,) if pattern else ("*.csv", "*.log")):
            for path in root.glob(glob):
                if not path.is_file():
                    continue
                try:
                    with path.open("rb") as source:
                        first = next((line for line in source if line.strip()), b"")
                    fields, _ = header(first)
                    if set(columns).issubset(fields):
                        selected[path] = kind
                except (FileNotFoundError, UnicodeError, csv.Error):
                    continue
    return sorted(selected.items(), key=lambda item: log_order(root, item[0]))


def read_stable(path):
    stat = signature(path)
    data = path.read_bytes()
    if signature(path) != stat:
        raise DataError("broker log changed while reading: {}".format(path))
    return stat, data


def snapshot(root, directory, throughput_glob=None, latency_glob=None):
    before = {}
    for path, kind in log_files(root, throughput_glob, latency_glob):
        stat, data = read_stable(path)
        before[str(path.relative_to(root))] = dict(
            signature=stat, sha256=hashlib.sha256(data).hexdigest(), kind=kind,
            complete_offset=len(data) if data.endswith(b"\n") else data.rfind(b"\n") + 1)
    (directory / "broker_logs_before.json").write_text(json.dumps(before, indent=2) + "\n")


def latency_summary(values):
    values = sorted(values)
    count = len(values)
    if not count:
        raise DataError("no B latency samples")
    result = dict(samples=count, mean_ms=math.fsum(values) / count,
                  min_ms=values[0], max_ms=values[-1])
    for percentile in (50, 95, 99):
        result["p{}_ms".format(percentile)] = values[math.ceil(count * percentile / 100) - 1]
    return result


def analyze(records, prefix):
    throughput = []
    latencies = {stage: [] for stage in STAGES}
    contents = {stage: set() for stage in STAGES}
    excluded = unknown = mismatches = 0
    for record in records:
        path = Path(record["archive"])
        data = path.read_bytes()
        if record["kind"] == "throughput":
            sample = read_samples(data, str(path))
            if sample is None:
                raise DataError("throughput schema changed: {}".format(path))
            throughput.append(sample)
            continue
        fields, delimiter = header(data)
        if len(fields) != len(set(fields)):
            raise DataError("duplicate latency columns: {}".format(path))
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig")), delimiter=delimiter)
        next((row for row in reader if any(cell.strip() for cell in row)), None)
        indices = {field: fields.index(field) for field in LATENCY_COLUMNS}
        for line, row in enumerate(reader, 2):
            if not any(cell.strip() for cell in row) or [cell.strip() for cell in row] == fields:
                continue
            try:
                content = row[indices["Content"]]
                if not content.startswith("R" + prefix + "_"):
                    excluded += 1
                    continue
                stage = row[indices["Status"]].strip()
                if stage not in STAGES:
                    unknown += 1
                    continue
                latency = number(row[indices["Latency"]], "Latency")
                requested = number(row[indices["RequestedAt"]], "RequestedAt")
                responded = number(row[indices["RespondedAt"]], "RespondedAt")
                value = float(latency)
                if not math.isfinite(value):
                    raise DataError("latency is not a finite float")
            except (IndexError, DataError) as error:
                raise DataError("{}:{}: {}".format(path, line, error))
            mismatches += int(latency != responded - requested)
            latencies[stage].append(value)
            contents[stage].add(content)
    totals = summarize(throughput)
    result = dict(
        b_store_rps=totals["average_throughput_req_per_sec"],
        b_store_success=totals["success_count_sum"], b_store_failed=totals["failed_count_sum"],
        b_active_duration_ms=totals["active_duration_ms"], b_total_duration_ms=totals["total_duration_ms"],
        b_throughput_rows=totals["rows"], b_zero_success_rows=totals["zero_success_rows"],
        b_zero_success_row_fraction=totals["zero_success_rows"] / totals["rows"],
        b_latency_excluded_rows=excluded, b_latency_unknown_status_rows=unknown,
        b_latency_timestamp_mismatch_rows=mismatches,
        b_latency_complete_contents=len(set.intersection(*contents.values())),
        b_latency_unit="ms", b_latency_percentile_method="nearest_rank")
    total_ms = Decimal(totals["total_duration_ms"])
    result["b_store_rps_all_intervals"] = float(Decimal(totals["success_count_sum"]) * 1000 / total_ms)
    for stage, label in STAGES.items():
        if not latencies[stage]:
            raise DataError("no current-run B samples for {}".format(stage))
        result.update({"b_{}_latency_{}".format(label, key): value
                       for key, value in latency_summary(latencies[stage]).items()})
        result["b_{}_latency_distinct_contents".format(label)] = len(contents[stage])
    return result


def capture(root, directory, prefix, wait=10, stable=0.5,
            throughput_glob=None, latency_glob=None):
    before = json.loads((directory / "broker_logs_before.json").read_text())
    by_identity = {tuple(item["signature"][:2]): item for item in before.values()}
    deadline, previous, stable_since = time.monotonic() + wait, None, time.monotonic()
    last_error = "no new throughput/latency logs after B production"
    while True:
        try:
            changed = []
            for path, kind in log_files(root, throughput_glob, latency_glob):
                stat = signature(path)
                old_name = before.get(str(path.relative_to(root)))
                if old_name is None or stat != old_name["signature"]:
                    changed.append((path, kind, stat))
            current = [(str(path), kind, stat) for path, kind, stat in changed]
            now = time.monotonic()
            if current != previous:
                previous, stable_since = current, now
            if changed and now - stable_since >= stable:
                records, identities = [], set()
                archive = directory / "broker_logs"
                archive.mkdir(exist_ok=True)
                for path, kind, stat in changed:
                    identity = tuple(stat[:2])
                    if identity in identities:
                        continue
                    identities.add(identity)
                    checked, data = read_stable(path)
                    if checked != stat:
                        raise DataError("broker log is still being written: {}".format(path))
                    old = by_identity.get(identity)
                    old_name = before.get(str(path.relative_to(root)))
                    renamed = old and (old_name is None or tuple(old_name["signature"][:2]) != identity)
                    if renamed and hashlib.sha256(data).hexdigest() == old["sha256"]:
                        continue
                    capture_kind = "new_or_rewritten"
                    if old and len(data) > old["signature"][2] > 0:
                        old_size = old["signature"][2]
                        if hashlib.sha256(data[:old_size]).hexdigest() == old["sha256"]:
                            first = next(line for line in data.splitlines() if line.strip())
                            appended = data[old_size:]
                            if old["complete_offset"] != old_size:
                                appended = appended.partition(b"\n")[2]
                            data = first + b"\n" + appended
                            capture_kind = "appended_rows"
                    destination = archive / ("{:03d}_".format(len(records)) + path.name)
                    destination.write_bytes(data)
                    records.append(dict(source=str(path), archive=str(destination),
                                        kind=kind, capture_kind=capture_kind))
                (directory / "broker_logs.json").write_text(json.dumps(records, indent=2) + "\n")
                if {record["kind"] for record in records} != {"throughput", "latency"}:
                    raise DataError("need both new throughput and latency logs")
                result = analyze(records, prefix)
                (directory / "b_metrics.json").write_text(json.dumps(result, indent=2) + "\n")
                return result
        except (DataError, OSError, UnicodeError, csv.Error, StopIteration) as error:
            last_error = str(error)
        if time.monotonic() >= deadline:
            (directory / "capture_error.txt").write_text(last_error + "\n")
            raise DataError(last_error)
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
