#!/usr/bin/env python3
"""Run the fixed-partition experiment and delete its topics after each client count."""

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile


RUNNER = Path(__file__).resolve().with_name("run_gst_fixed_partition_expr.sh")


def client_counts(value):
    if not re.fullmatch(r"[1-9][0-9]{0,9}(,[1-9][0-9]{0,9})*", value):
        raise argparse.ArgumentTypeError("expected comma-separated positive client counts")
    counts = list(map(int, value.split(",")))
    if max(counts) > 2147483647 or any(a >= b for a, b in zip(counts, counts[1:])):
        raise argparse.ArgumentTypeError("client counts must increase within 1..2147483647")
    return counts


def cleanup_completed(group_dir, expected_topics):
    reports = list(group_dir.glob("*/topic_cleanup.tsv"))
    if len(reports) != 1:
        return False
    with reports[0].open(newline="") as report:
        rows = list(csv.DictReader(report, delimiter="\t"))
    return len(rows) == expected_topics and all(row.get("status") == "DELETED" for row in rows)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, allow_abbrev=False,
        epilog=f"Other experiment options are forwarded to {RUNNER.name} (see its --help). "
               "Each group creates missing topics and deletes validated topics, including pre-existing ones.")
    parser.add_argument("--clients", type=client_counts, default="16,32,64,128,160,192")
    parser.add_argument("--modes", default="normal,gst")
    parser.add_argument("--output-root", type=Path, default=RUNNER.parents[1] / "out/gst_fixed_partition_sweep")
    parser.add_argument("--dry-run", action="store_true", help="print the grouped plan without broker access")
    args, forwarded = parser.parse_known_args()
    if "--cleanup-only" in forwarded:
        parser.error(f"use {RUNNER.name} directly for --cleanup-only")

    active = None
    interrupted = 0
    signal_forwarded = False

    def stop(signum, _frame):
        nonlocal interrupted, signal_forwarded
        if not interrupted:
            interrupted = signum
        if active is not None and not signal_forwarded:
            signal_forwarded = True
            active.send_signal(interrupted)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    if not args.dry_run:
        args.output_root.mkdir(parents=True, exist_ok=True)
        prefix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ.")
        sweep_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=args.output_root))
        print(f"Results: {sweep_dir}", flush=True)

    status = 0
    for clients in args.clients:
        if interrupted:
            break
        command = [str(RUNNER), "--clients", str(clients), "--modes", args.modes,
                   "--create-topics", "--delete-topics-after", *forwarded]
        if args.dry_run:
            command.append("--dry-run")
        else:
            group_dir = sweep_dir / f"clients_{clients}"
            command.extend(("--output-root", str(group_dir)))
        print(f"\n=== Client count {clients} ===", flush=True)
        # Forward a terminal interrupt once and let the runner finish its EXIT cleanup.
        active = subprocess.Popen(command, start_new_session=True)
        if interrupted:
            stop(interrupted, None)
        code = active.wait()
        active = None
        if interrupted:
            break
        if code not in (0, 1) or (args.dry_run and code):
            return code if code >= 0 else 128 - code
        if not args.dry_run and not cleanup_completed(group_dir, len(args.modes.split(","))):
            print(f"Topic deletion was not confirmed for clients={clients}. See {group_dir}", file=sys.stderr)
            return 1
        # A failed measurement does not prevent later groups after confirmed cleanup.
        status = max(status, code)
    return 128 + interrupted if interrupted else status


if __name__ == "__main__":
    sys.exit(main())
