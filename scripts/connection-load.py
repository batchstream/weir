#!/usr/bin/env python3
"""Read-only multi-process connection verification against an owned fixture.

This script never creates databases, containers, pods or Weir instances. Seed
the corpus once using the capacity setup mode before invoking it. Commands are
JSON argv arrays, never shell strings; each client command starts a real process.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import statistics
import subprocess
import time
from urllib.parse import urlsplit


def split_rates(rate, processes):
    if not 1 <= processes <= 64 or not processes <= rate <= 10000:
        raise ValueError("require 1-64 processes and processes <= rate <= 10000")
    rates = [rate // processes + (n < rate % processes) for n in range(processes)]
    return rates


def argv(raw):
    command = json.loads(raw)
    if not isinstance(command, list) or not command or any(type(v) is not str or not v for v in command):
        raise ValueError("executor must be a nonempty JSON string array")
    return command


def timestamp(raw):
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp requires timezone")
    return parsed


def read_jsonl(path):
    if path.stat().st_size > 8 << 20:
        raise ValueError("connection evidence file exceeds 8 MiB")
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not records:
        raise ValueError("empty connection evidence")
    return records


def summarize(root):
    reports = []
    for path in sorted(root.glob("client-*.jsonl")):
        records = read_jsonl(path)
        if len(records) != 1 or records[0].get("type") != "connection_probe":
            raise ValueError("invalid client connection receipt: " + path.name)
        report = records[0]
        if report["planned"] != report["started"] + report["client_drop"]:
            raise ValueError("client planned operation conservation")
        if report["started"] != report["success"] + sum(report["failures"].values()):
            raise ValueError("client completed operation conservation")
        reports.append(report)
    if not reports or len({r["pid"] for r in reports}) != len(reports):
        raise ValueError("missing or non-distinct client processes")
    starts = {timestamp(r["start"]) for r in reports}
    seconds = {r["options"]["seconds"] for r in reports}
    if len(starts) != 1 or len(seconds) != 1:
        raise ValueError("processes must share the same offered-work interval")
    start = starts.pop()
    duration = seconds.pop()
    end = start + timedelta(seconds=duration)
    idle_start = max(timestamp(r["end"]) for r in reports)
    idle_end = min(timestamp(r["idle_end"]) for r in reports)
    closed = max(timestamp(r["closed"]) for r in reports)
    samples = read_jsonl(root / "observer.jsonl")
    identities = {(s["pid"], s["start_ticks"]) for s in samples}
    if len(identities) != 1 or any(s["type"] != "connection_sample" or s["established"] != s["states"].get("01", 0) for s in samples):
        raise ValueError("observer identity or socket-state conservation")
    phases = {
        "baseline": (start - timedelta(seconds=2), start),
        "active": (start + timedelta(seconds=1), end - timedelta(milliseconds=250)),
        "idle_clients_open": (idle_start + timedelta(milliseconds=250), idle_end - timedelta(milliseconds=250)),
        "clients_closed_weir_retained": (closed + timedelta(milliseconds=500), timestamp(samples[-1]["time"])),
    }
    phase_results = {}
    for name, (begin, finish) in phases.items():
        selected = [s for s in samples if begin <= timestamp(s["time"]) <= finish]
        values = [s["established"] for s in selected]
        if len(values) < 2 and name != "idle_clients_open":
            raise ValueError("insufficient socket coverage for " + name)
        phase_results[name] = {
            "sample_count": len(values),
            "min": min(values) if values else None,
            "median": statistics.median(values) if values else None,
            "max": max(values) if values else None,
            "last": values[-1] if values else None,
            "fd_close_races": sum(s["lost_fds"] for s in selected),
        }
    result = {
        "client_processes": len(reports),
        "offered_rps_total": sum(r["rate"] for r in reports),
        "measurement_seconds": duration,
        "planned": sum(r["planned"] for r in reports),
        "success": sum(r["success"] for r in reports),
        "client_drop": sum(r["client_drop"] for r in reports),
        "api_failures": sum(sum(r["failures"].values()) for r in reports),
        "qualified": all(r["success"] == r["planned"] for r in reports),
        "observed_database_process": samples[0]["pid"],
        "observed_process_start_ticks": samples[0]["start_ticks"],
        "sampled_established_peak": max(s["established"] for s in samples),
        "phases": phase_results,
        "clients": reports,
        "scope": "Sampled database-owned ESTABLISHED TCP sockets; not configured pool sizes. Weir remains running after clients close.",
    }
    return result


def connection_budget(options):
    pools = [int(v) for v in options.store_concurrency.split(",")]
    if not pools or len(pools) > 16 or any(not 1 <= pool <= 32 for pool in pools):
        raise ValueError("one concurrency limit (1-32) for each LocalStore on this backend")
    if options.max_replicas < 1 or options.max_surge < 0 or options.terminating_pods < 0 or options.other_connections < 0 or options.database_budget < 1:
        raise ValueError("invalid aggregate connection budget")
    per_replica = sum(pool + 1 for pool in pools)
    active_and_terminating = options.max_replicas + options.max_surge + options.terminating_pods
    required = active_and_terminating * per_replica + options.other_connections
    available = options.database_budget - options.other_connections
    replica_cap = max(0, available // per_replica - options.max_surge - options.terminating_pods)
    result = {
        "local_stores_on_backend": len(pools),
        "raw_tcp_limit_per_replica": per_replica,
        "steady_weir_raw_tcp_limit": options.max_replicas * per_replica,
        "active_and_terminating_replica_budget": active_and_terminating,
        "other_connection_reservation": options.other_connections,
        "required_database_connection_budget": required,
        "configured_database_connection_budget": options.database_budget,
        "fits": required <= options.database_budget,
        "max_replicas_under_assumed_overlap": replica_cap,
        "assumption": "Each LocalStore owns max_concurrency + 1 raw TCP credits. Terminating-pod overlap is supplied, not guaranteed by maxSurge: 0 or HPA maxReplicas.",
    }
    return result


def run_probes(options):
    backend = urlsplit(options.backend)
    if backend.username is not None or backend.password is not None or backend.scheme not in ("http", "https", "mongodb"):
        raise ValueError("use an explicit credential-free owned fixture endpoint")
    rates = split_rates(options.rate, options.processes)
    client_command = argv(options.client_command_json)
    observer_command = argv(options.observer_command_json)
    if not 3 <= options.start_delay <= 30 or not 2 <= options.seconds <= 120 or not 0 <= options.idle_seconds <= 45 or not 2 <= options.closed_seconds <= 15:
        raise ValueError("connection experiment timing bounds")
    root = options.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    targets = options.targets.split(",") if options.targets else [""]
    start = datetime.now(timezone.utc) + timedelta(seconds=options.start_delay)
    total_seconds = math.ceil(options.start_delay + options.seconds + options.idle_seconds + options.closed_seconds + 2)
    if total_seconds > 180:
        raise ValueError("observer lifetime exceeds 180 seconds")
    shared_start = start.isoformat().replace("+00:00", "Z")
    commands = [("observer", observer_command + [
        "-mode", "connection-observe", "-pid", options.database_pid,
        "-connection-port", str(options.database_port), "-seconds", str(total_seconds),
        "-connection-interval-ms", str(options.interval_ms),
    ])]
    for n, rate in enumerate(rates):
        command = client_command + [
            "-mode", "connection-probe", "-backend", options.backend,
            "-rate", str(rate), "-seconds", str(options.seconds),
            "-workers", str(options.workers), "-pool", str(options.pool),
            "-connection-idle-seconds", str(options.idle_seconds),
            "-connection-start-at", shared_start,
        ]
        target = targets[n % len(targets)]
        if target:
            command += ["-target", target]
        commands.append(("client-" + str(n), command))
    (root / "commands.json").write_text(json.dumps(dict(commands), indent=2) + "\n")
    processes, files = [], []
    try:
        for name, command in commands:
            stdout = (root / (name + ".jsonl")).open("w")
            stderr = (root / (name + ".stderr.log")).open("w")
            files.extend((stdout, stderr))
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL)
            processes.append((name, process))
        deadline = time.monotonic() + total_seconds + 15
        for name, process in reversed(processes):
            result = process.wait(timeout=max(1, deadline - time.monotonic()))
            if result:
                raise RuntimeError(name + " failed; inspect its saved stderr")
    finally:
        for _, process in processes:
            if process.poll() is None:
                process.terminate()
        for _, process in processes:
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for file in files:
            file.close()
    result = summarize(root)
    (root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(root / "summary.json")
    return 0 if result["qualified"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    experiment = commands.add_parser("run", help="launch bounded probes in an existing fixture")
    experiment.add_argument("--client-command-json", required=True)
    experiment.add_argument("--observer-command-json", required=True)
    experiment.add_argument("--backend", required=True)
    experiment.add_argument("--targets", default="", help="comma-separated explicit Weir instances; empty means direct")
    experiment.add_argument("--database-pid", required=True)
    experiment.add_argument("--database-port", type=int, required=True)
    experiment.add_argument("--processes", type=int, default=16)
    experiment.add_argument("--rate", type=int, default=800)
    experiment.add_argument("--seconds", type=int, default=20)
    experiment.add_argument("--idle-seconds", type=int, default=12)
    experiment.add_argument("--closed-seconds", type=int, default=5)
    experiment.add_argument("--start-delay", type=int, default=8)
    experiment.add_argument("--interval-ms", type=int, default=200)
    experiment.add_argument("--workers", type=int, default=4)
    experiment.add_argument("--pool", type=int, default=4)
    experiment.add_argument("--output", type=Path, required=True)
    summary = commands.add_parser("summarize")
    summary.add_argument("root", type=Path)
    budget = commands.add_parser("budget", help="check one database's aggregate HPA/raw-socket budget")
    budget.add_argument("--store-concurrency", required=True, help="limits for all LocalStores targeting this backend, e.g. 4,4")
    budget.add_argument("--max-replicas", type=int, required=True)
    budget.add_argument("--max-surge", type=int, default=0)
    budget.add_argument("--terminating-pods", type=int, required=True, help="assumed bound on pods retaining backend sockets during termination")
    budget.add_argument("--other-connections", type=int, required=True)
    budget.add_argument("--database-budget", type=int, required=True)
    options = parser.parse_args()
    if options.command == "run":
        return run_probes(options)
    if options.command == "summarize":
        print(json.dumps(summarize(options.root), indent=2))
        return 0
    result = connection_budget(options)
    print(json.dumps(result, indent=2))
    return 0 if result["fits"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
