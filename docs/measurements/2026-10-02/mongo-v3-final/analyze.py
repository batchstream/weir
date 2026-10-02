import datetime
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tarfile


def load_jsonl(bundle, name):
    return [json.loads(line) for line in bundle.extractfile(name).read().splitlines()]


def metric(raw, name, label=None):
    values = [float(line.split()[-1]) for line in raw.splitlines()
              if (line.startswith(name + "{") or line.startswith(name + " "))
              and (label is None or label in line.split()[0])]
    return sum(values)


def resources(rows, quota):
    if len(rows) < 2:
        raise RuntimeError("missing measured resource samples")
    first, last = rows[0], rows[-1]
    if len({row["process_start_ticks"] for row in rows}) != 1:
        raise RuntimeError("observed process restarted")
    elapsed = last["monotonic"] - first["monotonic"]
    cores = (last["process_cpu_ticks"] - first["process_cpu_ticks"]) / 100 / elapsed
    return dict(sample_count=len(rows), sample_seconds=elapsed, cpu_cores=cores,
                quota_cpu=quota, quota_percent=cores / quota * 100,
                cgroup_cpu_cores=(last["cpu"]["usage_usec"]-first["cpu"]["usage_usec"]) / 1e6 / elapsed,
                rss_max_MiB=max(row["rss_pages"] for row in rows) / 256,
                cgroup_memory_max_MiB=max(row["memory_current"] for row in rows) / 2**20,
                oom_kills=last["memory_events"]["oom_kill"]-first["memory_events"]["oom_kill"])


def scalar_summary(value):
    if isinstance(value, dict):
        summary = {key: scalar_summary(item) for key, item in value.items() if key != "buckets"}
        return summary
    if isinstance(value, list):
        summary = [scalar_summary(item) for item in value]
        return summary
    return value


def analyze(archive, cases_path, tag="formal"):
    cases = json.loads(cases_path.read_text())
    runs = []
    with tarfile.open(archive) as bundle:
        for case in cases:
            prefix = case["prefix"]
            stem = tag + "/" + prefix
            records = load_jsonl(bundle, stem + "-client.jsonl")
            record = next(row for row in records if row.get("type") == "trial")
            audit = next(row for row in records if row.get("type") == "audit")
            trial = record["trial"]
            if record["run_error"] != "<nil>" or audit["error"] != "<nil>":
                raise RuntimeError("failed correctness or generation audit: " + prefix)
            begin = datetime.datetime.fromisoformat(trial["start"].replace("Z", "+00:00")).timestamp()+case["warm"]
            end = begin + case["seconds"]
            measured = lambda rows: [row for row in rows if begin <= row["wall_time"] <= end]
            db = measured(load_jsonl(bundle, stem + "-database.jsonl"))
            all_ops = trial["measure"]["all"]
            result = dict(case=case, success_rps=all_ops["success"] / case["seconds"],
                          p95_ms=all_ops["arrival"]["p95_us"] / 1000,
                          client_drop=all_ops["client_drop"], api_failures=all_ops.get("failures") or {},
                          unknown=all_ops["unknown"], audit=audit["audit"],
                          database=resources(db, .25), start=trial["start"], windows=scalar_summary(trial["ten_second_windows"]))
            if case["mode"] != "direct":
                rows = measured(load_jsonl(bundle, stem + "-weir.jsonl"))
                result["weir"] = resources(rows, 2)
                first, last = rows[0]["metrics"], rows[-1]["metrics"]
                count = metric(last, "weir_store_batch_operations_count")-metric(first, "weir_store_batch_operations_count")
                ops = metric(last, "weir_store_batch_operations_sum")-metric(first, "weir_store_batch_operations_sum")
                result["weir"].update(batch_operations_mean=ops / count if count else None,
                    execution_batches=int(count),
                    window_min=min(metric(row["metrics"], "weir_store_window") for row in rows),
                    window_max=max(metric(row["metrics"], "weir_store_window") for row in rows),
                    latency_events=int(metric(last,"weir_store_backpressure_events_total", 'reason="latency"')-metric(first,"weir_store_backpressure_events_total", 'reason="latency"')),
                    backend_events=int(metric(last,"weir_store_backpressure_events_total", 'reason="backend"')-metric(first,"weir_store_backpressure_events_total", 'reason="backend"')))
            runs.append(result)
    groups=[]
    for write_every in dict.fromkeys(case["write_every"] for case in cases):
        for rate in dict.fromkeys(case["rate"] for case in cases):
            for mode in dict.fromkeys(case["path"] for case in cases):
                selected=[run for run in runs if run["case"]["write_every"]==write_every and run["case"]["rate"]==rate and run["case"]["path"]==mode]
                if len(selected)!=3:raise RuntimeError("incomplete repeated group")
                median=lambda f:statistics.median(f(run) for run in selected)
                groups.append(dict(path=mode, write_every=write_every, offered_rps=rate,
                    success_rps=median(lambda r:r["success_rps"]), p95_ms=median(lambda r:r["p95_ms"]),
                    success_rps_range=[min(r["success_rps"] for r in selected),max(r["success_rps"] for r in selected)],
                    database_cpu=median(lambda r:r["database"]["cpu_cores"]), database_rss_MiB=median(lambda r:r["database"]["rss_max_MiB"]),
                    database_cpu_range=[min(r["database"]["cpu_cores"] for r in selected),max(r["database"]["cpu_cores"] for r in selected)],
                    weir_cpu=median(lambda r:r.get("weir",{}).get("cpu_cores",0)),weir_rss_MiB=median(lambda r:r.get("weir",{}).get("rss_max_MiB",0)),
                    client_drop_sum=sum(r["client_drop"] for r in selected), api_failures_sum=sum(sum(r["api_failures"].values()) for r in selected),
                    unknown_sum=sum(r["unknown"] for r in selected), batch_operations=median(lambda r:r.get("weir",{}).get("batch_operations_mean",0))))
    output=dict(method="3 repetitions with alternating path order; median process CPU and peak sampled RSS; 100Hz process ticks and 4096-byte pages verified in DB container; fixed20s measurement after10s warmup; all paths use the same unchanged client-v2 and 64 workers; Weir is v3; database process is shared across runs and RSS includes accumulated cache", archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),groups=groups,runs=runs)
    return output


if __name__ == "__main__":
    output=analyze(Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[4] if len(sys.argv)>4 else "formal")
    Path(sys.argv[3]).write_text(json.dumps(output,indent=2))
    print(json.dumps(output["groups"],indent=2))
