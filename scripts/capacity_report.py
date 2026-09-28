"""Fixed M22R evidence validation; missing observations never become zero."""
import datetime
import json
import math
import re

from capacity_contract import PLAN


def timestamp(value):
    result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    if not math.isfinite(result):
        raise ValueError("invalid timestamp")
    return result


def number(value, positive=False):
    if isinstance(value, bool):
        raise ValueError("boolean used as number")
    result = float(value)
    if not math.isfinite(result) or result < 0 or (positive and result == 0):
        raise ValueError("missing/nonfinite/negative resource value")
    return result


def prom(raw):
    if not isinstance(raw, str) or not raw.endswith("\n"):
        raise ValueError("missing/empty/truncated metrics")
    values = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^{}]*\})?) ([^ ]+)', line)
        if not match or match[1] in values:
            raise ValueError("malformed/duplicate metric")
        values[match[1]] = number(match[2])
    if not values:
        raise ValueError("empty metrics")
    return values


def metric(values, name):
    # This fixed profile has exactly one store. Never aggregate unrelated labels.
    key = name+'{store="records"}' if name.startswith(("weir_store_", "weir_backend_")) else name
    matches = [k for k in values if k.split("{")[0] == name]
    if matches != [key]:
        raise ValueError("missing/mismatched metric: "+name)
    if int(values[key]) != values[key]:
        raise ValueError("fractional fixed-profile gauge: "+name)
    return values[key]


def counter(raw):
    if not isinstance(raw, str) or not raw.endswith("\n"):
        raise ValueError("missing/truncated counter file")
    result = {}
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] in result or not parts[1].isdigit():
            raise ValueError("invalid/duplicate counter")
        result[parts[0]] = int(parts[1])
    return result


def histogram_count(hist, count):
    buckets = hist["buckets"]
    if any(not isinstance(b["count"], int) or b["count"] <= 0 for b in buckets):
        raise RuntimeError("invalid histogram bucket")
    bounds = [b["upper_us"] if b["upper_us"] >= 0 else math.inf for b in buckets]
    if bounds != sorted(set(bounds)) or sum(b["count"] for b in buckets) != count:
        raise RuntimeError("histogram count/order identity")
    for p in (50, 95, 99):
        want = (count*p+99)//100
        accumulated = 0
        calculated = -1
        for bucket in buckets:
            accumulated += bucket["count"]
            if accumulated >= want:
                calculated = bucket["upper_us"]
                break
        if hist["p"+str(p)+"_us"] != calculated:
            raise RuntimeError("histogram quantile mismatch")


def window_gate(window):
    reasons = []
    for key in ("planned", "started", "completed", "success", "client_drop", "client_late", "unknown"):
        if any(not isinstance(window[k][key], int) or window[k][key] < 0 for k in ("all", "read", "put")):
            raise RuntimeError("invalid count")
        if window["all"][key] != window["read"][key]+window["put"][key]:
            raise RuntimeError("all != read + put")
    for kind in ("all", "read", "put"):
        m = window[kind]
        if m["planned"] != m["completed"]+m["client_drop"] or m["started"] != m["completed"]:
            raise RuntimeError("count identity")
        if m["completed"] != m["success"]+sum((m["failures"] or {}).values()):
            raise RuntimeError("outcome identity")
        failures = m["failures"] or {}
        if any(failures.get(key, 0) for key in ("payload_or_response", "correlation", "invalid_bulk", "invalid_ack", "invalid_outcome")):
            raise RuntimeError("correctness response failure; stop calibration")
        if m["client_drop"] or m["unknown"] or m["success"] != m["planned"]:
            reasons.append(kind+": errors/drop/unknown")
        for name in ("arrival", "dispatch", "lag"):
            histogram_count(m[name], m["completed"])
        if "due" in m:
            if m["planned"] != m["due"]+m["cancelled_future"] or sum((m["drop_reasons"] or {}).values()) != m["client_drop"]:
                raise RuntimeError("due/drop timing identity")
            for name in ("wake", "decision", "construct"):
                histogram_count(m[name], m["due"])
            for name in ("handoff", "worker_start"):
                histogram_count(m[name], m["completed"]+m.get("worker_expired", 0))
        for histogram, quantile, bound in (("arrival", "p95_us", PLAN["thresholds"]["p95_us"]),
                                           ("arrival", "p99_us", PLAN["thresholds"]["p99_us"]),
                                           ("lag", "p99_us", PLAN["thresholds"]["lag_p99_us"])):
            if m["planned"] and not 0 <= m[histogram][quantile] <= bound:
                reasons.append(kind+":"+histogram+"/"+quantile)
    return reasons


def weir_metrics(raw, stable):
    m = prom(raw)
    required = dict(weir_memory_sample_observed=1, weir_memory_process_valid=1,
                    weir_memory_cgroup_valid=1, weir_memory_cgroup_finite=1,
                    weir_memory_unknown=0, weir_memory_budget_bytes=768*1024**2,
                    weir_memory_cgroup_limit_bytes=1024**3)
    for name, value in required.items():
        if metric(m, name) != value:
            raise ValueError("invalid Guard evidence: "+name)
    labelled = {
        'weir_memory_cgroup_state{state="v2"}': 1,
        'weir_memory_cgroup_state{state="unknown"}': 0,
        'weir_memory_cgroup_state{state="profile_changed"}': 0,
        'weir_memory_cgroup_state{state="not_applicable"}': 0,
        'weir_memory_cgroup_scope{scope="leaf"}': 1,
        'weir_memory_cgroup_scope{scope="ancestor"}': 0,
        'weir_memory_cgroup_scope{scope="none"}': 0,
        'weir_memory_sample_bytes{source="darwin_phys_footprint"}': 0,
        'weir_memory_sample_bytes{source="go_sys_minus_released"}': 0,
        'weir_memory_sample_bytes{source="unobserved"}': 0,
    }
    for key, expected in labelled.items():
        if m[key] != expected:
            raise ValueError("invalid Guard label semantics: "+key)
    if not 0 < m['weir_memory_sample_bytes{source="linux_rss"}'] <= 1024**3 or metric(m, "weir_memory_cgroup_levels") != 1:
        raise ValueError("Guard process/visible-cgroup profile")
    if number(metric(m, "weir_memory_cgroup_current_bytes"), True) > 1024**3:
        raise ValueError("Guard cgroup boundary")
    for name in ("native_reserved_bytes", "native_sessions", "retained_result_reserved_bytes", "retained_results", "scan_cleanups", "scan_page_reserved_bytes", "scan_pages", "scan_sessions"):
        if metric(m, "weir_store_"+name) != 0:
            raise ValueError("unexpected inactive ledger: "+name)
    limits = {"pending_entries": ("pending_entries_limit", 256),
              "pending_reserved_bytes": ("pending_reserved_bytes_limit", 8<<20),
              "result_reserved_entries": ("result_reserved_entries_limit", 128),
              "result_reserved_bytes": ("result_reserved_bytes_limit", 16<<20),
              "active_executions": ("window_limit", 4), "live_sessions": ("live_sessions_limit", 1)}
    for name, (bound, expected) in limits.items():
        if metric(m, "weir_store_"+bound) != expected or metric(m, "weir_store_"+name) > expected:
            raise ValueError("hard ledger boundary: "+name)
    for prefix, maximum in (("weir_backend_connections", 5), ("weir_ingress_connections", 16),
                            ("weir_ingress_sessions", 16), ("weir_diagnostic_connections", 4),
                            ("weir_diagnostic_handlers", 2)):
        current = prefix+"_owned" if prefix == "weir_backend_connections" else prefix
        if metric(m, prefix+"_limit") != maximum or metric(m, current) > maximum:
            raise ValueError("hard connection boundary: "+prefix)
    if metric(m, "weir_backend_connections_peak") > 5:
        raise ValueError("hard owner peak boundary")
    latched = metric(m, "weir_memory_latched")
    if latched not in (0, 1):
        raise ValueError("invalid latch")
    reasons = ["stable Guard latched"] if stable and latched else []
    return reasons, metric(m, "weir_backend_connections_owned"), metric(m, "weir_ingress_connections")


def tcp_connections(files):
    active = 0
    for key in ("net/tcp", "net/tcp6"):
        lines = files[key].splitlines()
        if not lines or "local_address" not in lines[0] or "st" not in lines[0].split():
            raise ValueError("missing/truncated TCP snapshot")
        for line in lines[1:]:
            if not line.strip():
                continue
            fields = line.split()
            if len(fields) < 10 or not re.fullmatch(r"[0-9A-F]{2}", fields[3]):
                raise ValueError("malformed TCP snapshot")
            state = int(fields[3], 16)
            if not 1 <= state <= 12:
                raise ValueError("invalid TCP state")
            if state in (1, 2, 3, 8):
                active += 1
    return active


def validate_sample(s, role):
    if s.get("role") != role or s.get("errors"):
        raise ValueError("observer role/errors")
    timestamp(s["time"])
    number(s["monotonic_ns"], True)
    if number(s["duration_ns"]) > PLAN["sampling"]["max_sample_seconds"]*1e9:
        raise ValueError("stale sample collection")
    files = s["files"]
    limits = PLAN["resources"][role]
    affinity = [line.split(":",1)[1].strip() for line in files["status"].splitlines() if line.startswith("Cpus_allowed_list:")]
    if affinity != [limits["cpuset"]]:
        raise ValueError("actual process affinity mismatch")
    tcp_connections(files)
    for name in ("memory.current", "memory.max", "memory.swap.max", "pids.current", "pids.max"):
        number(files[name])
    if (int(files["memory.max"]) != limits["memory_mib"]*1024**2 or int(files["memory.swap.max"]) != 0 or
            files["cpuset.cpus.effective"].strip() != limits["cpuset"] or
            int(files["pids.max"]) != limits["pids"] or not 0 < int(files["pids.current"]) <= limits["pids"]):
        raise ValueError("actual resource profile mismatch")
    quota, period = (number(x, True) for x in files["cpu.max"].split())
    if quota/period != limits["cpu"]:
        raise ValueError("CPU quota mismatch")
    if not 0 < number(s["rss_bytes"]) <= limits["memory_mib"]*1024**2 or not 0 < number(files["memory.current"]) <= number(files["memory.max"]) or not 0 < number(s["fd"]) <= 4096:
        raise ValueError("hard process/cgroup/handle boundary")
    events = counter(files["memory.events"])
    if any(events[k] for k in ("oom", "oom_kill", "oom_group_kill")):
        raise ValueError("OOM observed")
    for key in ("low", "high", "max"):
        number(events[key])
    cpu = counter(files["cpu.stat"])
    for key in ("usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"):
        number(cpu[key])
    if role == "client":
        number(s["gomaxprocs"], True)
        number(s["goroutines"], True)
    return cpu


def resource_gate(samples, stable=True, role="weir"):
    reasons = []
    if not samples:
        return ["evidence: no "+role+" resource samples"], {}
    peak = dict(rss_bytes=None, cgroup_bytes=None, fd=None, owner=None, connections=None, tcp_connections=None)
    previous = None
    for s in samples:
        try:
            cpu = validate_sample(s, role)
            if previous:
                delta = (s["monotonic_ns"]-previous[0]["monotonic_ns"])/1e9
                wall = timestamp(s["time"])-timestamp(previous[0]["time"])
                if not 0 < delta <= PLAN["sampling"]["max_gap_seconds"] or abs(wall-delta) > PLAN["sampling"]["clock_tolerance_seconds"]:
                    raise ValueError("nonmonotonic clock/sample gap")
                if any(cpu[k] < previous[1][k] for k in cpu if k in previous[1]):
                    raise ValueError("CPU counter reset")
            previous = (s, cpu)
            connections=tcp_connections(s["files"])
            values = dict(rss_bytes=s["rss_bytes"], cgroup_bytes=int(s["files"]["memory.current"]), fd=s["fd"], tcp_connections=connections, connections=connections)
            if role == "weir":
                why, values["owner"], values["connections"] = weir_metrics(s.get("metrics"), stable)
                reasons += why
            for key, value in values.items():
                peak[key] = max(peak[key] or 0, value)
        except (KeyError, ValueError, TypeError, OverflowError) as exc:
            reasons.append("evidence: "+role+": "+str(exc))
    if stable and role == "weir" and ((peak["rss_bytes"] or 0) > PLAN["thresholds"]["rss_mib"]*1024**2 or (peak["cgroup_bytes"] or 0) > PLAN["thresholds"]["cgroup_mib"]*1024**2):
        reasons.append("stable memory threshold")
    return sorted(set(reasons)), peak


def covered_samples(samples, start, end):
    if end <= start:
        raise ValueError("empty observation window")
    before = [s for s in samples if timestamp(s["time"]) <= start]
    after = [s for s in samples if timestamp(s["time"]) >= end]
    gap = PLAN["sampling"]["max_gap_seconds"]
    if not before or not after or start-timestamp(before[-1]["time"]) > gap or timestamp(after[0]["time"])-end > gap:
        raise ValueError("sample coverage missing at window boundary")
    return [s for s in samples if timestamp(before[-1]["time"]) <= timestamp(s["time"]) <= timestamp(after[0]["time"])]


def select_samples(samples, trial, warm=True):
    start = timestamp(trial["start"])+(trial["options"]["WarmSeconds"] if warm else 0)
    end = timestamp(trial["start"])+trial["options"]["WarmSeconds"]+trial["options"]["Seconds"]
    return covered_samples(samples, start, end)


def cpu_usage(samples, cpus):
    if len(samples) < 2:
        raise ValueError("CPU evidence unavailable")
    before, after = samples[0], samples[-1]
    seconds = (after["monotonic_ns"]-before["monotonic_ns"])/1e9
    b, a = counter(before["files"]["cpu.stat"]), counter(after["files"]["cpu.stat"])
    if seconds <= 0 or any(a[k] < b[k] for k in b):
        raise ValueError("CPU counter/time reset")
    intervals = []
    for left, right in zip(samples, samples[1:]):
        l, r = counter(left["files"]["cpu.stat"]), counter(right["files"]["cpu.stat"])
        dt = (right["monotonic_ns"]-left["monotonic_ns"])/1e9
        if dt <= 0 or any(r[k] < l[k] for k in l):
            raise ValueError("CPU interval reset")
        intervals.append((r["usage_usec"]-l["usage_usec"])/1e6/dt/cpus)
    result = dict(max_interval_fraction=max(intervals), quota_fraction=(a["usage_usec"]-b["usage_usec"])/1e6/seconds/cpus,
                  throttled_seconds=(a["throttled_usec"]-b["throttled_usec"])/1e6)
    return result


def db_gate(samples, stable):
    reasons = []
    previous = None
    for s in samples:
        nodes = json.loads(s["db"])["nodes"]
        if len(nodes) != 1:
            raise ValueError("missing/surplus DB nodes")
        node = next(iter(nodes.values()))
        if abs(timestamp(s["time"])-number(node["process"]["timestamp"])/1000) > PLAN["sampling"]["max_gap_seconds"]:
            raise ValueError("stale DB statistics")
        for pool in ("write", "get"):
            p = node["thread_pool"][pool]
            for key in ("queue", "rejected", "active", "completed"):
                number(p[key])
            if previous and p["rejected"] < previous["thread_pool"][pool]["rejected"]:
                raise ValueError("DB counter reset")
            if stable and (p["queue"] or (previous and p["rejected"] > previous["thread_pool"][pool]["rejected"])):
                reasons.append("DB queue/rejection limited")
        number(node["http"]["current_open"])
        number(node["jvm"]["mem"]["heap_used_in_bytes"])
        number(node["process"]["open_file_descriptors"], True)
        if "io_stats" not in node["fs"]:
            raise ValueError("missing DB IO evidence")
        previous = node
    return reasons


def evaluate(trial, observations, stable=True):
    samples, clients, db_samples = (observations[role] for role in ("weir", "client", "es"))
    window_gate(trial["warm"])
    measure_reasons = window_gate(trial["measure"])
    if trial["measure"]["all"]["planned"] != trial["options"]["Rate"]*trial["options"]["Seconds"]:
        raise RuntimeError("offered rate/count mismatch")
    reasons = measure_reasons if stable else []
    peak, client_cpu, db_cpu = {}, None, None
    for role, observations in (("weir", samples), ("client", clients), ("es", db_samples)):
        try:
            full = select_samples(observations, trial, warm=False)
            hard, _ = resource_gate(full, False, role)
            reasons += hard
            selected = select_samples(observations, trial)
            why, maximum = resource_gate(selected, stable, role)
            reasons += why
            if role == "weir":
                peak = maximum
                reasons += db_gate(full, False)
                reasons += db_gate(selected, stable)
            else:
                cpu = cpu_usage(selected, PLAN["resources"][role]["cpu"])
                if role == "client":
                    client_cpu = cpu
                else:
                    db_cpu = cpu
                if stable and cpu["max_interval_fraction"] >= .9:
                    reasons.append(role+" CPU limited")
        except (KeyError, ValueError, TypeError, OverflowError) as exc:
            reasons.append("evidence: "+role+": "+str(exc))
    opts, m = trial["options"], trial["measure"]
    result = dict(rate=opts["Rate"], prefix=opts["Prefix"], pass_=not reasons,
                  reasons=sorted(set(reasons)), measure=m, warm=trial["warm"], resource_peak=peak,
                  client_cpu=client_cpu, db_cpu=db_cpu,
                  planned_rps=m["all"]["planned"]/opts["Seconds"],
                  started_rps=m["all"]["started"]/opts["Seconds"],
                  success_rps=m["all"]["success"]/opts["Seconds"],
                  reads_per_second=m["read"]["success"]/opts["Seconds"],
                  writes_per_second=m["put"]["success"]/opts["Seconds"])
    result["pass"] = result.pop("pass_")
    return result


def require_evidence(report):
    invalid = [r for r in report["reasons"] if r.startswith("evidence:")]
    if invalid:
        raise RuntimeError("invalid observation; stop calibration: "+"; ".join(invalid))
