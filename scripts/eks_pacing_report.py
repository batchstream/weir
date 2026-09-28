"""M25 EKS-only evidence checks. The M22R Docker resource gate is unchanged."""
import re
from decimal import Decimal

from capacity_report import counter, cpu_usage, histogram_count, select_samples, timestamp, window_gate

SOURCE = "fc0eb867ac4511a5c29dbc32b02768a3ad7a3139"
IMAGES = {
    "version": dict(reference="ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10",
                    manifest="sha256:9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856",
                    binary="e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41"),
    "tool": dict(reference="ghcr.io/batchstream/weir-qualification@sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057",
                 manifest="sha256:012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05",
                 binary="fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc"),
}
RATES = [50, 50, 50, 200, 800]
COUNTS = ("planned", "started", "completed", "success", "client_drop", "client_late",
          "unknown", "due", "cancelled_future", "worker_expired")
HISTOGRAMS = ("wake", "decision", "construct", "handoff", "worker_start", "arrival", "dispatch", "lag")


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def integer(value, positive=False):
    require(type(value) is int and value >= int(positive), "missing/invalid integer")
    return value


def quantity(value):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([EPTGMK]i|[EPTGMkmun]|[eE][+-]?[0-9]+)?", str(value))
    require(match is not None, "unknown resource quantity")
    suffix = match[2] or ""
    require(len(str(value)) <= 64 and (not re.fullmatch(r"[eE][+-]?[0-9]+", suffix) or abs(int(suffix[1:])) <= 18), "resource quantity bound")
    powers = {"": 1, "n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal(".001"), "k": 1000}
    powers.update({s: 1024**i for i, s in enumerate(("Ki", "Mi", "Gi", "Ti", "Pi", "Ei"), 1)})
    powers.update({s: 1000**i for i, s in enumerate(("M", "G", "T", "P", "E"), 2)})
    factor = Decimal(10)**int(suffix[1:]) if re.fullmatch(r"[eE][+-]?[0-9]+", suffix) else powers[suffix]
    value = Decimal(match[1])*factor
    require(value.is_finite() and 0 <= value <= 2**63-1, "resource quantity overflow")
    return value


def node_gate(node, used, expected_uid=None, minimum=None):
    require(not used.get("errors"), "unaccountable Pod resources: "+str(used.get("errors")))
    minimum = minimum or dict(cpu=2, memory=1536*1024**2, pods=3)
    require(expected_uid is None or node["uid"] == expected_uid, "node UID drift")
    require(node["arch"] == "arm64" and node["os"] == "linux", "node architecture")
    conditions = {c["type"]: c["status"] for c in node["conditions"]}
    require(conditions.get("Ready") == "True", "node not Ready")
    require(all(conditions.get(k) == "False" for k in ("MemoryPressure", "DiskPressure", "PIDPressure")), "node pressure/unknown")
    require(not node["unschedulable"] and not node["taints"] and not node["deleting"], "node unavailable/tainted")
    remaining = {k: quantity(node["allocatable"][k])-used[k] for k in minimum}
    require(all(remaining[k] >= v for k, v in minimum.items()),
            "insufficient conservative spare capacity")
    result = {k: str(v) for k, v in remaining.items()}
    return result


def cpu_set(raw):
    require(isinstance(raw, str) and len(raw) < 4096, "affinity missing/bound")
    values = set()
    for item in raw.strip().split(","):
        require(re.fullmatch(r"\d+(?:-\d+)?", item) is not None, "affinity syntax")
        ends = list(map(int, item.split("-")))
        require(ends[0] <= ends[-1] < 65536, "affinity range")
        values.update(range(ends[0], ends[-1]+1))
    return values


def resources(samples, *, network="none"):
    require(network in ("none", "loopback"), "unknown network profile")
    require(bool(samples), "missing samples")
    peak = dict(rss_bytes=0, current_bytes=0, fd=0, goroutines=0, pids_current=0)
    previous = None
    fixed = None
    visible_pid_limit = None
    for sample in samples:
        require(sample["role"] == "client" and not sample.get("errors"), "sample role/error")
        require(integer(sample["gomaxprocs"], True) == 1, "GOMAXPROCS")
        integer(sample["monotonic_ns"], True)
        require(integer(sample["duration_ns"]) <= 2_000_000_000, "sample duration")
        timestamp(sample["time"])
        files = sample["files"]
        for key in ("memory.current", "memory.max", "memory.swap.max", "cpu.max", "cpu.stat", "memory.events",
                    "pids.current", "pids.max", "cpuset.cpus.effective", "status", "limits", "io.stat", "net/tcp", "net/tcp6"):
            require(key in files and isinstance(files[key], str), "missing resource file: "+key)
        require(int(files["memory.max"]) == 512*1024**2 and int(files["memory.swap.max"]) == 0, "memory/swap hard limit")
        quota, period = map(int, files["cpu.max"].split())
        require(quota == period and period > 0, "CPU quota must be one")
        status = dict(line.split(":", 1) for line in files["status"].splitlines() if ":" in line)
        require(status["Uid"].split() == ["65532"]*4 and status["Gid"].split() == ["65532"]*4, "runtime nonroot identity")
        require(status["NoNewPrivs"].strip() == "1" and status["Seccomp"].strip() == "2", "runtime security")
        require(all(int(status[k].strip(), 16) == 0 for k in ("CapEff", "CapPrm", "CapBnd", "CapAmb")), "runtime capabilities")
        affinity = cpu_set(status["Cpus_allowed_list"])
        require(affinity == cpu_set(files["cpuset.cpus.effective"]), "process/cgroup affinity mismatch")
        pid_limit = files["pids.max"].strip()
        pids = int(files["pids.current"])
        require(pids > 0 and (pid_limit == "max" or pids <= int(pid_limit)), "visible PID boundary")
        visible_pid_limit = None if pid_limit == "max" else int(pid_limit)
        current = int(files["memory.current"])
        rss = integer(sample["rss_bytes"], True)
        require(0 < current <= 512*1024**2 and rss <= 512*1024**2, "memory hard boundary")
        require(int(status["VmRSS"].split()[0])*1024 == rss and int(status["VmSwap"].split()[0]) == 0, "RSS/swap evidence")
        fd = integer(sample["fd"], True)
        limits = re.search(r"^Max open files\s+(\d+)\s+(\d+)\s+files", files["limits"], re.M)
        require(limits is not None and fd <= int(limits[1]), "FD limit/evidence")
        goroutines = integer(sample["goroutines"], True)
        events = counter(files["memory.events"])
        require(all(events[k] == 0 for k in ("oom", "oom_kill", "oom_group_kill")), "OOM observed")
        for key in ("low", "high", "max"):
            integer(events[key])
        cpu = counter(files["cpu.stat"])
        for key in ("usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"):
            integer(cpu[key])
        # An audited network-free path must also have no TCP listener/connection.
        for key in ("net/tcp", "net/tcp6"):
            rows = files[key].splitlines()
            require(rows and "local_address" in rows[0], "missing TCP table")
            if network == "none":
                require(not any(row.strip() for row in rows[1:]), "unexpected TCP socket")
            else:
                for row in rows[1:]:
                    fields = row.split()
                    require(len(fields) >= 10, "invalid TCP row")
                    loop = {"0100007F", "0000000000000000FFFF00000100007F"}
                    require(fields[1].split(":")[0] in loop and fields[2].split(":")[0] in loop | {"00000000", "0"*32}, "nonloopback TCP socket")
        identity = (quota, period, pid_limit, tuple(sorted(affinity)), limits.groups())
        require(fixed is None or identity == fixed, "resource identity drift")
        fixed = identity
        if previous:
            dt = (sample["monotonic_ns"]-previous[0]["monotonic_ns"])/1e9
            wall = timestamp(sample["time"])-timestamp(previous[0]["time"])
            require(0 < dt <= 6 and abs(wall-dt) <= .25, "sample gap/clock drift")
            require(all(cpu[k] >= previous[1][k] for k in previous[1]), "CPU counter reset/missing")
            require(cpu["nr_throttled"] == previous[1]["nr_throttled"] and cpu["throttled_usec"] == previous[1]["throttled_usec"], "new CPU throttling")
        previous = (sample, cpu)
        for key, value in dict(rss_bytes=rss, current_bytes=current, fd=fd, goroutines=goroutines, pids_current=pids).items():
            peak[key] = max(peak[key], value)
    usage = cpu_usage(samples, 1) if len(samples) > 1 else None
    require(usage is None or usage["max_interval_fraction"] < .9, "CPU interval fraction >= 0.9")
    result = dict(status="partial" if visible_pid_limit is None else "visible-boundaries-pass",
                  visible_pid_limit=visible_pid_limit, full_pid_boundary="unknown" if visible_pid_limit is None else "visible-finite",
                  affinity=sorted(affinity), exclusive_cpu=False, peak=peak, cpu=usage,
                  sampled_peak_only=True, sample_count=len(samples))
    return result


def bucket_map(hist):
    result = {b["upper_us"]: b["count"] for b in hist["buckets"]}
    return result


def validate_window(window):
    for kind in ("all", "read", "put"):
        metric = window[kind]
        for key in COUNTS:
            integer(metric[key])
        for key in ("failures", "drop_reasons"):
            require(metric[key] is None or type(metric[key]) is dict, "invalid outcome map")
            for value in (metric[key] or {}).values():
                integer(value)
        drops = metric["drop_reasons"] or {}
        require(metric["worker_expired"] == drops.get("worker_deadline_or_cancel", 0) and
                metric["client_late"] == drops.get("expired", 0) and metric["cancelled_future"] <= drops.get("cancelled", 0), "drop stage identity")
        for name in HISTOGRAMS:
            h = metric[name]
            count = metric["completed"]
            if name in ("wake", "decision", "construct"):
                count = metric["due"]
            elif name in ("handoff", "worker_start"):
                count += metric["worker_expired"]
            for bucket in h["buckets"]:
                upper = bucket["upper_us"]
                integer(bucket["count"], True)
                require(type(upper) is int and (upper == -1 or 100 <= upper <= 10000 and upper % 100 == 0 or
                        10000 < upper <= 1000000 and upper % 1000 == 0 or
                        1000000 < upper <= 2000000 and upper % 10000 == 0), "unknown histogram bucket")
            histogram_count(h, count)
            maximum = integer(h["max_ns"])
            require(count > 0 or maximum == 0, "empty histogram maximum")
            if count:
                upper = h["buckets"][-1]["upper_us"]
                lower = 2000000 if upper == -1 else upper-(100 if upper <= 10000 else 1000 if upper <= 1000000 else 10000)
                require((maximum == 0 and upper == 100) or lower*1000 < maximum and (upper == -1 or maximum <= upper*1000), "histogram maximum/bucket")
    for key in COUNTS:
        require(window["all"][key] == window["read"][key]+window["put"][key], "read+put count identity")
    for name in ("failures", "drop_reasons"):
        merged = dict(window["read"][name] or {})
        for key, count in (window["put"][name] or {}).items():
            merged[key] = merged.get(key, 0)+count
        require(merged == (window["all"][name] or {}), "read+put outcome map identity")
    for name in HISTOGRAMS:
        merged = bucket_map(window["read"][name])
        for upper, count in bucket_map(window["put"][name]).items():
            merged[upper] = merged.get(upper, 0)+count
        require(merged == bucket_map(window["all"][name]), "read+put bucket identity")
        require(window["all"][name]["max_ns"] == max(window[k][name]["max_ns"] for k in ("read", "put")), "read+put max identity")
    return window_gate(window)


def pacing(raw, rate):
    require(raw["goos"] == "linux" and raw["goarch"] == "arm64" and raw["exe_sha256"] == IMAGES["tool"]["binary"], "helper identity")
    require(raw["kind"] == "native pacing diagnostic only" and raw["kernel"].startswith("Linux version "), "native pacing identity")
    trial = raw["trial"]
    options = dict(Rate=rate, WarmSeconds=0, Seconds=20, Prefix="pace", Workers=64, TimingOnly=True, LegacyExpiry=False)
    require(trial["options"] == options and trial["planned"] == rate*20, "trial plan drift")
    require(19 <= timestamp(trial["end"])-timestamp(trial["start"]) <= 22, "trial duration")
    validate_window(trial["warm"])
    require(trial["warm"]["all"]["planned"] == 0, "unexpected warmup")
    reasons = validate_window(trial["measure"])
    require(trial["measure"]["all"]["planned"] == rate*20 and trial["measure"]["put"]["planned"] == rate*2, "offered count")
    windows = trial["ten_second_windows"]
    require(len(windows) == 2, "ten-second window count")
    for window in windows:
        validate_window(window)
        require(window["all"]["planned"] == rate*10, "ten-second offered count")
    for kind in ("all", "read", "put"):
        for key in COUNTS:
            require(trial["measure"][kind][key] == sum(w[kind][key] for w in windows), "window count sum")
        for name in HISTOGRAMS:
            merged = {}
            for w in windows:
                for upper, count in bucket_map(w[kind][name]).items():
                    merged[upper] = merged.get(upper, 0)+count
            require(merged == bucket_map(trial["measure"][kind][name]), "window bucket sum")
            require(trial["measure"][kind][name]["max_ns"] == max(w[kind][name]["max_ns"] for w in windows), "window max identity")
        for name in ("failures", "drop_reasons"):
            merged = {}
            for w in windows:
                for key, count in (w[kind][name] or {}).items():
                    merged[key] = merged.get(key, 0)+count
            require(merged == (trial["measure"][kind][name] or {}), "window outcome map sum")
    selected = select_samples(raw["samples"], trial)
    require(selected == raw["samples"] and 5 <= len(selected) <= 12, "sample coverage/count")
    resource = resources(selected)
    result = dict(rate=rate, timing_pass=not reasons, reasons=reasons, resource=resource,
                  counts={k: trial["measure"]["all"][k] for k in COUNTS},
                  dispatch_lag_p99_us={k: trial["measure"][k]["lag"]["p99_us"] for k in ("all", "read", "put")},
                  actual_database_mutations=0)
    return result
