#!/usr/bin/env python3
"""Explicit, disposable, real-ES direct/Weir load comparison. No retries.

Build binaries before invocation; every supplied binary must be linux/arm64.
--baseline-weir enables production baseline/current comparisons in one fixture.
All containers, the network, and imported image are removed in finally. Existing
Docker objects and backend images are never removed or modified.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import signal
import subprocess
import tarfile
import threading
import time
import urllib.request
import uuid

import config_yaml

REPO = Path(__file__).resolve().parent.parent
LABEL = "weir.load-comparison.owner"


def run(command, *, timeout=120, check=True):
    return subprocess.run(
        command, cwd=REPO, text=True, capture_output=True, timeout=timeout, check=check
    )


def http(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        raw = response.read((1 << 20) + 1)
        if len(raw) > 1 << 20:
            raise RuntimeError("diagnostic response exceeds 1 MiB")
        return raw.decode()


def metric(raw, name, *, optional=False):
    values = [
        float(line.split()[-1])
        for line in raw.splitlines()
        if line.startswith(name + "{") or line.startswith(name + " ")
    ]
    if not values and not optional:
        raise RuntimeError("required metric missing: " + name)
    return sum(values)


def labelled_metric(raw, name, label):
    return sum(
        float(line.split()[-1])
        for line in raw.splitlines()
        if line.startswith(name + "{") and label in line.split()[0]
    )


def delta(before, after, name, *, optional=False):
    return metric(after, name, optional=optional) - metric(before, name, optional=optional)


class Fixture:
    def __init__(self, args):
        self.args = args
        self.owner = "weir-load-" + uuid.uuid4().hex[:12]
        self.network = self.owner
        self.image = None
        self.ids = []
        self.binaries = {"client": args.client, "weir": args.weir}
        if args.baseline_weir is not None:
            self.binaries["baseline"] = args.baseline_weir
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.summary = {"owner": self.owner, "runs": [], "cleanup": None}

    def save(self, name, value):
        raw = value if isinstance(value, str) else json.dumps(value, indent=2)
        (self.root / name).write_text(raw)

    def create(self, name, options, command, image=None):
        args = [
            "docker",
            "create",
            "--name",
            self.owner + "-" + name,
            "--label",
            LABEL + "=" + self.owner,
            "--network",
            self.network,
        ]
        result = run(args + options + [image or self.image] + command)
        cid = result.stdout.strip()
        self.ids.append(cid)
        run(["docker", "start", cid])
        return cid

    def start(self):
        self.save("inventory-before.json", self.inventory())
        run(["docker", "network", "create", "--label", LABEL + "=" + self.owner, self.network])
        archive = self.root / "binaries.tar"
        binary_hashes = {}
        with tarfile.open(archive, "w") as tar:
            for name, path in self.binaries.items():
                data = path.read_bytes()
                binary_hashes[name] = hashlib.sha256(data).hexdigest()
                member = tarfile.TarInfo(name)
                member.size, member.mode = len(data), 0o555
                tar.addfile(member, io.BytesIO(data))
        self.image = run(
            [
                "docker",
                "import",
                "--platform",
                "linux/arm64",
                "--change",
                'ENTRYPOINT ["/client"]',
                "--change",
                "USER 65532:65532",
                "--change",
                "LABEL " + LABEL + "=" + self.owner,
                str(archive),
            ]
        ).stdout.strip()
        self.db = self.create(
            "db",
            [
                "--memory",
                "1536m",
                "--memory-swap",
                "1536m",
                "--cpus",
                "1",
                "--cpuset-cpus",
                "0",
                "--network-alias",
                "elasticsearch",
                "-p",
                "127.0.0.1::9200",
                "--tmpfs",
                "/usr/share/elasticsearch/data:rw,size=1073741824,uid=1000,gid=0",
                "-e",
                "discovery.type=single-node",
                "-e",
                "xpack.security.enabled=false",
                "-e",
                "xpack.ml.enabled=false",
                "-e",
                "ingest.geoip.downloader.enabled=false",
                "-e",
                "action.auto_create_index=false",
                "-e",
                "thread_pool.write.size=1",
                "-e",
                "thread_pool.write.queue_size=" + str(self.args.db_queue),
                "-e",
                "ES_JAVA_OPTS=-Xms512m -Xmx512m",
            ],
            [],
            self.args.es_image,
        )
        port = run(["docker", "port", self.db, "9200/tcp"]).stdout.strip().split(":")[-1]
        self.db_url = "http://127.0.0.1:" + port
        until = time.monotonic() + 120
        while time.monotonic() < until:
            try:
                identity = json.loads(http(self.db_url + "/"))
                if identity["version"]["number"] != "8.19.22":
                    raise RuntimeError("exact ES version required")
                self.save("database-identity.json", identity)
                break
            except (OSError, ValueError):
                time.sleep(1)
        else:
            raise TimeoutError("database startup")
        self.client = self.create(
            "client",
            [
                "--pids-limit",
                "256",
                "--memory",
                "2g",
                "--memory-swap",
                "2g",
                "--cpus",
                "2",
                "--cpuset-cpus",
                "1,2",
                "--read-only",
                "-e",
                "WEIR_CAPACITY_INTEGRATION=1",
            ],
            ["-mode", "idle"],
        )
        source_head = run(["git", "rev-parse", "HEAD"]).stdout.strip()
        sources = {
            "client": self.args.client_source or source_head,
            "weir": self.args.weir_source or source_head,
        }
        if self.args.baseline_weir is not None:
            sources["baseline"] = self.args.baseline_source
        self.summary["provenance"] = {
            "base_head": source_head,
            "dirty_files": run(["git", "status", "--short"]).stdout.splitlines(),
            "binary_sha256": binary_hashes,
            "declared_binary_sources": sources,
            "es_image": self.args.es_image,
            "host": run(["uname", "-sm"]).stdout.strip(),
            "docker": run(["docker", "info", "--format", "{{.ServerVersion}}"]).stdout.strip(),
            "vm": json.loads(
                run(
                    [
                        "docker",
                        "info",
                        "--format",
                        '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"kernel":{{json .KernelVersion}},"arch":{{json .Architecture}}}',
                    ]
                ).stdout
            ),
            "resources": {
                "database": {
                    "cpu": self.args.db_cpu,
                    "memory_mib": 1536,
                    "cpuset": "0",
                    "write_threads": 1,
                    "write_queue": self.args.db_queue,
                },
                "weir": {"cpu": 2, "memory_mib": 768, "cpuset": "3,4"},
                "client": {"cpu": 2, "memory_mib": 2048, "cpuset": "1,2"},
            },
            "options": {
                k: str(v) if isinstance(v, Path) else v for k, v in vars(self.args).items()
            },
        }
        self.summary["pacing"] = []
        for rate in [max(map(int, self.args.rates.split(",")))]:
            paced = run(
                [
                    "docker",
                    "exec",
                    self.client,
                    "/client",
                    "-mode",
                    "pace",
                    "-rate",
                    str(rate),
                    "-seconds",
                    "10",
                    "-write-every",
                    str(min(map(int, self.args.write_every.split(",")))),
                    "-arrival-expiry-ms",
                    "100",
                    "-max-catchup",
                    "512",
                    "-client-queue",
                    "256",
                ],
                timeout=30,
            )
            self.save("pace-" + str(rate) + ".jsonl", paced.stdout)
            probe = json.loads(paced.stdout)
            expected_hash = self.summary["provenance"]["binary_sha256"]["client"]
            if (
                probe["exe_sha256"] != expected_hash
                or probe["samples"][0]["process"]["identity"]["exe_sha256"] != expected_hash
            ):
                raise RuntimeError("pacing helper identity mismatch")
            m = probe["trial"]["measure"]["all"]
            qualified = (
                m["planned"] == m["success"]
                and m["client_drop"] == 0
                and m["lag"]["p99_us"] <= 5000
            )
            pacing_result = {
                "rate": rate,
                "qualified": qualified,
                "metrics": m,
                "samples": probe["samples"],
            }
            self.summary["pacing"].append(pacing_result)
            if not qualified:
                self.save("summary.json", self.summary)
                raise RuntimeError("load-generator pacing qualification failed")
        self.save("summary.json", self.summary)

    def inventory(self):
        inventory = {
            "containers": run(["docker", "ps", "-aq"]).stdout.splitlines(),
            "networks": dict(
                line.split()
                for line in run(
                    ["docker", "network", "ls", "--no-trunc", "--format", "{{.Name}} {{.ID}}"]
                ).stdout.splitlines()
            ),
            "volumes": run(["docker", "volume", "ls", "-q"]).stdout.splitlines(),
        }
        return inventory

    def prewarm(self, write_every):
        # Exercise every compared request path at the bootstrap CPU quota before
        # measurements. Each measured trial recreates the corpus afterwards.
        run(["docker", "update", "--cpus", "1", self.db])
        self.stop_weir()
        training = []
        modes = ("direct", "adaptive")
        if "baseline" in self.args.modes.split(","):
            modes = ("direct", "baseline", "weir")
        for mode in modes:
            if mode != "direct":
                self.start_weir(mode)
            prefix = "prewarm-" + str(write_every) + "-" + mode
            command = [
                "docker",
                "exec",
                self.client,
                "/client",
                "-mode",
                "trial",
                "-rate",
                "800",
                "-warm",
                "0",
                "-seconds",
                "20",
                "-prefix",
                prefix,
                "-pool",
                "1",
                "-write-every",
                str(write_every),
                "-workers",
                "64",
                "-arrival-expiry-ms",
                "100",
                "-max-catchup",
                "512",
                "-client-queue",
                "256",
                "-mutation-reservation",
                str(1000 + 16000 // write_every),
            ]
            if mode != "direct":
                command += ["-target", "weir:7447"]
            result = run(command, timeout=90, check=False)
            self.save(prefix + "-client.jsonl", result.stdout)
            self.save(prefix + "-stderr.log", result.stderr)
            if result.returncode:
                raise RuntimeError("common JVM prewarm failed: " + result.stderr)
            records = [json.loads(line) for line in result.stdout.splitlines()]
            trial = next(r for r in records if r.get("type") == "trial")
            audit = next(r for r in records if r.get("type") == "audit")
            if trial.get("run_error") != "<nil>" or audit["error"] != "<nil>":
                raise RuntimeError("common JVM prewarm load or audit failed")
            receipt = {
                "mode": mode,
                "write_every": write_every,
                "database_cpu": 1,
                "direct_http_pool": 1,
                "store_concurrency_limit": self.args.pool if mode != "direct" else None,
                "rate": 800,
                "seconds": 20,
                "metrics": trial["trial"]["measure"]["all"],
                "audit": audit,
            }
            if mode in ("baseline", "weir"):
                provenance = self.summary["provenance"]
                receipt["binary_sha256"] = provenance["binary_sha256"][mode]
                receipt["declared_source"] = provenance["declared_binary_sources"][mode]
            training.append(receipt)
        self.stop_weir()
        self.summary.setdefault("prewarm", []).extend(training)
        self.save("summary.json", self.summary)

    def stop_weir(self):
        if getattr(self, "weir", None):
            cid = self.weir
            run(["docker", "stop", "--time", "8", cid], check=False)
            self.save(cid[:12] + "-weir.log", run(["docker", "logs", cid], check=False).stdout)
            run(["docker", "rm", cid])
            self.ids.remove(cid)
            self.weir = None

    def start_weir(self, mode):
        self.stop_weir()
        config = {
            "listeners": {"application": "0.0.0.0:7447"},
            "diagnostics": {"address": "0.0.0.0:7449", "allow_intranet": True},
            "memory": "768MiB",
            "transport": {"max_connections": 16, "max_sessions": 64},
        }
        routes = {
            "services": [
                {
                    "name": "database",
                    "local": {
                        "max_concurrency": self.args.pool,
                        "max_batch_operations": self.args.batch_operations,
                        "search": {
                            "url": "http://elasticsearch:9200",
                        },
                    },
                }
            ],
            "routes": [{"store": "records", "service": "database"}],
        }
        self.save("node.yaml", config_yaml.dumps(config))
        self.save("routes.yaml", config_yaml.dumps(routes))
        # Resource targets must exist before record operations; startup qualifies the server.
        run(
            [
                "docker",
                "exec",
                self.client,
                "/client",
                "-mode",
                "setup",
                "-mutation-reservation",
                "1000",
            ],
            timeout=90,
        )
        entrypoint = "/client"
        if mode in ("weir", "baseline"):
            entrypoint = "/" + mode
        options = [
            "--pids-limit",
            "256",
            "--memory",
            "768m",
            "--memory-swap",
            "768m",
            "--cpus",
            "2",
            "--cpuset-cpus",
            "3,4",
            "--network-alias",
            "weir",
            "--read-only",
            "-p",
            "127.0.0.1::7449",
            "--mount",
            "type=bind,source=" + str(self.root / "node.yaml") + ",target=/node.yaml,readonly",
            "--mount",
            "type=bind,source=" + str(self.root / "routes.yaml") + ",target=/routes.yaml,readonly",
            "--entrypoint",
            entrypoint,
        ]
        command = ["serve", "--config", "/node.yaml", "--routes", "/routes.yaml"]
        if mode == "control":
            options += ["-e", "WEIR_CAPACITY_INTEGRATION=1"]
            command = [
                "-config", "/node.yaml",
                "-routes", "/routes.yaml",
                "-mode", "serve-control",
                "-suppress-congestion",
            ]
        elif mode == "adaptive":
            options += ["-e", "WEIR_CAPACITY_INTEGRATION=1"]
            command = [
                "-config", "/node.yaml",
                "-routes", "/routes.yaml",
                "-mode", "serve-control",
            ]
        self.weir = self.create(mode, options, command)
        port = run(["docker", "port", self.weir, "7449/tcp"]).stdout.strip().split(":")[-1]
        self.weir_url = "http://127.0.0.1:" + port
        until = time.monotonic() + 30
        while time.monotonic() < until:
            try:
                http(self.weir_url + "/metrics")
                return
            except OSError:
                time.sleep(0.2)
        raise TimeoutError("Weir startup")

    def trial(self, mode, rate, repetition, write_every):
        run(["docker", "update", "--cpus", "1", self.db])
        if mode != "direct":
            self.start_weir(mode)
        else:
            self.stop_weir()
        prefix = f"r{len(self.summary['runs']):03d}"
        samples, finished = [], threading.Event()

        def observe():
            while not finished.is_set():
                entry = {"wall_time": time.time()}
                try:
                    entry["database"] = json.loads(
                        http(
                            self.db_url
                            + "/_nodes/stats/thread_pool,http,jvm,process,indexing_pressure,fs?filter_path=nodes.*.thread_pool.write,nodes.*.http.current_open,nodes.*.http.total_opened,nodes.*.process.cpu,nodes.*.jvm.mem,nodes.*.jvm.gc,nodes.*.indexing_pressure,nodes.*.fs.io_stats"
                        )
                    )
                    if mode != "direct":
                        entry["metrics"] = http(self.weir_url + "/metrics")
                except Exception as exc:
                    entry["error"] = str(exc)
                samples.append(entry)
                finished.wait(0.5)

        observer = threading.Thread(target=observe)
        observer.start()
        planned = (
            rate * (self.args.warm + self.args.seconds)
            + self.args.recovery_rate * self.args.recovery_seconds
        )
        command = [
            "docker",
            "exec",
            self.client,
            "/client",
            "-mode",
            "trial",
            "-rate",
            str(rate),
            "-warm",
            str(self.args.warm),
            "-seconds",
            str(self.args.seconds),
            "-prefix",
            prefix,
            "-pool",
            (
                str(self.args.direct_pool or self.args.pool)
                if mode == "direct"
                else str(self.args.pool)
            ),
            "-write-every",
            str(write_every),
            "-workers",
            "64",
            "-arrival-expiry-ms",
            "100",
            "-max-catchup",
            "512",
            "-client-queue",
            "256",
            "-load-delay-ms",
            "2000",
            "-mutation-reservation",
            str(1000 + planned // write_every),
        ]
        if self.args.recovery_rate:
            command += [
                "-recovery-rate",
                str(self.args.recovery_rate),
                "-recovery-seconds",
                str(self.args.recovery_seconds),
            ]
        if mode != "direct":
            command += ["-target", "weir:7447"]
        lines, reader_errors, changed = [], [], threading.Event()
        process = subprocess.Popen(
            command, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        def read_output():
            try:
                for line in process.stdout:
                    lines.append(line)
                    if json.loads(line).get("type") == "client_start":
                        run(["docker", "update", "--cpus", str(self.args.db_cpu), self.db])
                        quota = run(
                            [
                                "docker",
                                "inspect",
                                "--format",
                                "{{.HostConfig.NanoCpus}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}",
                                self.db,
                            ]
                        ).stdout.split()
                        if quota != [str(int(self.args.db_cpu * 1e9)), "0", str(1536 * 1024**2)]:
                            raise RuntimeError("database resource limit verification failed")
                        changed.set()
            except Exception as exc:
                reader_errors.append(str(exc))

        reader = threading.Thread(target=read_output)
        errors_output = []
        stderr_reader = threading.Thread(target=lambda: errors_output.append(process.stderr.read()))
        reader.start()
        stderr_reader.start()
        try:
            process.wait(timeout=180)
            reader.join(5)
            stderr_reader.join(5)
            result = subprocess.CompletedProcess(
                command, process.returncode, "".join(lines), "".join(errors_output)
            )
            if reader_errors or not changed.is_set():
                raise RuntimeError("load start resource change failed: " + str(reader_errors))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(5)
            reader.join(5)
            stderr_reader.join(5)
            process.stdout.close()
            process.stderr.close()
            finished.set()
            observer.join(5)
        self.save(prefix + "-client.jsonl", result.stdout)
        self.save(prefix + "-stderr.log", result.stderr)
        self.save(prefix + "-samples.jsonl", "".join(json.dumps(s) + "\n" for s in samples))
        if result.returncode:
            raise RuntimeError("trial failed: " + prefix + " " + result.stderr)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        trial_record = next(r for r in records if r.get("type") == "trial")
        t = trial_record["trial"]
        audit = next(r for r in records if r.get("type") == "audit")
        if trial_record.get("run_error") != "<nil>" or audit["error"] != "<nil>":
            raise RuntimeError("load or correctness audit failed")
        begin = (
            __import__("datetime")
            .datetime.fromisoformat(t["start"].replace("Z", "+00:00"))
            .timestamp()
            + self.args.warm
        )
        end = begin + self.args.seconds
        measured = [s for s in samples if begin <= s["wall_time"] <= end and "database" in s]
        if any(s.get("error") for s in samples):
            raise RuntimeError(
                "observer error: " + str([s.get("error") for s in samples if s.get("error")])
            )
        if len(measured) < 2:
            raise RuntimeError("database evidence missing")
        node = lambda s: next(iter(s["database"]["nodes"].values()))
        first, last = node(measured[0]), node(measured[-1])
        out = {
            "prefix": prefix,
            "mode": mode,
            "rate": rate,
            "repetition": repetition,
            "write_every": write_every,
            "seconds": self.args.seconds,
            "measure": t["measure"],
            "windows": t["ten_second_windows"],
            "audit": audit["audit"],
            "recovery": next(
                (
                    r["trial"]["measure"]
                    for r in records
                    if r.get("type") == "trial"
                    and r["trial"]["options"]["Prefix"].endswith("-recovery")
                ),
                None,
            ),
            "database": {
                "write_rejected": last["thread_pool"]["write"]["rejected"]
                - first["thread_pool"]["write"]["rejected"],
                "write_completed": last["thread_pool"]["write"]["completed"]
                - first["thread_pool"]["write"]["completed"],
                "write_queue_max": max(node(s)["thread_pool"]["write"]["queue"] for s in measured),
                "sample_count": len(measured),
                "open_http_max": max(node(s)["http"]["current_open"] for s in measured),
                "cpu_time_ms": last["process"]["cpu"]["total_in_millis"]
                - first["process"]["cpu"]["total_in_millis"],
                "jvm_heap_max": max(node(s)["jvm"]["mem"]["heap_used_in_bytes"] for s in measured),
                "gc_ms": sum(
                    last["jvm"]["gc"]["collectors"][k]["collection_time_in_millis"]
                    - first["jvm"]["gc"]["collectors"][k]["collection_time_in_millis"]
                    for k in last["jvm"]["gc"]["collectors"]
                ),
            },
        }
        client_samples = [
            r["sample"]
            for r in records
            if r.get("type") in ("client_sample", "client_start", "client_end")
        ]
        if any(s.get("errors") for s in client_samples):
            raise RuntimeError("client resource observer error")
        usage = lambda s: int(
            next(
                line.split()[1]
                for line in s["files"]["cpu.stat"].splitlines()
                if line.startswith("usage_usec ")
            )
        )
        elapsed = (client_samples[-1]["monotonic_ns"] - client_samples[0]["monotonic_ns"]) / 1e9
        sample_time = (
            lambda s: __import__("datetime")
            .datetime.fromisoformat(s["time"].replace("Z", "+00:00"))
            .timestamp()
        )
        prior = [s for s in client_samples if sample_time(s) <= begin]
        later = [s for s in client_samples if sample_time(s) >= end]
        bracket_start = prior[-1] if prior else client_samples[0]
        bracket_end = later[0] if later else client_samples[-1]
        bracket_seconds = (bracket_end["monotonic_ns"] - bracket_start["monotonic_ns"]) / 1e9
        intervals = [
            (usage(b) - usage(a)) / 1e6 / ((b["monotonic_ns"] - a["monotonic_ns"]) / 1e9)
            for a, b in zip(client_samples, client_samples[1:])
            if sample_time(b) >= begin and sample_time(a) <= end
        ]
        out["client"] = {
            "cpu_cores_mean": (usage(client_samples[-1]) - usage(client_samples[0]))
            / 1e6
            / elapsed,
            "cpu_cores_measure_bracket": (usage(bracket_end) - usage(bracket_start))
            / 1e6
            / bracket_seconds,
            "cpu_cores_interval_max": max(intervals),
            "rss_max": max(s["rss_bytes"] for s in client_samples),
            "samples": len(client_samples),
            "lag_p99_us": t["measure"]["all"]["lag"]["p99_us"],
        }
        if mode != "direct":
            metrics = [s["metrics"] for s in measured if "metrics" in s]
            out["weir"] = {
                "window_min": min(metric(m, "weir_store_window") for m in metrics),
                "window_max": max(metric(m, "weir_store_window") for m in metrics),
                "window_start": metric(metrics[0], "weir_store_window"),
                "window_end": metric(metrics[-1], "weir_store_window"),
                "decreases": labelled_metric(
                    metrics[-1], "weir_store_window_changes_total", 'direction="decrease"'
                )
                - labelled_metric(
                    metrics[0], "weir_store_window_changes_total", 'direction="decrease"'
                ),
                "increases": labelled_metric(
                    metrics[-1], "weir_store_window_changes_total", 'direction="increase"'
                )
                - labelled_metric(
                    metrics[0], "weir_store_window_changes_total", 'direction="increase"'
                ),
                "cooldown_samples": sum(metric(m, "weir_store_cooldown") > 0 for m in metrics),
                "pending_max": max(metric(m, "weir_store_pending_entries") for m in metrics),
                "retained_max": max(
                    metric(m, "weir_store_result_reserved_entries") for m in metrics
                ),
                "rejections": delta(metrics[0], metrics[-1], "weir_store_rejections_total"),
                "executions": delta(metrics[0], metrics[-1], "weir_store_executions_total"),
                "batch_mean": delta(
                    metrics[0], metrics[-1], "weir_store_record_batch_operations_sum"
                )
                / max(
                    1, delta(metrics[0], metrics[-1], "weir_store_record_batch_operations_count")
                ),
                "rss_max": max(metric(m, "process_resident_memory_bytes") for m in metrics),
                "suppressed": delta(
                    metrics[0],
                    metrics[-1],
                    "weir_test_suppressed_congestion_total",
                    optional=mode != "control",
                ),
            }
        self.summary["runs"].append(out)
        self.save("summary.json", self.summary)
        m = out["measure"]["all"]
        progress_result = {
            "prefix": prefix,
            "mode": mode,
            "rate": rate,
            "write_every": write_every,
            "success_rps": m["success"] / self.args.seconds,
            "failure": m["failures"],
            "client_drop": m["client_drop"],
            "success_p95_us": m["success_arrival"]["p95_us"],
            "database": out["database"],
            "weir": out.get("weir"),
        }
        print(json.dumps(progress_result), flush=True)

    def cleanup(self):
        errors = []
        for cid in list(reversed(self.ids)):
            inspection = run(
                ["docker", "inspect", "--format", '{{index .Config.Labels "' + LABEL + '"}}', cid],
                check=False,
            )
            if inspection.returncode == 0 and inspection.stdout.strip() == self.owner:
                run(["docker", "stop", "--time", "10", cid], check=False)
                logs = run(["docker", "logs", cid], check=False)
                self.save(cid[:12] + "-container.log", logs.stdout + logs.stderr)
                result = run(["docker", "rm", cid], check=False)
                if result.returncode:
                    errors.append(result.stderr)
            elif inspection.returncode == 0:
                errors.append("ownership mismatch " + cid)
        network = run(
            [
                "docker",
                "network",
                "inspect",
                "--format",
                '{{index .Labels "' + LABEL + '"}}',
                self.network,
            ],
            check=False,
        )
        if network.returncode == 0 and network.stdout.strip() == self.owner:
            result = run(["docker", "network", "rm", self.network], check=False)
            if result.returncode:
                errors.append(result.stderr)
        if self.image:
            result = run(["docker", "image", "rm", self.image], check=False)
            if result.returncode:
                errors.append(result.stderr)
        after = self.inventory()
        self.save("inventory-after.json", after)
        before_file = self.root / "inventory-before.json"
        before = json.loads(before_file.read_text()) if before_file.exists() else None
        normalized_before = json.loads(json.dumps(before))
        normalized_after = json.loads(json.dumps(after))
        bridge_changed = before is not None and before["networks"].get("bridge") != after[
            "networks"
        ].get("bridge")
        if before is not None and "bridge" in before["networks"] and "bridge" in after["networks"]:
            normalized_before["networks"]["bridge"] = "Docker-managed default bridge"
            normalized_after["networks"]["bridge"] = "Docker-managed default bridge"
        if normalized_before != normalized_after:
            errors.append("non-default Docker inventory differs from pre-test snapshot")
        remaining = run(
            ["docker", "ps", "-aq", "--filter", "label=" + LABEL + "=" + self.owner]
        ).stdout.splitlines()
        remaining_networks = run(
            ["docker", "network", "ls", "-q", "--filter", "label=" + LABEL + "=" + self.owner]
        ).stdout.splitlines()
        if remaining or remaining_networks:
            errors.append("owned objects remain after cleanup")
        self.summary["cleanup"] = {
            "pass": not errors,
            "errors": errors,
            "inventory_restored": before == after,
            "non_default_inventory_restored": normalized_before == normalized_after,
            "default_bridge_id_changed": bridge_changed,
            "remaining_owned_containers": remaining,
            "remaining_owned_networks": remaining_networks,
        }
        self.save("summary.json", self.summary)
        return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--weir", type=Path, required=True)
    parser.add_argument("--baseline-weir", type=Path, help="optional production baseline binary")
    parser.add_argument("--baseline-source", help="commit or ref used to build --baseline-weir")
    parser.add_argument(
        "--client-source", help="commit or ref used to build --client; defaults to current HEAD"
    )
    parser.add_argument(
        "--weir-source", help="commit or ref used to build --weir; defaults to current HEAD"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rates", default="200,800,3200")
    parser.add_argument("--write-every", default="10,1")
    parser.add_argument("--modes", default="direct,weir")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--warm", type=int, default=10)
    parser.add_argument("--recovery-rate", type=int, default=0)
    parser.add_argument("--recovery-seconds", type=int, default=20)
    parser.add_argument("--pool", type=int, default=4)
    parser.add_argument(
        "--direct-pool",
        type=int,
        default=0,
        help="optional separately tuned direct pool; zero uses --pool",
    )
    parser.add_argument("--batch-operations", type=int, default=16)
    parser.add_argument("--db-cpu", type=float, default=1)
    parser.add_argument("--db-queue", type=int, default=200)
    parser.add_argument(
        "--prewarm",
        action="store_true",
        help="common JVM training at bootstrap CPU quota; required for baseline/current pairs",
    )
    parser.add_argument(
        "--es-image",
        default="sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160",
    )
    args = parser.parse_args()
    if (
        not 1 <= args.pool <= 32
        or not 0 <= args.direct_pool <= 32
        or not 1 <= args.repetitions <= 4
        or not 1 <= args.seconds <= 120
        or not 0 <= args.warm <= 20
    ):
        parser.error("bounded trial options required")
    modes = args.modes.split(",")
    if any(m not in ("direct", "weir", "baseline", "control", "adaptive") for m in modes):
        parser.error("mode bound")
    if "baseline" in modes and args.baseline_weir is None:
        parser.error("baseline mode requires --baseline-weir")
    if args.baseline_weir is not None and not args.baseline_source:
        parser.error("--baseline-weir requires --baseline-source for build provenance")
    if args.baseline_source and args.baseline_weir is None:
        parser.error("--baseline-source requires --baseline-weir")
    if "baseline" in modes and "weir" in modes and (not args.prewarm or args.repetitions < 3):
        parser.error("baseline/current pairs require --prewarm and at least three repetitions")
    for name in ("baseline_source", "client_source", "weir_source"):
        source = getattr(args, name)
        if source:
            try:
                resolved = run(
                    ["git", "rev-parse", "--verify", "--end-of-options", source + "^{commit}"]
                ).stdout.strip()
            except subprocess.CalledProcessError:
                parser.error("invalid build source: " + source)
            setattr(args, name, resolved)
    fixture = Fixture(args)

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("fixture interrupted; cleanup follows")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        fixture.start()
        for write_every in map(int, args.write_every.split(",")):
            if args.prewarm:
                fixture.prewarm(write_every)
            for rate in map(int, args.rates.split(",")):
                for repetition in range(args.repetitions):
                    order = modes if repetition % 2 == 0 else list(reversed(modes))
                    for mode in order:
                        fixture.trial(mode, rate, repetition, write_every)
    finally:
        if not fixture.cleanup():
            raise RuntimeError("fixture cleanup incomplete")


if __name__ == "__main__":
    main()
