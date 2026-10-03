#!/usr/bin/env python3
"""Owned Docker ES connection matrix; removes only its own labelled objects."""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import threading
import time
from types import SimpleNamespace
import urllib.request
import uuid

import config_yaml

ROOT = Path(__file__).resolve().parent.parent
LABEL = "weir.connection-goals.owner"
SPEC = importlib.util.spec_from_file_location("connection_load", ROOT / "scripts" / "connection-load.py")
LOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOAD)


def run(command, *, check=True, timeout=60):
    return subprocess.run(command, check=check, timeout=timeout, text=True, capture_output=True)


def inventory():
    result = {
        "containers": sorted(run(["docker", "ps", "-aq", "--no-trunc"]).stdout.splitlines()),
        "networks": sorted(run(["docker", "network", "ls", "--no-trunc", "--format", "{{.Name}} {{.ID}}"]).stdout.splitlines()),
        "images": sorted(set(run(["docker", "image", "ls", "-q", "--no-trunc"]).stdout.splitlines())),
        "volumes": sorted(run(["docker", "volume", "ls", "-q"]).stdout.splitlines()),
    }
    return result


def http(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        data = response.read((1 << 20) + 1)
        if len(data) > 1 << 20:
            raise RuntimeError("owned diagnostics response exceeds 1 MiB")
        return data.decode()


def owner_snapshot(raw):
    values = {}
    for field in ("owned", "peak", "limit", "acquired", "released"):
        prefix = "weir_backend_connections_" + field
        matches = [float(line.split()[-1]) for line in raw.splitlines()
                   if line.startswith(prefix + "{") or line.startswith(prefix + " ")]
        if len(matches) != 1:
            raise RuntimeError("expected exactly one local backend owner metric")
        values[field] = int(matches[0])
    if not 0 <= values["owned"] <= values["peak"] <= values["limit"] or values["limit"] != 5 or values["acquired"] - values["released"] != values["owned"]:
        raise RuntimeError("raw backend owner metric conservation")
    return values


def closed_owner(raw):
    lines = [line for line in raw.splitlines() if "backend_connections_closed" in line]
    if len(lines) != 1:
        raise RuntimeError("missing or duplicate backend closed-owner receipt")
    values = dict(re.findall(r"(owned|peak|limit|acquired|released)=(\d+)", lines[0]))
    values = {name: int(value) for name, value in values.items()}
    if values.get("owned") != 0 or values.get("limit") != 5 or not 1 <= values.get("peak", 0) <= 5 or values.get("acquired", 0) != values.get("released", -1):
        raise RuntimeError("closed backend owner did not balance raw sockets")
    return values


def metric_value(raw, name):
    values = [float(line.split()[-1]) for line in raw.splitlines()
              if line.startswith(name + " ") or line.startswith(name + "{")]
    if len(values) != 1:
        raise RuntimeError("missing or ambiguous process resource metric: " + name)
    return values[0]


def resource_summary(samples, report):
    start = LOAD.timestamp(report["clients"][0]["start"])
    end = start + timedelta(seconds=report["measurement_seconds"])
    active = [sample for sample in samples if start <= LOAD.timestamp(sample["time"]) <= end]
    if len(active) < 15:
        raise RuntimeError("insufficient active process-resource coverage")
    first, last = active[0], active[-1]
    elapsed = last["monotonic"] - first["monotonic"]
    if elapsed < 15:
        raise RuntimeError("short active process-resource coverage")
    result = {
        "sample_count": len(active), "sample_seconds": elapsed,
        "database_cpu_cores": (last["database"]["cpu_ticks"] - first["database"]["cpu_ticks"]) / first["database"]["ticks_per_second"] / elapsed,
        "database_rss_max": max(sample["database"]["rss_bytes"] for sample in active),
        "database_cgroup_memory_max": max(sample["database"]["cgroup_memory_current"] for sample in active),
        "database_oom_kills": last["database"]["memory_events"]["oom_kill"] - first["database"]["memory_events"]["oom_kill"],
        "weir": [],
    }
    for index in range(len(first["weir"])):
        first_metric, last_metric = first["weir"][index], last["weir"][index]
        result["weir"].append({
            "cpu_cores": (last_metric["cpu_seconds"] - first_metric["cpu_seconds"]) / elapsed,
            "rss_max": max(sample["weir"][index]["rss_bytes"] for sample in active),
            "admission_rejections": {reason: last_metric["admission_rejections"][reason] - first_metric["admission_rejections"][reason] for reason in first_metric["admission_rejections"]},
        })
    return result


class Fixture:
    def __init__(self, options):
        reserved_bytes = ((64 + 96 * options.max_sessions + 448 + 8) * (1 << 20)
                          + options.max_connections * (256 << 10))
        if options.weir_memory_mib * (1 << 20) < reserved_bytes:
            raise ValueError("Weir memory cannot cover configured transport and store reservations")
        self.options = options
        self.owner = "weir-connection-" + uuid.uuid4().hex[:12]
        self.root = options.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.ids = []
        self.image = None
        self.network_created = False
        self.before = inventory()
        self.summary = {"owner": self.owner, "inventory_before": self.before, "runs": []}

    def save(self, name, value):
        raw = value if isinstance(value, str) else json.dumps(value, indent=2) + "\n"
        (self.root / name).write_text(raw)

    def create(self, name, args, command, image=None):
        created = run(["docker", "create", "--name", self.owner + "-" + name,
                       "--label", LABEL + "=" + self.owner,
                       "--network", self.owner] + args + [image or self.image] + command).stdout.strip()
        self.ids.append(created)
        run(["docker", "start", created])
        return created

    def start(self):
        run(["docker", "network", "create", "--label", LABEL + "=" + self.owner, self.owner])
        self.network_created = True
        hashes = {}
        with tarfile.open(self.root / "binaries.tar", "w") as archive:
            for name, path in (("client", self.options.client), ("weir", self.options.weir)):
                data = path.read_bytes()
                hashes[name] = hashlib.sha256(data).hexdigest()
                member = tarfile.TarInfo(name)
                member.size, member.mode = len(data), 0o555
                archive.addfile(member, io.BytesIO(data))
        self.image = run(["docker", "import", "--platform", "linux/arm64",
                          "--change", 'ENTRYPOINT ["/client"]',
                          "--change", "USER 65532:65532",
                          "--change", "LABEL " + LABEL + "=" + self.owner,
                          str(self.root / "binaries.tar")], timeout=90).stdout.strip()
        (self.root / "binaries.tar").unlink()
        self.summary["binary_sha256"] = hashes
        self.summary["image"] = self.options.es_image
        self.summary["host"] = run(["docker", "info", "--format", '{"kernel":{{json .KernelVersion}},"cpus":{{.NCPU}},"memory":{{.MemTotal}},"architecture":{{json .Architecture}}}']).stdout.strip()
        self.db = self.create("database", [
            "--cap-add", "SYS_PTRACE",
            "--cpus", "1", "--cpuset-cpus", "0", "--memory", "1536m", "--memory-swap", "1536m",
            "--network-alias", "elasticsearch", "-p", "127.0.0.1::9200",
            "--tmpfs", "/usr/share/elasticsearch/data:rw,size=1073741824,uid=1000,gid=0",
            "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
            "-e", "xpack.ml.enabled=false", "-e", "ingest.geoip.downloader.enabled=false",
            "-e", "action.auto_create_index=false", "-e", "thread_pool.write.size=1",
            "-e", "thread_pool.write.queue_size=200", "-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m",
        ], [], self.options.es_image)
        db_port = run(["docker", "port", self.db, "9200/tcp"]).stdout.strip().rsplit(":", 1)[1]
        self.db_url = "http://127.0.0.1:" + db_port
        until = time.monotonic() + 120
        while time.monotonic() < until:
            try:
                identity = json.loads(http(self.db_url))
                if identity["version"]["number"] != "8.19.22":
                    raise RuntimeError("exact Elasticsearch 8.19.22 required")
                self.save("database-identity.json", identity)
                break
            except (OSError, ValueError):
                time.sleep(1)
        else:
            raise TimeoutError("Elasticsearch startup")
        self.db_pid = run(["docker", "exec", self.db, "sh", "-c",
                           'for f in /proc/[0-9]*/comm; do if [ "$(cat "$f" 2>/dev/null)" = java ]; then echo "${f#/proc/}"; break; fi; done']).stdout.strip().split("/")[0]
        if not self.db_pid.isdecimal():
            raise RuntimeError("database PID missing")
        run(["docker", "cp", str(self.options.client), self.db + ":/tmp/connection-client"])
        self.client = self.create("clients", [
            "--cpus", "2", "--cpuset-cpus", "1,2", "--memory", "2g", "--memory-swap", "2g",
            "--pids-limit", "1024", "--read-only", "-e", "WEIR_CAPACITY_INTEGRATION=1",
        ], ["-mode", "idle"])
        seed = run(["docker", "exec", self.client, "/client", "-mode", "setup", "-pool", "4",
                    "-backend", "http://elasticsearch:9200", "-mutation-reservation", "1000"], timeout=90)
        self.save("setup.jsonl", seed.stdout)
        self.db_ticks = int(run(["docker", "exec", self.db, "getconf", "CLK_TCK"]).stdout)
        self.db_page_size = int(run(["docker", "exec", self.db, "getconf", "PAGESIZE"]).stdout)
        for name, target in (("direct", ""), ("weir", "weir-0:7447")):
            instances = [self.start_weir(0, 2)] if target else []
            command = ["docker", "exec", self.client, "/client", "-mode", "connection-probe",
                       "-pool", "4", "-workers", "8", "-rate", "800", "-seconds", "20",
                       "-connection-idle-seconds", "0", "-backend", "http://elasticsearch:9200"]
            if target:
                command += ["-target", target]
            warmed = run(command, timeout=35)
            self.save("prewarm-" + name + ".jsonl", warmed.stdout)
            if instances:
                self.stop_weir_observed("prewarm", instances)
        run(["docker", "update", "--cpus", "0.25", self.db])
        self.summary["resources"] = {
            "database_cpu": 0.25, "database_memory_mib": 1536,
            "weir_cpu_total": 2, "weir_memory_mib_each": self.options.weir_memory_mib,
            "client_cpu_total": 2, "client_memory_mib_total": 2048,
            "store_concurrency_each": 4, "raw_owner_limit_each": 5,
            "transport_max_connections_each": self.options.max_connections, "transport_max_sessions_each": self.options.max_sessions,
            "max_read_size": self.options.max_read_size,
        }
        self.save("fixture-summary.json", self.summary)

    def start_weir(self, index, cpu):
        node = {
            "listeners": {"application": "0.0.0.0:7447"},
            "diagnostics": {"address": "0.0.0.0:7449", "allow_intranet": True},
            "memory": str(self.options.weir_memory_mib) + "MiB", "transport": {"max_connections": self.options.max_connections, "max_sessions": self.options.max_sessions},
        }
        routes = {
            "stores": [{"name": "records",
                "max_concurrency": 4, "max_batch_operations": 16,
                "search": {"url": "http://elasticsearch:9200"},
            }],
        }
        local = routes["stores"][0]
        if self.options.max_read_size is not None:
            local["max_read_size"] = self.options.max_read_size
        node["discovery"] = {"group": "records", "advertise": ["weir-" + str(index) + ":7447"]}
        self.save("node.yaml", config_yaml.dumps(node))
        self.save("routes.yaml", config_yaml.dumps(routes))
        cid = self.create("weir-" + str(index), [
            "--entrypoint", "/weir", "--cpus", str(cpu), "--cpuset-cpus", "3,4",
            "--memory", str(self.options.weir_memory_mib) + "m", "--memory-swap", str(self.options.weir_memory_mib) + "m", "--pids-limit", "256", "--read-only",
            "--network-alias", "weir-" + str(index), "-p", "127.0.0.1::7449",
            "--mount", "type=bind,source=" + str(self.root / "node.yaml") + ",target=/node.yaml,readonly",
            "--mount", "type=bind,source=" + str(self.root / "routes.yaml") + ",target=/routes.yaml,readonly",
        ], ["serve", "--config", "/node.yaml", "--routes", "/routes.yaml"])
        port = run(["docker", "port", cid, "7449/tcp"]).stdout.strip().rsplit(":", 1)[1]
        url = "http://127.0.0.1:" + port
        until = time.monotonic() + 30
        while time.monotonic() < until:
            try:
                http(url + "/readyz")
                return cid, url
            except OSError:
                if run(["docker", "inspect", "--format", "{{.State.Running}}", cid]).stdout.strip() != "true":
                    raise RuntimeError("Weir exited: " + run(["docker", "logs", cid], check=False).stdout)
                time.sleep(0.25)
        raise TimeoutError("Weir startup")

    def stop_weir_observed(self, name, instances):
        output = (self.root / (name + "-weir-shutdown-connections.jsonl")).open("w")
        stderr = (self.root / (name + "-weir-shutdown-observer.stderr.log")).open("w")
        observer = subprocess.Popen(self.observer_command() + [
            "-mode", "connection-observe", "-pid", self.db_pid,
            "-connection-port", "9200", "-seconds", "6", "-connection-interval-ms", "200",
        ], stdout=output, stderr=stderr)
        owners = []
        try:
            time.sleep(0.6)
            for index, (cid, _) in enumerate(instances):
                run(["docker", "stop", "--time", "8", cid])
                logs = run(["docker", "logs", cid], check=False)
                self.save(name + "-weir-" + str(index) + "-shutdown.log", logs.stdout + logs.stderr)
                owners.append(closed_owner(logs.stdout + logs.stderr))
                run(["docker", "rm", cid])
                self.ids.remove(cid)
            if observer.wait(timeout=12):
                raise RuntimeError("shutdown socket observer failed")
        finally:
            if observer.poll() is None:
                observer.terminate()
                observer.wait(timeout=5)
            output.close()
            stderr.close()
        samples = LOAD.read_jsonl(self.root / (name + "-weir-shutdown-connections.jsonl"))
        result = {
            "initial": samples[0]["established"], "peak": max(s["established"] for s in samples),
            "final": samples[-1]["established"],
            "last_five_samples": [s["established"] for s in samples[-5:]],
            "closed_owners": owners,
        }
        if any(s["established"] != 0 for s in samples[-5:]):
            raise RuntimeError("Weir shutdown did not restore database socket baseline")
        return result

    def observer_command(self):
        return ["docker", "exec", "--user", "0", "-e", "WEIR_CAPACITY_INTEGRATION=1", self.db, "/tmp/connection-client"]

    def resources(self, instances):
        raw = run(["docker", "exec", self.db, "cat", "/proc/" + self.db_pid + "/stat",
                   "/proc/" + self.db_pid + "/statm", "/sys/fs/cgroup/cpu.stat",
                   "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.events"], timeout=10).stdout
        lines = raw.splitlines()
        stat = lines[0][lines[0].rfind(")") + 2:].split()
        separator = next(n for n in range(2, len(lines)) if lines[n].isdecimal())
        db = {
            "cpu_ticks": int(stat[11]) + int(stat[12]), "ticks_per_second": self.db_ticks,
            "rss_bytes": int(lines[1].split()[1]) * self.db_page_size,
            "cgroup_cpu": dict((key, int(value)) for key, value in (line.split() for line in lines[2:separator])),
            "cgroup_memory_current": int(lines[separator]),
            "memory_events": dict((key, int(value)) for key, value in (line.split() for line in lines[separator+1:])),
            "raw": raw,
        }
        weir = []
        for _, url in instances:
            metrics = http(url + "/metrics")
            rejections = {}
            for line in metrics.splitlines():
                if line.startswith("weir_admission_rejections_total{"):
                    reason = line.split('reason="', 1)[1].split('"', 1)[0]
                    rejections[reason] = int(float(line.rsplit(" ", 1)[1]))
            weir.append({
                "cpu_seconds": metric_value(metrics, "process_cpu_seconds_total"),
                "rss_bytes": metric_value(metrics, "process_resident_memory_bytes"),
                "admission_rejections": rejections, "metrics": metrics,
            })
        sample = {"time": datetime.now(timezone.utc).isoformat(), "monotonic": time.monotonic(), "database": db, "weir": weir}
        return sample

    def trial(self, name, processes, replicas):
        instances = [self.start_weir(n, 2 / replicas) for n in range(replicas)]
        stop_resources = threading.Event()
        resource_samples, resource_errors = [], []
        def sample_resources():
            while not stop_resources.is_set():
                try:
                    resource_samples.append(self.resources(instances))
                except Exception as error:
                    resource_errors.append(str(error))
                    return
                stop_resources.wait(1)
        resource_worker = threading.Thread(target=sample_resources, daemon=True)
        resource_worker.start()
        try:
            for index, (_, url) in enumerate(instances):
                self.save(name + "-weir-" + str(index) + "-before.metrics", http(url + "/metrics"))
            options = SimpleNamespace(
                backend="http://elasticsearch:9200", rate=self.options.rate, processes=processes,
                client_command_json=json.dumps(["docker", "exec", self.client, "/client"]),
                observer_command_json=json.dumps(self.observer_command()),
                start_delay=8, seconds=20, idle_seconds=12, closed_seconds=5,
                database_pid=self.db_pid, database_port=9200, interval_ms=200,
                workers=8, pool=4,
                targets=",".join("weir-" + str(n) + ":7447" for n in range(replicas)),
                output=self.root / name,
            )
            code = LOAD.run_probes(options)
            stop_resources.set()
            resource_worker.join(timeout=12)
            self.save(name + "-resources.json", resource_samples)
            if resource_worker.is_alive() or resource_errors:
                raise RuntimeError("connection process-resource sampler failed: " + str(resource_errors))
            summary = json.loads((options.output / "summary.json").read_text())
            summary["name"] = name
            summary["weir_instances"] = replicas
            summary["process_resources"] = resource_summary(resource_samples, summary)
            summary["owner_metrics_after_clients_closed"] = []
            for index, (_, url) in enumerate(instances):
                raw = http(url + "/metrics")
                self.save(name + "-weir-" + str(index) + "-after.metrics", raw)
                summary["owner_metrics_after_clients_closed"].append(owner_snapshot(raw))
            if instances:
                summary["weir_shutdown"] = self.stop_weir_observed(name, instances)
                instances = []
            self.summary["runs"].append(summary)
            self.save("fixture-summary.json", self.summary)
            if code and not self.options.keep_unqualified:
                raise RuntimeError("connection trial failed its offered-work success qualification")
        finally:
            stop_resources.set()
            resource_worker.join(timeout=12)
            for cid, _ in instances:
                if cid in self.ids:
                    run(["docker", "stop", "--time", "8", cid], check=False)
                    run(["docker", "rm", cid], check=False)
                    self.ids.remove(cid)

    def close(self):
        errors = []
        for cid in list(reversed(self.ids)):
            label = run(["docker", "inspect", "--format", '{{index .Config.Labels "' + LABEL + '"}}', cid], check=False)
            if label.returncode or label.stdout.strip() != self.owner:
                errors.append("container ownership missing: " + cid)
                continue
            logs = run(["docker", "logs", cid], check=False)
            self.save(cid[:12] + "-container.log", logs.stdout + logs.stderr)
            run(["docker", "stop", "--time", "8", cid], check=False)
            removed = run(["docker", "rm", cid], check=False)
            if removed.returncode:
                errors.append("container remove failed: " + cid)
        if self.network_created and run(["docker", "network", "rm", self.owner], check=False).returncode:
            errors.append("owned network remove failed")
        if self.image and run(["docker", "image", "rm", self.image], check=False).returncode:
            errors.append("owned imported image remove failed")
        after = inventory()
        normalized_before = dict(self.before)
        normalized_after = dict(after)
        normalized_before["networks"] = [network for network in self.before["networks"] if not network.startswith("bridge ")]
        normalized_after["networks"] = [network for network in after["networks"] if not network.startswith("bridge ")]
        self.summary["cleanup"] = {
            "inventory_after": after, "restored": after == self.before,
            "restored_except_default_bridge_id": normalized_after == normalized_before,
            "errors": errors,
        }
        self.save("fixture-summary.json", self.summary)
        if errors or normalized_after != normalized_before:
            raise RuntimeError("owned fixture cleanup or inventory restoration failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--weir", type=Path, required=True)
    parser.add_argument("--es-image", required=True)
    parser.add_argument("--rate", type=int, default=800)
    parser.add_argument("--extra-two-replicas-rate", type=int, default=0, help="optional additional rate for the two-replica case")
    parser.add_argument("--single-weir-only", action="store_true", help="only the 16-process / single-Weir measured case")
    parser.add_argument("--max-connections", type=int, default=64)
    parser.add_argument("--max-sessions", type=int, default=32)
    parser.add_argument("--weir-memory-mib", type=int, default=4096)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--keep-unqualified", action="store_true", help="record failed offered-work qualification without hiding it or retrying operations")
    parser.add_argument("--max-read-size", default=None)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    if not 1 <= options.repetitions <= 3:
        raise ValueError("connection repetitions bound: 1-3")
    fixture = Fixture(options)
    try:
        fixture.start()
        matrix = (("weir-1-clients-16", 16, 1),) if options.single_weir_only else (("direct-1", 1, 0), ("direct-16", 16, 0), ("weir-1-clients-16", 16, 1), ("weir-2-clients-16", 16, 2))
        for repetition in range(options.repetitions):
            for name, processes, replicas in matrix:
                trial_name = name + ("-repeat" + str(repetition) if options.repetitions > 1 else "")
                fixture.trial(trial_name, processes, replicas)
        if options.extra_two_replicas_rate:
            options.rate = options.extra_two_replicas_rate
            fixture.trial("weir-2-clients-16-rate" + str(options.rate), 16, 2)
    finally:
        fixture.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
