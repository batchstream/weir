#!/usr/bin/env python3
"""Explicit bounded first-reference calibration. No default test starts Docker."""

if not __debug__:
    raise RuntimeError("optimized Python is unsupported")

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import tarfile
import time

import package as packaging
import config_yaml
from capacity_fixture import Fixture, FixtureInterrupted, REPO, LABEL, inventory_diff, sha, Observer
from capacity_artifact import docker_archive
from capacity_report import (
    evaluate,
    resource_gate,
    window_gate,
    timestamp,
    prom,
    metric,
    require_evidence,
    db_gate,
)
from capacity_contract import PLAN, PLAN_PATH, Budget
from observer_completion import (
    observation_samples,
    finish_observation,
    cancel_observation,
    abort_observation,
)


def prepare(f, plan, artifact):
    receipt = json.loads((artifact / "receipt.json").read_text())
    if receipt["source"] != plan["artifact_source"]:
        raise RuntimeError("product source/input identity")

    expected_inputs = {name for name, _ in packaging.source_files(REPO, receipt["source"])}
    if set(receipt["inputs"]) != expected_inputs:
        raise RuntimeError("product source/input identity")
    for name, digest in receipt["inputs"].items():
        if packaging.secret_path(name) or not packaging.allowed(name) or sha(REPO / name) != digest:
            raise RuntimeError("product input mismatch: " + name)

    binaries = {
        name: sha(artifact / "binaries" / name / "weir") for name in ("linux-arm64", "linux-amd64")
    }
    oci = packaging.oci_receipt(artifact / "weir-linux.oci.tar", binaries)
    if (
        binaries["linux-arm64"] != plan["binary_sha256"]
        or oci["images"]["arm64"]["config"] != plan["image_id"]
    ):
        raise RuntimeError("artifact mismatch")
    f.save(
        "artifact-identity.json",
        {"source": receipt["source"], "inputs": receipt["inputs"], "oci": oci},
    )
    for key in ("image_id", "es_image_id"):
        inspection = f.run(["docker", "image", "inspect", plan[key]], check=False)
        if inspection.returncode and key == "image_id":
            # Exact OCI has no tag/ref-name annotations: load cannot overwrite a tag.
            with tarfile.open(artifact / "weir-linux.oci.tar") as archive:
                for member in archive:
                    if member.isfile() and member.size < 65536:
                        data = archive.extractfile(member).read()
                        if (
                            b"org.opencontainers.image.ref.name" in data
                            or b"io.containerd.image.name" in data
                        ):
                            raise RuntimeError("tagged OCI load refused")
            f.product_archive = f.root / "product-docker.tar"
            docker_archive(
                artifact / "weir-linux.oci.tar", f.product_archive, oci["images"]["arm64"]
            )
            continue
        if inspection.returncode:
            raise RuntimeError("existing exact ES image required")
        obj = json.loads(inspection.stdout)[0]
        if obj["Id"] != plan[key] or obj["Os"] != "linux" or obj["Architecture"] != "arm64":
            raise RuntimeError("native existing image required")
        f.save(key + ".json", {k: obj[k] for k in ("Id", "Os", "Architecture", "RepoDigests")})

    # Explicit existing caches contain modules/build objects, never user Go settings.
    env = dict(
        f.env,
        GOROOT=str(REPO / ".tools/go1.27.1"),
        GOENV="off",
        GOTOOLCHAIN="local",
        GOWORK="off",
        GOPROXY="off",
        GOSUMDB="off",
        CGO_ENABLED="0",
        GOMODCACHE="/Users/liran/go/pkg/mod",
        GOCACHE="/Users/liran/Library/Caches/go-build",
        WEIR_CAPACITY_INTEGRATION="1",
    )
    go = str(REPO / ".tools/go1.27.1/bin/go")
    for system, name in (("linux", "client"), ("darwin", "client-host")):
        run_options = dict(env=dict(env, GOOS=system, GOARCH="arm64"))
        f.run(
            [
                go,
                "build",
                "-tags",
                "integration",
                "-trimpath",
                "-buildvcs=false",
                "-o",
                str(f.root / name),
                "./internal/testutil/testcapacity",
            ],
            120,
            options=run_options,
        )

    cfg = {
        "listeners": {"application": "0.0.0.0:7447"},
        "diagnostics": {"address": "127.0.0.1:7449"},
        "memory": "768MiB",
    }
    routes = {
        "services": [
            {
                "name": "database",
                "local": {
                    "max_concurrency": 4,
                    "max_batch_operations": 16,
                    "search": {
                        "url": "http://elasticsearch:9200",
                        "index": "records",
                        "profile": "elasticsearch-8.19.22",
                    },
                },
            }
        ],
        "routes": [{"store": "records", "service": "database"}],
    }
    f.save("node.yaml", config_yaml.dumps(cfg))
    f.save("routes.yaml", config_yaml.dumps(routes))
    run_options = dict(env=env)
    effective = json.loads(
        f.run(
            [
                str(f.root / "client-host"),
                "-mode", "config",
                "-config", str(f.root / "node.yaml"),
                "-routes", str(f.root / "routes.yaml"),
            ],
            options=run_options,
        ).stdout
    )
    if any(
        effective["timing"][key] != plan["client"][key]
        for key in ("expiry_ms", "max_catchup_per_wake", "deadline_ms")
    ):
        raise RuntimeError("compiled timing contract differs from versioned plan")
    f.save("effective-config.json", effective)

    raw = (f.root / "client").read_bytes()
    with tarfile.open(f.root / "client.tar", "w") as archive:
        entry = tarfile.TarInfo("client")
        entry.size = len(raw)
        entry.mode = 0o555
        archive.addfile(entry, io.BytesIO(raw))

    inputs = {
        str(p.relative_to(REPO)): sha(p)
        for p in sorted((REPO / "internal/testutil/testcapacity").glob("*.go"))
    }
    inputs.update(
        {
            str(p.relative_to(REPO)): sha(p)
            for p in [
                REPO / "scripts/test-capacity.py",
                REPO / "scripts/capacity_fixture.py",
                REPO / "scripts/capacity_report.py",
                REPO / "scripts/capacity_artifact.py",
                REPO / "scripts/capacity_contract.py",
                REPO / "scripts/capacity_pacing.py",
                REPO / "scripts/package.py",
                REPO / "scripts/config_yaml.py",
                REPO / "scripts/observer_completion.py",
                REPO / "scripts/resource_report.py",
                PLAN_PATH,
            ]
        }
    )
    host = {
        "uname": f.run(["uname", "-a"]).stdout.strip(),
        "cpu": f.run(["sysctl", "-n", "machdep.cpu.brand_string"]).stdout.strip(),
        "logical_cpu": f.run(["sysctl", "-n", "hw.logicalcpu"]).stdout.strip(),
        "shared_physical_host": True,
    }
    frozen = {
        "schema_version": 1,
        "profile": plan,
        "tool_inputs": inputs,
        "client_sha256": hashlib.sha256(raw).hexdigest(),
        "source": f.run(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "effective": effective,
        "host": host,
        "vm": json.loads((f.root / "vm.json").read_text()),
    }

    f.save("calibration-plan.json", frozen)
    (f.root / "calibration-plan.json").chmod(0o444)
    return sha(f.root / "calibration-plan.json")


def start(f, plan, budget):
    f.first_mutation()
    if hasattr(f, "product_archive"):
        f.run(
            [
                "docker",
                "image",
                "load",
                "--platform",
                "linux/arm64",
                "--input",
                str(f.product_archive),
            ],
            120,
        )
        obj = json.loads(f.run(["docker", "image", "inspect", plan["image_id"]]).stdout)[0]
        if obj["Id"] != plan["image_id"] or obj["Os"] != "linux" or obj["Architecture"] != "arm64":
            raise RuntimeError("loaded product mismatch")
        f.save(
            "loaded-product.json",
            {
                "id": obj["Id"],
                "repo_tags": obj.get("RepoTags"),
                "retained": "exact untagged product artifact cache",
            },
        )

    f.run(
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
            "LABEL " + LABEL + "=" + f.owner,
            str(f.root / "client.tar"),
            f.tag,
        ]
    )
    f.image = json.loads(f.run(["docker", "image", "inspect", f.tag]).stdout)[0]["Id"]
    f.create_network()
    net = ["--network", f.network]
    common = ["--read-only", "-e", "WEIR_CAPACITY_INTEGRATION=1"]
    client_spec = {
        "image": f.image,
        "limits": plan["resources"]["client"],
        "extra": net + common,
        "command": ["-mode", "idle"],
    }
    f.create("client", client_spec)

    es_extra = net + [
        "--user",
        "1000:0",
        "--mount",
        f"type=bind,source={f.root / 'client'},target=/qualification-client,readonly",
        "--network-alias",
        "elasticsearch",
        "--tmpfs",
        "/usr/share/elasticsearch/data:rw,size=1073741824,uid=1000,gid=0,mode=0770",
        "-e",
        "action.auto_create_index=false",
        "-e",
        "discovery.type=single-node",
        "-e",
        "xpack.security.enabled=false",
        "-e",
        "xpack.ml.enabled=false",
        "-e",
        "ingest.geoip.downloader.enabled=false",
        "-e",
        "ES_JAVA_OPTS=-Xms1024m -Xmx1024m",
    ]
    es_spec = {
        "image": plan["es_image_id"],
        "limits": plan["resources"]["es"],
        "extra": es_extra,
        "command": [],
    }
    f.create("es", es_spec)
    until = time.monotonic() + 150
    while time.monotonic() < until:
        result = f.run(
            [
                "docker",
                "exec",
                f.containers[f.owner + "-es"],
                "curl",
                "--silent",
                "--show-error",
                "--max-time",
                "1",
                "http://127.0.0.1:9200/",
            ],
            5,
            check=False,
        )
        if result.returncode == 0:
            identity = json.loads(result.stdout)
            if identity.get("version", {}).get("number") != "8.19.22":
                raise RuntimeError("backend version")
            f.save("es-version.json", identity)
            break
        time.sleep(1)
    else:
        raise TimeoutError("ES readiness")
    entry = budget.reserve("bootstrap", seeds=plan["client"]["corpus"])
    f.save("mutation-budget.json", budget.snapshot())
    setup = f.run(
        [
            "docker",
            "exec",
            f.containers[f.owner + "-client"],
            "/client",
            "-mode",
            "setup",
            "-mutation-reservation",
            str(entry["reserved"]),
        ],
        60,
        check=False,
    )
    records = [json.loads(line) for line in setup.stdout.splitlines()]
    budget.reconcile(entry, records, setup.returncode == 0)
    f.save("mutation-budget.json", budget.snapshot())
    if setup.returncode:
        raise RuntimeError("bootstrap setup failed; reserved/started evidence retained")
    weir_spec = {
        "image": plan["image_id"],
        "limits": plan["resources"]["weir"],
        "extra": net
        + [
            "--network-alias",
            "weir",
            "--read-only",
            "--mount",
            f"type=bind,source={f.root / 'client'},target=/qualification-client,readonly",
            "--mount",
            f"type=bind,source={f.root / 'node.yaml'},target=/node.yaml,readonly",
            "--mount",
            f"type=bind,source={f.root / 'routes.yaml'},target=/routes.yaml,readonly",
        ],
        "command": ["serve", "--config", "/node.yaml", "--routes", "/routes.yaml"],
    }
    f.create("weir", weir_spec)
    f.observers = {}
    native = f.run(
        [
            "docker",
            "exec",
            f.containers[f.owner + "-es"],
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-c",
            "set -eu; uname -smr; id; getconf CLK_TCK; sha256sum /usr/share/elasticsearch/jdk/bin/java",
        ],
        10,
    ).stdout
    f.save("native-identity.txt", native)
    f.observation_profiles = {
        role: dict(
            role=role,
            hashes=dict(weir=plan['binary_sha256'], client=sha(f.root / 'client')),
            native=native.splitlines(),
            seconds=2698,
            samples=1350,
        )
        for role in ('weir', 'es')
    }
    for role in ("weir", "es"):
        cid = f.containers[f.owner + "-" + role]
        command = [
            "docker",
            "exec",
            "-i",
            "--user",
            "65532:65532" if role == "weir" else "1000:0",
            "-e",
            "WEIR_CAPACITY_INTEGRATION=1",
            cid,
            "/qualification-client",
            "-mode",
            "observe",
            "-role",
            role,
            "-pid",
            "1" if role == "weir" else "java",
            "-seconds",
            "2698",
        ]
        options = dict(root=f.root, role=role, command=command, env=f.env)
        f.observers[role] = Observer(options)
    f.run(["docker", "exec", f.containers[f.owner + "-weir"], "/weir", "probe", "ready"], 10)
    time.sleep(3)
    samples = read_observer(f, "weir")
    if not samples or not any(s.get("metrics") for s in samples):
        raise RuntimeError("observer not available before load")
    client_sample = json.loads(
        f.run(
            ["docker", "exec", f.containers[f.owner + "-client"], "/client", "-mode", "snapshot"]
        ).stdout
    )
    baseline = {"weir": samples[-1], "es": read_observer(f, "es")[-1], "client": client_sample}
    for role, sample in baseline.items():
        limit = plan["resources"][role]
        if (
            sample.get("errors")
            or int(sample["files"]["memory.max"]) != limit["memory_mib"] * 1024**2
            or int(sample["files"]["memory.swap.max"]) != 0
            or sample["files"]["cpuset.cpus.effective"].strip() != limit["cpuset"]
        ):
            raise RuntimeError("actual cgroup/affinity profile mismatch: " + role)
        quota, period = map(int, sample["files"]["cpu.max"].split())
        if quota / period != limit["cpu"]:
            raise RuntimeError("CPU quota mismatch")
    f.save("resource-baseline.json", baseline)


def read_observer(f, role):
    if not hasattr(f, "observations"):
        f.observations = {}
    samples = f.observations.setdefault(role, [])
    observer = f.observers[role]
    current, complete = observation_samples(observer, f.observation_profiles[role])
    entries = observer.entries
    if not samples:
        if not entries or "exe_sha256" not in entries[0]:
            raise RuntimeError("missing observer process identity")
        f.save(role + "-process-identity.json", entries[0])
    if current[: len(samples)] != samples:
        raise RuntimeError("observer changed previously emitted samples")
    for entry in current[len(samples) :]:
        if samples and timestamp(entry["time"]) <= timestamp(samples[-1]["time"]):
            raise RuntimeError("nonmonotonic observer timestamps")
        samples.append(entry)
    if len(samples) > 1350:
        raise RuntimeError("resource sample bound")
    f.save(role + "-samples.jsonl", "".join(json.dumps(s) + "\n" for s in samples))
    if complete and observer.stopped is None:
        reasons, _ = resource_gate(samples, False, role)
        if role == 'es':
            reasons += db_gate(samples, False)
        if reasons:
            raise RuntimeError('observer resource boundary: ' + '; '.join(reasons))
        finish_observation(observer, f.observation_profiles[role], f.deadline)
    return samples


def stop_observers(f):
    # Calibration usually ends before the 2698-second observation cap. This is
    # intentional cancellation, never a normal complete observation receipt.
    errors = []
    for role, observer in list(getattr(f, 'observers', {}).items()):
        deadline = min(f.deadline, time.monotonic() + 8)
        try:
            read_observer(f, role)
            if observer.stopped is None:
                cancel_observation(observer, deadline)
        except BaseException as exc:
            errors.append(exc)
            try:
                abort_observation(observer, deadline)
            except BaseException as closing:
                exc.add_note('observer abort: ' + str(closing))
        finally:
            if observer.joined and observer.stopped is not None:
                del f.observers[role]  # Already Waited/closed; Fixture.cleanup owns the rest.
    if errors:
        for exc in errors[1:]:
            errors[0].add_note('observer stop: ' + str(exc))
        raise errors[0]


class Calibration:
    def __init__(self, f, plan, budget=None):
        self.f, self.plan = f, plan
        self.results = []
        self.budget = budget if budget is not None else Budget()

    def trial(self, name, rate, seconds, **options):
        direct = options.get("direct", False)
        recovery = options.get("recovery", 0)
        warm = options.get("warm", 20)
        planned = rate * (warm + seconds) + recovery * 120
        entry = self.budget.reserve(
            name,
            planned=planned,
            seeds=self.plan["client"]["corpus"],
            seconds=warm + seconds + (120 if recovery else 0),
        )
        f = self.f
        f.save("mutation-budget.json", self.budget.snapshot())
        command = [
            "docker",
            "exec",
            f.containers[f.owner + "-client"],
            "/client",
            "-mode",
            "trial",
            "-prefix",
            name,
            "-rate",
            str(rate),
            "-seconds",
            str(seconds),
            "-warm",
            str(warm),
            "-mutation-reservation",
            str(entry["reserved"]),
        ]
        if not direct:
            command += ["-target", "weir:7447"]
        if recovery:
            command += ["-recovery-rate", str(recovery)]
        run_options = dict(monitor=self.observe)
        result = f.run(command, 360, check=False, options=run_options)
        f.save(name + ".jsonl", result.stdout)
        entries = [json.loads(line) for line in result.stdout.splitlines() if line]
        self.budget.reconcile(entry, entries, result.returncode == 0)
        f.save("mutation-budget.json", self.budget.snapshot())
        if result.returncode:
            raise RuntimeError(f"trial {name} exit {result.returncode}; retained command evidence")
        audits = [e for e in entries if e["type"] == "audit"]
        if len(audits) != (2 if recovery else 1) or any(a["error"] != "<nil>" for a in audits):
            raise RuntimeError("full audit missing/failed")
        # Allow the next2s observer sample to bracket the trial's final arrival.
        time.sleep(2.1)
        self.observe()
        weir = read_observer(f, "weir")
        db = read_observer(f, "es")
        clients = [e["sample"] for e in entries if e["type"] in ("client_start", "client_sample")]
        trials = [e["trial"] for e in entries if e["type"] == "trial"]
        observations = dict(weir=weir, client=clients, es=db)
        report = evaluate(trials[0], observations, stable=not recovery)
        require_evidence(report)
        report.update(
            direct=direct, audits=[{k: v for k, v in a.items() if k != "ledger"} for a in audits]
        )
        if recovery:
            rt = trials[1]
            full = evaluate(rt, observations)
            require_evidence(full)
            recovered = None
            for n in range(min(3, len(rt["ten_second_windows"]))):
                checks = []
                for index in range(n, len(rt["ten_second_windows"])):
                    cohort = dict(rt)
                    cohort["options"] = dict(rt["options"], WarmSeconds=index * 10, Seconds=10)
                    cohort["measure"] = rt["ten_second_windows"][index]
                    check = evaluate(cohort, observations)
                    require_evidence(check)
                    checks.append(check["pass"])
                if all(checks):
                    recovered = (n + 1) * 10
                    break
            report["recovery"] = {
                "rate": recovery,
                "seconds": recovered,
                "pass": recovered is not None,
                "windows": rt["ten_second_windows"],
                "measure": rt["measure"],
            }
            offered = trials[0]["measure"]["all"]
            report["overload_applied"] = (
                offered["started"] == offered["planned"]
                and offered["client_drop"] == 0
                and 0 <= offered["lag"]["p99_us"] <= self.plan["thresholds"]["lag_p99_us"]
            )
        self.results.append(report)
        f.save("results.json", self.results)
        print(
            json.dumps(
                {"phase": name, "rate": rate, "pass": report["pass"], "reasons": report["reasons"]}
            ),
            flush=True,
        )
        return report

    def observe(self):
        for role in ("weir", "es"):
            samples = read_observer(self.f, role)
            reasons, _ = resource_gate(samples, False, role)
            if reasons:
                raise RuntimeError("observation/hard boundary; stop: " + "; ".join(reasons))
            if (
                not samples
                or time.time() - timestamp(samples[-1]["time"])
                > self.plan["sampling"]["max_gap_seconds"]
            ):
                raise RuntimeError("observer stale; stop")
            if role == "es":
                db_gate(samples, False)
        for role in ("weir", "es", "client"):
            obj = self.f.owned(self.f.owner + "-" + role)
            if not obj["State"]["Running"] or obj["State"]["OOMKilled"] or obj["RestartCount"]:
                raise RuntimeError("container stopped/OOM/restart")

    def run(self):
        candidate = None
        for rate in self.plan["rates"]:
            result = self.trial("search-" + str(rate), rate, 60)
            if not result["pass"]:
                break
            candidate = rate
        if candidate is None:
            result = {"candidate_rps": None, "status": "no qualifying search point"}
            return result
        confirmed = False
        for attempt in range(2):
            rounds = [self.trial(f"confirm-{attempt}-{i}", candidate, 120) for i in range(3)]
            if all(r["pass"] for r in rounds):
                confirmed = True
                break
            index = self.plan["rates"].index(candidate)
            if attempt or index == 0:
                break
            candidate = self.plan["rates"][index - 1]
        if not confirmed:
            result = {"candidate_rps": None, "status": "confirmation failed; failures retained"}
            return result
        overload = self.trial("overload", candidate * 2, 30, recovery=candidate * 7 // 10, warm=0)
        if not overload["overload_applied"] or not overload["recovery"]["pass"]:
            result = {
                "candidate_rps": None,
                "status": "overload not applied or recovery unqualified",
                "overload_applied": overload["overload_applied"],
                "recovery": overload["recovery"],
            }
            return result
        for i, rate in enumerate((candidate // 2, candidate, candidate * 2)):
            self.trial("direct-" + str(i), rate, 60, direct=True)
        for i in range(2):
            self.trial("direct-confirm-" + str(i), candidate, 120, direct=True)
        result = {
            "candidate_rps": candidate,
            "kind": "sustainable measured profile lower bound; not Weir maximum",
            "status": "calibration execution pending independent acceptance",
            "confirmation_prefixes": [r["prefix"] for r in rounds],
            "stable_rps_70_percent": candidate * 7 // 10,
            "overload_rps_2x": candidate * 2,
            "overload_applied": overload["overload_applied"],
            "recovery": {
                k: v for k, v in overload["recovery"].items() if k not in ("windows", "measure")
            },
        }
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--generator-evidence", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, default=REPO / "dist/m20/first")
    args = parser.parse_args()
    f = Fixture(args.evidence, args.owner)
    plan = PLAN
    result = {
        "schema_version": 1,
        "status": "failed",
        "profile": plan,
        "unknown": [
            "independent acceptance",
            "24h leak freedom",
            "Weir goroutine census",
            "physical host exclusivity",
            "other profiles/platforms/backends",
        ],
    }
    error = None
    budget = Budget()

    def interrupted(signum, frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise FixtureInterrupted(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        qualification = json.loads(args.generator_evidence.read_text())
        if not qualification.get("generator_qualified") or qualification["profile_sha256"] != sha(
            PLAN_PATH
        ):
            raise RuntimeError("native generator qualification required")
        for name, digest in qualification["tool_inputs"].items():
            if sha(REPO / name) != digest:
                raise RuntimeError("generator-qualified tool inputs changed")
        f.preflight()
        result["plan_sha256"] = prepare(f, plan, args.artifact)
        start(f, plan, budget)
        calibration = Calibration(f, plan, budget)
        result.update(calibration.run())
        result["counts"] = budget.snapshot()
        time.sleep(12)
        calibration.observe()
        final = read_observer(f, "weir")[-1]
        f.save("quiescent.json", final)
        m = prom(final["metrics"])
        for name in (
            "pending_entries",
            "result_reserved_entries",
            "active_executions",
            "live_sessions",
        ):
            if metric(m, "weir_store_" + name) != 0:
                raise RuntimeError("quiescence ledger")
    except BaseException as exc:
        error = str(exc)
        result["error"] = error
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            stop_observers(f)
        except BaseException as exc:
            error = error or str(exc)
            result['observer_stop_error'] = str(exc)
        clean = f.cleanup()
        result["cleanup_confirmed"] = clean
        result["counts"] = budget.snapshot()
        try:
            after = f.inventory()
            f.save("inventory-final.json", after)
            if f.first:
                diff = inventory_diff(f.first, after)
                result["inventory_difference"] = diff
                if diff["nondefault_changed"] or diff["default_bridge_changed"]:
                    error = error or "inventory changed during active fixture; not accepted"
                    result["error"] = error
        except BaseException as exc:
            error = error or str(exc)
            result["inventory_error"] = str(exc)
        result["elapsed_seconds"] = time.monotonic() - f.started
        result["qualified"] = bool(result.get("candidate_rps")) and not error and clean
        if not result["qualified"]:
            result["candidate_rps"] = None
        result["exit_code"] = int(bool(error) or not clean)
        result["cleanup_resources"] = f.cleanup_result
        try:
            f.save("capacity-baseline.json", result)
            manifest = {
                str(p.relative_to(f.root)): {"bytes": p.stat().st_size, "sha256": sha(p)}
                for p in sorted(f.root.rglob("*"))
                if p.is_file()
            }
            f.save("manifest.json", manifest)
        except (OSError, RuntimeError) as exc:
            result.update(qualified=False, candidate_rps=None, exit_code=1, evidence_error=str(exc))
            error = error or str(exc)
            print(json.dumps(result), flush=True)
    print(
        json.dumps(
            {
                "exit_code": result["exit_code"],
                "evidence": str(f.root),
                "error": error,
                "candidate_rps": result.get("candidate_rps"),
            }
        ),
        flush=True,
    )
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
