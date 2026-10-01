#!/usr/bin/env python3
"""One frozen native generator investigation, at most6x20s; no backend load."""

if not __debug__:
    raise RuntimeError("optimized Python is unsupported")

import argparse
import importlib.util
import json
from pathlib import Path
import signal
import time

from capacity_contract import PLAN, PLAN_PATH
from capacity_fixture import Fixture, FixtureInterrupted, LABEL, REPO, inventory_diff, sha
from capacity_report import resource_gate, window_gate, select_samples, cpu_usage

spec = importlib.util.spec_from_file_location(
    "capacity_entry", Path(__file__).with_name("test-capacity.py")
)
entry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entry)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--owner", required=True)
    args = parser.parse_args()
    f = Fixture(args.evidence, args.owner)
    result = dict(
        schema_version=2,
        generator_qualified=False,
        candidate_rps=None,
        full_calibration="not-run",
        probes=[],
        profile_sha256=sha(PLAN_PATH),
    )
    error = None

    def interrupted(signum, frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise FixtureInterrupted(f"signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        f.preflight()
        result["plan_sha256"] = entry.prepare(f, PLAN, REPO / "dist/m20/first")
        frozen = json.loads((f.root / "calibration-plan.json").read_text())
        result.update(
            tool_inputs=frozen["tool_inputs"],
            source=frozen["source"],
            client_sha256=frozen["client_sha256"],
        )
        f.save("remediation-plan.json", frozen)
        (f.root / "remediation-plan.json").chmod(0o444)
        f.first_mutation()
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
        client_spec = dict(
            image=f.image,
            limits=PLAN["resources"]["client"],
            extra=["--network", "none", "--read-only", "-e", "WEIR_CAPACITY_INTEGRATION=1"],
            command=["-mode", "idle"],
        )
        cid = f.create("client", client_spec)
        diagnosis_started = time.monotonic()
        for legacy in (True, False):
            if not legacy:
                first = result["probes"][0]
                if first["wake_p99_us"] > PLAN["thresholds"]["lag_p99_us"]:
                    result["stop_reason"] = (
                        "native50 wake p99 already exceeds5ms; no supported post-wake correction"
                    )
                    break
            for rate in PLAN["diagnostics"]["rates"]:
                if len(result["probes"]) >= 6 or time.monotonic() - diagnosis_started + 20 > 300:
                    raise RuntimeError("native diagnostic budget")
                name = ("original" if legacy else "bounded") + "-" + str(rate)
                args = [
                    "docker",
                    "exec",
                    cid,
                    "/client",
                    "-mode",
                    "pace",
                    "-rate",
                    str(rate),
                    "-seconds",
                    "20",
                ]
                if legacy:
                    args.append("-legacy-expiry")
                response = f.run(args, 30)
                raw = json.loads(response.stdout)
                f.save(name + ".json", raw)
                if (
                    raw["goos"] != "linux"
                    or raw["goarch"] != "arm64"
                    or raw["exe_sha256"] != frozen["client_sha256"]
                ):
                    raise RuntimeError("native executable identity")
                observed = f.owned(f.owner + "-client")
                if (
                    observed["RestartCount"]
                    or observed["State"]["OOMKilled"]
                    or not observed["State"]["Running"]
                ):
                    raise RuntimeError("client OOM/restart/stopped")
                trial = raw["trial"]
                samples = select_samples(raw["samples"], trial)
                hard, peak = resource_gate(samples, False, "client")
                if hard:
                    raise RuntimeError("; ".join(hard))
                cpu = cpu_usage(samples, 1)
                reasons = window_gate(trial["measure"])
                if cpu["max_interval_fraction"] >= 0.9:
                    reasons.append("client CPU limited")
                all_ = trial["measure"]["all"]
                report = dict(
                    name=name,
                    rate=rate,
                    qualified=not reasons,
                    reasons=reasons,
                    planned=all_["planned"],
                    started=all_["started"],
                    completed=all_["completed"],
                    drop=all_["client_drop"],
                    drop_reasons=all_["drop_reasons"],
                    wake_p99_us=all_["wake"]["p99_us"],
                    decision_p99_us=all_["decision"]["p99_us"],
                    dispatch_lag_p99_us=all_["lag"]["p99_us"],
                    resource_peak=peak,
                    cpu=cpu,
                    timing={
                        k: all_[k]
                        for k in ("wake", "decision", "construct", "handoff", "worker_start", "lag")
                    },
                    actual_database_mutations=0,
                )
                result["probes"].append(report)
                f.save("pacing-results.json", result)
                print(
                    json.dumps(
                        {k: v for k, v in report.items() if k not in ("timing", "resource_peak")}
                    ),
                    flush=True,
                )
        corrected = [r for r in result["probes"] if r["name"] == "bounded-50"]
        result["generator_qualified"] = bool(corrected and corrected[0]["qualified"])
        result["native_diagnostic_elapsed_seconds"] = time.monotonic() - diagnosis_started
    except BaseException as exc:
        error = str(exc)
        result["error"] = error
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        clean = f.cleanup()
        result["cleanup_confirmed"] = clean
        result["cleanup_resources"] = f.cleanup_result
        try:
            after = f.inventory()
            f.save("inventory-final.json", after)
            if f.first:
                diff = inventory_diff(f.first, after)
                result["inventory_difference"] = diff
                if diff["nondefault_changed"] or diff["default_bridge_changed"]:
                    error = error or "inventory changed during activity"
        except BaseException as exc:
            error = error or str(exc)
        result["generator_qualified"] = result["generator_qualified"] and not error and clean
        result["error"] = error
        result["exit_code"] = int(bool(error) or not clean)
        result["elapsed_seconds"] = time.monotonic() - f.started
        try:
            f.save("pacing-results.json", result)
            manifest = {
                str(p.relative_to(f.root)): dict(bytes=p.stat().st_size, sha256=sha(p))
                for p in sorted(f.root.rglob("*"))
                if p.is_file()
            }
            f.save("manifest.json", manifest)
        except (OSError, RuntimeError) as exc:
            result.update(generator_qualified=False, exit_code=1, evidence_error=str(exc))
            error = error or str(exc)
            print(json.dumps(result), flush=True)
    print(
        json.dumps(
            dict(
                evidence=str(f.root),
                generator_qualified=result["generator_qualified"],
                error=error,
                cleanup_confirmed=clean,
            )
        ),
        flush=True,
    )
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
