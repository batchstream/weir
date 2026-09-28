#!/usr/bin/env python3
"""Single frozen M26R3 read-only init; never dispatch a product main or trial."""
import argparse
import json
import os
from pathlib import Path
import signal
import time

import eks_loopback as loop
import eks_pacing as common
from eks_pacing_report import require

FILES = loop.FILES + ("scripts/eks_socket_diagnostic.py", "scripts/eks_socket_diagnostic_test.py")
BUDGETS = dict(remote_seconds=900, cleanup_seconds=180, startup_seconds=150,
               document_mutations=0, management_mutations=0, planned=0)
RECEIPT = '{"diagnostic":"complete","stop_exit":42,"management_mutations":0,"document_mutations":0}\n'


def objects(plan):
    result = loop.objects(plan)
    result["config"]["data"]["bootstrap.sh"] = (common.REPO/"scripts/eks_loopback_diagnostic.sh").read_text()
    return result


def prepare(run, owner):
    context = loop.prepare_context(run, owner)
    plan = dict(context, schema_version=1, profile="m26r3-readonly-socket-diagnostic", target=loop.TARGET,
                namespace=owner, owner=owner, sampled_at=time.time(), atomic_snapshot=False,
                images=dict(loop.IMAGES, es=loop.ES), image_source=loop.SOURCE, minimum=loop.MINIMUM,
                budgets=BUDGETS, expected_stop=42, sequence=["elasticsearch-startup", "readonly-terminal-init"],
                main_execution="prohibited", network_isolation="unqualified", candidate=None,
                output=dict(stream_bytes=common.LIMIT, combined_bytes=common.LIMIT, total_bytes=common.TOTAL),
                tool_inputs={name:common.digest(common.REPO/name) for name in FILES},
                evidence_inputs=run.registry_evidence,
                product_inputs={name:common.digest(common.REPO/name) for name in run.run(
                    ["git", "ls-files", "*.go", "go.mod", "go.sum", "packaging/Dockerfile", "scripts/qualification.Dockerfile", ".github"]).splitlines()})
    plan["objects"] = objects(plan)
    run.save("plan.json", plan)
    (run.root/"plan.json").chmod(0o400)
    receipt = dict(plan=str(run.root/"plan.json"), sha256=common.digest(run.root/"plan.json"), node_uid=plan["node"]["uid"])
    print(json.dumps(receipt), flush=True)


def collect(run):
    """Only exact owned runtime and logs; failures do not prevent cleanup."""
    pod = run.selected_object("Pod", run.pod_entry["name"])
    identity = dict(job=run.job, template=run.template, pod_uid=run.pod_entry["uid"])
    common.pod_identity(pod, identity)
    common.admitted_spec(pod["spec"], run.template["spec"]["template"]["spec"], pod=True)
    job = run.selected_object("Job", run.job["name"])
    common.owner_check(job, run.job)
    common.job_check(job, run.template)
    run.save("final-pod.json", pod)
    status = pod.get("status") or {}
    run.save("final-container-outcomes.json", loop.container_outcomes(status))
    require(not status.get("ephemeralContainerStatuses"), "ephemeral runtime")
    mains = status.get("containerStatuses", [])
    require(len({s["name"] for s in mains}) == len(mains) and {s["name"] for s in mains} <= {"weir", "qualification"}, "main runtime names")
    require(all(not s.get("containerID") and not s.get("imageID") and not s.get("lastState") and s["restartCount"] == 0 and
                set(s.get("state", {})) <= {"waiting"} for s in mains), "main started/history")
    states = status.get("initContainerStatuses", [])
    require(len({s["name"] for s in states}) == len(states) and {s["name"] for s in states} <= {"elasticsearch", "bootstrap"}, "init runtime names")
    logs = {}
    errors = []
    for state in states:
        name = state["name"]
        try:
            raw = run.kube(["logs", run.pod_entry["name"], "--container="+name, "--limit-bytes="+str(common.LIMIT), "--tail=-1"], run.plan["namespace"])
            run.save("final-"+name+".log", raw)
            require(len(raw.encode()) < common.LIMIT, "potentially truncated log")
            require(state["restartCount"] == 0 and not state.get("lastState"), "restart/history: "+name)
            require(state.get("state", {}).get("terminated", {}).get("reason") != "OOMKilled", "OOMKilled: "+name)
            require(state.get("imageID") == loop.ES["reference"] and state.get("containerID", "").startswith("containerd://"), "runtime image/container identity: "+name)
            logs[name] = raw
        except BaseException as exc:
            errors.append(name+": "+str(exc))
    run.save("collection-errors.json", errors)
    require(not errors, "runtime/log collection: "+str(errors))
    bootstrap = next((s for s in states if s["name"] == "bootstrap"), {})
    terminal = bootstrap.get("state", {}).get("terminated", {})
    complete = set(logs) == {"elasticsearch", "bootstrap"} and terminal.get("exitCode") == 42 and logs["bootstrap"].endswith(RECEIPT)
    result = dict(diagnostic_complete=complete, bootstrap_exit=terminal.get("exitCode"), main_started=False,
                  runtime={s["name"]:dict(imageID=s["imageID"], containerID=s["containerID"]) for s in states})
    return result


def execute(run, options):
    plan = json.loads((run.root/"plan.json").read_text())
    require(common.digest(run.root/"plan.json") == options.plan_sha256 and plan["target"] == loop.TARGET, "plan hash/context")
    require(plan["node"]["uid"] == options.node_uid and plan["minimum"] == loop.MINIMUM, "node/minimum drift")
    require(plan["budgets"] == BUDGETS and plan["expected_stop"] == 42 and plan["main_execution"] == "prohibited", "readonly contract drift")
    require(plan["images"] == dict(loop.IMAGES, es=loop.ES) and plan["objects"] == objects(plan), "frozen images/templates drift")
    require(plan["tool_inputs"] == {name:common.digest(common.REPO/name) for name in FILES}, "tool inputs drift")
    require(all(common.digest(common.REPO/name) == value for name,value in plan["product_inputs"].items()), "product input drift")
    proof = plan["evidence_inputs"]
    require(common.digest(proof["path"]) == proof["sha256"], "registry evidence drift")
    require(all(common.digest(Path(proof["path"]).parent/name) == value for name,value in proof["proof"]["files"].items()), "registry raw drift")
    require(run.run(["git", "rev-parse", "HEAD"]).strip() == plan["source"] and not run.run(["git", "status", "--porcelain"]).strip(), "source/worktree drift")
    run.plan = plan
    run.pod_entry = None
    run.pod_ready = False
    run.runtime_evidence = False
    run.job_create_attempted = False
    invocation = dict(start=time.time(), plan_sha256=options.plan_sha256)
    with (run.root/"invocation.json").open("x") as handle:
        json.dump(invocation, handle)
    result = dict(profile=plan["profile"], diagnostic_complete=False, evidence_collected=False, functional_pass=False,
                  candidate=None, full_calibration="not-run", overload="not-run", recovery="not-run", soak="not-run",
                  network_isolation="unqualified", management_mutations=0, document_mutations=0,
                  plan_sha256=options.plan_sha256)
    try:
        run.check_node()
        require(not run.kube(["get", "namespace", plan["namespace"], "--ignore-not-found", "-o", "name"]).strip(), "namespace collision")
        run.remote_started = time.monotonic()
        run.deadline = run.remote_started+BUDGETS["remote_seconds"]
        loop.namespace_start(run)
        run.template = plan["objects"]["job"]
        startup = time.monotonic()
        run.job = run.create(run.template)
        until = startup+BUDGETS["startup_seconds"]
        while time.monotonic() < until:
            # Any terminal init, including the planned 42, stops this functional
            # readiness checker. There is deliberately no exec/load continuation.
            run.current_pod()
            require(not run.pod_ready, "unexpected functional readiness")
            time.sleep(1)
        raise ValueError("150 second diagnostic startup budget")
    except BaseException as exc:
        result["observation_stop"] = str(exc)
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        # Creation can consume the startup deadline before the first observation.
        # Register only the already-created Job's verified controller Pod, once,
        # so a timeout still has an exact UID for evidence and cleanup.
        if not run.pod_entry and hasattr(run, "job"):
            try:
                run.current_pod()
            except BaseException as exc:
                result["final_observation_stop"] = str(exc)
        if run.pod_entry:
            try:
                result.update(collect(run))
                result["evidence_collected"] = True
            except BaseException as exc:
                result["collection_error"] = str(exc)
        result["cleanup"] = run.cleanup()
        result["remote_elapsed_seconds"] = time.monotonic()-run.remote_started if run.remote_started else 0
        run.save("result.json", result)
    print(json.dumps(result), flush=True)
    return 0 if result["diagnostic_complete"] and result["cleanup"]["confirmed"] else 1


def main():
    require(os.environ.get("WEIR_EKS_SOCKET_DIAGNOSTIC") == "1", "explicit diagnostic opt-in required")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run"))
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--owner")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--node-uid")
    parser.add_argument("--registry-evidence", type=Path)
    args = parser.parse_args()
    root = args.evidence.absolute()
    require(root.parent.resolve() == (common.REPO/".testdata/m26r3").resolve() and not root.is_symlink(), "controlled evidence path")
    if args.mode == "prepare":
        root.mkdir(mode=0o700)
    else:
        require(root.is_dir() and root.stat().st_mode & 0o077 == 0, "private prepared evidence")
    run = loop.Run(root, loop.TARGET)
    run.number = max([int(p.stem.split("-")[1]) for p in root.glob("command-*.json")]+[0])
    def interrupted(signum, frame):raise KeyboardInterrupt("signal "+str(signum))
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    if args.mode == "prepare":
        require(args.registry_evidence and args.registry_evidence.is_file(), "registry proof required")
        proof = json.loads(args.registry_evidence.read_text())
        require(proof["manifest"] == loop.ES["manifest"] and proof["config"] == loop.ES["config"] and
                proof["architecture"] == "arm64" and proof["version"] == "8.19.22" and proof["user"] == "1000:0", "ES registry identity")
        run.registry_evidence = dict(path=str(args.registry_evidence.absolute()), sha256=common.digest(args.registry_evidence), proof=proof)
        prepare(run, args.owner)
        return 0
    return execute(run, args)


if __name__ == "__main__":raise SystemExit(main())
