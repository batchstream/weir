#!/usr/bin/env python3
"""One frozen, stopped-fixture cleanup; uses the shared UID/CLI lifecycle only."""
import json
import os
from pathlib import Path
import signal
import sys
import time

from eks_pacing import Run, digest, owner_check, require, target_check


def execute(root, plan_sha256):
    require(not (root/"invocation.json").exists(), "cleanup already invoked")
    require(digest(root/"plan.json") == plan_sha256, "cleanup plan hash")
    require(root.stat().st_mode & 0o777 == 0o700 and
            (root/"plan.json").stat().st_mode & 0o777 == 0o400, "cleanup evidence permissions")
    plan = json.loads((root/"plan.json").read_text())
    target_check(plan["target"])
    require(plan["namespace"] == plan["owner"] and plan["seconds"] == 180, "cleanup scope/budget")
    require(all(digest(p) == sha for p, sha in plan["inputs"].items()), "frozen cleanup input drift")
    owned = plan["owned"]
    expected = {("Namespace", plan["namespace"]), ("Job", "loopback"), ("ConfigMap", "configuration"),
                ("NetworkPolicy", "default-deny"), ("ResourceQuota", "budget")}
    require(len(owned) == 5 and {(e["kind"], e["name"]) for e in owned} == expected and
            len({e["uid"] for e in owned}) == 5 and
            all(e["uid"] and e["owner"] == plan["owner"] for e in owned), "exact five registered targets required")
    run = Run(root, plan["target"])
    require(run.run(["git", "rev-parse", "HEAD"]).strip() == plan["implementation"] and
            not run.run(["git", "status", "--porcelain"]).strip(), "clean implementation required")
    run.plan, run.owned = plan, owned
    run.namespace = next(e for e in owned if e["kind"] == "Namespace")
    run.defaults = {(e["kind"], e["name"]): e["uid"] for e in plan["defaults"]}
    require(set(run.defaults) == {("ConfigMap", "kube-root-ca.crt"), ("ServiceAccount", "default")}, "recorded defaults required")
    # Historical Pod UID permits only its existing Events, never a Pod DELETE.
    require(plan["stopped_pod"]["kind"] == "Pod" and bool(plan["stopped_pod"]["uid"]), "historical Pod identity")
    run.event_uids = {plan["stopped_pod"]["uid"]}
    run.cleaning = True
    started = time.monotonic()
    run.deadline = started+180
    invocation = dict(start=time.time(), monotonic_start=started, deadline=run.deadline, plan_sha256=plan_sha256)
    with (root/"invocation.json").open("x") as output:
        json.dump(invocation, output, indent=2)
    result = dict(confirmed=False, resources=[])
    try:
        namespace = run.selected_object("Namespace", plan["namespace"])
        run.save("initial-namespace.json", namespace)
        if namespace is None:
            result.update(confirmed=True, namespace_absent=True, already_absent=True)
        else:
            owner_check(namespace, run.namespace)
            for entry in owned:
                if entry["kind"] == "Namespace":
                    continue
                obj = run.selected_object(entry["kind"], entry["name"])
                run.save("initial-"+entry["kind"]+".json", obj)
                if obj is not None:
                    owner_check(obj, entry)
                    if entry["kind"] == "Job":
                        status = obj.get("status") or {}
                        require(all(status.get(k, 0) == 0 for k in ("active", "ready", "terminating")) and
                                any(c.get("status") == "True" and c.get("type") in ("Failed", "Complete")
                                    for c in status.get("conditions", [])), "Job not terminal")
            args = ["get", "pods", "-o", 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}']
            require(not run.kube(args, plan["namespace"]).strip(), "Pod present; stop without adopting/deleting")
            result = run.cleanup(deadline=run.deadline)
        if result["confirmed"]:
            # Independent readback, never infer namespace absence from DELETE exit 0.
            namespace = run.selected_object("Namespace", plan["namespace"])
            run.save("final-namespace.json", namespace)
            require(namespace is None, "namespace absence not confirmed")
            for kind, name in (("Job", "loopback"), ("Pod", plan["stopped_pod"]["name"])):
                obj = run.selected_object(kind, name)
                run.save("final-"+kind+".json", obj)
                require(obj is None, "workload absence not confirmed")
            result.update(namespace_absent=True, job_absent=True, pod_absent=True)
    except BaseException as exc:
        result.update(confirmed=False, error=str(exc))
    result.update(start=invocation["start"], end=time.time(), elapsed_seconds=time.monotonic()-started,
                  deadline=run.deadline, command_count=run.number, diagnostic_errors=run.diagnostic_errors)
    run.save("result.json", result)
    return result


def main():
    require(__debug__ and os.environ.get("WEIR_EKS_CLEANUP") == "1", "unoptimized explicit cleanup opt-in required")
    require(len(sys.argv) == 3, "usage: eks_cleanup.py EVIDENCE_DIRECTORY PLAN_SHA256")
    def cancelled(signum, frame):
        raise KeyboardInterrupt("cleanup cancelled")
    signal.signal(signal.SIGINT, cancelled)
    signal.signal(signal.SIGTERM, cancelled)
    result = execute(Path(sys.argv[1]).resolve(), sys.argv[2])
    print(json.dumps(result))
    return 0 if result["confirmed"] else 1


if __name__ == "__main__":
    sys.exit(main())
