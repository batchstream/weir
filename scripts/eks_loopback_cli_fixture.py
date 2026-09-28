"""Fixed offline CLI responses for the M26 loopback lifecycle tests only."""
import copy
import json
from pathlib import Path
import sys


def main():
    root = Path(sys.argv[0]).parent
    cfg = json.loads((root/"scenario.json").read_text())
    state_file = root/"state.json"
    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    args = sys.argv[1:]
    program = Path(sys.argv[0]).name
    with (root/"calls.jsonl").open("a") as output:
        output.write(json.dumps([program]+args)+"\n")
    if program == "git":
        if args == ["rev-parse", "HEAD"]:
            print("synthetic-implementation")
        elif args not in (["status", "--porcelain"],) and args[0] not in ("diff", "ls-files"):
            raise ValueError(args)
        return
    if program == "aws":
        print(json.dumps(cfg["cluster"]))
        return
    if args[:3] != ["--context", cfg["cluster"]["arn"], "--request-timeout=10s"]:
        raise ValueError("context")
    args = args[3:]
    if args[0] == "--namespace":
        if args[1] not in (cfg["namespace"], "kube-system"):
            raise ValueError("namespace")
        args = args[2:]
    verb = args[0]
    if verb == "create":
        request = json.loads(Path(args[args.index("-f")+1]).read_text())
        kind = request["kind"]
        dry = "--dry-run=server" in args
        if kind == "Pod" and not dry:
            raise ValueError("persistent Pod prohibited")
        if kind == "Job":
            obj = copy.deepcopy(cfg["job_response"] if dry else cfg["created_job"])
        elif kind == "Pod":
            obj = copy.deepcopy(cfg["pod_response"])
        else:
            obj = copy.deepcopy(request)
            obj["metadata"]["uid"] = "synthetic-"+kind
            if kind == "ResourceQuota":
                obj["spec"]["hard"].update(cfg.get("quota_updates", {}))
                obj["status"] = dict(used={"count/secrets":"0"})
        if not dry:
            key = kind+"/"+obj["metadata"]["name"]
            if key in state:
                raise ValueError("AlreadyExists")
            state[key] = obj
            if kind == "Job":
                state["Pod/synthetic-pod"] = cfg["actual_pod"]
        print(json.dumps(obj))
    elif verb == "get":
        kind = args[1]
        if kind == "nodes":
            print(json.dumps([cfg["node"]]))
        elif kind == "pods" and "--all-namespaces" in args:
            print("[]")
        elif kind == "pods":
            if "Pod/synthetic-pod" in state:
                print("synthetic-pod")
        elif kind == "events":
            print("[]")
        elif kind == "daemonset":
            print("aws-node synthetic --enable-network-policy=false")
        elif "," in kind:
            for obj in state.values():
                if obj["kind"] == "Namespace":
                    continue
                meta = obj["metadata"]
                refs = "".join(r["uid"]+"," for r in meta.get("ownerReferences") or [])
                print("|".join([obj["apiVersion"],obj["kind"],meta["name"],meta["uid"],meta["labels"][cfg["label"]],refs,""]))
        else:
            key = ("Namespace" if kind == "namespace" else kind)+"/"+args[2]
            if key in state:
                print(json.dumps(state[key]))
    elif verb == "delete":
        if args[1] != "--raw":
            raise ValueError("unconditional delete")
        body = json.loads(Path(args[args.index("-f")+1]).read_text())
        uid = body["preconditions"]["uid"]
        matches = [k for k, v in state.items() if v["metadata"]["uid"] == uid]
        if len(matches) != 1 or body.get("propagationPolicy") != "Orphan" or "gracePeriodSeconds" in body:
            raise ValueError("delete identity/options")
        obj = state[matches[0]]
        plural = cfg["plurals"][obj["kind"]]
        if not args[2].endswith("/"+plural+"/"+obj["metadata"]["name"]):
            raise ValueError("delete URL")
        del state[matches[0]]
    elif verb == "auth":
        print("yes")
    elif verb == "api-resources":
        print("jobs.batch\npods\nresourcequotas\nconfigmaps\nnetworkpolicies.networking.k8s.io\nsecrets")
    elif verb == "logs":
        print(cfg["bootstrap_log"] if "--container=bootstrap" in args else "synthetic lifecycle log")
    elif verb == "exec":
        command = args[args.index("--")+1:]
        if command[-1] == "main":
            print("own-pod-ip=10.0.0.2\nloopback-check-complete")
        elif command == ["/weir", "-probe", "ready"]:
            pass
        elif command == ["/weir", "-version"]:
            print(json.dumps(cfg["version"]))
        elif "-mode" in command:
            mode = command[command.index("-mode")+1]
            if mode == "trial":
                prefix = command[command.index("-prefix")+1]
                if prefix == "through-weir" and command[-2:] != ["-target", "127.0.0.1:7447"]:
                    raise ValueError("trial target")
                if prefix == "direct-es" and "-target" in command:
                    raise ValueError("direct target")
                for record in cfg["trials"][prefix]:
                    print(json.dumps(record))
            else:
                print(json.dumps(cfg[mode]))
        elif command[-1].endswith("/metrics"):
            print(cfg["metrics"], end="")
        elif command[-1].startswith("http://127.0.0.1:9200/_nodes/stats/"):
            print(cfg["stats"])
        else:
            raise ValueError(command)
    else:
        raise ValueError(args)
    state_file.write_text(json.dumps(state))


if __name__ == "__main__":
    main()
