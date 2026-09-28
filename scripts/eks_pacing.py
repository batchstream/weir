#!/usr/bin/env python3
"""Explicit M25 preflight/run; no services, databases, arbitrary args or retries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time
from decimal import Decimal

from capacity_fixture import stop_group
from eks_pacing_report import IMAGES, RATES, SOURCE, node_gate, pacing, quantity, require, resources

REPO = Path(__file__).resolve().parent.parent
LABEL = "qualification.weir.io/owner"
LIMIT = 8 << 20
TOTAL = 128 << 20
FILES = ("scripts/eks_pacing.py", "scripts/eks_pacing_report.py", "scripts/capacity_fixture.py",
         "scripts/capacity_report.py", "scripts/capacity_contract.py", "scripts/capacity-plan-m22r.json", "scripts/eks_pacing_test.py")
KINDS = {"Namespace": ("v1", "namespaces"), "Job": ("batch/v1", "jobs"),
         "Pod": ("v1", "pods"), "ResourceQuota": ("v1", "resourcequotas"),
         "NetworkPolicy": ("networking.k8s.io/v1", "networkpolicies")}
# Emit the admitted spec as JSON, but never return unexpected literal env values.
JSON_TEMPLATE = '''{{define "json"}}{{$t := printf "%T" .}}{{if eq $t "map[string]interface {}"}}{ {{$first := true}}{{range $k,$v := .}}{{if not $first}},{{end}}{{$first = false}}{{printf "%q" $k}}:{{if eq $k "env"}}[{{range $i,$e := $v}}{{if $i}},{{end}}{"name":{{printf "%q" $e.name}},"value":{{if or (eq $e.name "GOMAXPROCS") (eq $e.name "WEIR_CAPACITY_INTEGRATION")}}{{template "json" $e.value}}{{else}}"REDACTED"{{end}}{{if $e.valueFrom}},"valueFrom":{{template "json" $e.valueFrom}}{{end}}}{{end}}]{{else}}{{template "json" $v}}{{end}}{{end}} }{{else if eq $t "[]interface {}"}}[{{range $i,$v := .}}{{if $i}},{{end}}{{template "json" $v}}{{end}}]{{else if eq $t "string"}}{{printf "%q" .}}{{else if eq $t "<nil>"}}null{{else}}{{.}}{{end}}{{end}}'''
OBJECT_TEMPLATE = JSON_TEMPLATE + '''{"apiVersion":{{printf "%q" .apiVersion}},"kind":{{printf "%q" .kind}},"metadata":{"name":{{printf "%q" .metadata.name}},"namespace":{{template "json" .metadata.namespace}},"uid":{{template "json" .metadata.uid}},"labels":{{template "json" .metadata.labels}},"ownerReferences":{{template "json" .metadata.ownerReferences}}},"spec":{{template "json" .spec}},"status":{{template "json" .status}}}'''
META_TEMPLATE = r'''{{range .items}}{{.apiVersion}}|{{.kind}}|{{.metadata.name}}|{{.metadata.uid}}|{{index .metadata.labels "qualification.weir.io/owner"}}|{{range .metadata.ownerReferences}}{{.uid}},{{end}}|{{if .involvedObject}}{{.involvedObject.uid}}{{else if .regarding}}{{.regarding.uid}}{{end}}{{"\n"}}{{end}}'''


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def target_check(target):
    require(set(target) == {"context", "cluster", "region"}, "target fields")
    require(re.fullmatch(r"[a-z]{2}-[a-z]+-\d", target["region"]) is not None, "region syntax")
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", target["cluster"]) is not None, "cluster syntax")
    expected = r"arn:aws:eks:"+re.escape(target["region"])+r":\d{12}:cluster/"+re.escape(target["cluster"])
    require(re.fullmatch(expected, target["context"]) is not None, "context/cluster/region mismatch")


def owner_check(obj, expected):
    require(isinstance(obj, dict), "owned object disappeared")
    meta = obj["metadata"]
    require(meta["uid"] == expected["uid"] and meta["name"] == expected["name"] and
            meta.get("labels", {}).get(LABEL) == expected["owner"], "object UID/owner drift")


def job_template(plan, step):
    tool = step != "version"
    args = ["-mode", "snapshot"] if step == "snapshot" else ["-version"]
    if step.startswith("pace-"):
        index = int(step.split("-")[1])
        require(0 <= index < len(RATES), "probe index")
        args = ["-mode", "pace", "-rate", str(RATES[index]), "-seconds", "20"]
    require(step in ("version", "snapshot") or step in ["pace-"+str(i) for i in range(5)], "unknown step")
    security = dict(runAsUser=65532, runAsGroup=65532, runAsNonRoot=True,
                    allowPrivilegeEscalation=False, readOnlyRootFilesystem=True,
                    capabilities=dict(drop=["ALL"]), seccompProfile=dict(type="RuntimeDefault"))
    limits = dict(cpu="1", memory="512Mi")
    container = dict(name="probe", image=IMAGES["tool" if tool else "version"]["reference"],
                     imagePullPolicy="Always", args=args, securityContext=security,
                     resources=dict(requests=limits, limits=limits))
    if tool:
        container["env"] = [dict(name="GOMAXPROCS", value="1"), dict(name="WEIR_CAPACITY_INTEGRATION", value="1")]
    spec = dict(nodeName=plan["node"]["name"], restartPolicy="Never", activeDeadlineSeconds=100,
                terminationGracePeriodSeconds=10, automountServiceAccountToken=False, enableServiceLinks=False,
                preemptionPolicy="Never", dnsPolicy="None", dnsConfig=dict(nameservers=["127.0.0.1"]),
                securityContext=dict(runAsNonRoot=True, runAsUser=65532, runAsGroup=65532,
                                     seccompProfile=dict(type="RuntimeDefault")), containers=[container])
    labels = {LABEL: plan["owner"]}
    result = dict(apiVersion="batch/v1", kind="Job",
                  metadata=dict(name=step, namespace=plan["namespace"], labels=labels),
                  spec=dict(completions=1, parallelism=1, backoffLimit=0, activeDeadlineSeconds=120,
                            template=dict(metadata=dict(labels=labels), spec=spec)))
    return result


def admitted_spec(actual, expected):
    """Reject unknown admission fields; allow only standard, inert API defaults."""
    extra = dict(actual)
    for key, value in expected.items():
        if key == "containers":
            require(len(actual[key]) == 1, "injected container")
            container = dict(actual[key][0])
            wanted = value[0]
            for field, expected_value in wanted.items():
                require(container.pop(field, None) == expected_value, "container admission drift: "+field)
            for field, expected_value in dict(terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File").items():
                require(container.pop(field, expected_value) == expected_value, "container default drift")
            require(not container, "extra container fields: "+str(sorted(container)))
            extra.pop(key)
        else:
            require(extra.pop(key, None) == value, "Pod admission drift: "+key)
    defaults = dict(schedulerName="default-scheduler", serviceAccountName="default", serviceAccount="default",
                    priority=0, hostNetwork=False, hostPID=False, hostIPC=False)
    for key, value in defaults.items():
        require(extra.pop(key, value) == value, "Pod default drift: "+key)
    # The API normally injects these two node-lifecycle tolerations. Freeze them
    # here, do not send them and never use them to accept a tainted node.
    tolerations = extra.pop("tolerations", [])
    allowed = [dict(key="node.kubernetes.io/"+key, operator="Exists", effect="NoExecute", tolerationSeconds=300)
               for key in ("not-ready", "unreachable")]
    require(tolerations in ([], allowed), "unexpected toleration")
    require(not extra, "extra Pod fields: "+str(sorted(extra)))


def job_check(actual, expected):
    for key in ("completions", "parallelism", "backoffLimit", "activeDeadlineSeconds"):
        require(actual["spec"][key] == expected["spec"][key], "Job admission drift: "+key)
    require(not actual["spec"].get("suspend") and actual["spec"].get("completionMode", "NonIndexed") == "NonIndexed", "Job execution mode")
    admitted_spec(actual["spec"]["template"]["spec"], expected["spec"]["template"]["spec"])


def pod_check(pod, options):
    job, template, uid = (options[k] for k in ("job", "template", "pod_uid"))
    meta = pod["metadata"]
    require(not uid or meta["uid"] == uid, "Pod UID drift")
    require(meta["namespace"] == template["metadata"]["namespace"] and meta["labels"].get(LABEL) == template["metadata"]["labels"][LABEL], "Pod namespace/owner")
    refs = meta["ownerReferences"]
    require(len(refs) == 1 and refs[0]["uid"] == job["uid"] and refs[0]["name"] == job["name"] and
            refs[0]["kind"] == "Job" and refs[0]["apiVersion"] == "batch/v1" and refs[0]["controller"] is True, "Job to Pod owner chain")
    admitted_spec(pod["spec"], template["spec"]["template"]["spec"])
    status = pod.get("status") or {}
    require(not status.get("initContainerStatuses") and not status.get("ephemeralContainerStatuses"), "extra container statuses")
    statuses = status.get("containerStatuses", [])
    require(len(statuses) <= 1, "extra runtime container")
    if statuses:
        container = statuses[0]
        require(container["name"] == "probe" and container["restartCount"] == 0 and not container.get("lastState"), "container restart/history")
        image = IMAGES["version" if job["name"] == "version" else "tool"]
        if container.get("imageID"):
            allowed = {image["reference"], image["reference"].split("@")[0]+"@"+image["manifest"]}
            require(container["imageID"] in allowed, "runtime imageID drift")
        if status.get("phase") == "Succeeded":
            require(container.get("imageID") and container.get("containerID", "").startswith("containerd://"), "missing runtime identity")
            require(container["state"]["terminated"]["exitCode"] == 0 and container["state"]["terminated"]["reason"] == "Completed", "container exit/OOM")


def decode_fragments(raw):
    decoder = json.JSONDecoder()
    values = []
    while raw.strip():
        raw = raw.lstrip()
        value, end = decoder.raw_decode(raw)
        values.append(value)
        raw = raw[end:]
    return values


def allocated(raw):
    used = {}
    for row in raw.splitlines():
        cells = row.split("\t")
        require(len(cells) == 11, "resource projection columns")
        node, phase = cells[:2]
        if not node or phase in ("Succeeded", "Failed"):
            continue
        current = used.setdefault(node, dict(cpu=Decimal(0), memory=Decimal(0), pods=Decimal(0)))
        current["pods"] += 1
        # Sum regular + all init + pod-level + overhead + allocated/runtime
        # resources. This deliberately overcounts resize/sidecar overlap.
        for cell in cells[2:]:
            for resource in decode_fragments(cell):
                requests = resource.get("requests", resource)
                limits = resource.get("limits", {})
                for key in ("cpu", "memory"):
                    current[key] += quantity(requests.get(key, limits.get(key, "0")))
    return used


class Run:
    def __init__(self, root, target):
        self.root, self.target = root, target
        self.number = 0
        self.deadline = time.monotonic()+900
        self.cleaning = False
        self.diagnostic_errors = []
        self.plan = None
        self.owned = []
        self.namespace = None
        self.remote_started = None
        self.defaults = {}

    def save(self, name, data):
        raw = data.encode() if isinstance(data, str) else (json.dumps(data, indent=2)+"\n").encode()
        require(len(raw) <= LIMIT, "single evidence limit")
        target = self.root/name
        size = sum(p.stat().st_size for p in self.root.iterdir() if p.is_file())
        require(size+len(raw)-(target.stat().st_size if target.exists() else 0) <= TOTAL, "total evidence limit")
        target.write_bytes(raw)

    def diagnostic(self, name, data):
        try:
            self.save(name, data)
        except (OSError, ValueError) as exc:
            self.diagnostic_errors.append(str(exc))
            if not self.cleaning:
                raise

    def run(self, argv, timeout=25):
        require(time.monotonic() < self.deadline, "invocation/cleanup deadline")
        self.number += 1
        number = self.number
        record = dict(argv=argv, start=time.time(), monotonic_start=time.monotonic())
        self.diagnostic(f"command-{number:04d}.json", record)
        streams = [bytearray(), bytearray()]
        child = None
        try:
            child = subprocess.Popen(argv, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            until = min(self.deadline, time.monotonic()+timeout)
            with selectors.DefaultSelector() as selector:
                for index, pipe in enumerate((child.stdout, child.stderr)):
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, index)
                while selector.get_map():
                    require(time.monotonic() < until, "command timeout")
                    for key, _ in selector.select(.05):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        capacity = LIMIT-sum(map(len, streams))
                        streams[key.data].extend(chunk[:capacity])
                        require(len(chunk) <= capacity, "command output overflow")
                child.wait(timeout=max(.01, until-time.monotonic()))
            require(child.returncode == 0, f"command {number} exit {child.returncode}; see retained stderr")
            return streams[0].decode()
        finally:
            if child is not None:
                try:
                    stop_group(child)
                finally:
                    child.stdout.close()
                    child.stderr.close()
            record.update(end=time.time(), monotonic_end=time.monotonic(), exit=child.returncode if child else None)
            for suffix, raw in zip(("out", "err"), streams):
                self.diagnostic(f"command-{number:04d}.{suffix}", raw.decode(errors="replace"))
            self.diagnostic(f"command-{number:04d}.json", record)

    def kube(self, args, namespace=None):
        argv = ["kubectl", "--context", self.target["context"], "--request-timeout=10s"]
        if namespace:
            argv += ["--namespace", namespace]
        return self.run(argv+args)

    def selected_object(self, kind, name):
        namespace = None if kind == "Namespace" else self.plan["namespace"]
        raw = self.kube(["get", kind, name, "--ignore-not-found", "-o", "go-template="+OBJECT_TEMPLATE], namespace)
        return json.loads(raw) if raw.strip() else None

    def nodes(self):
        fields = dict(name=".metadata.name", uid=".metadata.uid", arch=".status.nodeInfo.architecture",
                      os=".status.nodeInfo.operatingSystem", kernel=".status.nodeInfo.kernelVersion",
                      kubelet=".status.nodeInfo.kubeletVersion", allocatable=".status.allocatable",
                      conditions=".status.conditions", taints=".spec.taints", unschedulable=".spec.unschedulable",
                      deleting=".metadata.deletionTimestamp")
        template = JSON_TEMPLATE+'[{{range $i,$n := .items}}{{if $i}},{{end}}{'+','.join('"'+k+'":{{template "json" '+v+'}}' for k, v in fields.items())+'}{{end}}]'
        nodes = json.loads(self.kube(["get", "nodes", "-o", "go-template="+template]))
        projection = r'{range .items[*]}{.spec.nodeName}{"\t"}{.status.phase}{"\t"}{.spec.containers[*].resources}{"\t"}{.spec.initContainers[*].resources}{"\t"}{.spec.resources}{"\t"}{.spec.overhead}{"\t"}{.status.containerStatuses[*].allocatedResources}{"\t"}{.status.containerStatuses[*].resources}{"\t"}{.status.initContainerStatuses[*].resources}{"\t"}{.status.initContainerStatuses[*].allocatedResources}{"\t"}{.status.resources}{"\n"}{end}'
        raw = self.kube(["get", "pods", "--all-namespaces", "--field-selector=status.phase!=Succeeded,status.phase!=Failed", "-o", "jsonpath="+projection])
        return nodes, allocated(raw)

    def check_node(self):
        nodes, used = self.nodes()
        matches = [n for n in nodes if n["name"] == self.plan["node"]["name"]]
        require(len(matches) == 1, "frozen node disappeared")
        spare = node_gate(matches[0], used.get(matches[0]["name"], dict(cpu=0, memory=0, pods=0)), self.plan["node"]["uid"])
        result = dict(node=matches[0], spare=spare, sampled_at=time.time(), atomic_snapshot=False)
        self.save(f"node-check-{self.number}.json", result)
        return result

    def create(self, obj):
        kind, name = obj["kind"], obj["metadata"]["name"]
        require(kind in KINDS and kind != "Pod", "unsupported creation")
        namespace = None if kind == "Namespace" else self.plan["namespace"]
        filename = f"create-{kind}-{name}.json"
        self.save(filename, obj)
        args = ["create", "-f", str(self.root/filename), "-o", "go-template="+OBJECT_TEMPLATE]
        dry = json.loads(self.kube(args+["--dry-run=server"], namespace))
        if kind == "Job":
            job_check(dry, obj)
            self.check_node()
        # CREATE only; an AlreadyExists or ambiguous response is never adopted.
        result = json.loads(self.kube(args, namespace))
        entry = dict(kind=kind, name=name, uid=result["metadata"]["uid"], owner=self.plan["owner"])
        require(bool(entry["uid"]), "missing create UID")
        self.owned.append(entry)
        if kind == "Namespace":
            self.namespace = entry
        self.save("owned.json", self.owned)
        owner_check(result, entry)
        if kind == "Job":
            job_check(result, obj)
        elif kind == "Namespace":
            require(all(result["metadata"]["labels"].get(k) == v for k, v in obj["metadata"]["labels"].items()), "namespace security label drift")
        elif kind == "NetworkPolicy":
            expected = obj["spec"]
            actual = result["spec"]
            require(actual.get("podSelector") == {} and actual.get("policyTypes") == expected["policyTypes"] and
                    not actual.get("ingress") and not actual.get("egress"), "network policy admission drift")
        return entry

    def delete(self, entry):
        current = self.selected_object(entry["kind"], entry["name"])
        if current is None:
            return
        owner_check(current, entry)
        version, plural = KINDS[entry["kind"]]
        prefix = "/api/v1" if version == "v1" else "/apis/"+version
        namespace = None if entry["kind"] == "Namespace" else self.plan["namespace"]
        uri = prefix+("/namespaces/"+namespace if namespace else "")+"/"+plural+"/"+entry["name"]
        options = dict(apiVersion="v1", kind="DeleteOptions", preconditions=dict(uid=entry["uid"]), propagationPolicy="Orphan")
        filename = "delete-"+entry["uid"]+".json"
        # Operational precondition body has its own small write, independent of
        # diagnostic recording. Disk failure still refuses the unsafe delete.
        (self.root/filename).write_text(json.dumps(options))
        require((self.root/filename).is_file() and json.loads((self.root/filename).read_text()) == options, "delete body unavailable")
        self.kube(["delete", "--raw", uri, "-f", str(self.root/filename)], namespace)
        until = min(self.deadline, time.monotonic()+35)
        while time.monotonic() < until:
            current = self.selected_object(entry["kind"], entry["name"])
            if current is None:
                return
            owner_check(current, entry)
            time.sleep(.5)
        raise ValueError("deletion not confirmed: "+entry["kind"]+"/"+entry["name"])

    def inventory(self):
        names = self.kube(["api-resources", "--namespaced=true", "--verbs=list", "-o", "name"]).split()
        require(names and all(re.fullmatch(r"[a-z0-9.-]+", n) for n in names), "API discovery")
        names = [n for n in names if n != "secrets"]
        # No Secret query: the namespace quota supplies a conservative count.
        quota = self.selected_object("ResourceQuota", "budget")
        require(quota is not None and quota["status"]["used"]["count/secrets"] == "0", "Secret count unavailable/nonzero")
        raw = self.kube(["get", ",".join(names), "-o", "go-template="+META_TEMPLATE], self.plan["namespace"])
        rows = [r.split("|") for r in raw.splitlines()]
        require(all(len(r) == 7 for r in rows), "inventory projection")
        return rows

    def foreign_check(self, rows):
        known = {e["uid"] for e in self.owned}
        for version, kind, name, uid, owner, refs, regarding in rows:
            if uid in known:
                require(owner == self.plan["owner"], "owned object relabelled")
            elif (kind, name) in self.defaults:
                require(uid == self.defaults[kind, name], "namespace default UID changed")
            elif kind == "Event":
                require(regarding in known, "foreign event")
            else:
                raise ValueError("foreign resource: "+kind+"/"+name+"/"+uid)

    def run_job(self, step):
        self.check_node()
        template = job_template(self.plan, step)
        job = self.create(template)
        pod_entry = None
        until = min(self.deadline, time.monotonic()+125)
        failure = None
        try:
            while time.monotonic() < until:
                names = self.kube(["get", "pods", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"], self.plan["namespace"]).split()
                require(len(names) <= 1, "extra Pod")
                if names:
                    pod = self.selected_object("Pod", names[0])
                    options = dict(job=job, template=template, pod_uid=pod_entry["uid"] if pod_entry else None)
                    # Register only a proven controller chain, before further
                    # admission checks so injected owned Pods can be reclaimed.
                    refs = pod["metadata"].get("ownerReferences") or []
                    controller_matches = len(refs) == 1 and all(refs[0].get(k) == v for k, v in
                        dict(uid=job["uid"], name=job["name"], kind="Job", apiVersion="batch/v1", controller=True).items())
                    if not pod_entry and controller_matches and pod["metadata"].get("namespace") == self.plan["namespace"] and pod["metadata"]["labels"].get(LABEL) == self.plan["owner"]:
                        pod_entry = dict(kind="Pod", name=names[0], uid=pod["metadata"]["uid"], owner=self.plan["owner"])
                        self.owned.append(pod_entry)
                        self.save("owned.json", self.owned)
                    pod_check(pod, options)
                    self.save(step+"-pod.json", pod)
                    if pod["status"].get("phase") in ("Succeeded", "Failed"):
                        raw = self.kube(["logs", "pod/"+names[0], "--container=probe", "--limit-bytes="+str(LIMIT), "--tail=-1"], self.plan["namespace"])
                        self.save(step+"-raw.json", raw)
                        require(len(raw.encode()) < LIMIT, "potentially truncated logs")
                        require(pod["status"]["phase"] == "Succeeded", "Job failed")
                        self.check_node()
                        return json.loads(raw)
                job_state = self.selected_object("Job", job["name"])
                owner_check(job_state, job)
                require(not any(c["type"] == "Failed" and c["status"] == "True" for c in (job_state.get("status") or {}).get("conditions", [])), "Job controller failure")
                time.sleep(1)
            raise ValueError("Job deadline exceeded")
        except BaseException as exc:
            failure = exc
            raise
        finally:
            # Orphaning the Job preserves explicit Pod UID deletion checks.
            cleanup_errors = []
            for entry in [job]+([pod_entry] if pod_entry else []):
                try:
                    self.delete(entry)
                except BaseException as exc:
                    cleanup_errors.append(str(exc))
            if cleanup_errors:
                self.diagnostic(step+"-cleanup-errors.json", cleanup_errors)
                if failure is None:
                    raise ValueError("Job cleanup failed: "+"; ".join(cleanup_errors))

    def cleanup(self):
        self.cleaning = True
        self.deadline = time.monotonic()+180
        results = []
        if not self.namespace:
            result = dict(confirmed=not self.remote_started, resources=results, diagnostic_errors=self.diagnostic_errors)
            return result
        try:
            current = self.selected_object("Namespace", self.namespace["name"])
            owner_check(current, self.namespace)
            rows = self.inventory()
            self.diagnostic("cleanup-inventory.json", rows)
            self.foreign_check(rows)
        except BaseException as exc:
            result = dict(confirmed=False, resources=results, error="cleanup ownership uncertain: "+str(exc))
            return result
        for entry in sorted((e for e in self.owned if e["kind"] != "Namespace"), key=lambda e: (e["kind"] != "Job", e["kind"] != "Pod")):
            try:
                self.delete(entry)
                results.append(dict(resource=entry, clean=True))
            except BaseException as exc:
                results.append(dict(resource=entry, clean=False, error=str(exc)))
        if all(r["clean"] for r in results):
            try:
                # Quota is now gone; use its last zero-secret observation plus
                # immutable namespace UID. No unowned object is individually removed.
                self.delete(self.namespace)
                results.append(dict(resource=self.namespace, clean=True))
            except BaseException as exc:
                results.append(dict(resource=self.namespace, clean=False, error=str(exc)))
        result = dict(confirmed=bool(results) and all(r["clean"] for r in results) and not self.diagnostic_errors,
                      resources=results, diagnostic_errors=self.diagnostic_errors)
        return result


def prepare(run, owner):
    require(re.fullmatch(r"weir-qual-m25-[a-z0-9-]{1,25}", owner) is not None, "owner syntax")
    cluster = json.loads(run.run(["aws", "eks", "describe-cluster", "--name", run.target["cluster"], "--region", run.target["region"],
                                 "--query", "cluster.{arn:arn,name:name,version:version,status:status}", "--output", "json", "--cli-connect-timeout", "5", "--cli-read-timeout", "10", "--no-cli-pager"]))
    require(cluster["arn"] == run.target["context"] and cluster["status"] == "ACTIVE", "cluster identity/status")
    require(not run.kube(["get", "namespace", owner, "--ignore-not-found", "-o", "name"]).strip(), "namespace collision")
    for kind, verbs in dict(namespaces=("create", "get", "delete"), jobs=("create", "get", "list", "delete"),
                            pods=("get", "list", "delete"), resourcequotas=("create", "get", "delete"),
                            networkpolicies=("create", "get", "delete"), **{"pods/log": ("get",)}).items():
        for verb in verbs:
            require(run.kube(["auth", "can-i", verb, kind], None if kind == "namespaces" else owner).strip() == "yes", "missing permission: "+verb+" "+kind)
    nodes, used = run.nodes()
    run.save("preflight-nodes.json", nodes)
    run.save("preflight-allocated.json", {n: {k: str(v) for k, v in fields.items()} for n, fields in used.items()})
    selected = None
    for node in sorted(nodes, key=lambda n: n["name"]):
        try:
            spare = node_gate(node, used.get(node["name"], dict(cpu=0, memory=0, pods=0)))
            selected = node
            break
        except ValueError:
            continue
    require(selected is not None, "no suitable existing node; stop before writes")
    cni_template = r'''{{range .spec.template.spec.containers}}{{.name}} {{.image}}{{range .env}}{{if or (eq .name "ENABLE_NETWORK_POLICY") (eq .name "NETWORK_POLICY_ENFORCING_MODE")}} {{.name}}={{.value}}{{end}}{{end}}{{range .args}}{{if or (eq . "--enable-network-policy=true") (eq . "--enable-network-policy=false")}} {{.}}{{end}}{{end}}{{"\n"}}{{end}}'''
    cni_args = ["get", "daemonset", "aws-node", "-o", "go-template="+cni_template]
    cni = run.kube(cni_args, "kube-system")
    run.save("cni-selected.txt", cni)
    head = run.run(["git", "rev-parse", "HEAD"]).strip()
    require(not run.run(["git", "status", "--porcelain"]).strip(), "clean committed implementation required")
    run.run(["git", "diff", "--exit-code", SOURCE, "--", "*.go", "go.mod", "go.sum", "scripts/qualification.Dockerfile"])
    plan = dict(schema_version=1, profile="eks-m25-timing-only-v1", target=run.target, namespace=owner, owner=owner,
                node=selected, initial_spare=spare, cluster=cluster, sampled_at=time.time(), atomic_snapshot=False,
                source=head, image_source=SOURCE, images=IMAGES, tool_inputs={name: digest(REPO/name) for name in FILES},
                sequence=["version", "snapshot"]+["pace-"+str(i) for i in range(5)], rates=RATES,
                seconds=20, planned=23000, database_mutations=0, cpu=1, memory_bytes=512*1024**2,
                dispatch_p99_us=5000, arrival_p95_us=100000, arrival_p99_us=250000,
                remote_seconds=900, cleanup_seconds=180, job_seconds=120, single_output_bytes=LIMIT, total_bytes=TOTAL,
                network_isolation="unqualified; CNI metadata only, no endpoint probes", full_calibration="not-run")
    plan["templates"] = {step: job_template(plan, step) for step in plan["sequence"]}
    run.save("plan.json", plan)
    (run.root/"plan.json").chmod(0o400)
    print(json.dumps(dict(plan=str(run.root/"plan.json"), sha256=digest(run.root/"plan.json"), node_uid=selected["uid"])), flush=True)


def execute(run, options):
    plan = json.loads((run.root/"plan.json").read_text())
    require(digest(run.root/"plan.json") == options.plan_sha256 and plan["target"] == run.target, "frozen plan/context mismatch")
    require(plan["node"]["uid"] == options.node_uid, "node confirmation mismatch")
    require(plan["namespace"] == plan["owner"] and re.fullmatch(r"weir-qual-m25-[a-z0-9-]{1,25}", plan["owner"]) is not None, "frozen namespace/owner")
    require(plan["images"] == IMAGES and plan["image_source"] == SOURCE and plan["rates"] == RATES and plan["planned"] == 23000, "fixed profile drift")
    sequence = ["version", "snapshot"]+["pace-"+str(i) for i in range(5)]
    require(plan["sequence"] == sequence and plan["templates"] == {step: job_template(plan, step) for step in sequence}, "frozen template/sequence drift")
    for name in FILES:
        require(digest(REPO/name) == plan["tool_inputs"][name], "implementation drift")
    require(run.run(["git", "rev-parse", "HEAD"]).strip() == plan["source"], "implementation commit drift")
    require(not run.run(["git", "status", "--porcelain"]).strip(), "worktree drift")
    run.plan = plan
    with (run.root/"invocation.json").open("x") as handle:
        json.dump(dict(start=time.time(), plan_sha256=options.plan_sha256), handle)
    result = dict(probes=[], timing_pass=False, resource_evidence="not-run", generator_ready_for_next_investigation=False,
                  candidate_rps=None, full_calibration="not-run", actual_database_mutations=0, plan_sha256=options.plan_sha256)
    try:
        run.check_node()
        require(not run.kube(["get", "namespace", plan["namespace"], "--ignore-not-found", "-o", "name"]).strip(), "namespace collision")
        run.remote_started = time.monotonic()
        run.deadline = run.remote_started+900
        labels = {LABEL: plan["owner"], "pod-security.kubernetes.io/enforce": "restricted",
                  "pod-security.kubernetes.io/enforce-version": "v1.36", "pod-security.kubernetes.io/audit": "restricted",
                  "pod-security.kubernetes.io/warn": "restricted"}
        ns = dict(apiVersion="v1", kind="Namespace", metadata=dict(name=plan["namespace"], labels=labels))
        run.create(ns)
        metadata = dict(name="default-deny", namespace=plan["namespace"], labels={LABEL: plan["owner"]})
        policy = dict(apiVersion="networking.k8s.io/v1", kind="NetworkPolicy", metadata=metadata,
                      spec=dict(podSelector={}, policyTypes=["Ingress", "Egress"], ingress=[], egress=[]))
        run.create(policy)
        metadata = dict(name="budget", namespace=plan["namespace"], labels={LABEL: plan["owner"]})
        hard = {"pods": "1", "count/pods": "1", "count/jobs.batch": "1", "requests.cpu": "1", "limits.cpu": "1",
                "requests.memory": "512Mi", "limits.memory": "512Mi", "services": "0", "persistentvolumeclaims": "0", "count/secrets": "0"}
        quota = dict(apiVersion="v1", kind="ResourceQuota", metadata=metadata, spec=dict(hard=hard))
        quota_entry = run.create(quota)
        until = min(run.deadline, time.monotonic()+20)
        while True:
            observed = run.selected_object("ResourceQuota", "budget")
            owner_check(observed, quota_entry)
            require(observed["spec"]["hard"] == hard, "quota admission drift")
            if (observed.get("status") or {}).get("used", {}).get("count/secrets") == "0":
                break
            require(time.monotonic() < until, "quota accounting not initialized")
            time.sleep(.5)
        rows = run.inventory()
        for _, kind, name, uid, _, _, _ in rows:
            if (kind, name) in (("ServiceAccount", "default"), ("ConfigMap", "kube-root-ca.crt")):
                run.defaults[kind, name] = uid
        run.foreign_check(rows)
        run.save("namespace-defaults.json", [dict(kind=k[0], name=k[1], uid=v) for k, v in run.defaults.items()])
        version = run.run_job("version")
        expected = dict(product="weir", revision=SOURCE, go="go1.27.1", target="linux/arm64", state="clean-commit", dirty="false", version="local-"+SOURCE)
        require(version == expected, "product version identity")
        result["version"] = version
        snapshot = run.run_job("snapshot")
        result["snapshot"] = resources([snapshot])
        result["resource_evidence"] = result["snapshot"]["status"]
        for index, rate in enumerate(RATES):
            raw = run.run_job("pace-"+str(index))
            report = pacing(raw, rate)
            result["probes"].append(report)
            run.save("result.json", result)
            print(json.dumps(dict(probe=index, rate=rate, timing_pass=report["timing_pass"], dispatch_lag_p99_us=report["dispatch_lag_p99_us"])), flush=True)
            if not report["timing_pass"]:
                result["stop_reason"] = "timing gate failed; no later probe or retry"
                break
        result["timing_pass"] = len(result["probes"]) == 5 and all(p["timing_pass"] for p in result["probes"])
        result["generator_ready_for_next_investigation"] = result["timing_pass"]
    except BaseException as exc:
        result["error"] = str(exc)
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        result["cleanup"] = run.cleanup()
        result["remote_elapsed_seconds"] = time.monotonic()-run.remote_started if run.remote_started else 0
        result["generator_ready_for_next_investigation"] &= result["cleanup"]["confirmed"] and not result.get("error")
        run.save("result.json", result)
    print(json.dumps(dict(evidence=str(run.root), error=result.get("error"), timing_pass=result["timing_pass"], cleanup=result["cleanup"]["confirmed"])), flush=True)
    return int(bool(result.get("error")) or not result["cleanup"]["confirmed"])


def main():
    require(__debug__ and os.environ.get("WEIR_EKS_M25") == "1", "unoptimized explicit WEIR_EKS_M25=1 required")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run"))
    parser.add_argument("--evidence", type=Path, required=True)
    for name in ("context", "cluster", "region"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--owner")
    parser.add_argument("--node-uid")
    parser.add_argument("--plan-sha256")
    args = parser.parse_args()
    target = dict(context=args.context, cluster=args.cluster, region=args.region)
    target_check(target)
    root = args.evidence.absolute()
    require(root.parent.resolve() == (REPO/".testdata/m25").resolve() and not root.is_symlink(), "controlled evidence location required")
    if args.mode == "prepare":
        require(args.owner is not None, "owner required")
        root.mkdir(mode=0o700)
    else:
        require(args.node_uid and args.plan_sha256 and root.is_dir() and root.stat().st_mode & 0o077 == 0, "private prepared evidence required")
    run = Run(root, target)
    # Continue numbering after the read-only preparation; no evidence overwrite.
    run.number = max([int(p.stem.split("-")[1]) for p in root.glob("command-*.json")]+[0])
    def interrupted(signum, frame):
        raise KeyboardInterrupt("signal "+str(signum))
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    if args.mode == "prepare":
        prepare(run, args.owner)
        return 0
    return execute(run, args)


if __name__ == "__main__":
    raise SystemExit(main())
