#!/usr/bin/env python3
"""Explicit M25 preflight/run; no services, databases, arbitrary args or retries."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time

from eks_resources import allocated, projection, serializable
from capacity_fixture import stop_group
from eks_pacing_report import IMAGES, RATES, SOURCE, node_gate, pacing, quantity, require, resources

REPO = Path(__file__).resolve().parent.parent
LABEL = "qualification.weir.io/owner"
LIMIT = 8 << 20
TOTAL = 128 << 20
FILES = ("scripts/eks_pacing.py", "scripts/eks_pacing_report.py", "scripts/eks_resources.py", "scripts/capacity_fixture.py",
         "scripts/capacity_report.py", "scripts/capacity_contract.py", "scripts/capacity-plan-m22r.json", "scripts/eks_pacing_test.py")
KINDS = {"Namespace": ("v1", "namespaces"), "Job": ("batch/v1", "jobs"),
         "Pod": ("v1", "pods"), "ResourceQuota": ("v1", "resourcequotas"),
         "NetworkPolicy": ("networking.k8s.io/v1", "networkpolicies"),
         "ConfigMap": ("v1", "configmaps")}
# Emit the admitted spec as JSON, but never return unexpected literal env values.
JSON_TEMPLATE = '''{{define "json"}}{{$t := printf "%T" .}}{{if eq $t "map[string]interface {}"}}{ {{$first := true}}{{range $k,$v := .}}{{if not $first}},{{end}}{{$first = false}}{{printf "%q" $k}}:{{if eq $k "env"}}[{{range $i,$e := $v}}{{if $i}},{{end}}{"name":{{printf "%q" $e.name}}{{if $e.valueFrom}},"valueFrom":{{template "json" $e.valueFrom}}{{else}},"value":{{if or (eq $e.name "GOMAXPROCS") (eq $e.name "WEIR_CAPACITY_INTEGRATION") (eq $e.name "ES_JAVA_OPTS") (eq $e.name "AWS_EC2_METADATA_DISABLED")}}{{template "json" $e.value}}{{else}}"REDACTED"{{end}}{{end}}}{{end}}]{{else}}{{template "json" $v}}{{end}}{{end}} }{{else if eq $t "[]interface {}"}}[{{range $i,$v := .}}{{if $i}},{{end}}{{template "json" $v}}{{end}}]{{else if eq $t "string"}}{{printf "%q" .}}{{else if eq $t "<nil>"}}null{{else}}{{.}}{{end}}{{end}}'''
OBJECT_TEMPLATE = JSON_TEMPLATE + '''{"apiVersion":{{printf "%q" .apiVersion}},"kind":{{printf "%q" .kind}},"metadata":{"name":{{printf "%q" .metadata.name}},"namespace":{{template "json" .metadata.namespace}},"uid":{{template "json" .metadata.uid}},"labels":{{template "json" .metadata.labels}},"ownerReferences":{{template "json" .metadata.ownerReferences}},"deletionTimestamp":{{template "json" .metadata.deletionTimestamp}}},"spec":{{template "json" .spec}},"status":{{template "json" .status}},"data":{{template "json" .data}},"immutable":{{template "json" .immutable}}}'''
META_TEMPLATE = r'''{{range .items}}{{.apiVersion}}|{{.kind}}|{{.metadata.name}}|{{.metadata.uid}}|{{if .metadata.labels}}{{index .metadata.labels "qualification.weir.io/owner"}}{{end}}|{{range .metadata.ownerReferences}}{{.uid}},{{end}}|{{if .involvedObject}}{{.involvedObject.uid}}{{else if .regarding}}{{.regarding.uid}}{{end}}|{{.metadata.namespace}}{{"\n"}}{{end}}'''
EVENT_TEMPLATE = JSON_TEMPLATE + '''[{{range $i,$e := .items}}{{if $i}},{{end}}{"uid":{{template "json" .metadata.uid}},"namespace":{{template "json" .metadata.namespace}},"involvedObject":{{template "json" .involvedObject}},"reason":{{template "json" .reason}},"message":{{template "json" .message}},"count":{{template "json" .count}},"firstTimestamp":{{template "json" .firstTimestamp}},"lastTimestamp":{{template "json" .lastTimestamp}},"eventTime":{{template "json" .eventTime}},"series":{{template "json" .series}}}{{end}}]'''


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
            (meta.get("labels") or {}).get(LABEL) == expected["owner"], "object UID/owner drift")


def job_template(plan, step):
    require(isinstance(plan["node"]["name"], str) and bool(plan["node"]["name"]) and
            bool(plan["node"]["uid"]), "frozen node name/UID required")
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
                dnsPolicy="None", dnsConfig=dict(nameservers=["127.0.0.1"]),
                securityContext=dict(runAsNonRoot=True, runAsUser=65532, runAsGroup=65532,
                                     seccompProfile=dict(type="RuntimeDefault")), containers=[container])
    labels = {LABEL: plan["owner"]}
    result = dict(apiVersion="batch/v1", kind="Job",
                  metadata=dict(name=step, namespace=plan["namespace"], labels=labels),
                  spec=dict(completions=1, parallelism=1, backoffLimit=0, activeDeadlineSeconds=120,
                            template=dict(metadata=dict(labels=labels), spec=spec)))
    return result


def exact_value(actual, expected):
    """JSON equality including scalar types (Python otherwise equates False/0)."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(exact_value(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(exact_value(a, b) for a, b in zip(actual, expected))
    return actual == expected


def quantity_map_check(actual, expected):
    """Exact values and keys, for resource lists including Quota hard."""
    require(isinstance(actual, dict) and isinstance(expected, dict) and actual.keys() == expected.keys(), "resource keys drift")
    for key, value in expected.items():
        require(quantity(actual[key]) == quantity(value), "resource quantity drift: "+key)


def quota_check(actual, expected):
    actual, expected = dict(actual), dict(expected)
    quantity_map_check(actual.pop("hard", None), expected.pop("hard"))
    require(exact_value(actual, expected), "quota spec drift")


def container_field_check(field, actual, expected):
    if field == "resources":
        require(isinstance(actual, dict) and actual.keys() == expected.keys() and
                set(expected) <= {"requests", "limits"}, "resource requirements drift")
        for name, values in expected.items():
            quantity_map_check(actual[name], values)
    elif field in ("readinessProbe", "startupProbe", "livenessProbe"):
        require(isinstance(actual, dict), "probe type")
        actual, expected = dict(actual), dict(expected)
        # core/v1 Probe scalar omitempty and v1.36 SetDefaults_Probe only.
        # Pointer fields, handlers and unknown fields get no normalization.
        defaults = dict(initialDelaySeconds=0, timeoutSeconds=1, periodSeconds=10,
                        successThreshold=1, failureThreshold=3)
        for key, value in defaults.items():
            actual.setdefault(key, value)
            expected.setdefault(key, value)
        require(exact_value(actual, expected), "probe admission drift: "+field)
    else:
        require(exact_value(actual, expected), "container admission drift: "+field)


def volumes_check(actual, expected):
    require(isinstance(actual, list) and len(actual) == len(expected), "volume list drift")
    actual, expected = copy.deepcopy(actual), copy.deepcopy(expected)
    for observed, wanted in zip(actual, expected):
        if "sizeLimit" in wanted.get("emptyDir", {}):
            require(isinstance(observed.get("emptyDir"), dict), "emptyDir drift")
            left, right = observed["emptyDir"], wanted["emptyDir"]
            require(quantity(left.pop("sizeLimit", None)) == quantity(right.pop("sizeLimit")), "emptyDir sizeLimit drift")
    require(exact_value(actual, expected), "volume admission drift")


def admitted_spec(actual, expected, *, pod=False):
    """Reject unknown admission fields; allow only standard, inert API defaults."""
    require(bool(expected.get("nodeName")), "frozen nodeName required")
    require(not any(k in expected for k in ("preemptionPolicy", "priority", "priorityClassName")),
            "priority fields must be omitted from the frozen template")
    extra = dict(actual)
    for key, value in expected.items():
        if key in ("containers", "initContainers"):
            require(isinstance(actual.get(key), list) and len(actual[key]) == len(value), "injected/missing container")
            for observed, wanted in zip(actual[key], value):
                container = dict(observed)
                for field, expected_value in wanted.items():
                    container_field_check(field, container.pop(field, None), expected_value)
                for field, expected_value in dict(terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File").items():
                    require(container.pop(field, expected_value) == expected_value, "container default drift")
                require(not container, "extra container fields: "+str(sorted(container)))
            extra.pop(key)
        elif key == "volumes":
            volumes_check(extra.pop(key, None), value)
        else:
            require(exact_value(extra.pop(key, None), value), "Pod admission drift: "+key)
    # Job templates do not go through Pod Priority admission. Actual/dry-run
    # Pods must contain its ordinary defaults, bound to the exact frozen node.
    priority = extra.pop("priority", None if pod else 0)
    require(type(priority) is int and priority == 0, "Pod priority drift/missing")
    policy = extra.pop("preemptionPolicy", None if pod else "PreemptLowerPriority")
    require(policy == "PreemptLowerPriority", "Pod preemption policy drift/missing")
    require(extra.pop("priorityClassName", "") == "", "unexpected PriorityClass")
    defaults = dict(schedulerName="default-scheduler", serviceAccountName="default", serviceAccount="default",
                    hostNetwork=False, hostPID=False, hostIPC=False)
    for key, value in defaults.items():
        require(exact_value(extra.pop(key, value), value), "Pod default drift: "+key)
    # The API normally injects these two node-lifecycle tolerations. Freeze them
    # here, do not send them and never use them to accept a tainted node.
    tolerations = extra.pop("tolerations", [])
    allowed = [dict(key="node.kubernetes.io/"+key, operator="Exists", effect="NoExecute", tolerationSeconds=300)
               for key in ("not-ready", "unreachable")]
    require(exact_value(tolerations, []) or exact_value(tolerations, allowed), "unexpected toleration")
    require(not extra, "extra Pod fields: "+str(sorted(extra)))


def job_check(actual, expected):
    require(actual["apiVersion"] == expected["apiVersion"] and actual["kind"] == "Job", "Job kind drift")
    meta, wanted_meta = actual["metadata"], expected["metadata"]
    require(all(meta.get(k) == wanted_meta[k] for k in ("name", "namespace")) and
            all((meta.get("labels") or {}).get(k) == v for k, v in wanted_meta["labels"].items()) and
            not meta.get("ownerReferences"), "Job metadata drift")
    spec = dict(actual["spec"])
    for key in ("completions", "parallelism", "backoffLimit", "activeDeadlineSeconds"):
        require(exact_value(spec.pop(key, None), expected["spec"][key]), "Job admission drift: "+key)
    defaults = dict(suspend=False, manualSelector=False, completionMode="NonIndexed", podReplacementPolicy="TerminatingOrFailed")
    for key, value in defaults.items():
        require(exact_value(spec.pop(key, value), value), "Job execution mode: "+key)
    selector = spec.pop("selector", None)
    if selector is not None:
        wanted = {"matchLabels": {"batch.kubernetes.io/controller-uid": actual["metadata"]["uid"]}}
        require(selector == wanted, "Job selector drift")
    template = spec.pop("template")
    labels = template.get("metadata", {}).get("labels") or {}
    require(all(labels.get(k) == v for k, v in expected["spec"]["template"]["metadata"]["labels"].items()), "Job template labels")
    admitted_spec(template["spec"], expected["spec"]["template"]["spec"])
    require(not spec, "extra Job fields: "+str(sorted(spec)))


def pod_identity(pod, options):
    job, template, uid = (options[k] for k in ("job", "template", "pod_uid"))
    meta = pod["metadata"]
    require(not uid or meta["uid"] == uid, "Pod UID drift")
    require(meta["namespace"] == template["metadata"]["namespace"] and meta["labels"].get(LABEL) == template["metadata"]["labels"][LABEL], "Pod namespace/owner")
    refs = meta["ownerReferences"]
    require(len(refs) == 1 and refs[0]["uid"] == job["uid"] and refs[0]["name"] == job["name"] and
            refs[0]["kind"] == "Job" and refs[0]["apiVersion"] == "batch/v1" and refs[0]["controller"] is True, "Job to Pod owner chain")


def pod_check(pod, options):
    pod_identity(pod, options)
    job, template = options["job"], options["template"]
    admitted_spec(pod["spec"], template["spec"]["template"]["spec"], pod=True)
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


class CommandFailure(ValueError):
    def __init__(self, number, code, stderr):
        super().__init__(f"command {number} exit {code}; see retained stderr")
        self.number, self.code, self.stderr = number, code, stderr


class Run:
    node_minimum = None

    def job_template(self, step):
        return job_template(self.plan, step)

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
        self.event_uids = set()
        self.node_scope = None
        self.resource_preflight = None
        self.mutation_attempted = False

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

    def run(self, argv, timeout=25, *, monitor=False):
        require(time.monotonic() < self.deadline, "invocation/cleanup deadline")
        self.number += 1
        number = self.number
        record = dict(argv=argv, start=time.time(), monotonic_start=time.monotonic())
        self.save(f"command-{number:04d}.json", record)
        streams = [bytearray(), bytearray()]
        child = None
        try:
            # Reserve Stop/Wait time inside both the CLI and invocation budgets.
            allowance = min(timeout, 21) if self.cleaning else timeout
            until = min(self.deadline-(4 if self.cleaning else 0), record["monotonic_start"]+allowance)
            require(time.monotonic() < until, "insufficient command/Stop/Wait budget")
            child = subprocess.Popen(argv, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            next_observation = time.monotonic()+2
            with selectors.DefaultSelector() as selector:
                for index, pipe in enumerate((child.stdout, child.stderr)):
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, index)
                while selector.get_map():
                    require(time.monotonic() < until, "command timeout")
                    if monitor and time.monotonic() >= next_observation:
                        self.monitor()
                        next_observation = time.monotonic()+2
                    for key, _ in selector.select(.05):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        capacity = LIMIT-sum(map(len, streams))
                        streams[key.data].extend(chunk[:capacity])
                        require(len(chunk) <= capacity, "command output overflow")
                child.wait(timeout=max(.01, until-time.monotonic()))
            if child.returncode != 0:
                failure = CommandFailure(number, child.returncode, streams[1].decode(errors="replace"))
                raise failure
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
            require(not self.diagnostic_errors, "command evidence unavailable")

    def kube(self, args, namespace=None):
        if args[0] not in ("get", "auth", "api-resources"):
            self.mutation_attempted = True
        argv = ["kubectl", "--context", self.target["context"], "--request-timeout=10s"]
        if namespace:
            argv += ["--namespace", namespace]
        return self.run(argv+args)

    def selected_object(self, kind, name):
        namespace = None if kind == "Namespace" else self.plan["namespace"]
        raw = self.kube(["get", kind, name, "--ignore-not-found", "-o", "go-template="+OBJECT_TEMPLATE], namespace)
        return json.loads(raw) if raw.strip() else None

    def nodes(self, selection):
        require(isinstance(selection, dict) and isinstance(selection.get("name"), str) and
                re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", selection["name"]) and
                isinstance(selection.get("uid"), str) and re.fullmatch(r"[A-Za-z0-9-]{1,128}", selection["uid"]), "explicit node name/UID required")
        scope = dict(name=selection["name"], uid=selection["uid"])
        if self.resource_preflight is None:
            started = time.monotonic()
            self.resource_preflight = dict(node=scope, started=started, deadline=started+120, recovery=None)
        require(self.resource_preflight["node"] == scope, "frozen resource node scope drift")
        self.save("resource-preflight.json", self.resource_preflight)
        fields = dict(name=".metadata.name", uid=".metadata.uid", arch=".status.nodeInfo.architecture",
                      os=".status.nodeInfo.operatingSystem", kernel=".status.nodeInfo.kernelVersion",
                      kubelet=".status.nodeInfo.kubeletVersion", allocatable=".status.allocatable",
                      conditions=".status.conditions", taints=".spec.taints", unschedulable=".spec.unschedulable",
                      deleting=".metadata.deletionTimestamp")
        template = JSON_TEMPLATE+'{'+','.join('"'+k+'":{{template "json" '+v+'}}' for k, v in fields.items())+'}'
        commands = [["get", "node", scope["name"], "-o", "go-template="+template],
                    ["get", "pods", "--all-namespaces", "--field-selector=spec.nodeName="+scope["name"], "--chunk-size=0", "-o", "go-template="+projection(JSON_TEMPLATE, scoped=True)]]
        outputs = []
        for args in commands:
            argv = ["kubectl", "--context", self.target["context"], "--request-timeout=10s"]+args
            # The common read-only window is never renewed. After a write,
            # dispatch checks use only the original native deadline, no recovery.
            written = self.mutation_attempted or bool(self.owned)
            deadline = self.deadline if written else min(self.deadline, self.resource_preflight["deadline"])
            require(deadline-time.monotonic() >= 25+4, "资源窗口余额不足 (resource preflight deadline): need 25s CLI + 4s Stop/Wait")
            try:
                raw = self.run(argv, 25)
            except CommandFailure as exc:
                read_timeout = exc.code == 1 and any(message in exc.stderr for message in (
                    "Client.Timeout or context cancellation while reading body", "net/http: timeout awaiting response headers"))
                read_timeout &= not any(message in exc.stderr.lower() for message in ("forbidden", "unauthorized", "notfound"))
                require(read_timeout and not written and
                        self.resource_preflight["recovery"] is None, "resource GET failed; recovery ineligible: "+str(exc))
                require(deadline-time.monotonic() >= 25+4,
                        "资源窗口余额不足 (resource preflight deadline): need 25s CLI + 4s Stop/Wait; original GET: "+str(exc))
                recovery = dict(failed_command=exc.number, reason=exc.stderr, argv=argv,
                                stdout=f"command-{exc.number:04d}.out", stderr=f"command-{exc.number:04d}.err", exit=exc.code)
                self.resource_preflight["recovery"] = recovery
                self.save("resource-preflight.json", self.resource_preflight)
                raw = self.run(argv, 25)
            require(time.monotonic() < deadline, "resource preflight deadline")
            outputs.append(raw)
            if len(outputs) == 1:
                node = json.loads(raw)
                require(isinstance(node, dict) and set(node) == set(fields) and
                        node["name"] == scope["name"] and node["uid"] == scope["uid"], "frozen node identity/incomplete projection")
                empty = dict(cpu=0, memory=0, pods=0, **{"ephemeral-storage":0})
                # Reject known node failures before any subsequent resource GET.
                # This is only a prerequisite; the real ledger is checked below.
                node_gate(node, empty, scope["uid"], self.node_minimum)
        nodes = [node]
        used, details = allocated(outputs[1], node_name=scope["name"])
        self.save(f"resource-accounting-{self.number}.json", serializable(dict(nodes=used, pods=details)))
        return nodes, used

    def check_node(self):
        nodes, used = self.nodes(self.plan["node"])
        matches = [n for n in nodes if n["name"] == self.plan["node"]["name"]]
        require(len(matches) == 1, "frozen node disappeared")
        spare = node_gate(matches[0], used.get(matches[0]["name"], dict(cpu=0, memory=0, pods=0, **{"ephemeral-storage": 0})), self.plan["node"]["uid"], self.node_minimum)
        result = dict(node=matches[0], spare=spare, sampled_at=time.time(), atomic_snapshot=False)
        self.save(f"node-check-{self.number}.json", result)
        return result

    def create(self, obj):
        kind, name = obj["kind"], obj["metadata"]["name"]
        require(kind in KINDS and kind != "Pod", "unsupported creation")
        if kind == "Job":
            require(obj == self.job_template(name), "frozen Job template drift")
        namespace = None if kind == "Namespace" else self.plan["namespace"]
        filename = f"create-{kind}-{name}.json"
        self.save(filename, obj)
        args = ["create", "-f", str(self.root/filename), "-o", "go-template="+OBJECT_TEMPLATE]
        dry = json.loads(self.kube(args+["--dry-run=server"], namespace))
        if kind == "Job":
            job_check(dry, obj)
            self.pod_dry_run(obj)
            self.check_node()
        elif kind == "ResourceQuota":
            quota_check(dry["spec"], obj["spec"])
        # CREATE only; an AlreadyExists or ambiguous response is never adopted.
        if kind == "Job":
            self.job_create_attempted = True
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
        elif kind == "ResourceQuota":
            quota_check(result["spec"], obj["spec"])
        elif kind == "ConfigMap":
            require(result.get("data") == obj["data"], "ConfigMap admission drift")
        elif kind == "Namespace":
            require(all(result["metadata"]["labels"].get(k) == v for k, v in obj["metadata"]["labels"].items()), "namespace security label drift")
        elif kind == "NetworkPolicy":
            expected = obj["spec"]
            actual = result["spec"]
            require(actual.get("podSelector") == {} and actual.get("policyTypes") == expected["policyTypes"] and
                    not actual.get("ingress") and not actual.get("egress"), "network policy admission drift")
        return entry

    def pod_dry_run(self, job):
        # This request tests Pod admission only. It has no fabricated controller
        # UID and never falls back to persistent creation. The real chain is
        # checked separately when the Job controller creates its Pod.
        template = copy.deepcopy(job["spec"]["template"])
        template["metadata"].update(name=job["metadata"]["name"]+"-admission", namespace=self.plan["namespace"])
        request = dict(apiVersion="v1", kind="Pod", metadata=template["metadata"], spec=template["spec"])
        filename = "dry-run-Pod-"+job["metadata"]["name"]+".json"
        self.save(filename, request)
        args = ["create", "--dry-run=server", "-f", str(self.root/filename), "-o", "go-template="+OBJECT_TEMPLATE]
        response = json.loads(self.kube(args, self.plan["namespace"]))
        self.save("admitted-Pod-"+job["metadata"]["name"]+".json", response)
        meta = response["metadata"]
        require(response["apiVersion"] == "v1" and response["kind"] == "Pod", "Pod dry-run kind drift")
        # Admission may append labels; only requested labels establish fixture
        # identity. Execution fields still go through the strict spec check.
        labels = meta.get("labels")
        require(meta.get("name") == request["metadata"]["name"] and
                meta.get("namespace") == request["metadata"]["namespace"] and
                isinstance(labels, dict) and
                all(labels.get(k) == v for k, v in request["metadata"]["labels"].items()) and
                not meta.get("ownerReferences"), "Pod dry-run metadata drift")
        admitted_spec(response["spec"], request["spec"], pod=True)

    def job_events(self, job):
        args = ["get", "events", "--field-selector=involvedObject.uid="+job["uid"], "-o", "go-template="+EVENT_TEMPLATE]
        events = json.loads(self.kube(args, self.plan["namespace"]))
        require(isinstance(events, list) and len(events) <= 32, "Job Event count/type")
        for event in events:
            ref = event["involvedObject"]
            expected = dict(uid=job["uid"], name=job["name"], namespace=self.plan["namespace"], kind="Job", apiVersion="batch/v1")
            require(event["namespace"] == self.plan["namespace"] and
                    all(ref.get(k) == v for k, v in expected.items()), "Job Event UID/identity drift")
            require(isinstance(event["reason"], str) and len(event["reason"]) <= 256 and
                    isinstance(event["message"], str) and len(event["message"]) <= 4096, "Job Event text bound")
        failed = [event for event in events if event["reason"] == "FailedCreate"]
        if failed:
            filename = job["name"]+"-failed-create.json"
            observed = dict(observed_at=time.time(), events=failed)
            self.save(filename, observed)
            raise ValueError("Job FailedCreate; see "+filename)

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
        require(not self.diagnostic_errors, "delete evidence unavailable")
        try:
            self.kube(["delete", "--raw", uri, "-f", str(self.root/filename)], namespace)
        except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
            # Never replay an ambiguous DELETE. A fresh GET can confirm absence.
            current = self.selected_object(entry["kind"], entry["name"])
            readback = dict(error=str(exc), current=current)
            self.save("delete-"+entry["uid"]+"-readback.json", readback)
            if current is None:
                return
            owner_check(current, entry)
            raise
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
        require(names and len(names) == len(set(names)) and
                all(re.fullmatch(r"[a-z][a-z0-9.-]*", n) for n in names), "API discovery")
        names = [n for n in names if n != "secrets"]
        metrics = None
        if "pods.metrics.k8s.io" in names:
            # API discovery describes server semantics, not the caller's RBAC.
            metrics = json.loads(self.kube(["get", "--raw", "/apis/metrics.k8s.io/v1beta1"]))
            self.save(f"metrics-discovery-{self.number:04d}.json", metrics)
            require(metrics.get("kind") == "APIResourceList" and
                    metrics.get("groupVersion") == "metrics.k8s.io/v1beta1", "metrics discovery identity")
            pods = [r for r in metrics.get("resources", []) if r.get("name") == "pods"]
            require(len(pods) == 1 and pods[0].get("namespaced") is True and
                    pods[0].get("kind") == "PodMetrics" and
                    set(pods[0].get("verbs", [])) == {"get", "list"}, "metrics API is not the readonly Pod view")
        # No Secret query: retain the registered quota until the final inventory.
        quota = self.selected_object("ResourceQuota", "budget")
        expected = next((e for e in self.owned if e["kind"] == "ResourceQuota" and e["name"] == "budget"), None)
        require(expected is not None, "quota not registered")
        owner_check(quota, expected)
        require(quota.get("spec", {}).get("hard", {}).get("count/secrets") == "0" and
                quota.get("status", {}).get("hard", {}).get("count/secrets") == "0" and
                quota.get("status", {}).get("used", {}).get("count/secrets") == "0", "Secret count unavailable/nonzero")
        # Small sequential batches keep a large API catalog within each CLI's
        # time limit without filtering out unknown lifecycle resources.
        rows = []
        for start in range(0, len(names), 5):
            raw = self.kube(["get", ",".join(names[start:start+5]), "-o", "go-template="+META_TEMPLATE], self.plan["namespace"])
            batch = [r.split("|") for r in raw.splitlines()]
            require((not raw or raw.endswith("\n")) and
                    all(len(r) == 8 and r[7] == self.plan["namespace"] for r in batch), "inventory namespace/projection")
            rows.extend(batch)
        self.save(f"inventory-raw-{self.number:04d}.json", rows)
        persistent, views = [], []
        for row in rows:
            if row[:2] == ["metrics.k8s.io/v1beta1", "PodMetrics"]:
                require(metrics is not None and bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", row[2])) and
                        row[3] in ("", "<no value>") and not row[5] and not row[6], "uncertain PodMetrics projection")
                views.append(row)
            else:
                persistent.append(row[:7])
        classified = dict(persistent=persistent, readonly_pod_metrics=views)
        self.save(f"inventory-classified-{self.number:04d}.json", classified)
        return persistent

    def foreign_check(self, rows):
        known = {e["uid"]: e for e in self.owned}
        for version, kind, name, uid, owner, refs, regarding in rows:
            require(uid not in ("", "<no value>"), "persistent UID missing")
            if uid in known:
                expected = known[uid]
                require((version, kind, name) == (KINDS[expected["kind"]][0], expected["kind"], expected["name"]),
                        "owned object API/name drift")
                require(owner == self.plan["owner"], "owned object relabelled")
            elif (kind, name) in self.defaults:
                require(version == "v1" and uid == self.defaults[kind, name] and not owner and not refs,
                        "namespace default identity changed")
            elif kind == "Event" and version in ("v1", "events.k8s.io/v1"):
                require(regarding in known or regarding in self.event_uids, "foreign event")
            else:
                raise ValueError("foreign resource: "+kind+"/"+name+"/"+uid)

    def run_job(self, step):
        self.check_node()
        template = self.job_template(step)
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
                self.job_events(job)
                if names and pod["status"].get("phase") in ("Succeeded", "Failed"):
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
                try:
                    self.diagnostic(step+"-cleanup-errors.json", cleanup_errors)
                except (OSError, ValueError):
                    if failure is None:
                        raise
                if failure is None:
                    raise ValueError("Job cleanup failed: "+"; ".join(cleanup_errors))

    def cleanup(self, *, deadline=None):
        self.cleaning = True
        self.deadline = time.monotonic()+180 if deadline is None else deadline
        results = []
        if not self.namespace:
            result = dict(confirmed=not self.remote_started, resources=results, diagnostic_errors=self.diagnostic_errors)
            return result
        deleting = None
        overall_deadline = self.deadline
        # Leave 45 seconds of the same cleanup budget for namespace DELETE
        # and successful absence readback, including the CLI Stop/Wait reserve.
        self.deadline = overall_deadline-45
        try:
            budget = dict(deadline=overall_deadline, objects_deadline=self.deadline, namespace_reserve_seconds=45)
            self.save("cleanup-budget.json", budget)
            current = self.selected_object("Namespace", self.namespace["name"])
            owner_check(current, self.namespace)
            require(not self.diagnostic_errors, "cleanup evidence unavailable")
            order = {"Job": 0, "Pod": 1, "ConfigMap": 2, "NetworkPolicy": 3, "ResourceQuota": 4}
            entries = sorted((e for e in self.owned if e["kind"] != "Namespace"), key=lambda e: order[e["kind"]])
            for entry in entries:
                if entry["kind"] == "ResourceQuota":
                    # Recheck the complete inventory while zero-secret enforcement
                    # and its observed count still exist. Namespace goes last.
                    rows = self.inventory()
                    self.save("cleanup-final-inventory.json", rows)
                    self.foreign_check(rows)
                    owner_check(self.selected_object("Namespace", self.namespace["name"]), self.namespace)
                deleting = entry
                self.delete(entry)
                receipt = dict(resource=entry, clean=True)
                results.append(receipt)
                deleting = None
            require(any(e["kind"] == "ResourceQuota" for e in entries), "final quota/inventory unavailable")
            require(time.monotonic() < self.deadline-4, "namespace confirmation reserve exhausted")
            self.deadline = overall_deadline
            deleting = self.namespace
            self.delete(self.namespace)
            receipt = dict(resource=self.namespace, clean=True)
            results.append(receipt)
        except BaseException as exc:
            if deleting is not None:
                receipt = dict(resource=deleting, clean=False, error=str(exc))
                results.append(receipt)
            result = dict(confirmed=False, resources=results, error=str(exc), diagnostic_errors=self.diagnostic_errors)
            return result
        result = dict(confirmed=bool(results) and not self.diagnostic_errors,
                      resources=results, diagnostic_errors=self.diagnostic_errors)
        return result


def prepare(run, owner):
    require(re.fullmatch(r"weir-qual-m25-[a-z0-9-]{1,25}", owner) is not None, "owner syntax")
    cluster = json.loads(run.run(["aws", "eks", "describe-cluster", "--name", run.target["cluster"], "--region", run.target["region"],
                                 "--query", "cluster.{arn:arn,name:name,version:version,status:status}", "--output", "json", "--cli-connect-timeout", "5", "--cli-read-timeout", "10", "--no-cli-pager"]))
    require(cluster["arn"] == run.target["context"] and cluster["status"] == "ACTIVE", "cluster identity/status")
    require(not run.kube(["get", "namespace", owner, "--ignore-not-found", "-o", "name"]).strip(), "namespace collision")
    for kind, verbs in dict(namespaces=("create", "get", "delete"), jobs=("create", "get", "list", "delete"),
                            pods=("create", "get", "list", "delete"), events=("list",), resourcequotas=("create", "get", "delete"),
                            networkpolicies=("create", "get", "delete"), **{"pods/log": ("get",)}).items():
        for verb in verbs:
            require(run.kube(["auth", "can-i", verb, kind], None if kind == "namespaces" else owner).strip() == "yes", "missing permission: "+verb+" "+kind)
    cni_template = r'''{{range .spec.template.spec.containers}}{{.name}} {{.image}}{{range .env}}{{if or (eq .name "ENABLE_NETWORK_POLICY") (eq .name "NETWORK_POLICY_ENFORCING_MODE")}} {{.name}}={{.value}}{{end}}{{end}}{{range .args}}{{if or (eq . "--enable-network-policy=true") (eq . "--enable-network-policy=false")}} {{.}}{{end}}{{end}}{{"\n"}}{{end}}'''
    cni_args = ["get", "daemonset", "aws-node", "-o", "go-template="+cni_template]
    cni = run.kube(cni_args, "kube-system")
    run.save("cni-selected.txt", cni)
    head = run.run(["git", "rev-parse", "HEAD"]).strip()
    require(not run.run(["git", "status", "--porcelain"]).strip(), "clean committed implementation required")
    run.run(["git", "diff", "--exit-code", SOURCE, "--", "*.go", "go.mod", "go.sum", "scripts/qualification.Dockerfile"])
    tool_inputs = {name:digest(REPO/name) for name in FILES}
    static = dict(completed=time.time(), monotonic_completed=time.monotonic(), source=head)
    run.save("static-preflight.json", static)
    run.plan = dict(node=run.node_scope)
    assessment = run.check_node()
    selected, spare = assessment["node"], assessment["spare"]
    plan = dict(schema_version=1, profile="eks-m25-timing-only-v1", target=run.target, namespace=owner, owner=owner,
                node=selected, initial_spare=spare, resource_preflight=run.resource_preflight, cluster=cluster, sampled_at=time.time(), atomic_snapshot=False,
                source=head, image_source=SOURCE, images=IMAGES, tool_inputs=tool_inputs,
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
    run.resource_preflight = plan.get("resource_preflight")
    require(isinstance(run.resource_preflight, dict), "frozen resource preflight required; cannot start a new window")
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
            quota_check(observed["spec"], quota["spec"])
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
    parser.add_argument("--node-name")
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
        run.node_scope = dict(name=args.node_name, uid=args.node_uid)
        prepare(run, args.owner)
        return 0
    return execute(run, args)


if __name__ == "__main__":
    raise SystemExit(main())
