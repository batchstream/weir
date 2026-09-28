"""Offline M25 boundaries. No kubectl, AWS, Docker or external service is called."""
import copy
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import eks_pacing as entry
from eks_pacing_report import HISTOGRAMS, IMAGES, node_gate, pacing, resources
from capacity_report_test import sample as old_sample, metrics as old_metrics


# M25R2 recorded Pod response shape; environment bindings replaced. The
# extra unrelated label proves acceptance is not a topology-label allowlist.
ADMITTED_POD = json.loads('''
{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "version-admission",
    "namespace": "weir-qual-m25-offline",
    "uid": "dry-uid",
    "labels": {
      "qualification.weir.io/owner": "weir-qual-m25-offline",
      "topology.kubernetes.io/region": "offline-region",
      "topology.kubernetes.io/zone": "offline-zone",
      "example.test/extra": "unrelated"
    },
    "ownerReferences": null
  },
  "spec": {
    "activeDeadlineSeconds": 100,
    "automountServiceAccountToken": false,
    "containers": [
      {
        "args": [
          "-version"
        ],
        "image": "ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10",
        "imagePullPolicy": "Always",
        "name": "probe",
        "resources": {
          "limits": {
            "cpu": "1",
            "memory": "512Mi"
          },
          "requests": {
            "cpu": "1",
            "memory": "512Mi"
          }
        },
        "securityContext": {
          "allowPrivilegeEscalation": false,
          "capabilities": {
            "drop": [
              "ALL"
            ]
          },
          "readOnlyRootFilesystem": true,
          "runAsGroup": 65532,
          "runAsNonRoot": true,
          "runAsUser": 65532,
          "seccompProfile": {
            "type": "RuntimeDefault"
          }
        },
        "terminationMessagePath": "/dev/termination-log",
        "terminationMessagePolicy": "File"
      }
    ],
    "dnsConfig": {
      "nameservers": [
        "127.0.0.1"
      ]
    },
    "dnsPolicy": "None",
    "enableServiceLinks": false,
    "nodeName": "node",
    "preemptionPolicy": "PreemptLowerPriority",
    "priority": 0,
    "restartPolicy": "Never",
    "schedulerName": "default-scheduler",
    "securityContext": {
      "runAsGroup": 65532,
      "runAsNonRoot": true,
      "runAsUser": 65532,
      "seccompProfile": {
        "type": "RuntimeDefault"
      }
    },
    "serviceAccount": "default",
    "serviceAccountName": "default",
    "terminationGracePeriodSeconds": 10,
    "tolerations": [
      {
        "effect": "NoExecute",
        "key": "node.kubernetes.io/not-ready",
        "operator": "Exists",
        "tolerationSeconds": 300
      },
      {
        "effect": "NoExecute",
        "key": "node.kubernetes.io/unreachable",
        "operator": "Exists",
        "tolerationSeconds": 300
      }
    ]
  },
  "status": {
    "phase": "Pending",
    "qosClass": "Guaranteed"
  }
}
''')


def sample(second):
    value = old_sample("client", second)
    value["files"].update({"pids.max": "max\n", "cpuset.cpus.effective": "0-7\n", "io.stat": "",
                           "limits": "Max open files            65536                65536                files\n",
                           "status": "Uid:\t65532\t65532\t65532\t65532\nGid:\t65532\t65532\t65532\t65532\nCpus_allowed_list:\t0-7\nNoNewPrivs:\t1\nSeccomp:\t2\nCapEff:\t0\nCapPrm:\t0\nCapBnd:\t0\nCapAmb:\t0\nVmRSS:\t32768 kB\nVmSwap:\t0 kB\n"})
    return value


def metrics(count):
    value = old_metrics(count)
    value.update(due=count, cancelled_future=0, worker_expired=0, drop_reasons={})
    value["arrival"]["max_ns"] = 1000000 if count else 0
    for name in HISTOGRAMS:
        value[name] = copy.deepcopy(value["arrival"])
    return value


def window(count):
    value = {k: metrics(n) for k, n in (("all", count), ("read", count*9//10), ("put", count//10))}
    return value


def raw_probe():
    trial = dict(options=dict(Rate=50, WarmSeconds=0, Seconds=20, Prefix="pace", Workers=64, TimingOnly=True, LegacyExpiry=False),
                 start=sample(0)["time"], end=sample(20)["time"], planned=1000,
                 warm=window(0), measure=window(1000), ten_second_windows=[window(500), window(500)])
    raw = dict(goos="linux", goarch="arm64", exe_sha256=IMAGES["tool"]["binary"], kernel="Linux version test",
               kind="native pacing diagnostic only", trial=trial, samples=[sample(i) for i in range(0, 21, 2)])
    return raw


def plan():
    value = dict(namespace="weir-qual-m25-offline", owner="weir-qual-m25-offline", node=dict(name="node", uid="node-uid"))
    return value


def node():
    value = dict(name="node", uid="node-uid", arch="arm64", os="linux", unschedulable=False, deleting=None, taints=[],
                 conditions=[dict(type=k, status="True" if k == "Ready" else "False") for k in ("Ready", "MemoryPressure", "DiskPressure", "PIDPressure")],
                 allocatable=dict(cpu="8", memory="16Gi", pods="30"))
    return value


def pod():
    template = entry.job_template(plan(), "pace-0")
    ref = dict(apiVersion="batch/v1", kind="Job", name="pace-0", uid="job-uid", controller=True)
    status = dict(name="probe", restartCount=0, lastState={}, containerID="containerd://test",
                  imageID=IMAGES["tool"]["reference"], state=dict(terminated=dict(exitCode=0, reason="Completed")))
    value = dict(metadata=dict(name="pace-0-test", namespace=plan()["namespace"], uid="pod-uid", labels={entry.LABEL: plan()["owner"]}, ownerReferences=[ref]),
                 spec=copy.deepcopy(template["spec"]["template"]["spec"]), status=dict(phase="Succeeded", containerStatuses=[status]))
    value["spec"].update(priority=0, preemptionPolicy="PreemptLowerPriority")
    return value


class PureBoundaries(unittest.TestCase):
    def test_context_node_capacity_fail_closed(self):
        valid = dict(context="arn:aws:eks:us-west-1:000000000000:cluster/offline", region="us-west-1", cluster="offline")
        entry.target_check(valid)
        for key in valid:
            value = dict(valid)
            value[key] = "wrong"
            with self.assertRaises(ValueError):
                entry.target_check(value)
        used = dict(cpu=6, memory=14*1024**3, pods=27)
        node_gate(node(), used, "node-uid")
        with self.assertRaises(ValueError):
            node_gate(node(), used, "replacement")
        for key in used:
            excessive = dict(used)
            excessive[key] = dict(cpu=7, memory=16*1024**3, pods=28)[key]
            with self.assertRaises(ValueError):
                node_gate(node(), excessive)
        for key, value in (("arch", "amd64"), ("unschedulable", True), ("taints", [dict(effect="NoSchedule")]), ("conditions", [])):
            broken = node()
            broken[key] = value
            with self.assertRaises(ValueError):
                node_gate(broken, used)

    def test_flat_resource_projection_is_rejected(self):
        with self.assertRaises(ValueError):
            entry.allocated("node\tRunning")

    def test_pod_and_job_injection_uid_image_runtime(self):
        options = dict(job=dict(name="pace-0", uid="job-uid"), template=entry.job_template(plan(), "pace-0"), pod_uid="pod-uid")
        entry.pod_check(pod(), options)
        mutations = [lambda p: p["spec"].update(hostNetwork=True),
                     lambda p: p["spec"].update(volumes=[dict(name="injected")]),
                     lambda p: p["spec"]["containers"].append(dict(name="sidecar")),
                     lambda p: p["spec"]["containers"][0].update(envFrom=[dict(secretRef=dict(name="unread"))]),
                     lambda p: p["metadata"].update(uid="changed"),
                     lambda p: p["metadata"]["ownerReferences"][0].update(uid="foreign"),
                     lambda p: p["status"]["containerStatuses"][0].update(imageID="sha256:wrong"),
                     lambda p: p["status"]["containerStatuses"][0].update(restartCount=1)]
        mutations += [lambda p: p["spec"].update(nodeName=""), lambda p: p["spec"].pop("nodeName"),
                      lambda p: p["spec"].update(nodeName="another-node"),
                      lambda p: p["spec"].update(preemptionPolicy="Never"),
                      lambda p: p["spec"].pop("preemptionPolicy"), lambda p: p["spec"].pop("priority"),
                      lambda p: p["spec"].update(priority=1), lambda p: p["spec"].update(priority=False),
                      lambda p: p["spec"].update(priorityClassName="injected")]
        for mutate in mutations:
            value = pod()
            mutate(value)
            with self.assertRaises(ValueError):
                entry.pod_check(value, options)
        template = entry.job_template(plan(), "pace-0")
        actual = copy.deepcopy(template)
        actual["spec"]["backoffLimit"] = 1
        with self.assertRaises(ValueError):
            entry.job_check(actual, template)

    def test_template_omission_is_distinct_from_pod_admission_defaults(self):
        frozen = plan()
        template = entry.job_template(frozen, "version")
        spec = template["spec"]["template"]["spec"]
        for field in ("priority", "priorityClassName", "preemptionPolicy"):
            self.assertNotIn(field, spec)
        entry.job_check(template, template)
        with self.assertRaises(ValueError):
            entry.admitted_spec(spec, spec, pod=True)
        for value in ("", None):
            frozen["node"]["name"] = value
            with self.assertRaises(ValueError):
                entry.job_template(frozen, "version")

    def test_resource_unknown_is_not_zero_or_docker_qualification(self):
        result = resources([sample(0), sample(2)])
        self.assertEqual(result["status"], "partial")
        self.assertIsNone(result["visible_pid_limit"])
        self.assertFalse(result["exclusive_cpu"])
        mutations = [lambda s: s.update(errors=["read denied"]), lambda s: s.update(gomaxprocs=2),
                     lambda s: s.update(duration_ns=2000000001), lambda s: s.update(rss_bytes=513*1024**2),
                     lambda s: s["files"].update({"memory.swap.max": "max"}),
                     lambda s: s["files"].update({"cpu.max": "max 100000"}),
                     lambda s: s["files"].update({"memory.events": "oom 1\n"})]
        for mutate in mutations:
            value = sample(0)
            mutate(value)
            with self.assertRaises((ValueError, KeyError)):
                resources([value])
        for key in sample(0)["files"]:
            value = sample(0)
            value["files"].pop(key)
            with self.assertRaises((ValueError, KeyError)):
                resources([value])
        for second in (0, 7):
            with self.assertRaises(ValueError):
                resources([sample(0), sample(second)])
        value = sample(2)
        value["files"]["cpu.stat"] = value["files"]["cpu.stat"].replace("nr_throttled 0", "nr_throttled 1")
        with self.assertRaises(ValueError):
            resources([sample(0), value])

    def test_counts_buckets_quantiles_due_cancel_and_coverage(self):
        self.assertTrue(pacing(raw_probe(), 50)["timing_pass"])
        mutations = [lambda r: r.update(exe_sha256="wrong"), lambda r: r.update(samples=[]),
                     lambda r: r["samples"].pop(), lambda r: r["samples"].pop(0),
                     lambda r: r["trial"]["options"].update(LegacyExpiry=True),
                     lambda r: r["trial"]["measure"]["all"].pop("due"),
                     lambda r: r["trial"]["measure"]["all"].update(cancelled_future=1),
                     lambda r: r["trial"]["measure"]["all"]["lag"].update(p99_us=1),
                     lambda r: r["trial"]["measure"]["read"]["construct"]["buckets"][0].update(count=899),
                     lambda r: r["trial"]["ten_second_windows"][0]["all"].update(planned=499)]
        for mutate in mutations:
            value = raw_probe()
            mutate(value)
            with self.assertRaises((ValueError, KeyError, RuntimeError)):
                pacing(value, 50)


class LocalBoundaries(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        target = dict(context="unused", region="unused", cluster="unused")
        self.run = entry.Run(self.root, target)
        self.run.plan = plan()

    def test_actual_go_templates_handle_unlabelled_defaults_and_redact_env(self):
        # Use the real standard-library template engine used by kubectl, with
        # synthetic JSON only. No CLI identity, API, cluster or module download.
        fixed = entry.REPO/".tools/go1.27.1"
        go = str(fixed/"bin/go") if (fixed/"bin/go").is_file() else shutil.which("go")
        self.assertIsNotNone(go, "Go compiler required for offline template regression")
        env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
        env.update(GOENV="off", GOTOOLCHAIN="local", GOWORK="off", GOPROXY="off", GOSUMDB="off", CGO_ENABLED="0")
        if (fixed/"bin/go").is_file():
            env["GOROOT"] = str(fixed)
        source = '''package main
import("encoding/json";"os";"text/template")
func main(){
 var payload struct{Template string; Object any}
 file,err:=os.Open(os.Args[1]); if err!=nil{panic(err)}; defer file.Close()
 if err=json.NewDecoder(file).Decode(&payload);err!=nil{panic(err)}
 tmpl,err:=template.New("offline").Parse(payload.Template);if err!=nil{panic(err)}
 if err=tmpl.Execute(os.Stdout,payload.Object);err!=nil{panic(err)}
}
'''
        driver = self.root/"template.go"
        driver.write_text(source)
        metadata = dict(items=[dict(apiVersion="v1", kind="ConfigMap", metadata=dict(name="kube-root-ca.crt", uid="root-uid"), data=dict(marker="not-for-output")),
                               dict(apiVersion="v1", kind="ServiceAccount", metadata=dict(name="default", uid="sa-uid"))])
        for name, labels in (("nil", None), ("empty", {}), ("owned", {entry.LABEL: "owner"})):
            meta = dict(name=name, uid=name+"-uid", labels=labels)
            obj = dict(apiVersion="v1", kind="ConfigMap", metadata=meta)
            metadata["items"].append(obj)
        value = dict(pod(), apiVersion="v1", kind="Pod")
        value["spec"]["containers"][0]["env"].append(dict(name="INJECTED", value="not-for-output"))
        ref = dict(apiVersion="batch/v1", kind="Job", namespace=plan()["namespace"], name="version", uid="job-uid")
        event_meta = dict(uid="event-uid", namespace=plan()["namespace"])
        event = dict(metadata=event_meta, involvedObject=ref, reason="FailedCreate", message="admission refused", count=2,
                     firstTimestamp="2026-09-28T00:00:00Z", lastTimestamp="2026-09-28T00:00:01Z")
        event_list = dict(items=[event])
        for template, obj in ((entry.META_TEMPLATE, metadata), (entry.OBJECT_TEMPLATE, value), (entry.EVENT_TEMPLATE, event_list)):
            filename = self.root/"input.json"
            filename.write_text(json.dumps(dict(Template=template, Object=obj)))
            process = subprocess.run([go, "run", str(driver), str(filename)], env=env, cwd=self.root,
                                     capture_output=True, text=True, timeout=60)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertNotIn("not-for-output", process.stdout)
            if template == entry.META_TEMPLATE:
                self.assertEqual(process.stdout, "v1|ConfigMap|kube-root-ca.crt|root-uid|||\nv1|ServiceAccount|default|sa-uid|||\n"
                                 "v1|ConfigMap|nil|nil-uid|||\nv1|ConfigMap|empty|empty-uid|||\nv1|ConfigMap|owned|owned-uid|owner||\n")
            elif template == entry.OBJECT_TEMPLATE:
                rendered = json.loads(process.stdout)
                self.assertEqual(rendered["spec"]["containers"][0]["env"][-1]["value"], "REDACTED")
            else:
                rendered = json.loads(process.stdout)
                self.assertEqual(rendered[0]["involvedObject"], ref)
                self.assertEqual(rendered[0]["reason"], "FailedCreate")
                self.assertEqual(rendered[0]["count"], 2)
                self.assertIsNone(rendered[0]["series"])

    def test_bounded_children_timeout_output_and_signal(self):
        cases = [("import time; time.sleep(10)", .05), ("import os; os.write(1,b'x'*(9<<20))", 5),
                 ("import os,signal; os.kill(os.getpid(),signal.SIGTERM)", 5)]
        for program, timeout in cases:
            with self.assertRaises(ValueError):
                self.run.run([sys.executable, "-c", program], timeout)
            record = json.loads((self.root/f"command-{self.run.number:04d}.json").read_text())
            self.assertIsNotNone(record["exit"])
        self.assertLessEqual((self.root/"command-0002.out").stat().st_size, entry.LIMIT)

    def test_parent_cancellation_reaps_active_child(self):
        def interrupt(signum, frame):
            raise KeyboardInterrupt("offline cancellation")
        previous = signal.signal(signal.SIGALRM, interrupt)
        try:
            signal.setitimer(signal.ITIMER_REAL, .08)
            with self.assertRaises(KeyboardInterrupt):
                self.run.run([sys.executable, "-c", "import time; time.sleep(30)"], 5)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        record = json.loads((self.root/"command-0001.json").read_text())
        self.assertIsNotNone(record["exit"])

    def test_evidence_collision_and_optimized_opt_in(self):
        with self.assertRaises(FileExistsError):
            self.root.mkdir()
        script = Path(entry.__file__)
        for args in (["--help"], ["--help"]):
            env = dict(os.environ)
            env.pop("WEIR_EKS_M25", None)
            result = subprocess.run([sys.executable, str(script), *args], env=env, capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
        env = dict(os.environ, WEIR_EKS_M25="1")
        result = subprocess.run([sys.executable, "-O", str(script), "--help"], env=env, capture_output=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)

    def test_delete_uid_precondition_and_foreign_never_deleted(self):
        owned = dict(kind="Job", name="pace-0", uid="job-uid", owner=plan()["owner"])
        obj = dict(metadata=dict(name="pace-0", uid="job-uid", labels={entry.LABEL: plan()["owner"]}))
        with patch.object(self.run, "selected_object", side_effect=[obj, None]), patch.object(self.run, "kube", return_value="") as kube:
            self.run.delete(owned)
            args = kube.call_args[0][0]
            self.assertIn("--raw", args)
            body = json.loads(Path(args[-1]).read_text())
            self.assertEqual(body["preconditions"], dict(uid="job-uid"))
            self.assertNotIn("gracePeriodSeconds", body)
        obj["metadata"]["uid"] = "foreign"
        with patch.object(self.run, "selected_object", return_value=obj), patch.object(self.run, "kube") as kube:
            with self.assertRaises(ValueError):
                self.run.delete(owned)
            kube.assert_not_called()

    def test_diagnostic_failure_still_cleans_independent_owned_objects(self):
        ns = dict(kind="Namespace", name=plan()["namespace"], uid="ns-uid", owner=plan()["owner"])
        job = dict(kind="Job", name="pace-0", uid="job-uid", owner=plan()["owner"])
        pod_entry = dict(kind="Pod", name="probe", uid="pod-uid", owner=plan()["owner"])
        self.run.namespace = ns
        self.run.owned = [ns, job, pod_entry]
        obj = dict(metadata=dict(name=ns["name"], uid=ns["uid"], labels={entry.LABEL: ns["owner"]}))
        with patch.object(self.run, "selected_object", return_value=obj), patch.object(self.run, "inventory", return_value=[]), \
                patch.object(self.run, "save", side_effect=OSError("diagnostic disk failure")), patch.object(self.run, "delete") as delete:
            result = self.run.cleanup()
            self.assertEqual(delete.call_count, 3)
            self.assertFalse(result["confirmed"])
        foreign = [["v1", "Pod", "foreign", "foreign-uid", "another-owner", "", ""]]
        with patch.object(self.run, "selected_object", return_value=obj), patch.object(self.run, "inventory", return_value=foreign), patch.object(self.run, "delete") as delete:
            result = self.run.cleanup()
            self.assertFalse(result["confirmed"])
            delete.assert_not_called()

    def test_wrong_plan_context_node_image_and_namespace_collision_before_create(self):
        frozen = dict(plan(), target=dict(context="expected"))
        self.run.save("plan.json", frozen)
        options = argparse_options(self.root)
        with self.assertRaises(ValueError):
            entry.execute(self.run, options)
        self.assertFalse((self.root/"invocation.json").exists())
        frozen["target"] = self.run.target
        self.run.save("plan.json", frozen)
        options.plan_sha256 = entry.digest(self.root/"plan.json")
        options.node_uid = "unconfirmed"
        with self.assertRaises(ValueError):
            entry.execute(self.run, options)
        options.node_uid = "node-uid"
        frozen.update(images={}, image_source="wrong", rates=[])
        self.run.save("plan.json", frozen)
        options.plan_sha256 = entry.digest(self.root/"plan.json")
        with self.assertRaises(ValueError):
            entry.execute(self.run, options)
        with patch.object(self.run, "kube", side_effect=ValueError("AlreadyExists")) as kube:
            obj = dict(apiVersion="v1", kind="Namespace", metadata=dict(name=plan()["namespace"]))
            with self.assertRaises(ValueError):
                self.run.create(obj)
            self.assertIsNone(self.run.namespace)
            self.assertEqual(self.run.owned, [])


def argparse_options(root):
    import argparse
    value = argparse.Namespace(plan_sha256=entry.digest(root/"plan.json"), node_uid="node-uid")
    return value


class AdmissionFlow(unittest.TestCase):
    """Run the real entry/child boundary with a local CLI, never cluster access."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        target = dict(context="offline-context", region="unused", cluster="unused")
        self.run = entry.Run(self.root, target)
        self.run.plan = plan()
        cli = self.root/"kubectl"
        cli.write_text("#!"+sys.executable+'''\nimport json, pathlib, sys
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
with (root/"calls.jsonl").open("a") as output:
    output.write(json.dumps(args)+"\\n")
assert args[:3] == ["--context", "offline-context", "--request-timeout=10s"]
args = args[3:]
if args[0] == "--namespace":
    assert args[1] == "weir-qual-m25-offline"
    args = args[2:]
cfg = json.loads((root/"scenario.json").read_text())
verb, kind = args[:2]
if verb == "create":
    obj = json.loads(pathlib.Path(args[args.index("-f")+1]).read_text())
    obj["metadata"]["uid"] = "job-uid" if obj["kind"] == "Job" else "dry-uid"
    if obj["kind"] == "Pod":
        assert "--dry-run=server" in args
        assert not obj["metadata"].get("ownerReferences")
        if cfg.get("reject_pod"):
            sys.stderr.write("synthetic Pod admission refused")
            sys.exit(1)
        obj = cfg["pod_response"]
        obj.update(cfg.get("object_updates", {}))
        obj["metadata"].update(cfg.get("metadata_updates", {}))
        obj["spec"].update(cfg.get("pod_updates", {}))
        if cfg.get("remove_node"):
            obj["spec"].pop("nodeName")
    if obj["kind"] == "Job" and "--dry-run=server" not in args:
        (root/"actual-job.json").write_text(json.dumps(obj))
    print(json.dumps(obj))
elif verb == "get" and kind == "nodes":
    print(json.dumps([cfg["node"]]))
elif verb == "get" and kind == "pods":
    if "--all-namespaces" in args:
        print("[]")
    elif cfg.get("actual_pod"):
        print(cfg["actual_pod"]["metadata"]["name"])
elif verb == "get" and kind == "Pod":
    if not (root/"pod-deleted").exists():
        print(json.dumps(cfg["actual_pod"]))
elif verb == "logs":
    response = dict(synthetic=True)
    print(json.dumps(response))
elif verb == "get" and kind == "events":
    assert "--field-selector=involvedObject.uid=job-uid" in args
    print(json.dumps(cfg.get("events", [])))
elif verb == "get" and kind == "Job":
    if not (root/"deleted").exists():
        obj = json.loads((root/"actual-job.json").read_text())
        obj["status"] = cfg.get("job_status", {})
        print(json.dumps(obj))
elif verb == "delete":
    assert kind == "--raw"
    body = json.loads(pathlib.Path(args[-1]).read_text())
    expected_uid = "job-uid" if args[2].endswith("/jobs/version") else cfg["actual_pod"]["metadata"]["uid"]
    assert body["preconditions"] == {"uid": expected_uid}
    if cfg.get("reject_delete"):
        sys.stderr.write("synthetic cleanup refused")
        sys.exit(1)
    marker = "deleted" if body["preconditions"]["uid"] == "job-uid" else "pod-deleted"
    (root/marker).touch()
else:
    raise AssertionError(args)
''')
        cli.chmod(0o700)
        environment = dict(PATH=str(self.root)+os.pathsep+os.environ["PATH"])
        patched = patch.dict(os.environ, environment)
        patched.start()
        self.addCleanup(patched.stop)
        self.configure()

    def configure(self, **fields):
        config = dict(node=node(), pod_response=ADMITTED_POD, **fields)
        (self.root/"scenario.json").write_text(json.dumps(config))

    def calls(self):
        filename = self.root/"calls.jsonl"
        result = [json.loads(line) for line in filename.read_text().splitlines()] if filename.exists() else []
        return result

    def failed_event(self):
        ref = dict(apiVersion="batch/v1", kind="Job", name="version", namespace=plan()["namespace"], uid="job-uid")
        event = dict(uid="event-uid", namespace=plan()["namespace"], involvedObject=ref, reason="FailedCreate",
                     message="synthetic controller admission refusal", count=3,
                     firstTimestamp="2026-09-28T00:00:00Z", lastTimestamp="2026-09-28T00:00:01Z")
        return event

    def test_pod_dry_run_failure_prevents_real_job_or_pod_creation(self):
        self.configure(reject_pod=True)
        with self.assertRaisesRegex(ValueError, "exit 1"):
            self.run.run_job("version")
        creates = [args for args in self.calls() if "create" in args]
        self.assertEqual(len(creates), 2)
        self.assertTrue(all("--dry-run=server" in args for args in creates))
        self.assertFalse((self.root/"actual-job.json").exists())
        self.assertEqual(self.run.owned, [])
        request = json.loads((self.root/"dry-run-Pod-version.json").read_text())
        template = entry.job_template(plan(), "version")
        self.assertEqual(request["spec"], template["spec"]["template"]["spec"])
        self.assertNotIn("ownerReferences", request["metadata"])

    def test_unexpected_pod_defaults_and_binding_stop_before_job_creation(self):
        template = entry.job_template(plan(), "version")
        container = template["spec"]["template"]["spec"]["containers"][0]
        sidecar = dict(name="sidecar")
        volume = dict(name="injected")
        secret_ref = dict(name="unread")
        env_from = dict(secretRef=secret_ref)
        injected = dict(container, envFrom=[env_from])
        wrong_image = dict(container, image="foreign")
        wrong_resources = copy.deepcopy(container)
        wrong_resources["resources"]["limits"]["cpu"] = "2"
        changes = [dict(nodeName=""), dict(nodeName="other"), dict(preemptionPolicy="Never"), dict(priority=10),
                   dict(priorityClassName="injected"), dict(containers=[container, sidecar]), dict(volumes=[volume]),
                   dict(containers=[injected]), dict(hostNetwork=True), dict(hostPID=True), dict(hostIPC=True),
                   dict(dnsPolicy="ClusterFirst"), dict(containers=[wrong_image]), dict(containers=[wrong_resources]),
                   dict(securityContext={}), dict(unknownField=True)]
        for changed in changes:
            with self.subTest(changed=changed):
                self.configure(pod_updates=changed)
                with self.assertRaises(ValueError):
                    self.run.create(template)
                self.assertFalse((self.root/"actual-job.json").exists())
        self.configure(remove_node=True)
        with self.assertRaises(ValueError):
            self.run.create(template)
        self.assertTrue(all("--dry-run=server" in args for args in self.calls() if "create" in args))

    def test_recorded_extra_labels_reach_simulated_create_and_cleanup(self):
        event = self.failed_event()
        self.configure(events=[event])
        with self.assertRaisesRegex(ValueError, "Job FailedCreate"):
            self.run.run_job("version")
        self.assertTrue((self.root/"actual-job.json").exists())
        self.assertTrue((self.root/"deleted").exists())
        admitted = json.loads((self.root/"admitted-Pod-version.json").read_text())
        self.assertEqual(admitted, ADMITTED_POD)
        self.assertEqual(sum("create" in args and "--dry-run=server" not in args for args in self.calls()), 1)
        self.assertTrue(all("end" in json.loads(p.read_text()) for p in self.root.glob("command-*.json")))

    def test_dry_run_identity_changes_rejected_before_simulated_create(self):
        template = entry.job_template(plan(), "version")
        labels = ADMITTED_POD["metadata"]["labels"]
        missing = {k: v for k, v in labels.items() if k != entry.LABEL}
        overwritten = dict(labels)
        overwritten[entry.LABEL] = "foreign"
        ref = dict(apiVersion="batch/v1", kind="Job", name="version", uid="fabricated", controller=True)
        changes = [dict(labels=missing), dict(labels=overwritten), dict(labels=None), dict(labels=[]),
                   dict(name="another"), dict(namespace="another"), dict(ownerReferences=[ref])]
        for changed in changes:
            with self.subTest(changed=changed):
                self.configure(metadata_updates=changed)
                with self.assertRaisesRegex(ValueError, "metadata drift"):
                    self.run.create(template)
        for changed in (dict(apiVersion="other/v1"), dict(kind="Job")):
            self.configure(object_updates=changed)
            with self.assertRaisesRegex(ValueError, "kind drift"):
                self.run.create(template)
        self.assertFalse((self.root/"actual-job.json").exists())
        self.assertEqual(self.run.owned, [])
        self.assertTrue(all("--dry-run=server" in args for args in self.calls() if "create" in args))

    def test_every_requested_fixture_label_is_preserved(self):
        template = entry.job_template(plan(), "version")
        template["spec"]["template"]["metadata"]["labels"]["example.test/selector"] = "fixture"
        response = copy.deepcopy(ADMITTED_POD)
        response["metadata"]["labels"]["example.test/selector"] = "fixture"
        self.configure(metadata_updates=response["metadata"])
        self.run.pod_dry_run(template)
        for value in ("changed", None):
            response["metadata"]["labels"]["example.test/selector"] = value
            self.configure(metadata_updates=response["metadata"])
            with self.assertRaisesRegex(ValueError, "metadata drift"):
                self.run.pod_dry_run(template)
        self.assertFalse((self.root/"actual-job.json").exists())

    def test_actual_pod_extra_labels_preserve_controller_and_uid_boundaries(self):
        template = entry.job_template(plan(), "version")
        actual = pod()
        actual["metadata"].update(name="version-test", labels=copy.deepcopy(ADMITTED_POD["metadata"]["labels"]))
        actual["metadata"]["ownerReferences"][0]["name"] = "version"
        actual["spec"] = copy.deepcopy(ADMITTED_POD["spec"])
        actual["status"]["containerStatuses"][0]["imageID"] = IMAGES["version"]["reference"]
        self.configure(actual_pod=actual)
        expected = dict(synthetic=True)
        self.assertEqual(self.run.run_job("version"), expected)
        self.assertEqual([e["kind"] for e in self.run.owned], ["Job", "Pod"])
        self.assertTrue((self.root/"pod-deleted").exists())
        options = dict(job=self.run.owned[0], template=template, pod_uid="replacement-uid")
        with self.assertRaisesRegex(ValueError, "Pod UID drift"):
            entry.pod_check(actual, options)
        mutations = [lambda p: p["metadata"]["labels"].__setitem__(entry.LABEL, "foreign"),
                     lambda p: p["metadata"]["ownerReferences"][0].update(uid="foreign"),
                     lambda p: p["metadata"]["ownerReferences"][0].update(controller=False),
                     lambda p: p["metadata"].update(ownerReferences=[])]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                (self.root/"deleted").unlink()
                (self.root/"pod-deleted").unlink(missing_ok=True)
                self.run.owned = []
                foreign = copy.deepcopy(actual)
                mutate(foreign)
                self.configure(actual_pod=foreign)
                before = len(self.calls())
                with self.assertRaises(ValueError):
                    self.run.run_job("version")
                self.assertEqual([e["kind"] for e in self.run.owned], ["Job"])
                deletes = [a for a in self.calls()[before:] if "delete" in a]
                self.assertEqual(len(deletes), 1)
                self.assertIn("/jobs/version", deletes[0][-3])
                self.assertFalse((self.root/"pod-deleted").exists())

    def test_changed_request_node_never_reaches_cli(self):
        for value in ("", "another-node", None):
            template = entry.job_template(plan(), "version")
            template["spec"]["template"]["spec"]["nodeName"] = value
            with self.assertRaisesRegex(ValueError, "template drift"):
                self.run.create(template)
        self.assertEqual(self.calls(), [])

    def test_first_failed_create_stops_and_reclaims_exact_job(self):
        event = self.failed_event()
        self.configure(events=[event])
        started = time.monotonic()
        with self.assertRaisesRegex(ValueError, "Job FailedCreate"):
            self.run.run_job("version")
        self.assertLess(time.monotonic()-started, 10)
        calls = self.calls()
        self.assertEqual(sum("events" in args for args in calls), 1)
        self.assertEqual(sum("create" in args and "--dry-run=server" not in args for args in calls), 1)
        self.assertTrue((self.root/"deleted").exists())
        evidence = json.loads((self.root/"version-failed-create.json").read_text())
        self.assertEqual(evidence["events"], [event])

    def test_foreign_event_uid_is_rejected_and_cleanup_error_preserves_original(self):
        event = self.failed_event()
        event["involvedObject"]["uid"] = "another-job"
        self.configure(events=[event])
        with self.assertRaisesRegex(ValueError, "Event UID/identity drift"):
            self.run.run_job("version")
        self.assertFalse((self.root/"version-failed-create.json").exists())
        # A fresh local fixture state, not an application retry.
        (self.root/"deleted").unlink()
        event = self.failed_event()
        self.configure(events=[event], reject_delete=True)
        with self.assertRaisesRegex(ValueError, "Job FailedCreate"):
            self.run.run_job("version")
        errors = json.loads((self.root/"version-cleanup-errors.json").read_text())
        self.assertIn("exit 1", errors[0])

    def test_no_event_keeps_job_deadline_failure_fallback(self):
        condition = dict(type="Failed", status="True", reason="DeadlineExceeded")
        status = dict(conditions=[condition])
        self.configure(job_status=status)
        with self.assertRaisesRegex(ValueError, "Job controller failure"):
            self.run.run_job("version")
        self.assertTrue((self.root/"deleted").exists())


if __name__ == "__main__":
    unittest.main()
