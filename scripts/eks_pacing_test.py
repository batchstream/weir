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
from eks_loopback_cli_fixture import resource_name
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
    value = dict(name="node", uid="node-uid", kernel="offline", kubelet="offline", arch="arm64", os="linux", unschedulable=False, deleting=None, taints=[],
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
        flag = dict(name="AWS_EC2_METADATA_DISABLED", value="true")
        value["spec"]["containers"][0]["env"].append(flag)
        value["spec"]["containers"][0]["env"].append(dict(name="INJECTED", value="not-for-output"))
        init_env = [flag, dict(name="INJECTED", value="not-for-output")]
        value["spec"]["initContainers"] = [dict(name="synthetic-es", env=init_env)]
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
                self.assertEqual(process.stdout, "v1|ConfigMap|kube-root-ca.crt|root-uid||||<no value>\nv1|ServiceAccount|default|sa-uid||||<no value>\n"
                                 "v1|ConfigMap|nil|nil-uid||||<no value>\nv1|ConfigMap|empty|empty-uid||||<no value>\nv1|ConfigMap|owned|owned-uid|owner|||<no value>\n")
            elif template == entry.OBJECT_TEMPLATE:
                rendered = json.loads(process.stdout)
                self.assertEqual(rendered["spec"]["containers"][0]["env"][-1]["value"], "REDACTED")
                self.assertEqual(rendered["spec"]["containers"][0]["env"][-2], flag)
                expected_env = [flag, dict(name="INJECTED", value="REDACTED")]
                self.assertEqual(rendered["spec"]["initContainers"][0]["env"], expected_env)
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

    def test_diagnostic_failure_or_foreign_object_stops_cleanup(self):
        ns = dict(kind="Namespace", name=plan()["namespace"], uid="ns-uid", owner=plan()["owner"])
        job = dict(kind="Job", name="pace-0", uid="job-uid", owner=plan()["owner"])
        pod_entry = dict(kind="Pod", name="probe", uid="pod-uid", owner=plan()["owner"])
        self.run.namespace = ns
        self.run.owned = [ns, job, pod_entry]
        obj = dict(metadata=dict(name=ns["name"], uid=ns["uid"], labels={entry.LABEL: ns["owner"]}))
        with patch.object(self.run, "selected_object", return_value=obj), patch.object(self.run, "inventory", return_value=[]), \
                patch.object(self.run, "save", side_effect=OSError("diagnostic disk failure")), patch.object(self.run, "delete") as delete:
            result = self.run.cleanup()
            delete.assert_not_called()
            self.assertFalse(result["confirmed"])
        foreign = [["v1", "Pod", "foreign", "foreign-uid", "another-owner", "", ""]]
        with patch.object(self.run, "selected_object", return_value=obj), patch.object(self.run, "inventory", return_value=foreign), patch.object(self.run, "delete") as delete:
            result = self.run.cleanup()
            self.assertFalse(result["confirmed"])
            self.assertEqual([c.args[0]["kind"] for c in delete.call_args_list], ["Job", "Pod"])

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
elif verb == "get" and kind == "node":
    if args[2] != cfg["node"]["name"]: raise ValueError("node scope")
    print(json.dumps(cfg["node"]))
elif verb == "get" and kind == "pods":
    if "--all-namespaces" in args:
        if "--field-selector=spec.nodeName="+cfg["node"]["name"] not in args or "--chunk-size=0" not in args: raise ValueError("Pod scope")
        response = dict(kind="List",apiVersion="v1",itemsType="[]interface {}",remainingItemCount=None,items=[],**{"continue":None})
        print(json.dumps(response))
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


class CleanupReplay(unittest.TestCase):
    """Replay the complete retained M26R5 metadata and actual API discovery."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.fixture = json.loads((entry.REPO/"scripts/fixtures/eks-cleanup-m26r5.json").read_text())
        self.run = entry.Run(self.root, self.fixture["target"])
        self.run.plan = dict(namespace=self.fixture["namespace"], owner=self.fixture["namespace"])
        self.run.owned = copy.deepcopy(self.fixture["owned"])
        self.run.namespace = self.run.owned[0]
        self.run.defaults = {(e["kind"], e["name"]): e["uid"] for e in self.fixture["defaults"]}
        objects = {}
        for e in self.run.owned+self.fixture["defaults"]:
            version = entry.KINDS[e["kind"]][0] if e["kind"] in entry.KINDS else "v1"
            labels = {entry.LABEL: e["owner"]} if "owner" in e else None
            meta = dict(name=e["name"], uid=e["uid"], namespace=self.fixture["namespace"], labels=labels)
            obj = dict(apiVersion=version, kind=e["kind"], metadata=meta)
            if e["kind"] == "ResourceQuota":
                obj.update(spec=dict(hard={"count/secrets": "0"}),
                           status=dict(hard={"count/secrets": "0"}, used={"count/secrets": "0"}))
            if e["kind"] == "Job":
                obj["status"] = dict(failed=1, conditions=[dict(type="Failed", status="True")])
            objects[e["kind"]+"/"+e["name"]] = obj
        rows = [row+[self.fixture["namespace"]] for row in self.fixture["rows"]]
        self.cfg = dict(namespace=self.fixture["namespace"], cluster=dict(arn=self.fixture["target"]["context"]),
                        label=entry.LABEL, cleanup_objects=objects, cleanup_rows=rows,
                        discovery=self.fixture["discovery"], api_resources=self.fixture["api_resources"],
                        plurals={kind: value[1] for kind, value in entry.KINDS.items()})
        cli = self.root/"kubectl"
        source = (entry.REPO/"scripts/eks_loopback_cli_fixture.py").read_text()
        cli.write_text("#!"+sys.executable+"\n"+source)
        cli.chmod(0o700)
        (self.root/"git").symlink_to(cli)
        environment = dict(PATH=str(self.root)+os.pathsep+os.environ["PATH"])
        patched = patch.dict(os.environ, environment)
        patched.start()
        self.addCleanup(patched.stop)
        self.configure()

    def configure(self):
        (self.root/"scenario.json").write_text(json.dumps(self.cfg))

    def calls(self):
        p = self.root/"calls.jsonl"
        return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []

    def test_real_inventory_old_rejection_new_classification_and_uid_cleanup(self):
        # The old foreign_check receives the unclassified rows and rejects them.
        with self.assertRaisesRegex(ValueError, "UID missing"):
            self.run.foreign_check(self.fixture["rows"])
        owned_before = copy.deepcopy(self.run.owned)
        rows = self.run.inventory()
        self.assertCountEqual(rows, [r for r in self.fixture["rows"] if r[1] != "PodMetrics"])
        self.run.foreign_check(rows)
        self.assertEqual(self.run.owned, owned_before)
        classified = json.loads(next(self.root.glob("inventory-classified-*.json")).read_text())
        self.assertEqual(len(classified["readonly_pod_metrics"]), 1)
        result = self.run.cleanup()
        self.assertTrue(result["confirmed"], result)
        deleted = [r["resource"]["kind"] for r in result["resources"]]
        self.assertEqual(deleted[-2:], ["ResourceQuota", "Namespace"])
        self.assertEqual(len(deleted), 6)
        for call in self.calls():
            if "get" in call:
                self.assertNotIn("secrets", call[call.index("get")+1].split(","))
            if "delete" in call:
                body = json.loads(Path(call[-1]).read_text())
                self.assertIn(body["preconditions"]["uid"], {e["uid"] for e in owned_before})
                self.assertNotIn("metrics.k8s.io", call[call.index("--raw")+1])

    def assert_scoped_deletes(self):
        deletes = [c for c in self.calls() if 'delete' in c]
        safe = {e['uid'] for e in self.run.owned if e['kind'] not in ('ResourceQuota','Namespace')}
        self.assertTrue(deletes)
        self.assertTrue(all(json.loads(Path(c[-1]).read_text())['preconditions']['uid'] in safe for c in deletes))
        state = json.loads((self.root/'state.json').read_text())
        self.assertIn('ResourceQuota/budget',state)
        self.assertIn('Namespace/'+self.fixture['namespace'],state)
        self.assertFalse(any(k.startswith('Pod/') or k.startswith('Job/') for k in state))

    def test_one_fresh_inventory_after_owned_processes_before_quota(self):
        result=self.run.cleanup()
        self.assertTrue(result['confirmed'],result)
        calls=self.calls()
        inventories=[i for i,c in enumerate(calls) if 'api-resources' in c]
        self.assertEqual(len(inventories),1)
        deletes=[(i,json.loads(Path(c[-1]).read_text())['preconditions']['uid']) for i,c in enumerate(calls) if 'delete' in c]
        kinds={e['uid']:e['kind'] for e in self.run.owned}
        self.assertEqual([kinds[uid] for _,uid in deletes],['Job','Pod','ConfigMap','NetworkPolicy','ResourceQuota','Namespace'])
        self.assertLess(deletes[3][0],inventories[0]);self.assertLess(inventories[0],deletes[4][0])
        budget=json.loads((self.root/'cleanup-budget.json').read_text())
        self.assertEqual(budget['objects_deadline'],budget['deadline']-45)
        self.assertEqual(budget['namespace_reserve_seconds'],45)
        self.assertLessEqual(budget['deadline']-time.monotonic(),180)

    def test_each_fresh_uid_or_owner_change_prevents_that_delete(self):
        original=copy.deepcopy(self.cfg)
        for owned in self.run.owned:
            for field in ('uid','owner'):
                with self.subTest(kind=owned['kind'],field=field):
                    self.cfg=copy.deepcopy(original)
                    obj=self.cfg['cleanup_objects'][owned['kind']+'/'+owned['name']]
                    if field=='uid':obj['metadata']['uid']='replacement'
                    else:obj['metadata']['labels'][entry.LABEL]='foreign'
                    (self.root/'state.json').write_text(json.dumps(self.cfg['cleanup_objects']))
                    before=len(self.calls());self.configure()
                    self.assertFalse(self.run.cleanup()['confirmed'])
                    deletes=[c for c in self.calls()[before:] if 'delete' in c]
                    self.assertFalse(any(json.loads(Path(c[-1]).read_text())['preconditions']['uid']==owned['uid'] for c in deletes))

    def test_final_namespace_nonzero_empty_get_is_not_absence(self):
        self.cfg['namespace_readback_failure']=True
        self.configure()
        result=self.run.cleanup()
        self.assertFalse(result['confirmed'])
        last=json.loads(sorted(self.root.glob('command-*.json'))[-1].read_text())
        self.assertNotEqual(last['exit'],0)
        self.assertEqual(sorted(self.root.glob('command-*.out'))[-1].read_text(),'')
        self.assertEqual(sum('delete' in c and any(a.endswith('/namespaces/'+self.fixture['namespace']) for a in c) for c in self.calls()),1)

    def test_unknown_api_identity_and_foreign_rows_keep_quota_and_namespace(self):
        original = copy.deepcopy(self.cfg["cleanup_rows"])
        metrics = next(i for i, r in enumerate(original) if r[1] == "PodMetrics")
        pod_index = next(i for i, r in enumerate(original) if r[1] == "Pod")
        default_index = next(i for i, r in enumerate(original) if r[2] == "default")
        cases = [(metrics, 0, "metrics.k8s.io/v9"), (metrics, 0, "unknown.test/v1"),
                 (metrics, 1, "UnknownView"), (metrics, 3, "unexpected-uid"),
                 (metrics, 7, "foreign-namespace"), (default_index, 3, "replacement-default"),
                 (default_index, 4, self.fixture["namespace"])]
        for index, field, value in cases:
            with self.subTest(index=index, field=field, value=value):
                self.cfg["cleanup_rows"] = copy.deepcopy(original)
                self.cfg["cleanup_rows"][index][field] = value
                resource = resource_name(self.cfg["cleanup_rows"][index])
                if resource not in self.cfg["api_resources"]:
                    self.cfg["api_resources"].append(resource)
                self.configure()
                (self.root/"state.json").write_text(json.dumps(self.cfg["cleanup_objects"]))
                result = self.run.cleanup()
                self.assertFalse(result["confirmed"], result)
                self.assert_scoped_deletes()
        self.cfg["cleanup_rows"] = original+[original[pod_index][:]]
        self.cfg["cleanup_rows"][-1][2:4] = ["foreign", "foreign-uid"]
        self.configure()
        self.assertFalse(self.run.cleanup()["confirmed"])
        self.assert_scoped_deletes()

    def test_discovery_mutation_missing_failure_and_secret_count_fail_closed(self):
        original = copy.deepcopy(self.cfg)
        for scenario in ("mutation", "missing", "failure", "secret", "secret-missing", "secret-hard", "quota-owner", "discovery-identity"):
            with self.subTest(scenario=scenario):
                self.cfg = copy.deepcopy(original)
                if scenario == "mutation":
                    self.cfg["discovery"]["resources"][1]["verbs"].append("delete")
                elif scenario == "missing":
                    self.cfg["discovery"]["resources"] = []
                elif scenario == "failure":
                    self.cfg["discovery_failure"] = True
                elif scenario == "discovery-identity":
                    self.cfg["discovery"]["groupVersion"] = "metrics.k8s.io/v9"
                elif scenario == "secret":
                    self.cfg["cleanup_objects"]["ResourceQuota/budget"]["status"]["used"]["count/secrets"] = "1"
                elif scenario == "secret-missing":
                    self.cfg["cleanup_objects"]["ResourceQuota/budget"]["status"]["used"] = {}
                elif scenario == "secret-hard":
                    self.cfg["cleanup_objects"]["ResourceQuota/budget"]["status"]["hard"]["count/secrets"] = "1"
                else:
                    self.cfg["cleanup_objects"]["ResourceQuota/budget"]["metadata"]["labels"] = None
                self.configure()
                # The external CLI normally persists its simulated server state.
                (self.root/"state.json").write_text(json.dumps(self.cfg["cleanup_objects"]))
                result = self.run.cleanup()
                self.assertFalse(result["confirmed"], result)
                self.assert_scoped_deletes()

    def test_final_inventory_keeps_quota_and_namespace_on_new_foreign_object(self):
        row = ["v1", "Pod", "foreign", "foreign-uid", self.fixture["namespace"], "", "", self.fixture["namespace"]]
        self.cfg["foreign_after_delete"] = row
        self.configure()
        result = self.run.cleanup()
        self.assertFalse(result["confirmed"])
        state = json.loads((self.root/"state.json").read_text())
        self.assertIn("ResourceQuota/budget", state)
        self.assertIn("Namespace/"+self.fixture["namespace"], state)

    def test_ambiguous_delete_is_read_back_without_replaying(self):
        self.cfg["ambiguous_delete"] = True
        self.configure()
        job = next(e for e in self.run.owned if e["kind"] == "Job")
        self.run.delete(job)
        deletes = [c for c in self.calls() if "delete" in c]
        self.assertEqual(len(deletes), 1)
        receipt = json.loads((self.root/("delete-"+job["uid"]+"-readback.json")).read_text())
        self.assertIsNone(receipt["current"])

    def test_deadline_and_evidence_failure_prevent_deletion_and_reap_cli(self):
        self.cfg["discovery_timeout"] = True
        self.configure()
        result = self.run.cleanup(deadline=time.monotonic()+55)
        self.assertFalse(result["confirmed"])
        self.assert_scoped_deletes()
        self.assertGreater(self.run.deadline, time.monotonic()-4)
        records = [json.loads(p.read_text()) for p in self.root.glob("command-*.json")]
        self.assertTrue(all("end" in r and r["exit"] is not None for r in records))
        self.cfg.pop("discovery_timeout")
        self.configure()
        with patch.object(self.run, "save", side_effect=OSError("disk unavailable")):
            result = self.run.cleanup()
        self.assertFalse(result["confirmed"])
        self.assert_scoped_deletes()

    def stopped_plan(self, target_count):
        original = self.root/"original-owned.json"
        original.write_text(json.dumps(self.fixture["owned"]))
        stopped_kinds = {"Pod"} if target_count == 5 else {"Pod", "Job", "ConfigMap", "NetworkPolicy"}
        stopped = [e for e in self.run.owned if e["kind"] in stopped_kinds]
        self.run.owned = [e for e in self.run.owned if e not in stopped]
        for e in stopped:
            del self.cfg["cleanup_objects"][e["kind"]+"/"+e["name"]]
        stopped_uids = {e["uid"] for e in stopped}
        self.cfg["cleanup_rows"] = [r for r in self.cfg["cleanup_rows"] if r[3] not in stopped_uids and r[1] != "PodMetrics"]
        inputs = {str(original): entry.digest(original)}
        frozen = dict(self.run.plan, target=self.fixture["target"], seconds=180, inputs=inputs,
                      implementation="synthetic-implementation", owned=self.run.owned, stopped=stopped,
                      defaults=self.fixture["defaults"], original_owned=str(original))
        return frozen

    def execute_stopped(self, frozen):
        import eks_cleanup
        self.configure()
        (self.root/"plan.json").write_text(json.dumps(frozen))
        (self.root/"plan.json").chmod(0o400)
        plan_sha = entry.digest(self.root/"plan.json")
        with patch("eks_pacing.stop_group", wraps=entry.stop_group) as stop:
            result = eks_cleanup.execute(self.root, plan_sha)
        records = [json.loads(p.read_text()) for p in self.root.glob("command-*.json")]
        self.assertEqual(len(records), stop.call_count)
        self.assertTrue(all("end" in r and r["exit"] is not None for r in records))
        self.assertTrue(all(c.args[0].returncode is not None for c in stop.call_args_list))
        return result

    def test_stopped_cleanup_entry_uses_only_five_uids_and_one_invocation(self):
        import eks_cleanup
        frozen = self.stopped_plan(5)
        pod = frozen["stopped"][0]
        result = self.execute_stopped(frozen)
        self.assertTrue(result["confirmed"], result)
        self.assertTrue(result["namespace_absent"] and result["job_absent"] and result["pod_absent"])
        deletes = [c for c in self.calls() if "delete" in c]
        self.assertEqual(len(deletes), 5)
        self.assertTrue(all(json.loads(Path(c[-1]).read_text())["preconditions"]["uid"] != pod["uid"] for c in deletes))
        with self.assertRaises((FileExistsError, ValueError)):
            eks_cleanup.execute(self.root, entry.digest(self.root/"plan.json"))
        self.assertEqual(len([c for c in self.calls() if "delete" in c]), 5)

    def test_two_residuals_preserve_history_events_and_delete_only_two_once(self):
        import eks_cleanup
        frozen = self.stopped_plan(2)
        events = [r for r in self.cfg["cleanup_rows"] if r[1] == "Event"]
        self.assertEqual({r[6] for r in events}, {e["uid"] for e in frozen["stopped"] if e["kind"] in ("Pod", "Job")})
        result = self.execute_stopped(frozen)
        self.assertTrue(result["confirmed"], result)
        self.assertTrue(result["namespace_absent"] and result["job_absent"] and result["pod_absent"])
        deletes = [c for c in self.calls() if "delete" in c]
        self.assertEqual([json.loads(Path(c[-1]).read_text())["preconditions"]["uid"] for c in deletes],
                         [e["uid"] for kind in ("ResourceQuota", "Namespace") for e in frozen["owned"] if e["kind"] == kind])
        self.assertEqual(sum("api-resources" in c for c in self.calls()), 1)
        self.assertEqual(sum("go-template="+entry.META_TEMPLATE in c for c in self.calls()), 13)
        before = self.calls()
        with self.assertRaisesRegex(ValueError, "already invoked"):
            eks_cleanup.execute(self.root, entry.digest(self.root/"plan.json"))
        self.assertEqual(self.calls(), before)
        last = json.loads(sorted(self.root.glob("command-*.json"))[-1].read_text())
        self.assertIn("Namespace", last["argv"])
        self.assertEqual(last["exit"], 0)
        self.assertEqual(sorted(self.root.glob("command-*.out"))[-1].read_text(), "")

    def test_two_residuals_reject_foreign_and_reappearing_objects(self):
        scenarios = ("event", "persistent", "default", "root-ca", "Pod", "Job", "ConfigMap", "NetworkPolicy")
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                self.setUp()
                frozen = self.stopped_plan(2)
                if scenario == "event":
                    next(r for r in self.cfg["cleanup_rows"] if r[1] == "Event")[6] = "foreign-uid"
                elif scenario in ("default", "root-ca"):
                    name = "default" if scenario == "default" else "kube-root-ca.crt"
                    next(r for r in self.cfg["cleanup_rows"] if r[2] == name)[3] = "replacement-default"
                else:
                    historical = next((e for e in frozen["stopped"] if e["kind"] == scenario), None)
                    if historical:
                        version = entry.KINDS[scenario][0]
                        row = [version, scenario, historical["name"], historical["uid"], frozen["owner"], "", "", frozen["namespace"]]
                    else:
                        row = ["unknown.test/v1", "UnknownPersistent", "foreign", "new-uid", frozen["owner"], "", "", frozen["namespace"]]
                    self.cfg["cleanup_rows"].append(row)
                    resource = resource_name(row)
                    if resource not in self.cfg["api_resources"]:
                        self.cfg["api_resources"].append(resource)
                    if scenario == "Pod":
                        meta = dict(name=historical["name"], uid=historical["uid"])
                        self.cfg["cleanup_objects"]["Pod/"+historical["name"]] = dict(metadata=meta)
                result = self.execute_stopped(frozen)
                self.assertFalse(result["confirmed"], result)
                self.assertFalse(any("delete" in c for c in self.calls()))

    def test_two_residuals_reject_identity_secret_and_incomplete_inventory(self):
        scenarios = ("namespace-uid", "namespace-owner", "quota-uid", "quota-owner", "quota-absent",
                     "secret-spec", "secret-hard", "secret-used", "secret-unknown", "inventory-failed", "inventory-malformed")
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                self.setUp()
                frozen = self.stopped_plan(2)
                objects = self.cfg["cleanup_objects"]
                quota = objects["ResourceQuota/budget"]
                if scenario.startswith(("namespace-", "quota-")):
                    obj = objects["Namespace/"+frozen["namespace"]] if scenario.startswith("namespace-") else quota
                    if scenario.endswith("uid"):
                        obj["metadata"]["uid"] = "same-name-new-uid"
                    elif scenario.endswith("owner"):
                        obj["metadata"]["labels"][entry.LABEL] = "other-owner"
                    else:
                        del objects["ResourceQuota/budget"]
                elif scenario == "secret-spec":
                    quota["spec"]["hard"]["count/secrets"] = "1"
                elif scenario == "secret-hard":
                    quota["status"]["hard"]["count/secrets"] = "1"
                elif scenario == "secret-used":
                    quota["status"]["used"]["count/secrets"] = "1"
                elif scenario == "secret-unknown":
                    quota["status"]["used"] = {}
                elif scenario == "inventory-failed":
                    self.cfg["inventory_failure_batch"] = "elasticmapsservers.maps.k8s.elastic.co"
                else:
                    self.cfg["cleanup_rows"][0] = self.cfg["cleanup_rows"][0][:-1]
                result = self.execute_stopped(frozen)
                self.assertFalse(result["confirmed"], result)
                self.assertFalse(any("delete" in c for c in self.calls()))

    def test_two_residuals_timeout_and_disk_failure_stop_without_mutation(self):
        frozen = self.stopped_plan(2)
        self.cfg["discovery_timeout"] = True
        original_run = entry.Run.run
        def short_discovery(run, argv, timeout=25, *, monitor=False):
            return original_run(run, argv, .2 if "/apis/metrics.k8s.io/v1beta1" in argv else timeout, monitor=monitor)
        with patch.object(entry.Run, "run", short_discovery):
            result = self.execute_stopped(frozen)
        self.assertFalse(result["confirmed"])
        self.assertFalse(any("delete" in c for c in self.calls()))
        self.assertTrue(any(json.loads(p.read_text())["exit"] == -15 for p in self.root.glob("command-*.json")))
        self.setUp()
        frozen = self.stopped_plan(2)
        original_save = entry.Run.save
        def failed_inventory_save(run, name, data):
            if name == "cleanup-final-inventory.json":
                raise OSError("disk unavailable")
            return original_save(run, name, data)
        with patch.object(entry.Run, "save", failed_inventory_save):
            result = self.execute_stopped(frozen)
        self.assertFalse(result["confirmed"])
        self.assertFalse(any("delete" in c for c in self.calls()))

    def test_two_residuals_absent_namespace_stops_after_first_get(self):
        frozen = self.stopped_plan(2)
        self.cfg["cleanup_objects"] = {}
        result = self.execute_stopped(frozen)
        self.assertTrue(result["confirmed"] and result["already_absent"])
        self.assertEqual(len([c for c in self.calls() if c[0] == "kubectl"]), 1)

    def test_two_residuals_ambiguous_delete_never_replayed_and_failed_get_not_absence(self):
        frozen = self.stopped_plan(2)
        self.cfg["ambiguous_delete"] = True
        result = self.execute_stopped(frozen)
        self.assertTrue(result["confirmed"], result)
        self.assertEqual(sum("delete" in c for c in self.calls()), 2)
        self.setUp()
        frozen = self.stopped_plan(2)
        self.cfg["namespace_readback_failure"] = True
        result = self.execute_stopped(frozen)
        self.assertFalse(result["confirmed"], result)
        self.assertEqual(sum("delete" in c for c in self.calls()), 2)

    def test_all_61_types_once_with_rows_in_their_requested_batches(self):
        self.run.inventory()
        batches = []
        observed = []
        for path in sorted(self.root.glob("command-*.json")):
            record = json.loads(path.read_text())
            args = record["argv"]
            if "go-template="+entry.META_TEMPLATE not in args:
                continue
            requested = args[args.index("get")+1].split(",")
            batches.append(requested)
            rows = [line.split("|") for line in path.with_suffix(".out").read_text().splitlines()]
            self.assertTrue(all(resource_name(row) in requested for row in rows))
            observed.extend(row[:7] for row in rows)
        catalog = [n for n in self.fixture["api_resources"] if n != "secrets"]
        self.assertEqual(len(catalog), 61)
        self.assertEqual([n for batch in batches for n in batch], catalog)
        self.assertEqual([len(batch) for batch in batches], [5]*12+[1])
        self.assertCountEqual(observed, self.fixture["rows"])
        self.assertFalse(any("secrets" in batch for batch in batches))

    def test_last_single_type_and_late_unknown_object_are_checked(self):
        for resource, version, kind in (("securitygrouppolicies.vpcresources.k8s.aws", "vpcresources.k8s.aws/v1beta1", "SecurityGroupPolicy"),
                                        ("unknownpersistents.unknown.test", "unknown.test/v1", "UnknownPersistent")):
            with self.subTest(resource=resource):
                self.setUp()
                frozen = self.stopped_plan(2)
                if resource not in self.cfg["api_resources"]:
                    self.cfg["api_resources"].append(resource)
                row = [version, kind, "late", "foreign-uid", "", "", "", frozen["namespace"]]
                self.cfg["cleanup_rows"].append(row)
                result = self.execute_stopped(frozen)
                self.assertFalse(result["confirmed"], result)
                self.assertIn("foreign resource", result["error"])
                self.assertFalse(any("delete" in c for c in self.calls()))
                batches = [c for c in self.calls() if "go-template="+entry.META_TEMPLATE in c]
                self.assertIn(resource, batches[-1][batches[-1].index("get")+1].split(","))

    def test_first_middle_last_batch_fault_stops_without_partial_acceptance(self):
        real_clock = time.monotonic
        for resource in ("configmaps", "elasticsearches.elasticsearch.k8s.elastic.co", "securitygrouppolicies.vpcresources.k8s.aws"):
            for mode in ("failure", "timeout", "truncated", "malformed", "overflow"):
                with self.subTest(resource=resource, mode=mode):
                    self.setUp()
                    frozen = self.stopped_plan(2)
                    frozen["seconds"] = 300
                    self.cfg["inventory_fault"] = dict(resource=resource, mode=mode)
                    # One virtual second is 50 ms for every CLI and deadline.
                    start = real_clock()
                    def scaled_clock():
                        return start+(real_clock()-start)*20
                    with patch("time.monotonic", side_effect=scaled_clock):
                        result = self.execute_stopped(frozen)
                    self.assertFalse(result["confirmed"], result)
                    self.assertFalse(any("delete" in c for c in self.calls()))
                    self.assertFalse((self.root/"cleanup-final-inventory.json").exists())
                    batches = [c for c in self.calls() if "go-template="+entry.META_TEMPLATE in c]
                    self.assertIn(resource, batches[-1][batches[-1].index("get")+1].split(","))
                    records = [json.loads(p.read_text()) for p in sorted(self.root.glob("command-*.json"))]
                    if mode == "timeout":
                        self.assertEqual(records[-1]["exit"], -15)

    def test_fixed_startup_plus_per_api_cost_old20_fails_new5_passes(self):
        # Virtual cost 2s startup + 1.05s/API: 20 types cost 23s, five 7.25s.
        # Scale both real child sleep and the shared clock by 20. Production
        # run(timeout=25), 21s execution and 4s Stop/Wait are unchanged.
        self.cfg["inventory_delay"] = dict(startup=.1, per_api=.0525)
        self.configure()
        real_clock = time.monotonic
        start = real_clock()
        def scaled_clock():
            return start+(real_clock()-start)*20
        self.run.cleaning = True
        old_args = ["get", ",".join(self.fixture["api_resources"][:20]), "-o", "go-template="+entry.META_TEMPLATE]
        with patch("time.monotonic", side_effect=scaled_clock):
            with patch("eks_pacing.stop_group", wraps=entry.stop_group) as stop:
                with self.assertRaisesRegex(ValueError, "command timeout"):
                    self.run.kube(old_args, self.fixture["namespace"])
            self.assertEqual(stop.call_count, 1)
            self.assertEqual(stop.call_args.args[0].returncode, -15)
            old_record = json.loads((self.root/"command-0001.json").read_text())
            self.assertGreaterEqual(old_record["monotonic_end"]-old_record["monotonic_start"], 21)
            with patch("eks_pacing.stop_group", wraps=entry.stop_group) as stop:
                rows = self.run.inventory()
            self.assertTrue(all(c.args[0].returncode == 0 for c in stop.call_args_list))
        self.assertCountEqual(rows, [r for r in self.fixture["rows"] if r[1] != "PodMetrics"])
        self.assertEqual(sum("go-template="+entry.META_TEMPLATE in c for c in self.calls()), 14)

    def test_300_second_plan_keeps_original_deadline_and_single_invocation(self):
        import eks_cleanup
        frozen = self.stopped_plan(2)
        frozen["seconds"] = 300
        result = self.execute_stopped(frozen)
        self.assertTrue(result["confirmed"], result)
        invocation = json.loads((self.root/"invocation.json").read_text())
        budget = json.loads((self.root/"cleanup-budget.json").read_text())
        self.assertEqual(invocation["deadline"], invocation["monotonic_start"]+300)
        self.assertEqual(budget["deadline"], invocation["deadline"])
        self.assertEqual(budget["objects_deadline"], invocation["deadline"]-45)
        before = self.calls()
        with self.assertRaisesRegex(ValueError, "already invoked"):
            eks_cleanup.execute(self.root, entry.digest(self.root/"plan.json"))
        self.assertEqual(self.calls(), before)

    def test_total_deadline_and_namespace_reserve_cannot_fund_inventory(self):
        real_clock = time.monotonic
        original_run = entry.Run.run
        for elapsed in (255, 300):
            with self.subTest(elapsed=elapsed):
                self.setUp()
                frozen = self.stopped_plan(2)
                frozen["seconds"] = 300
                offset = [0]
                def clock():
                    return real_clock()+offset[0]
                def consume_inventory(run, argv, timeout=25, *, monitor=False):
                    raw = original_run(run, argv, timeout, monitor=monitor)
                    if "go-template="+entry.META_TEMPLATE in argv:
                        offset[0] = elapsed
                    return raw
                with patch("time.monotonic", side_effect=clock), patch.object(entry.Run, "run", consume_inventory):
                    result = self.execute_stopped(frozen)
                self.assertFalse(result["confirmed"], result)
                self.assertFalse(any("delete" in c for c in self.calls()))

    def test_inventory_output_save_failure_prevents_deletes(self):
        frozen = self.stopped_plan(2)
        original_save = entry.Run.save
        def fail_output(run, name, data):
            if name == "command-0010.out":
                raise OSError("disk full during first inventory output")
            return original_save(run, name, data)
        with patch.object(entry.Run, "save", fail_output):
            result = self.execute_stopped(frozen)
        self.assertFalse(result["confirmed"], result)
        self.assertFalse(any("delete" in c for c in self.calls()))
        self.assertIn("disk full", result["diagnostic_errors"][0])

    def test_uncertain_delete_or_lost_evidence_never_replays_or_deletes_namespace(self):
        original_save = entry.Run.save
        for scenario in ("refused", "lost-evidence"):
            with self.subTest(scenario=scenario):
                self.setUp()
                frozen = self.stopped_plan(2)
                self.cfg["refuse_delete"] = scenario == "refused"
                def save(run, name, data):
                    if scenario == "lost-evidence" and run.mutation_attempted and name.endswith(".out"):
                        raise OSError("DELETE output evidence lost")
                    return original_save(run, name, data)
                with patch.object(entry.Run, "save", save):
                    result = self.execute_stopped(frozen)
                self.assertFalse(result["confirmed"], result)
                deletes = [i for i, c in enumerate(self.calls()) if "delete" in c]
                self.assertEqual(len(deletes), 1)
                self.assertTrue(all("get" in c for c in self.calls()[deletes[0]+1:]))
                self.assertTrue(self.calls()[deletes[0]+1:])
                body = json.loads(Path(self.calls()[deletes[0]][-1]).read_text())
                quota = next(e for e in frozen["owned"] if e["kind"] == "ResourceQuota")
                self.assertEqual(body["preconditions"]["uid"], quota["uid"])

    def test_frozen_budget_and_input_tampering_refused_before_cli(self):
        import eks_cleanup
        for scenario in ("hash", "input", "unsupported-budget"):
            with self.subTest(scenario=scenario):
                self.setUp()
                frozen = self.stopped_plan(2)
                frozen["seconds"] = 300 if scenario != "unsupported-budget" else 301
                (self.root/"plan.json").write_text(json.dumps(frozen))
                expected = entry.digest(self.root/"plan.json")
                if scenario == "hash":
                    frozen["seconds"] = 180
                    (self.root/"plan.json").write_text(json.dumps(frozen))
                elif scenario == "input":
                    Path(frozen["original_owned"]).write_text("[]")
                (self.root/"plan.json").chmod(0o400)
                with self.assertRaises(ValueError):
                    eks_cleanup.execute(self.root, expected)
                self.assertEqual(self.calls(), [])

    def test_stopped_partition_must_match_frozen_original_evidence(self):
        import eks_cleanup
        for scenario in ("replacement", "overlap", "extra-target", "unfrozen"):
            with self.subTest(scenario=scenario):
                self.setUp()
                frozen = self.stopped_plan(2)
                if scenario == "replacement":
                    frozen["stopped"][0]["uid"] = "adopted-uid"
                elif scenario == "overlap":
                    frozen["owned"].append(frozen["stopped"][0])
                elif scenario == "extra-target":
                    frozen["owned"] = self.fixture["owned"]
                else:
                    frozen["inputs"] = {}
                (self.root/"plan.json").write_text(json.dumps(frozen))
                (self.root/"plan.json").chmod(0o400)
                with self.assertRaises(ValueError):
                    eks_cleanup.execute(self.root, entry.digest(self.root/"plan.json"))
                self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
