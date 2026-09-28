"""Offline M25 boundaries. No kubectl, AWS, Docker or external service is called."""
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import eks_pacing as entry
from eks_pacing_report import HISTOGRAMS, IMAGES, node_gate, pacing, resources
from capacity_report_test import sample as old_sample, metrics as old_metrics


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

    def test_conservative_init_overhead_pod_and_resize_requests(self):
        cells = ["node", "Running", '{"requests":{"cpu":"100m","memory":"1Gi"}}',
                 '{"requests":{"cpu":"200m"}} {"requests":{"cpu":"300m"}}',
                 '{"requests":{"cpu":"400m"}}', '{"cpu":"500m"}', '{"cpu":"600m"}',
                 '{"requests":{"cpu":"700m"}}', '{"requests":{"cpu":"800m"}}', '{"cpu":"900m"}', '{"requests":{"cpu":"1"}}']
        result = entry.allocated("\t".join(cells))["node"]
        self.assertEqual(str(result["cpu"]), "5.500")
        self.assertEqual(result["memory"], 1024**3)
        self.assertEqual(result["pods"], 1)
        cells[1] = "Succeeded"
        self.assertEqual(entry.allocated("\t".join(cells)), {})
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


if __name__ == "__main__":
    unittest.main()
