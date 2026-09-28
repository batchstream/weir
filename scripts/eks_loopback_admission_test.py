"""Real M26R Job response replay plus synthetic Pod/trial lifecycle; no network."""
import argparse
import copy
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

import eks_loopback as loop
import eks_pacing as common
from eks_loopback_test import live_pod, plan, trial_records, tcp_table
from eks_pacing_test import ADMITTED_POD, node, sample
from capacity_report_test import sample as capacity_sample

FIXTURE = Path(__file__).with_name("fixtures")/"eks-loopback-admitted-job.json"


def responses():
    job = json.loads(FIXTURE.read_text())
    # Preserve the original API recording; add only the new fixture setting.
    setting = dict(name="AWS_EC2_METADATA_DISABLED", value="true")
    job["spec"]["template"]["spec"]["initContainers"][0]["env"].insert(1, setting)
    pod = copy.deepcopy(ADMITTED_POD)
    pod["metadata"].update(name="loopback-admission", namespace=plan()["namespace"])
    pod["metadata"]["labels"][common.LABEL] = plan()["owner"]
    # Use the M26 actual spec plus only the observed M25 Pod-specific defaults.
    pod["spec"] = copy.deepcopy(job["spec"]["template"]["spec"])
    for key in ("priority", "preemptionPolicy", "serviceAccount", "serviceAccountName", "tolerations"):
        pod["spec"][key] = copy.deepcopy(ADMITTED_POD["spec"][key])
    actual = live_pod()
    actual["spec"] = copy.deepcopy(pod["spec"])
    actual["metadata"]["labels"].update(pod["metadata"]["labels"])
    result = dict(job_response=job, created_job=copy.deepcopy(job), pod_response=pod, actual_pod=actual)
    return result


class AdmissionReplay(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.evidence = self.root/"evidence"
        self.evidence.mkdir()
        script = Path(__file__).with_name("eks_loopback_cli_fixture.py").read_text()
        for name in ("kubectl", "aws", "git"):
            executable = self.root/name
            executable.write_text("#!"+sys.executable+"\n"+script)
            executable.chmod(0o700)
        env = dict(PATH=str(self.root)+os.pathsep+os.environ["PATH"])
        patched = patch.dict(os.environ, env)
        patched.start()
        self.addCleanup(patched.stop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))
        n = node()
        n["uid"] = "synthetic-node"
        n["allocatable"]["ephemeral-storage"] = "20Gi"
        self.cfg = responses()
        self.cfg.update(namespace=plan()["namespace"], label=common.LABEL, node=n,
                        plurals={kind:value[1] for kind,value in common.KINDS.items()},
                        cluster=dict(arn=loop.TARGET["context"],name="data-team",status="ACTIVE",version="1.36"))
        self.write_config()
        self.run = loop.Run(self.evidence, loop.TARGET)
        self.run.plan = plan()
        self.run.template = loop.objects(plan())["job"]
        self.run.pod_entry = None
        self.run.pod_ready = False

    def write_config(self):
        (self.root/"scenario.json").write_text(json.dumps(self.cfg))

    def calls(self):
        path = self.root/"calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def creates(self):
        records = []
        for args in self.calls():
            if args[0] == "kubectl" and "create" in args and "-f" in args:
                obj = json.loads(Path(args[args.index("-f")+1]).read_text())
                records.append((obj["kind"], "--dry-run=server" in args))
        return records

    def test_recorded_response_through_both_dry_runs_create_and_current_pod(self):
        self.run.job = self.run.create(self.run.template)
        self.assertTrue(self.run.current_pod())
        self.assertTrue(self.run.pod_ready)
        self.assertEqual(self.creates(), [("Job",True),("Pod",True),("Job",False)])
        self.assertEqual(self.run.identities["weir"]["imageID"],loop.IMAGES["version"]["reference"])

    def test_metadata_flag_drift_never_persists_job(self):
        extra = dict(name="AWS_REGION", value="us-west-1")
        field = dict(apiVersion="v1", fieldPath="metadata.name")
        value_from = dict(fieldRef=field)
        reference = dict(name="AWS_EC2_METADATA_DISABLED", valueFrom=value_from)
        changes = [
            ("missing", lambda e: e.pop(1)),
            ("false", lambda e: e[1].update(value="false")),
            ("capitalized", lambda e: e[1].update(value="True")),
            ("uppercase", lambda e: e[1].update(value="TRUE")),
            ("boolean", lambda e: e[1].update(value=True)),
            ("empty", lambda e: e[1].update(value="")),
            ("order", lambda e: e.reverse()),
            ("extra", lambda e: e.append(extra)),
            ("unknown-field", lambda e: e[1].update(unknown="true")),
            ("valueFrom", lambda e: e.__setitem__(1, reference)),
        ]
        baseline = copy.deepcopy(self.cfg)
        for stage in ("job_response", "pod_response"):
            for name, change in changes:
                with self.subTest(stage=stage, change=name):
                    self.cfg = copy.deepcopy(baseline)
                    obj = self.cfg[stage]
                    spec = obj["spec"]["template"]["spec"] if stage == "job_response" else obj["spec"]
                    change(spec["initContainers"][0]["env"])
                    self.write_config()
                    with self.assertRaisesRegex(ValueError, "env"):
                        self.run.create(self.run.template)
                    self.assertEqual(self.run.owned, [])
        self.assertFalse(any(kind == "Job" and not dry for kind, dry in self.creates()))

    def test_all_quantity_locations_and_probe_zero_equivalence(self):
        for name in ("job_response", "created_job", "pod_response", "actual_pod"):
            obj = self.cfg[name]
            spec = obj["spec"]["template"]["spec"] if obj["kind"] == "Job" else obj["spec"]
            for c in spec["containers"]+spec["initContainers"]:
                for field in ("requests", "limits"):
                    for key, value in c["resources"][field].items():
                        number = common.quantity(value)
                        c["resources"][field][key] = str(number*1000)+"m" if key == "cpu" else str(number)
            spec["volumes"][1]["emptyDir"]["sizeLimit"] = "1024Mi"
            spec["containers"][0]["readinessProbe"]["initialDelaySeconds"] = 0
        self.write_config()
        self.run.job = self.run.create(self.run.template)
        self.assertTrue(self.run.current_pod())
        self.assertTrue(self.run.pod_ready)

    def test_negative_pairs_never_persist_job(self):
        mutations = []
        for group in ("containers", "initContainers"):
            for category in ("requests", "limits"):
                for key in ("cpu", "memory", "ephemeral-storage"):
                    for delta in ("-0.001", "0.001") if key == "cpu" else ("-1", "1", "0.000000001"):
                        def change(spec, g=group, c=category, k=key, d=delta):
                            values = spec[g][0]["resources"][c]
                            values[k] = str(common.quantity(values[k])+common.quantity(d.lstrip("-"))*(-1 if d.startswith("-") else 1))
                        mutations.append((group+"/"+category+"/"+key+"/"+delta, change))
        mutations.extend([
            ("sub-context-precision",lambda s:s["containers"][1]["resources"]["limits"].update(cpu="1.0000000000000000000000000000000000000000001")),
            ("missing-limits",lambda s:s["containers"][0]["resources"].pop("limits")),
            ("missing-resource",lambda s:s["containers"][0]["resources"]["limits"].pop("memory")),
            ("unknown-resource",lambda s:s["containers"][0]["resources"]["requests"].update(unknown="1")),
            ("unknown-probe",lambda s:s["containers"][0]["readinessProbe"].update(unknown=0)),
            ("probe-pointer-zero",lambda s:s["containers"][0]["readinessProbe"].update(terminationGracePeriodSeconds=0)),
            ("probe-bool",lambda s:s["containers"][0]["readinessProbe"].update(initialDelaySeconds=False)),
            ("probe-null",lambda s:s["containers"][0]["readinessProbe"].update(initialDelaySeconds=None)),
            ("probe-float",lambda s:s["containers"][0]["readinessProbe"].update(initialDelaySeconds=0.0)),
            ("probe-command",lambda s:s["containers"][0]["readinessProbe"]["exec"].update(command=["wrong"])),
            ("image",lambda s:s["containers"][0].update(image="wrong")),
            ("command",lambda s:s["containers"][0].update(command=["/bin/sh"])),
            ("env",lambda s:s["containers"][1]["env"][0].update(value="2")),
            ("security",lambda s:s["initContainers"][0]["securityContext"].update(runAsUser=0)),
            ("security-bool",lambda s:s["initContainers"][0]["securityContext"].update(runAsGroup=False)),
            ("security-pointer",lambda s:s["initContainers"][0]["securityContext"].pop("allowPrivilegeEscalation")),
            ("volume",lambda s:s["volumes"][0]["configMap"].update(name="wrong")),
            ("volume-byte",lambda s:s["volumes"][1]["emptyDir"].update(sizeLimit="1073741825")),
            ("extra-init",lambda s:s["initContainers"].append(copy.deepcopy(s["initContainers"][1]))),
            ("init-order",lambda s:s["initContainers"].reverse()),
            ("always",lambda s:s["initContainers"][1].update(restartPolicy="Always")),
            ("node",lambda s:s.update(nodeName="foreign")),
            ("priority",lambda s:s.update(priority=1)),
            ("unknown",lambda s:s.update(unknown=False))])
        for key in ("initialDelaySeconds", "periodSeconds", "timeoutSeconds", "failureThreshold", "successThreshold"):
            for group, probe in (("containers","readinessProbe"),("initContainers","startupProbe")):
                mutations.append((group+"/"+key,lambda s,k=key,g=group,p=probe:s[g][0][p].update({k:99})))
        baseline = copy.deepcopy(self.cfg)
        for stage in ("job_response", "pod_response"):
            for name, mutate in mutations:
                with self.subTest(stage=stage, mutation=name):
                    self.cfg = copy.deepcopy(baseline)
                    obj = self.cfg[stage]
                    obj["metadata"]["labels"]["example.test/valid-extra"] = "allowed"
                    spec = obj["spec"]["template"]["spec"] if stage == "job_response" else obj["spec"]
                    mutate(spec)
                    self.write_config()
                    with self.assertRaises(ValueError):
                        self.run.create(self.run.template)
                    self.assertEqual(self.run.owned, [])
        self.assertFalse(any(kind == "Job" and not dry for kind,dry in self.creates()))

    def test_metadata_uid_and_ownership_rejected_before_job_create(self):
        for stage, field, value in (("job_response","uid","wrong"),("job_response","namespace","foreign"),
                                     ("pod_response","ownerReferences",[dict(uid="foreign")]),
                                     ("pod_response","name","foreign")):
            baseline = copy.deepcopy(self.cfg)
            self.cfg[stage]["metadata"][field] = value
            self.write_config()
            with self.assertRaises(ValueError):
                self.run.create(self.run.template)
            self.assertEqual(self.run.owned, [])
            self.cfg = baseline
        self.assertFalse(any(not dry for _,dry in self.creates()))

    def test_quota_drift_fails_before_job_creation(self):
        for update in ({"requests.memory":"4831838209"}, {"limits.cpu":"5.999"}, {"unknown":"0"}):
            self.cfg["quota_updates"] = update
            self.write_config()
            with self.assertRaises(ValueError):
                loop.namespace_start(self.run)
            self.assertFalse(any(kind == "Job" for kind,_ in self.creates()))
            # The fake Namespace is the only persisted object in these cases.
            self.run.delete(self.run.namespace)

    def prepare_lifecycle(self):
        fixture = capacity_sample("weir",0)
        self.cfg.update(metrics=fixture["metrics"],stats=fixture["db"],
                        bootstrap_log='{"management":"create-empty-records","completed":1,"document_mutations":0}',
                        version=dict(product="weir",revision=loop.SOURCE,go="go1.27.1",target="linux/arm64",state="clean-commit",dirty="false",version="local-"+loop.SOURCE))
        self.cfg["config"] = dict(timing=dict(expiry_ms=20,max_catchup_per_wake=8,deadline_ms=1000),store_effective=dict(Concurrency=4,BatchOperations=16))
        self.cfg["snapshot"] = sample(0)
        self.cfg["snapshot"]["files"]["net/tcp"] = tcp_table()
        self.cfg["trials"] = {}
        for prefix in ("through-weir", "direct-es"):
            records = trial_records()
            records[0]["trial"]["options"]["Prefix"] = prefix
            records[1]["prefix"] = prefix
            records.extend([dict(type="setup_progress",started=1000),dict(type="mutation_receipt",started=1200)])
            self.cfg["trials"][prefix] = records
        self.write_config()
        proof = self.root/"proof.json"
        proof.write_text('{"files":{}}')
        self.run.registry_evidence = dict(path=str(proof),sha256=common.digest(proof),proof=dict(files={}))
        loop.prepare(self.run, plan()["owner"])
        opts = argparse.Namespace(plan_sha256=common.digest(self.evidence/"plan.json"),node_uid="synthetic-node")
        return opts

    def execute_lifecycle(self, opts):
        code = loop.execute(self.run, opts)
        result = json.loads((self.evidence/"result.json").read_text())
        self.assertTrue(result["cleanup"]["confirmed"], result)
        self.assertEqual(json.loads((self.root/"state.json").read_text()), {})
        for path in self.evidence.glob("command-*.json"):
            record = json.loads(path.read_text())
            self.assertIn("end", record)
            self.assertIsNotNone(record["exit"])
        return code, result

    def test_complete_synthetic_lifecycle_both_trials_and_uid_cleanup(self):
        opts = self.prepare_lifecycle()
        self.cfg["quota_updates"] = {"requests.cpu":"6000m","limits.cpu":"6000m","requests.memory":"4718592Ki","limits.memory":"4718592Ki"}
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 0, result)
        self.assertEqual(len(result["trials"]), 2)
        self.assertEqual(result["budget"]["reserved"], 2400)
        self.assertEqual(result["budget"]["planned"], 6000)
        self.assertEqual([e["actually_started"] for e in result["budget"]["entries"]], [1200,1200])
        self.assertTrue(result["resource_evidence"].startswith("partial"))
        self.assertEqual(result["bootstrap_management"]["completed"], 1)
        self.assertEqual(len(result["cleanup"]["resources"]), 6)

    def test_admission_failure_stays_not_run_and_reservation_is_not_actual(self):
        opts = self.prepare_lifecycle()
        self.cfg["job_response"]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "3"
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 1)
        self.assertEqual(result["resource_evidence"], "not-run")
        self.assertEqual(result["bootstrap_management"]["reserved"], 1)
        self.assertEqual(result["bootstrap_management"]["started_upper_bound"], 0)
        self.assertEqual(json.loads((self.evidence/"bootstrap-management.json").read_text())["started_upper_bound"], 1)
        self.assertFalse(any(kind == "Job" and not dry for kind,dry in self.creates()))

    def test_post_create_failure_cleans_owned_pod_and_stops_trials(self):
        opts = self.prepare_lifecycle()
        self.cfg["actual_pod"]["spec"]["containers"][0]["securityContext"]["runAsUser"] = 0
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 1)
        self.assertEqual(result["trials"], [])
        self.assertEqual(result["budget"]["reserved"], 0)
        self.assertEqual(len(result["cleanup"]["resources"]), 6)
        self.assertTrue(result["resource_evidence"].startswith("partial"))

    def test_bootstrap_failure_stops_before_trial_and_cleans_exact_uids(self):
        opts = self.prepare_lifecycle()
        status = self.cfg["actual_pod"]["status"]["initContainerStatuses"][1]
        status["state"]["terminated"].update(exitCode=23, reason="Error")
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 1)
        self.assertIn("container failure", result["error"])
        self.assertEqual(result["budget"]["reserved"], 0)
        self.assertFalse(any("exec" in args for args in self.calls()))
        self.assertEqual(len(result["cleanup"]["resources"]), 6)

    def test_unknown_stops_second_trial_and_keeps_reservation(self):
        opts = self.prepare_lifecycle()
        records = self.cfg["trials"]["through-weir"]
        records[0]["run_error"] = "synthetic UNKNOWN mutation"
        records[1]["audit"].update(applied=199, unknown_found=1)
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 1)
        self.assertEqual(result["budget"]["reserved"], 1200)
        self.assertEqual(result["budget"]["entries"][0]["actually_started"], 1200)
        trials = [args for args in self.calls() if "-mode" in args and "trial" in args]
        self.assertEqual(len(trials), 1)
        self.assertNotIn("direct-es", trials[0])

    def test_cancelled_trial_reaps_cli_and_retains_unknown_budget(self):
        def interrupted(signum, frame):
            raise KeyboardInterrupt("signal "+str(signum))
        signal.signal(signal.SIGTERM, interrupted)
        opts = self.prepare_lifecycle()
        self.cfg["cancel_trial"] = True
        self.write_config()
        code, result = self.execute_lifecycle(opts)
        self.assertEqual(code, 1)
        self.assertIn("signal", result["error"])
        self.assertEqual(result["budget"]["reserved"], 1200)
        self.assertIsNone(result["budget"]["entries"][0]["actually_started"])
        trials = [args for args in self.calls() if "-mode" in args and "trial" in args]
        self.assertEqual(len(trials), 1)
        self.assertTrue(list(self.evidence.glob("through-weir-incomplete.jsonl")))

    def test_cleanup_failure_does_not_hide_original_bootstrap_failure(self):
        opts = self.prepare_lifecycle()
        self.cfg["actual_pod"]["status"]["initContainerStatuses"][1]["state"]["terminated"].update(exitCode=23, reason="Error")
        self.cfg["refuse_delete"] = True
        self.write_config()
        code = loop.execute(self.run, opts)
        result = json.loads((self.evidence/"result.json").read_text())
        self.assertEqual(code, 1)
        self.assertIn("container failure", result["error"])
        self.assertFalse(result["cleanup"]["confirmed"])
        self.assertTrue(any(entry.get("error") for entry in result["cleanup"]["resources"]))
        self.assertEqual(result["budget"]["reserved"], 0)
        self.assertFalse(any("--force" in args for args in self.calls()))

    def test_runtime_uid_replacement_never_adopted_or_deleted(self):
        self.run.job = self.run.create(self.run.template)
        self.run.current_pod()
        state_file = self.root/"state.json"
        state = json.loads(state_file.read_text())
        state["Pod/synthetic-pod"]["metadata"]["uid"] = "foreign-replacement"
        state_file.write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,"UID drift"):
            self.run.current_pod()
        with self.assertRaisesRegex(ValueError,"UID/owner drift"):
            self.run.delete(self.run.pod_entry)
        self.assertFalse(any("delete" in a for a in self.calls()))


class ExactSemantics(unittest.TestCase):
    def test_quantity_precision_and_bounds(self):
        for left,right in (("1","1000m"),("1024Mi","1Gi"),("3072Mi","3Gi"),("2Gi","2147483648"),
                           ("1e3","1k"),("+1.","1"),(".1","100m"),("1n","0.000000001")):
            self.assertEqual(common.quantity(left),common.quantity(right))
        self.assertNotEqual(common.quantity("1"),common.quantity("1.0000000000000000000000000000000000000001"))
        self.assertNotEqual(common.quantity("1Gi"),common.quantity("1.0000000000000000000000000000000000000001Gi"))
        for value in (False,0,None,"-1","NaN","1e999","9"*65,"9223372036854775808"):
            with self.assertRaises(ValueError):common.quantity(value)

    def test_probe_defaults_are_typed_and_pointer_fields_are_not_defaulted(self):
        expected = dict(exec=dict(command=["/probe"]))
        actual = dict(expected,initialDelaySeconds=0,timeoutSeconds=1,periodSeconds=10,successThreshold=1,failureThreshold=3)
        common.container_field_check("readinessProbe",actual,expected)
        for key,value in (("initialDelaySeconds",False),("initialDelaySeconds",None),("timeoutSeconds",0),
                          ("terminationGracePeriodSeconds",0),("terminationGracePeriodSeconds",None),("unknown",False)):
            changed = dict(actual)
            changed[key] = value
            with self.assertRaises(ValueError):common.container_field_check("readinessProbe",changed,expected)


if __name__ == "__main__":
    unittest.main()
