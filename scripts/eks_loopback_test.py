"""Offline fixed-layout, admission, startup and actual trial accounting boundaries."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import eks_loopback as loop
import eks_pacing as common
from eks_pacing_test import sample, window
from eks_resources import pod_requests


def plan():
    value=dict(owner="weir-qual-m26r-synthetic",namespace="weir-qual-m26r-synthetic",node=dict(name="node",uid="synthetic-node"))
    return value


def live_pod():
    job=loop.objects(plan())["job"]
    spec=copy.deepcopy(job["spec"]["template"]["spec"])
    spec.update(priority=0,preemptionPolicy="PreemptLowerPriority")
    ref=dict(uid="synthetic-job",name="loopback",kind="Job",apiVersion="batch/v1",controller=True)
    metadata=dict(name="synthetic-pod",namespace=plan()["namespace"],uid="synthetic-pod-uid",labels=job["metadata"]["labels"],ownerReferences=[ref])
    status=dict(phase="Running",podIP="10.0.0.2")
    for key,containers in (("containerStatuses",spec["containers"]),("initContainerStatuses",spec["initContainers"])):
        states=[]
        for c in containers:
            state=dict(name=c["name"],restartCount=0,lastState={},imageID=c["image"],containerID="containerd://synthetic-"+c["name"],
                       resources=c["resources"],allocatedResources=c["resources"]["requests"],state=dict(running=dict(startedAt="synthetic-time")),started=True,ready=True)
            if c["name"]=="bootstrap":state["state"]=dict(terminated=dict(exitCode=0,reason="Completed"))
            states.append(state)
        status[key]=states
    result=dict(apiVersion="v1",kind="Pod",metadata=metadata,spec=spec,status=status)
    return result


def options():
    value=dict(job=dict(name="loopback",uid="synthetic-job"),template=loop.objects(plan())["job"],pod_uid="synthetic-pod-uid")
    return value


def tcp_table():
    rows=["  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode"]
    for i,port in enumerate((7447,7449,9200,9300)):
        rows.append(f"{i}: 0100007F:{port:04X} 00000000:0000 0A 0:0 0:0 0 1000 0 123 1")
    return "\n".join(rows)+"\n"


def trial_records():
    start=sample(0)["time"]
    t=dict(options=dict(Rate=50,WarmSeconds=20,Seconds=20,Prefix="through-weir",Workers=64,TimingOnly=False,LegacyExpiry=False),
           start=start,end=sample(40)["time"],planned=2000,warm=window(1000),measure=window(1000),ten_second_windows=[window(500),window(500)])
    audit=dict(planned_writes=200,found_version1=200,applied=200,unknown_found=0,absent=0,pages=2)
    records=[dict(type="trial",trial=t,run_error="<nil>"),dict(type="audit",audit=audit,error="<nil>",prefix="through-weir",ledger=base64.b64encode(bytes([1])*200).decode())]
    for n in range(0,41,2):
        s=sample(n);s["files"]["net/tcp"]=tcp_table()
        records.append(dict(type="client_start" if n==0 else "client_sample",sample=s))
    return records


class Layout(unittest.TestCase):
    def test_fixed_native_sidecar_order_resources_and_direct_product(self):
        objects=loop.objects(plan());spec=objects["job"]["spec"]["template"]["spec"]
        self.assertEqual([c["name"] for c in spec["initContainers"]],["elasticsearch","bootstrap"])
        self.assertEqual(spec["initContainers"][0]["restartPolicy"],"Always")
        self.assertNotIn("restartPolicy",spec["initContainers"][1])
        self.assertEqual(spec["containers"][0]["command"],["/weir"])
        self.assertEqual(spec["restartPolicy"],"Never")
        p=live_pod()
        projected=dict(containers=p["spec"]["containers"],initContainers=p["spec"]["initContainers"],containerStatuses=p["status"]["containerStatuses"],initContainerStatuses=p["status"]["initContainerStatuses"],
                       resources=None,statusResources=None,allocatedResources=None,overhead=None,unsupported=False,resize=None,resizeConditions=[])
        effective,detail=pod_requests(projected)
        self.assertEqual(effective,dict(cpu=6,memory=4608*1024**2,**{"ephemeral-storage":2560*1024**2}))
        self.assertEqual(detail["init_phases"][1]["requests"]["cpu"],4)
        self.assertEqual(detail["init_phases"][1]["requests"]["memory"],3584*1024**2)
        for c in spec["containers"]+spec["initContainers"]:
            self.assertEqual(c["resources"]["requests"],c["resources"]["limits"])
        probe=spec["initContainers"][0]["startupProbe"]["exec"]["command"]
        self.assertEqual(probe[-1],"http://127.0.0.1:9200/")
        self.assertNotIn("PUT",probe)
        self.assertNotIn("setup",objects["config"]["data"]["bootstrap.sh"])
        self.assertTrue(loop.pod_check(p,options()))

    def test_functional_and_readonly_entries_share_fixed_metadata_setting(self):
        import eks_socket_diagnostic as diagnostic
        functional = loop.objects(plan())
        readonly = diagnostic.objects(plan())
        self.assertEqual(functional["job"], readonly["job"])
        spec = functional["job"]["spec"]["template"]["spec"]
        flags = [entry for entry in spec["initContainers"][0]["env"]
                 if entry["name"].startswith("AWS_")]
        expected = [dict(name="AWS_EC2_METADATA_DISABLED", value="true")]
        self.assertEqual(flags, expected)
        # An old frozen object cannot be passed off as a new runnable plan.
        old = copy.deepcopy(functional["job"])
        old["spec"]["template"]["spec"]["initContainers"][0]["env"].remove(flags[0])
        with self.assertRaisesRegex(ValueError, "env"):
            common.job_check(old, functional["job"])

    def test_injections_order_always_and_runtime_fail_closed(self):
        mutations=[lambda p:p["spec"]["initContainers"].reverse(),
                   lambda p:p["spec"]["initContainers"].append(copy.deepcopy(p["spec"]["initContainers"][1])),
                   lambda p:p["spec"]["initContainers"][1].update(restartPolicy="Always"),
                   lambda p:p["spec"]["containers"][0].update(command=["/bin/sh"]),
                   lambda p:p["spec"]["initContainers"][0].update(image="wrong"),
                   lambda p:p["spec"]["initContainers"][0]["securityContext"].update(runAsUser=0),
                   lambda p:p["metadata"]["ownerReferences"][0].update(uid="replacement"),
                   lambda p:p["metadata"].update(uid="replacement"),
                   lambda p:p["status"]["initContainerStatuses"][0].update(restartCount=1),
                   lambda p:p["status"]["initContainerStatuses"][1]["state"]["terminated"].update(exitCode=1),
                   lambda p:p["status"]["containerStatuses"][0].update(imageID="wrong"),
                   lambda p:p["status"]["containerStatuses"][0].update(state=dict(terminated=dict(exitCode=137,reason="OOMKilled"))),
                   lambda p:p["status"]["containerStatuses"][0]["allocatedResources"].update(cpu="5")]
        for change in mutations:
            p=live_pod();change(p)
            with self.assertRaises(ValueError):loop.pod_check(p,options())
        p=live_pod();p["status"]["initContainerStatuses"][0]["started"]=False
        self.assertFalse(loop.pod_check(p,options()))
        # M25 still rejects *all* init; no permissive global Always profile.
        from eks_pacing_test import pod,plan as m25plan
        p=pod();p["spec"]["initContainers"]=live_pod()["spec"]["initContainers"]
        opts=dict(job=dict(name="pace-0",uid="job-uid"),template=common.job_template(m25plan(),"pace-0"),pod_uid="pod-uid")
        with self.assertRaises(ValueError):common.pod_check(p,opts)

    def test_shared_job_unknown_execution_fields(self):
        j=loop.objects(plan())["job"]
        for field,value in (("managedBy","foreign"),("podFailurePolicy",{}),("ttlSecondsAfterFinished",0),("manualSelector",True)):
            actual=copy.deepcopy(j);actual["spec"][field]=value
            with self.assertRaises(ValueError):common.job_check(actual,j)

    def test_actual_go_projection_preserves_downward_api_and_scripts(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            driver=root/"template.go"
            driver.write_text('package main\nimport("os";"encoding/json";"text/template")\nfunc main(){var p struct{Template string; Object any};f,e:=os.Open(os.Args[1]);if e!=nil{panic(e)};defer f.Close();if e=json.NewDecoder(f).Decode(&p);e!=nil{panic(e)};t,e:=template.New("x").Parse(p.Template);if e!=nil{panic(e)};if e=t.Execute(os.Stdout,p.Object);e!=nil{panic(e)}}')
            obj=live_pod()
            payload=dict(Template=common.OBJECT_TEMPLATE,Object=obj)
            (root/"input.json").write_text(json.dumps(payload))
            env=dict(os.environ,GOROOT=str(common.REPO/".tools/go1.27.1"),GOENV="off",GOWORK="off",GOTOOLCHAIN="local",GOPROXY="off",GOSUMDB="off")
            result=subprocess.run([str(common.REPO/".tools/go1.27.1/bin/go"),"run",str(driver),str(root/"input.json")],env=env,capture_output=True,text=True,timeout=45)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertTrue(loop.pod_check(json.loads(result.stdout),options()))
            # Evaluate actual Go projection, then strict runtime admission.
            field = dict(apiVersion="v1", fieldPath="metadata.name")
            value_from = dict(fieldRef=field)
            reference = dict(name="AWS_EC2_METADATA_DISABLED", valueFrom=value_from)
            extra = dict(name="UNEXPECTED", value="synthetic-not-for-output")
            changes = [lambda e: e.pop(1), lambda e: e[1].update(value="false"),
                       lambda e: e[1].update(value="TRUE"), lambda e: e[1].update(value=True),
                       lambda e: e.__setitem__(1, reference), lambda e: e.reverse(),
                       lambda e: e.append(extra)]
            for change in changes:
                obj = live_pod()
                change(obj["spec"]["initContainers"][0]["env"])
                payload = dict(Template=common.OBJECT_TEMPLATE, Object=obj)
                (root/"input.json").write_text(json.dumps(payload))
                command = [str(common.REPO/".tools/go1.27.1/bin/go"), "run", str(driver), str(root/"input.json")]
                result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=45)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("synthetic-not-for-output", result.stdout)
                with self.assertRaisesRegex(ValueError, "env"):
                    loop.pod_check(json.loads(result.stdout), options())

    def test_trial_uses_warm_gate_full_audit_and_socket_boundary(self):
        result=loop.trial_report(trial_records(),"through-weir")
        self.assertTrue(result["passed"])
        records=trial_records()
        for kind in ("all","read","put"):
            # Coherent histogram lateness must fail warm even if measure passes.
            for h in ("lag","dispatch"):
                metric=records[0]["trial"]["warm"][kind][h]
                for bucket in metric["buckets"]:bucket["upper_us"]=6000
                metric.update(p50_us=6000,p95_us=6000,p99_us=6000,max_ns=5950000)
        result=loop.trial_report(records,"through-weir")
        self.assertFalse(result["passed"])
        self.assertTrue(any(r.startswith("warm:") for r in result["reasons"]))
        for modify in (lambda r:r[1]["audit"].update(found_version1=199),lambda r:r[1].update(ledger=""),
                       lambda r:r[-1]["sample"]["files"].update({"net/tcp":tcp_table().replace("0100007F","00000000")})):
            records=trial_records();modify(records)
            with self.assertRaises((ValueError,RuntimeError)):loop.trial_report(records,"through-weir")

    def test_mutation_reservation_is_not_released_on_unknown(self):
        budget=loop.Budget()
        e=budget.reserve("through-weir",planned=2000,seeds=1000,seconds=40)
        budget.reconcile(e,[],False)
        self.assertEqual(budget.snapshot()["reserved"],1200)
        self.assertIsNone(e["actually_started"])
        with self.assertRaises(RuntimeError):budget.reserve("through-weir",planned=2000,seeds=1000,seconds=40)

    def test_monitor_reaps_cli_and_preserves_original_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            run=loop.Run(Path(temp),loop.TARGET)
            with patch.object(run,"current_pod",side_effect=ValueError("container restart/history")):
                with self.assertRaisesRegex(ValueError,"restart/history"):
                    run.run([sys.executable,"-c","import time;time.sleep(20)"],10,monitor=True)
            record=json.loads((Path(temp)/"command-0001.json").read_text())
            self.assertIsNotNone(record["exit"])

    def test_bootstrap_readonly_checks_precede_single_management_attempt(self):
        import shutil
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            check=(common.REPO/"scripts/eks_loopback_check.sh").read_text()
            check=check.replace("/proc/net/",str(root)+"/")
            (root/"loopback-check.sh").write_text(check)
            bootstrap=(common.REPO/"scripts/eks_loopback_bootstrap.sh").read_text().replace("/qualification/",str(root)+"/")
            (root/"bootstrap.sh").write_text(bootstrap)
            keys=("network.host","http.host","transport.host","discovery.type","action.auto_create_index","xpack.security.enabled","xpack.ml.enabled","ingest.geoip.downloader.enabled","node.store.allow_mmap")
            values=("127.0.0.1","127.0.0.1","127.0.0.1","single-node","false","false","false","false","false")
            settings=dict(nodes=dict(synthetic=dict(settings=dict(zip(keys,values)))))
            (root/"settings.json").write_text(json.dumps(settings))
            curl=root/"curl"
            curl.write_text('#!'+sys.executable+'''\nimport json,pathlib,sys
r=pathlib.Path(__file__).parent
with (r/"calls.jsonl").open("a") as f:f.write(json.dumps(sys.argv[1:])+"\\n")
if "PUT" in sys.argv:
 print((r/"reply.json").read_text())
elif sys.argv[-1].endswith("9200/"):
 print((r/"version.json").read_text())
else: print((r/"settings.json").read_text())
''')
            curl.chmod(0o700)
            for name in ("nc","timeout"):
                exe=root/name;exe.write_text('#!'+sys.executable+'\nraise SystemExit(1)\n');exe.chmod(0o700)
            header=tcp_table().splitlines()[0]+"\n"
            es_table=header+"\n".join(tcp_table().splitlines()[3:])+"\n"
            for name in ("tcp6","udp","udp6"):(root/name).write_text(header)
            env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ["PATH"],POD_IP="10.0.0.2")
            for scenario in ("valid","bad-version","foreign-listener","uncertain-put"):
                (root/"calls.jsonl").write_text("")
                (root/"tcp").write_text(es_table.replace("0100007F","00000000") if scenario=="foreign-listener" else es_table)
                (root/"version.json").write_text(json.dumps(dict(version=dict(number="bad" if scenario=="bad-version" else "8.19.22"))))
                response="uncertain" if scenario=="uncertain-put" else '{"acknowledged":true,"shards_acknowledged":true,"index":"records"}'
                (root/"reply.json").write_text(response)
                result=subprocess.run([shutil.which("bash"),"--noprofile","--norc",str(root/"bootstrap.sh")],env=env,capture_output=True,text=True,timeout=5)
                calls=[json.loads(line) for line in (root/"calls.jsonl").read_text().splitlines()]
                self.assertEqual(sum("PUT" in c for c in calls),int(scenario in ("valid","uncertain-put")),scenario+result.stderr)
                self.assertEqual(result.returncode==0,scenario=="valid",scenario+result.stderr)
                for call in calls:
                    self.assertEqual(call[0],"-q")
                    self.assertIn("--retry",call)
                    self.assertTrue(call[-1].startswith("http://127.0.0.1:9200/"))
                if scenario=="uncertain-put":self.assertIn('"reserved":1,"started":1',result.stdout)



if __name__=="__main__":unittest.main()
