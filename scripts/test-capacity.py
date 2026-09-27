#!/usr/bin/env python3
"""Explicit bounded first-reference calibration. No default test starts Docker."""
if not __debug__:
    raise RuntimeError("optimized Python is unsupported")

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import tarfile
import time

import package as packaging
from capacity_fixture import Fixture, REPO, LABEL, inventory_diff, sha
from capacity_report import evaluate, resource_gate, window_gate, timestamp, prom, metric


def prepare(f, plan, artifact):
    receipt=json.loads((artifact/"receipt.json").read_text())
    if receipt["source"]!=plan["artifact_source"] or len(receipt["inputs"])!=70:
        raise RuntimeError("product source/input identity")
    for name,digest in receipt["inputs"].items():
        if packaging.secret_path(name) or not packaging.allowed(name) or sha(REPO/name)!=digest:
            raise RuntimeError("product input mismatch: "+name)
    binaries={name:sha(artifact/"binaries"/name/"weir") for name in ("linux-arm64","linux-amd64")}
    oci=packaging.oci_receipt(artifact/"weir-linux.oci.tar",binaries)
    if binaries["linux-arm64"]!=plan["binary_sha256"] or oci["images"]["arm64"]["config"]!=plan["image_id"]:
        raise RuntimeError("artifact mismatch")
    f.save("artifact-identity.json",{"source":receipt["source"],"inputs":receipt["inputs"],"oci":oci})
    for key in ("image_id","es_image_id"):
        obj=json.loads(f.run(["docker","image","inspect",plan[key]]).stdout)[0]
        if obj["Id"]!=plan[key] or obj["Os"]!="linux" or obj["Architecture"]!="arm64":
            raise RuntimeError("native existing image required")
        f.save(key+".json",{k:obj[k] for k in ("Id","Os","Architecture","RepoDigests")})
    # Explicit existing caches contain modules/build objects, never user Go settings.
    env=dict(f.env, GOROOT=str(REPO/".tools/go1.27.1"), GOENV="off",GOTOOLCHAIN="local",GOWORK="off",GOPROXY="off",GOSUMDB="off",CGO_ENABLED="0",GOMODCACHE="/Users/liran/go/pkg/mod",GOCACHE="/Users/liran/Library/Caches/go-build",WEIR_CAPACITY_INTEGRATION="1")
    go=str(REPO/".tools/go1.27.1/bin/go")
    for system,name in (("linux","client"),("darwin","client-host")):
        f.run([go,"build","-tags","integration","-trimpath","-buildvcs=false","-o",str(f.root/name),"./internal/testutil/testcapacity"],120,env=dict(env,GOOS=system,GOARCH="arm64"))
    cfg=json.loads((REPO/"deploy/kubernetes/node.example.json").read_text())
    cfg["services"][0]["local"].update(concurrency=4,batch_operations=16)
    f.save("node.json",cfg)
    effective=json.loads(f.run([str(f.root/"client-host"),"-mode","config","-config",str(f.root/"node.json")],env=env).stdout)
    f.save("effective-config.json",effective)
    raw=(f.root/"client").read_bytes()
    with tarfile.open(f.root/"client.tar","w") as archive:
        entry=tarfile.TarInfo("client");entry.size=len(raw);entry.mode=0o555
        archive.addfile(entry,io.BytesIO(raw))
    inputs={str(p.relative_to(REPO)):sha(p) for p in sorted((REPO/"internal/testutil/testcapacity").glob("*.go"))}
    inputs.update({str(p.relative_to(REPO)):sha(p) for p in [REPO/"scripts/test-capacity.py",REPO/"scripts/capacity_fixture.py",REPO/"scripts/capacity_report.py",REPO/"scripts/capacity-plan.json"]})
    host={"uname":f.run(["uname","-a"]).stdout.strip(),"cpu":f.run(["sysctl","-n","machdep.cpu.brand_string"]).stdout.strip(),"logical_cpu":f.run(["sysctl","-n","hw.logicalcpu"]).stdout.strip(),"shared_physical_host":True}
    frozen={"schema_version":1,"profile":plan,"tool_inputs":inputs,"client_sha256":hashlib.sha256(raw).hexdigest(),"source":f.run(["git","rev-parse","HEAD"]).stdout.strip(),"effective":effective,"host":host,"vm":json.loads((f.root/"vm.json").read_text())}
    f.save("calibration-plan.json",frozen)
    (f.root/"calibration-plan.json").chmod(0o444)
    return sha(f.root/"calibration-plan.json")


def start(f,plan):
    f.first_mutation()
    f.run(["docker","import","--platform","linux/arm64","--change",'ENTRYPOINT ["/client"]',"--change","USER 65532:65532","--change","LABEL "+LABEL+"="+f.owner,str(f.root/"client.tar"),f.tag])
    f.image=json.loads(f.run(["docker","image","inspect",f.tag]).stdout)[0]["Id"]
    f.create_network()
    net=["--network",f.network]
    common=["--read-only","-e","WEIR_CAPACITY_INTEGRATION=1"]
    client_spec={"image":f.image,"limits":plan["resources"]["client"],"extra":net+common,"command":["-mode","idle"]}
    f.create("client",client_spec)
    es_extra=net+["--network-alias","elasticsearch","--tmpfs","/usr/share/elasticsearch/data:rw,size=1073741824,uid=1000,gid=0,mode=0770","-e","action.auto_create_index=false","-e","discovery.type=single-node","-e","xpack.security.enabled=false","-e","xpack.ml.enabled=false","-e","ingest.geoip.downloader.enabled=false","-e","ES_JAVA_OPTS=-Xms1024m -Xmx1024m"]
    es_spec={"image":plan["es_image_id"],"limits":plan["resources"]["es"],"extra":es_extra,"command":[]}
    f.create("es",es_spec)
    until=time.monotonic()+150
    while time.monotonic()<until:
        result=f.run(["docker","exec",f.containers[f.owner+"-es"],"curl","--silent","--show-error","--max-time","1","http://127.0.0.1:9200/"],5,check=False)
        if result.returncode==0:
            identity=json.loads(result.stdout)
            if identity.get("version",{}).get("number")!="8.19.22":
                raise RuntimeError("backend version")
            f.save("es-version.json",identity)
            break
        time.sleep(1)
    else:
        raise TimeoutError("ES readiness")
    f.run(["docker","exec",f.containers[f.owner+"-client"],"/client","-mode","setup"],60)
    weir_spec={"image":plan["image_id"],"limits":plan["resources"]["weir"],"extra":net+["--network-alias","weir","--read-only","--mount",f"type=bind,source={f.root/'node.json'},target=/node.json,readonly"],"command":["-config","/node.json"]}
    f.create("weir",weir_spec)
    for role,observed in (("observer-weir","weir"),("observer-es","es")):
        cid=f.containers[f.owner+"-"+observed]
        extra=["--network","container:"+cid,"--pid","container:"+cid,"--user","0:0","--cap-add","SYS_PTRACE"]+common
        command=["-mode","observe","-pid","1"]
        if observed=="weir":command+=["-diagnostics"]
        observer_spec={"image":f.image,"limits":plan["resources"][role.replace("-","_")],"extra":extra,"command":command}
        f.create(role,observer_spec)
    f.run(["docker","exec",f.containers[f.owner+"-weir"],"/weir","-probe","ready"],10)
    time.sleep(3)
    samples=read_observer(f,"weir")
    if not samples or not any(s.get("metrics") for s in samples):
        raise RuntimeError("observer not available before load")
    client_sample=json.loads(f.run(["docker","exec",f.containers[f.owner+"-client"],"/client","-mode","snapshot"]).stdout)
    baseline={"weir":samples[-1],"es":read_observer(f,"es")[-1],"client":client_sample}
    for role,sample in baseline.items():
        limit=plan["resources"][role]
        if sample.get("errors") or int(sample["files"]["memory.max"])!=limit["memory_mib"]*1024**2 or int(sample["files"]["memory.swap.max"])!=0 or sample["files"]["cpuset.cpus.effective"].strip()!=limit["cpuset"]:
            raise RuntimeError("actual cgroup/affinity profile mismatch: "+role)
        quota,period=map(int,sample["files"]["cpu.max"].split())
        if quota/period!=limit["cpu"]:
            raise RuntimeError("CPU quota mismatch")
    f.save("resource-baseline.json",baseline)


def read_observer(f,role):
    if not hasattr(f,"observations"):
        f.observations={}
    samples=f.observations.setdefault(role,[])
    cid=f.containers[f.owner+"-observer-"+role]
    args=["docker","logs",cid]
    if samples:
        args=["docker","logs","--since",samples[-1]["time"],cid]
    result=f.run(args,10)
    entries=[json.loads(line) for line in result.stdout.splitlines() if line]
    if not samples:
        if not entries or "exe_sha256" not in entries[0]:
            raise RuntimeError("missing observer process identity")
        if role=="weir" and entries[0]["exe_sha256"]!="9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32":
            raise RuntimeError("running product identity")
        f.save(role+"-process-identity.json",entries[0])
    for entry in entries:
        if "time" in entry and (not samples or timestamp(entry["time"])>timestamp(samples[-1]["time"])):
            samples.append(entry)
    if len(samples)>1350:
        raise RuntimeError("resource sample bound")
    f.save(role+"-samples.jsonl","".join(json.dumps(s)+"\n" for s in samples))
    return samples


class Calibration:
    def __init__(self,f,plan):
        self.f,self.plan=f,plan
        self.results=[]
        self.planned=self.mutations=self.load_seconds=0

    def trial(self,name,rate,seconds,**options):
        direct=options.get("direct",False)
        recovery=options.get("recovery",0)
        warm=options.get("warm",20)
        planned=rate*(warm+seconds)+recovery*120
        self.planned+=planned;self.mutations+=planned//10+1000;self.load_seconds+=warm+seconds+(120 if recovery else 0)
        if self.planned>5000000 or self.mutations>500000 or self.load_seconds>2100:
            raise RuntimeError("cumulative calibration budget")
        f=self.f
        command=["docker","exec",f.containers[f.owner+"-client"],"/client","-mode","trial","-prefix",name,"-rate",str(rate),"-seconds",str(seconds),"-warm",str(warm)]
        if not direct:command += ["-target","weir:7447"]
        if recovery:command += ["-recovery-rate",str(recovery)]
        result=f.run(command,360,check=False)
        f.save(name+".jsonl",result.stdout)
        entries=[json.loads(line) for line in result.stdout.splitlines() if line]
        if result.returncode:
            raise RuntimeError(f"trial {name} exit {result.returncode}; retained command evidence")
        audits=[e for e in entries if e["type"]=="audit"]
        if len(audits)!=(2 if recovery else 1) or any(a["error"]!="<nil>" for a in audits):
            raise RuntimeError("full audit missing/failed")
        weir=read_observer(f,"weir");db=read_observer(f,"es")
        for samples in (weir,db):
            for s in samples:
                if s.get("errors"):
                    raise RuntimeError("resource observation failure")
        resource_gate(weir,False)
        resource_gate(db,False)
        for role in ("weir","es","client","observer-weir","observer-es"):
            obj=f.owned(f.owner+"-"+role)
            if not obj["State"]["Running"] or obj["State"]["OOMKilled"] or obj["RestartCount"]:
                raise RuntimeError("container stopped/OOM/restart")
        clients=[e["sample"] for e in entries if e["type"]=="client_sample"]
        trials=[e["trial"] for e in entries if e["type"]=="trial"]
        report=evaluate(trials[0],weir,clients,db)
        report.update(direct=direct,audits=[{k:v for k,v in a.items() if k!="ledger"} for a in audits])
        if recovery:
            rt=trials[1];passing=[not window_gate(w) for w in rt["ten_second_windows"]]
            recovered=None
            for n in range(min(3,len(passing))):
                begin=timestamp(rt["start"])+n*10
                tail=[s for s in weir if timestamp(s["time"])>=begin]
                reasons,_=resource_gate(tail)
                if all(passing[n:]) and not reasons:
                    recovered=(n+1)*10
                    break
            report["recovery"]={"rate":recovery,"seconds":recovered,"pass":recovered is not None,"windows":rt["ten_second_windows"],"measure":rt["measure"]}
            report["overload_applied"] = trials[0]["measure"]["all"]["started"]==trials[0]["measure"]["all"]["planned"] and trials[0]["measure"]["all"]["client_drop"]==0
        self.results.append(report)
        f.save("results.json",self.results)
        print(json.dumps({"phase":name,"rate":rate,"pass":report["pass"],"reasons":report["reasons"]}),flush=True)
        return report

    def run(self):
        candidate=None
        for rate in self.plan["rates"]:
            result=self.trial("search-"+str(rate),rate,60)
            if not result["pass"]:break
            candidate=rate
        if candidate is None:
            return {"candidate_rps":None,"status":"no qualifying search point"}
        confirmed=False
        for attempt in range(2):
            rounds=[self.trial(f"confirm-{attempt}-{i}",candidate,120) for i in range(3)]
            if all(r["pass"] for r in rounds):
                confirmed=True;break
            index=self.plan["rates"].index(candidate)
            if attempt or index==0:break
            candidate=self.plan["rates"][index-1]
        if not confirmed:
            return {"candidate_rps":None,"status":"confirmation failed; failures retained"}
        overload=self.trial("overload",candidate*2,30,recovery=candidate*7//10,warm=0)
        for i,rate in enumerate((candidate//2,candidate,candidate*2)):
            self.trial("direct-"+str(i),rate,60,direct=True)
        for i in range(2):
            self.trial("direct-confirm-"+str(i),candidate,120,direct=True)
        return {"candidate_rps":candidate,"kind":"sustainable measured profile lower bound; not Weir maximum","status":"calibration execution pending independent acceptance","confirmation_prefixes":[r["prefix"] for r in rounds],"stable_rps_70_percent":candidate*7//10,"overload_rps_2x":candidate*2,"overload_applied":overload["overload_applied"],"recovery":{k:v for k,v in overload["recovery"].items() if k not in ("windows","measure")}}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--evidence",required=True);parser.add_argument("--owner",required=True)
    parser.add_argument("--artifact",type=Path,default=REPO/"dist/m20/first")
    args=parser.parse_args()
    f=Fixture(args.evidence,args.owner)
    plan=json.loads((REPO/"scripts/capacity-plan.json").read_text())
    result={"schema_version":1,"status":"failed","profile":plan,"unknown":["independent acceptance","24h leak freedom","Weir goroutine census","physical host exclusivity","other profiles/platforms/backends"]}
    error=None
    def interrupted(signum,frame):
        raise InterruptedError(f"signal {signum}")
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
    try:
        f.preflight()
        result["plan_sha256"]=prepare(f,plan,args.artifact)
        start(f,plan)
        calibration=Calibration(f,plan)
        result.update(calibration.run())
        result["counts"]={"planned":calibration.planned,"mutations_with_seed":calibration.mutations,"load_seconds":calibration.load_seconds}
        time.sleep(12)
        final=read_observer(f,"weir")[-1]
        f.save("quiescent.json",final)
        m=prom(final["metrics"])
        for name in ("pending_entries","result_reserved_entries","active_executions","live_sessions"):
            if metric(m,"weir_store_"+name)!=0:raise RuntimeError("quiescence ledger")
    except BaseException as exc:
        error=str(exc);result["error"]=error
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.signal(signal.SIGINT,signal.SIG_IGN)
        clean=f.cleanup();result["cleanup_confirmed"]=clean
        try:
            after=f.inventory();f.save("inventory-final.json",after)
            if f.first:
                diff=inventory_diff(f.first,after);result["inventory_difference"]=diff
                if diff["nondefault_changed"] or diff["default_bridge_changed"]:
                    error=error or "inventory changed during active fixture; not accepted"
                    result["error"]=error
        except BaseException as exc:
            error=error or str(exc);result["inventory_error"]=str(exc)
        result["elapsed_seconds"]=time.monotonic()-f.started
        result["exit_code"]=int(bool(error) or not clean)
        f.save("capacity-baseline.json",result)
        manifest={str(p.relative_to(f.root)):{"bytes":p.stat().st_size,"sha256":sha(p)} for p in sorted(f.root.rglob("*")) if p.is_file()}
        f.save("manifest.json",manifest)
    print(json.dumps({"exit_code":result["exit_code"],"evidence":str(f.root),"error":error,"candidate_rps":result.get("candidate_rps")}),flush=True)
    return result["exit_code"]


if __name__=="__main__":
    raise SystemExit(main())
