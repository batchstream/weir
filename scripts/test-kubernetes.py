#!/usr/bin/env python3
"""Explicit, bounded M21 fixture. Creates three owned kind nodes; always cleans them."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import time

from kubernetes_fixture import Fixture, REPO

IMAGE_ID = "sha256:caa699e6ca172cbfa24ed4d311f4cb346817a354b05df52601abd954edcb4dab"
BINARY = "9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32"


def prepare(f, artifact, profile):
    receipt = json.loads((artifact/"receipt.json").read_text())
    assert receipt["source"] == profile["artifact_source"]
    for name, digest in receipt["inputs"].items():
        assert hashlib.sha256((REPO/name).read_bytes()).hexdigest() == digest, name
    f.save("artifact-reuse.json", {"source":receipt["source"], "inputs":receipt["inputs"], "image_id":IMAGE_ID,"binary":BINARY})
    for name,digest in {"kind-darwin-arm64":"0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f",
                        "kubectl":"c9e4f713d6fee0043a3d835cca13077cda2bc0973840eb9779360df0b5bdfc69"}.items():
        assert hashlib.sha256((REPO/".tools/m20"/name).read_bytes()).hexdigest()==digest
    env={k:os.environ[k] for k in ("PATH","HOME","TMPDIR") if k in os.environ}
    env.update(PATH=str(REPO/".tools/go1.27.1/bin")+":"+env["PATH"],GOENV="off",GOTOOLCHAIN="local",GOWORK="off",GOPROXY="off",GOSUMDB="off",CGO_ENABLED="0",WEIR_KUBE_INTEGRATION="1")
    for arch,output in (("darwin","client-host"),("linux","client")):
        result=subprocess.run(["go","build","-tags","integration","-trimpath","-buildvcs=false","-o",str(f.root/output),"./internal/testutil/testkube"],cwd=REPO,env=dict(env,GOOS=arch,GOARCH="arm64"),capture_output=True,text=True,timeout=120)
        f.save("build-"+arch+".log",result.stdout+result.stderr)
        assert result.returncode==0
    for concurrency in (1,2):
        cfg=json.loads((REPO/"deploy/kubernetes/node.example.json").read_text())
        cfg["memory_mib"]=256
        cfg["services"][0]["local"]["concurrency"]=concurrency
        filename=f.root/f"node-c{concurrency}.json"
        f.save(filename.name,cfg)
        result=subprocess.run([str(f.root/"client-host"),"-validate",str(filename)],env=env,capture_output=True,text=True,timeout=10)
        assert result.returncode==0,result.stderr
    raw=(f.root/"client").read_bytes()
    f.save("client-identity.json",{"sha256":hashlib.sha256(raw).hexdigest(),"source_head":f.run(["git","rev-parse","HEAD"]).stdout.strip(),"inputs":{str(p.relative_to(REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((REPO/"internal/testutil/testkube").glob("*.go"))}})
    tarpath=f.root/"client.tar"
    with tarfile.open(tarpath,"w") as archive:
        entry=tarfile.TarInfo("client");entry.size=len(raw);entry.mode=0o555
        archive.addfile(entry,io.BytesIO(raw))
    client_image=f.owner+"-client:local"
    f.run(["docker","import","--platform","linux/arm64","--change",'ENTRYPOINT ["/client"]',"--change","USER 65532:65532","--change","LABEL weir.fixture="+f.owner,str(tarpath),client_image])
    backend="weir-m20-es:8.19.22"
    for name,image in (("client",client_image),("es",backend)):
        obj=json.loads(f.run(["docker","image","inspect","--format","{{json .}}",image]).stdout)
        assert obj["Architecture"]=="arm64" and obj["Os"]=="linux"
        if name=="es": assert obj["Id"]==profile["backend"]["image_id"]
        f.save("image-"+name+".json",{k:obj[k] for k in ("Id","Architecture","Os","RepoDigests")})
    return client_image,backend


class Exercise:
    def __init__(self,f):
        self.f=f
        self.known={}
        self.log_followers={}
        self.observations=[]
        self.paused=None

    def client_args(self,mode,identity="smoke",target=None):
        args=self.f.kube("exec","client","--","/client","-mode",mode,"-id",identity)
        if target: args += ["-target",target]
        return args

    def client(self,mode,identity="smoke",target=None):
        result=self.f.run(self.client_args(mode,identity,target),65)
        self.f.save(f"client-{mode}-{identity}.log",result.stdout+result.stderr)
        return result.stdout

    def pods(self):
        return json.loads(self.f.run(self.f.kube("get","pods","-l","app=weir","-o","json")).stdout)["items"]

    def sample(self,label):
        f=self.f
        pods=self.pods()
        slices=json.loads(f.run(f.kube("get","endpointslice","-l","kubernetes.io/service-name=weir","-o","json")).stdout)
        sample={"time":time.time(),"label":label,"pods":pods,"endpoints":slices,"processes":[],"running_cri":{}}
        for node in (f.owner+"-worker",f.owner+"-worker2"):
            if node!=self.paused:
                result=f.run(["docker","exec",node,"crictl","ps","--state","Running","-o","json"])
                sample["running_cri"][node]=[c for c in json.loads(result.stdout)["containers"] if c["metadata"]["name"]=="weir" and c.get("labels",{}).get("io.kubernetes.pod.namespace")=="m21"]
        assert sum(len(cs) for cs in sample["running_cri"].values())<=6
        assert all(len(cs)<=4 for cs in sample["running_cri"].values())
        counts={}
        for pod in pods:
            uid=pod["metadata"]["uid"];name=pod["metadata"]["name"]
            node=pod["spec"].get("nodeName")
            status=pod["status"].get("containerStatuses",[])
            if not node or not status or not status[0].get("containerID"):continue
            assert node!=f.owner+"-control-plane"
            assert status[0]["restartCount"]==0
            assert status[0]["imageID"].endswith(IMAGE_ID.removeprefix("sha256:"))
            if node==self.paused:
                sample["processes"].append({"uid":uid,"node":node,"paused":True,"last_observation":self.known.get(uid)})
                continue
            cid=status[0]["containerID"].split("://")[1]
            result=f.run(["docker","exec",node,"crictl","inspect",cid],check=False)
            if result.returncode:continue
            cri=json.loads(result.stdout);pid=cri["info"]["pid"]
            item={"uid":uid,"pod":name,"node":node,"cid":cid,"pid":pid,"state":cri["status"]["state"],"config":pod["spec"]["volumes"][0]["secret"]["secretName"], "runtime_status":cri["status"]}
            if uid not in self.known and pid>0:
                identity=f.run(["docker","exec",node,"sha256sum",f"/proc/{pid}/exe"]).stdout
                assert identity.split()[0]==BINARY
                f.save(uid+"-identity.json",{"process":item,"binary":identity,"runtimeSpec":cri["info"]["runtimeSpec"],"startedAt":cri["status"]["startedAt"]})
                # Open the official log stream before termination. A file poll can
                # lose the final owner summary when kubelet removes a fast Pod.
                assert len(self.log_followers)<9
                self.log_followers[uid]=f.start(uid+"-follow",f.kube("logs","--follow","--timestamps=true",name,"--request-timeout=0"))
            self.known[uid]=item
            if pid>0:
                counts[node]=counts.get(node,0)+1
                command=["docker","exec",node,"nsenter","-t",str(pid),"-n","curl","--silent","--show-error","--max-time","0.5","http://127.0.0.1:7449/metrics"]
                result=f.run(command,5,check=False)
                if result.returncode==0:
                    item["metrics"]=result.stdout
                    f.save(f"{label}-{uid}.prom",result.stdout)
                    values={line.split()[0].split("{")[0]:float(line.split()[-1]) for line in result.stdout.splitlines() if line and not line.startswith("#")}
                    assert values["weir_backend_connections_owned"]<=values["weir_backend_connections_limit"]
                    assert values["weir_backend_connections_peak"]<=values["weir_backend_connections_limit"]
                    assert values["weir_store_active_executions"]<=values["weir_store_window_limit"]
                sockets=f.run(["docker","exec",node,"nsenter","-t",str(pid),"-n","ss","-tn"],5,check=False)
                item["sockets"]=sockets.stdout
            sample["processes"].append(item)
        assert sum(counts.values())<=6 and all(n<=4 for n in counts.values())
        # Logs are in owned kubelet files and survive container GC until fixture cleanup.
        for uid,item in self.known.items():
            if item["node"]==self.paused:continue
            path=f"/var/log/pods/m21_{item['pod']}_{uid}/weir/0.log"
            result=f.run(["docker","exec",item["node"],"cat",path],5,check=False)
            if result.returncode==0: f.save(uid+"-process.log",result.stdout)
        self.observations.append(sample)
        f.save("observations.json",self.observations)
        return pods

    def ready(self,number,timeout=120):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            pods=self.pods()
            live=[p for p in pods if not p["metadata"].get("deletionTimestamp")]
            if len(live)==number and all(p["status"].get("containerStatuses") and p["status"]["containerStatuses"][0]["ready"] for p in live):return live
            time.sleep(.5)
        raise TimeoutError("ready replicas")

    def barrier(self,children):
        until=time.monotonic()+8
        for name,child in children:
            while "ACTIVE mutation persisted" not in (self.f.root/(name+".log")).read_text():
                assert child.poll() is None,"active child ended before barrier"
                if time.monotonic()>until:raise TimeoutError("persisted barrier")
                time.sleep(.01)

    def roll(self,revision):
        f=self.f
        old=self.ready(3)
        load=f.start("load-"+revision,self.client_args("load",revision))
        children=[]
        for i,pod in enumerate(old):
            name=f"active-{revision}-{i}"
            target=pod["status"]["podIP"]+":7447"
            child=f.start(name,self.client_args("active",name,target))
            children.append((name,child))
        self.barrier(children)
        started=time.monotonic()
        patch={"spec":{"template":{"spec":{"volumes":[{"name":"config","secret":{"secretName":"weir-"+revision,"defaultMode":292}}]}}}}
        f.run(f.kube("patch","deployment","weir","--type=strategic","-p",json.dumps(patch)))
        old_uids={p["metadata"]["uid"] for p in old}
        # A new revision cannot start until every previous PID has gone, not merely Ready3.
        n=0
        while time.monotonic()-started<120:
            pods=self.sample(f"roll-{revision}-{n:03d}");n+=1
            current={p["metadata"]["uid"] for p in pods}
            if not old_uids & current and len(pods)==3 and all(p["status"].get("containerStatuses") and p["status"]["containerStatuses"][0]["ready"] for p in pods):break
            if n==1: self.client("db","overlap-"+revision)
            time.sleep(.2)
        else:raise TimeoutError("revision roll")
        for uid in old_uids:
            item=self.known[uid]
            result=f.run(["docker","exec",item["node"],"crictl","inspect",item["cid"]],check=False)
            if result.returncode==0: f.save(uid+"-exited.json",json.loads(result.stdout)["status"])
            result=f.run(["docker","exec",item["node"],"crictl","ps","--state","Running","-o","json"])
            assert item["cid"] not in {c["id"] for c in json.loads(result.stdout)["containers"]}
        f.save("roll-"+revision+".json",{"seconds":time.monotonic()-started,"old_uids":sorted(old_uids),"new_uids":sorted(current)})
        for name,child in children:f.wait(child,25)
        f.wait(load,65)
        self.client("refresh",revision)
        for name,_ in children:self.client("inspect",name)
        self.client("smoke","after-"+revision)
        self.sample("after-"+revision)

    def node_fault(self):
        f=self.f;fault=f.owner+"-worker2"
        target=next(p for p in self.ready(3) if p["spec"]["nodeName"]==fault)
        name="node-held"
        child=f.start(name,self.client_args("active",name,target["status"]["podIP"]+":7447"))
        self.barrier([(name,child)])
        obj=f.owner_node(fault)
        assert obj["Id"]==f.nodes[fault]["id"]
        started=time.monotonic();self.paused=fault
        try:
            f.run(["docker","pause",obj["Id"]],10)
            self.client("probe","paused-worker-request",target["status"]["podIP"]+":7447")
            self.client("probe","stale-service-request")
            found=False
            while time.monotonic()-started<90:
                node=json.loads(f.run(["kubectl","get","node",fault,"-o","json"]).stdout)
                ready=next(c for c in node["status"]["conditions"] if c["type"]=="Ready")
                f.save("fault-node-latest.json",node)
                if ready["status"]!="True":
                    self.sample("node-unreachable")
                    found=True
                    break
                time.sleep(2)
            assert found,"node NotReady not observed within frozen budget"
            # Wait for normal endpoint propagation without changing controller timers.
            for i in range(5):
                self.sample("node-endpoints-"+str(i))
                endpoints=[e for s in self.observations[-1]["endpoints"]["items"] for e in s.get("endpoints",[])]
                if not any(e.get("nodeName")==fault and e["conditions"].get("ready") for e in endpoints):break
                time.sleep(1)
            else:raise AssertionError("faulted endpoint still ready")
            self.client("smoke","healthy-during-node")
        finally:
            # Separate fault restoration from whole-fixture cleanup.
            f.owner_node(fault)
            f.run(["docker","unpause",obj["Id"]],10)
            self.paused=None
            f.save("pause-seconds.json",time.monotonic()-started)
        f.wait(child,5)
        f.run(["kubectl","wait","--for=condition=Ready","node/"+fault,"--timeout=120s"],130)
        self.ready(3)
        self.client("refresh","node")
        self.client("inspect",name)
        self.client("smoke","node-recovered")
        self.client("smoke","node-direct-recovered",target["status"]["podIP"]+":7447")
        self.sample("node-recovered")


def workloads(f,images,artifact,profile):
    client_image,backend=images
    workers=[f.owner+"-worker",f.owner+"-worker2"]
    weir="docker.io/library/weir-m21:"+profile["artifact_source"]
    for node in workers:
        with (artifact/"weir-linux.oci.tar").open("rb") as archive:
            result=subprocess.run(["docker","exec","-i",node,"ctr","-n","k8s.io","images","import","--platform","linux/arm64","--index-name",weir,"-"],env=f.env,stdin=archive,capture_output=True,text=True,timeout=120)
        f.save(node+"-import.log",result.stdout+result.stderr);assert result.returncode==0
    f.run(["kind","load","docker-image","--name",f.owner,"--nodes",workers[0],client_image,backend],150)
    f.apply("namespace",{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"m21","labels":{"weir.fixture":f.owner}}})
    security={"runAsNonRoot":True,"runAsUser":1000,"runAsGroup":1000,"fsGroup":1000,"seccompProfile":{"type":"RuntimeDefault"}}
    env={"cluster.name":f.owner,"discovery.type":"single-node","xpack.security.enabled":"false","action.auto_create_index":"false","ES_JAVA_OPTS":"-Xms384m -Xmx384m"}
    backendpod={"apiVersion":"v1","kind":"Pod","metadata":{"name":"elasticsearch","labels":{"app":"elasticsearch","weir.fixture":f.owner}},"spec":{"nodeSelector":{"kubernetes.io/hostname":workers[0]},"automountServiceAccountToken":False,"terminationGracePeriodSeconds":15,"securityContext":security,"containers":[{"name":"elasticsearch","image":backend,"imagePullPolicy":"Never","env":[{"name":k,"value":v} for k,v in env.items()],"securityContext":{"allowPrivilegeEscalation":False,"capabilities":{"drop":["ALL"]}},"resources":{"requests":{"cpu":"500m","memory":"1536Mi"},"limits":{"cpu":"1","memory":"1536Mi"}},"readinessProbe":{"httpGet":{"path":"/","port":9200},"periodSeconds":2,"timeoutSeconds":2,"failureThreshold":60},"volumeMounts":[{"name":"data","mountPath":"/usr/share/elasticsearch/data"}]}],"volumes":[{"name":"data","emptyDir":{"sizeLimit":"512Mi"}}]}}
    f.apply("backend",backendpod)
    f.apply("backend-service",{"apiVersion":"v1","kind":"Service","metadata":{"name":"elasticsearch"},"spec":{"selector":{"app":"elasticsearch"},"ports":[{"port":9200,"targetPort":9200}]}})
    f.run(f.kube("wait","--for=condition=Ready","pod/elasticsearch","--timeout=150s"),160)
    security={"runAsNonRoot":True,"runAsUser":65532,"runAsGroup":65532,"seccompProfile":{"type":"RuntimeDefault"}}
    f.apply("client",{"apiVersion":"v1","kind":"Pod","metadata":{"name":"client","labels":{"weir.fixture":f.owner}},"spec":{"nodeSelector":{"kubernetes.io/hostname":workers[0]},"automountServiceAccountToken":False,"restartPolicy":"Never","securityContext":security,"containers":[{"name":"client","image":client_image,"imagePullPolicy":"Never","env":[{"name":"WEIR_KUBE_INTEGRATION","value":"1"}],"resources":{"requests":{"cpu":"100m","memory":"256Mi"},"limits":{"cpu":"500m","memory":"256Mi"}},"securityContext":{"readOnlyRootFilesystem":True,"allowPrivilegeEscalation":False,"capabilities":{"drop":["ALL"]}}}]}})
    f.run(f.kube("wait","--for=condition=Ready","pod/client","--timeout=45s"),55)
    exercise=Exercise(f)
    exercise.client("setup")
    for c in (1,2):
        # Only a new credential-free config is sent. Existing Secret content is never read.
        name=f"weir-c{c}"
        f.run(f.kube("create","secret","generic",name,"--from-file=node.json="+str(f.root/f"node-c{c}.json")))
        f.run(f.kube("patch","secret",name,"--type=merge","-p",'{"immutable":true}'))
    manifest=json.loads((REPO/"deploy/kubernetes/weir.json").read_text())
    deployment=manifest["items"][0]
    patch=json.loads((REPO/"deploy/kubernetes/three-replicas.patch.json").read_text())["spec"]
    deployment["spec"]["strategy"]=patch["strategy"]
    spec=deployment["spec"]["template"]["spec"]
    spec.update(patch["template"]["spec"])
    spec["containers"][0]["image"]=weir
    spec["containers"][0]["imagePullPolicy"]="Never"
    spec["containers"][0]["env"]=[{"name":"GODEBUG","value":profile["diagnostics"]["GODEBUG"]}]
    spec["containers"][0]["resources"]={"requests":{"cpu":"100m","memory":"384Mi"},"limits":{"cpu":"500m","memory":"384Mi"}}
    spec["volumes"][0]["secret"]["secretName"]="weir-c2"
    f.save("weir.json",manifest)
    f.save("dry-run.log",f.run(f.kube("apply","--dry-run=server","-f",str(f.root/"weir.json"))).stdout)
    f.run(f.kube("apply","-f",str(f.root/"weir.json")))
    exercise.ready(1)
    return exercise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence",required=True,type=Path)
    parser.add_argument("--owner",required=True)
    parser.add_argument("--artifact",type=Path,default=REPO/"dist/m20/first")
    args=parser.parse_args()
    assert os.environ.get("WEIR_KUBE_INTEGRATION")=="1","explicit opt-in required"
    assert re.fullmatch(r"weir-m21-[a-z0-9-]{1,32}",args.owner)
    f=Fixture(args.evidence,args.owner)
    assert not (f.root/"owned-nodes.json").exists(),"use fresh evidence/owner"
    profile=json.loads((REPO/"scripts/kubernetes-smoke.json").read_text());profile["owner"]=args.owner
    f.save("freeze.json",profile)
    failure=None
    try:
        images=prepare(f,args.artifact.resolve(),profile)
        f.bootstrap(profile)
        f.deadline=time.monotonic()+900
        started=time.monotonic()
        exercise=workloads(f,images,args.artifact.resolve(),profile)
        exercise.client("smoke","initial")
        exercise.sample("initial")
        hold=f.start("hold",exercise.client_args("hold","old-stream"))
        until=time.monotonic()+8
        while "HOLD index=0" not in (f.root/"hold.log").read_text():
            assert time.monotonic()<until and hold.poll() is None
            time.sleep(.05)
        exercise.sample("old-stream")
        f.run(f.kube("scale","deployment/weir","--replicas=3"))
        pods=exercise.ready(3)
        assert len({p["spec"]["nodeName"] for p in pods})==2
        exercise.sample("scaled")
        for i in range(6):exercise.client("smoke",f"new-{i}")
        exercise.sample("new-connections")
        f.wait(hold)
        exercise.sample("hold-finished")
        exercise.client("db","steady")
        exercise.roll("c1")
        exercise.roll("c2")
        exercise.node_fault()
        exercise.client("db","recovered")
        exercise.client("audit","final")
        time.sleep(12)
        exercise.sample("quiescent")
        f.run(f.kube("scale","deployment/weir","--replicas=0"))
        until=time.monotonic()+30
        while exercise.pods():
            exercise.sample("final-drain")
            assert time.monotonic()<until
            time.sleep(.2)
        exercise.sample("zero")
        exercise.client("db","zero")
        for child in exercise.log_followers.values():
            try: child.wait(timeout=5)
            except subprocess.TimeoutExpired: child.terminate(); child.wait(timeout=3)
        for uid,item in exercise.known.items():
            log=(f.root/(uid+"-follow.log")).read_text()
            assert "backend_connections_closed" in log and "owned=0" in log
        f.save("final-pods.json",json.loads(f.run(f.kube("get","pods","-o","json")).stdout))
        f.save("events.log",f.run(f.kube("get","events","--sort-by=.lastTimestamp")).stdout)
        for node in f.nodes:
            f.save(node+"-final.log",f.run(["docker","exec",node,"sh","-c","cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.events; cat /sys/fs/cgroup/pids.current"]).stdout)
        f.save("functional-seconds.json",time.monotonic()-started)
        print("functional PASS",flush=True)
    except BaseException as exc:
        failure=exc
        f.save("failure.txt",repr(exc))
        f.deadline=time.monotonic()+30
        if (f.root/"kubeconfig").exists():
            for label,command in (("pods",f.kube("get","pods","-o","json")),("events",f.kube("get","events")),("backend",f.kube("logs","elasticsearch","--tail=100"))):
                try:
                    result=f.run(command,8,check=False)
                    f.save("failure-"+label+".log",result.stdout+result.stderr)
                except Exception as observation_error: f.save("failure-"+label+".log",str(observation_error))
        raise
    finally:
        clean=f.cleanup()
        if not clean and failure is None:raise RuntimeError("cleanup failed; see cleanup.json")


if __name__=="__main__":main()
