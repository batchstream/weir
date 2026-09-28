#!/usr/bin/env python3
"""One frozen M26R same-Pod loopback attempt using only the M24 artifacts."""
import argparse
from datetime import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import time

import eks_pacing as common
from eks_pacing_report import IMAGES, SOURCE, require, node_gate, resources, validate_window, COUNTS, HISTOGRAMS, bucket_map
from eks_resources import pod_requests
from capacity_report import select_samples, cpu_usage, weir_metrics, timestamp
from capacity_contract import Budget

TARGET = dict(context="arn:aws:eks:us-west-1:956540890581:cluster/data-team", cluster="data-team", region="us-west-1")
ES = dict(reference="docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237",
          manifest="sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237",
          config="sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160")
MINIMUM = dict(cpu=7, memory=5632*1024**2, pods=3, **{"ephemeral-storage":5*1024**3})
FILES = common.FILES + ("scripts/eks_resources_test.py", "scripts/eks_loopback.py", "scripts/eks_loopback_test.py",
                        "scripts/eks_socket_diagnostics_test.py", "scripts/fixtures/eks-loopback-check-before-m26r3.sh",
                        "scripts/eks_loopback_admission_test.py", "scripts/eks_loopback_cli_fixture.py",
                        "scripts/fixtures/eks-loopback-admitted-job.json", "scripts/fixtures/README.md",
                        "scripts/eks_loopback_check.sh", "scripts/eks_loopback_bootstrap.sh", "scripts/eks_loopback_diagnostic.sh",
                        "scripts/capacity_report_test.py", "deploy/kubernetes/node.example.json")
CURL = ["curl", "-q", "--silent", "--show-error", "--fail", "--noproxy", "*", "--proxy", "", "--proto", "=http",
        "--max-redirs", "0", "--retry", "0", "--connect-timeout", "1", "--max-time", "3", "--max-filesize", "262144"]


def configuration():
    cfg = json.loads((common.REPO/"deploy/kubernetes/node.example.json").read_text())
    cfg["application"] = "127.0.0.1:7447"
    cfg["services"][0]["local"].update(concurrency=4, batch_operations=16)
    cfg["services"][0]["local"]["search"]["url"] = "http://127.0.0.1:9200"
    return cfg


def objects(plan):
    labels = {common.LABEL:plan["owner"]}
    security = dict(runAsNonRoot=True, runAsUser=65532, runAsGroup=65532, allowPrivilegeEscalation=False,
                    readOnlyRootFilesystem=True, capabilities=dict(drop=["ALL"]), seccompProfile=dict(type="RuntimeDefault"))
    es_security = dict(security, runAsUser=1000, runAsGroup=0, readOnlyRootFilesystem=False)
    mount = dict(name="configuration", mountPath="/qualification", readOnly=True)
    weir_res = dict(cpu="2", memory="1024Mi", **{"ephemeral-storage":"256Mi"})
    client_res = dict(cpu="1", memory="512Mi", **{"ephemeral-storage":"256Mi"})
    es_res = dict(cpu="3", memory="3072Mi", **{"ephemeral-storage":"2Gi"})
    weir = dict(name="weir", image=IMAGES["version"]["reference"], imagePullPolicy="Always", command=["/weir"],
                args=["-config","/qualification/node.json"], securityContext=security,
                resources=dict(requests=weir_res, limits=weir_res), volumeMounts=[mount])
    weir["readinessProbe"] = dict(exec=dict(command=["/weir","-probe","ready"]), initialDelaySeconds=0, periodSeconds=2, timeoutSeconds=2, successThreshold=1, failureThreshold=60)
    client = dict(name="qualification", image=IMAGES["tool"]["reference"], imagePullPolicy="Always", command=["/qualification"],
                  args=["-mode","idle"], securityContext=security, resources=dict(requests=client_res,limits=client_res),
                  env=[dict(name="GOMAXPROCS",value="1"),dict(name="WEIR_CAPACITY_INTEGRATION",value="1")],
                  volumeMounts=[dict(mount, mountPath="/config")])
    pod_ip = dict(name="POD_IP",valueFrom=dict(fieldRef=dict(apiVersion="v1",fieldPath="status.podIP")))
    args = ["eswrapper"]+["-E"+value for value in ("network.host=127.0.0.1", "http.host=127.0.0.1", "transport.host=127.0.0.1",
             "http.port=9200", "transport.port=9300", "discovery.type=single-node", "discovery.seed_hosts=[]", "action.auto_create_index=false",
             "xpack.security.enabled=false", "xpack.security.enrollment.enabled=false", "xpack.ml.enabled=false",
             "ingest.geoip.downloader.enabled=false", "node.store.allow_mmap=false", "cluster.name=weir-m26r", "node.name=loopback")]
    probe = dict(exec=dict(command=CURL+["http://127.0.0.1:9200/"]), initialDelaySeconds=0, periodSeconds=2,
                 timeoutSeconds=4, successThreshold=1, failureThreshold=60)
    metadata_disabled = dict(name="AWS_EC2_METADATA_DISABLED",value="true")
    es = dict(name="elasticsearch",image=ES["reference"], imagePullPolicy="Always", restartPolicy="Always", args=args,
              securityContext=es_security,resources=dict(requests=es_res,limits=es_res),startupProbe=probe,
              env=[dict(name="ES_JAVA_OPTS",value="-Xms1024m -Xmx1024m"),metadata_disabled,pod_ip],
              volumeMounts=[mount,dict(name="data",mountPath="/usr/share/elasticsearch/data")])
    bootstrap = dict(name="bootstrap", image=ES["reference"],imagePullPolicy="Always", securityContext=es_security,
                     resources=dict(requests=client_res,limits=client_res), env=[pod_ip], volumeMounts=[mount],
                     command=["/usr/bin/timeout","30","/bin/bash","--noprofile","--norc","/qualification/bootstrap.sh"])
    spec = dict(nodeName=plan["node"]["name"],restartPolicy="Never",activeDeadlineSeconds=900,terminationGracePeriodSeconds=20,
                automountServiceAccountToken=False,enableServiceLinks=False,dnsPolicy="None",dnsConfig=dict(nameservers=["127.0.0.1"]),
                securityContext=dict(runAsNonRoot=True,seccompProfile=dict(type="RuntimeDefault"),fsGroup=1000,fsGroupChangePolicy="OnRootMismatch"),
                containers=[weir,client],initContainers=[es,bootstrap],
                volumes=[dict(name="configuration",configMap=dict(name="configuration",defaultMode=292)),dict(name="data",emptyDir=dict(sizeLimit="1Gi"))])
    metadata = dict(name="loopback",namespace=plan["namespace"],labels=labels)
    job = dict(apiVersion="batch/v1",kind="Job",metadata=metadata,
               spec=dict(completions=1,parallelism=1,backoffLimit=0,activeDeadlineSeconds=900,template=dict(metadata=dict(labels=labels),spec=spec)))
    data = {"node.json":json.dumps(configuration()),"loopback-check.sh":(common.REPO/"scripts/eks_loopback_check.sh").read_text(),
            "bootstrap.sh":(common.REPO/"scripts/eks_loopback_bootstrap.sh").read_text()}
    config = dict(apiVersion="v1",kind="ConfigMap",metadata=dict(metadata,name="configuration"),data=data)
    result = dict(job=job,config=config)
    return result


def pod_check(pod, options):
    common.pod_identity(pod, options)
    template = options["template"]
    common.admitted_spec(pod["spec"],template["spec"]["template"]["spec"],pod=True)
    status = pod.get("status") or {}
    require(not status.get("ephemeralContainerStatuses"),"injected ephemeral runtime")
    outcomes = container_outcomes(status)
    require(not outcomes["failures"], "container failure: "+json.dumps(outcomes, sort_keys=True))
    complete = True
    for key,names in (("containerStatuses",("weir","qualification")),("initContainerStatuses",("elasticsearch","bootstrap"))):
        states = status.get(key,[])
        require(len({s["name"] for s in states})==len(states) and set(s["name"] for s in states)<=set(names),"runtime container names")
        complete &= len(states)==len(names)
        for state in states:
            name = state["name"]
            image = ES if name in ("elasticsearch","bootstrap") else IMAGES["version" if name=="weir" else "tool"]
            if state.get("imageID"):
                require(state["imageID"] in {image["reference"],image["reference"].split("@")[0]+"@"+image["manifest"]},"imageID drift")
            live = bool(state.get("state",{}).get("running"))
            terminated = state.get("state",{}).get("terminated")
            ready = bool(terminated) if name=="bootstrap" else live
            if name=="elasticsearch": ready &= state.get("started") is True
            if name=="weir": ready &= state.get("ready") is True
            if ready:
                require(state.get("imageID") and state.get("containerID","").startswith("containerd://"),"runtime identity missing")
            complete &= ready
    require(status.get("phase") not in ("Succeeded","Failed"),"service Pod ended")
    if complete:
        projected = dict(uid=pod["metadata"]["uid"], nodeName=pod["spec"]["nodeName"], phase=status["phase"], deleting=None,
                         resources=pod["spec"].get("resources"), statusResources=status.get("resources"),
                         allocatedResources=status.get("allocatedResources"), overhead=pod["spec"].get("overhead"),
                         resize=status.get("resize"), resizeConditions=[c for c in status.get("conditions",[]) if c["type"].startswith("PodResize")],
                         unsupported=False, containers=pod["spec"]["containers"], initContainers=pod["spec"]["initContainers"],
                         containerStatuses=status["containerStatuses"], initContainerStatuses=status["initContainerStatuses"])
        effective, _ = pod_requests(projected)
        wanted = dict(cpu=6, memory=4608*1024**2, **{"ephemeral-storage":2560*1024**2})
        require(effective==wanted,"actual admitted resource peak drift")
    return complete


def container_outcomes(status):
    """Report all observations before rejecting; finish order is not causality."""
    rows = []
    failures = []
    finished = []
    for key in ("initContainerStatuses", "containerStatuses"):
        for state in status.get(key, []):
            terminal = state.get("state", {}).get("terminated")
            row = dict(name=state["name"], restartCount=state.get("restartCount"),
                       lastState=state.get("lastState"), state=state.get("state"))
            rows.append(row)
            failed = state.get("restartCount") != 0 or bool(state.get("lastState"))
            if terminal:
                failed |= not (state["name"] == "bootstrap" and terminal.get("exitCode") == 0 and terminal.get("reason") == "Completed")
            if failed:
                failures.append(state["name"])
                try:
                    moment = datetime.fromisoformat(terminal["finishedAt"].replace("Z", "+00:00"))
                    require(moment.tzinfo is not None, "missing timezone")
                    # A previous restart may have failed earlier than this state.
                    require(state.get("restartCount") == 0 and not state.get("lastState"), "prior state")
                    finished.append((moment, state["name"]))
                except (AttributeError, KeyError, TypeError, ValueError):
                    pass
    earliest = None
    if finished and len(finished) == len(failures):
        finished.sort()
        if len(finished) == 1 or finished[0][0] < finished[1][0]:
            earliest = finished[0][1]
    result = dict(containers=sorted(rows, key=lambda row: row["name"]), failures=sorted(failures),
                  earliest_failed_finish=earliest, ordering="finishedAt only; cause and cleanup attribution unknown")
    return result


class Run(common.Run):
    node_minimum = MINIMUM

    def job_template(self, step):
        require(step=="loopback","fixed single Job")
        return objects(self.plan)["job"]

    def current_pod(self):
        current_job = self.selected_object("Job",self.job["name"])
        common.owner_check(current_job,self.job)
        common.job_check(current_job,self.template)
        require(not current_job["metadata"].get("deletionTimestamp"),"Job deleting")
        if self.pod_entry:
            names = [self.pod_entry["name"]]
        else:
            names = self.kube(["get","pods","-o",'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'],self.plan["namespace"]).split()
            require(len(names)<=1,"extra Pod")
        if not names:
            self.job_events(self.job)
            return None
        pod = self.selected_object("Pod",names[0])
        require(pod is not None and not pod["metadata"].get("deletionTimestamp"),"Pod missing/deleting")
        refs = pod["metadata"].get("ownerReferences") or []
        expected = dict(uid=self.job["uid"],name=self.job["name"],kind="Job",apiVersion="batch/v1",controller=True)
        if not self.pod_entry and len(refs)==1 and all(refs[0].get(k)==v for k,v in expected.items()) and pod["metadata"].get("labels",{}).get(common.LABEL)==self.plan["owner"] and pod["metadata"]["namespace"]==self.plan["namespace"]:
            self.pod_entry = dict(kind="Pod",name=names[0],uid=pod["metadata"]["uid"],owner=self.plan["owner"])
            self.owned.append(self.pod_entry)
            self.save("owned.json",self.owned)
        self.save(f"pod-observation-{self.number}.json",pod)
        status = pod.get("status") or {}
        self.save(f"container-outcomes-{self.number}.json", container_outcomes(status))
        if any(s.get("containerID") for key in ("containerStatuses", "initContainerStatuses") for s in status.get(key, [])):
            self.runtime_evidence = True
        options = dict(job=self.job,template=self.template,pod_uid=self.pod_entry["uid"] if self.pod_entry else None)
        self.pod_ready = pod_check(pod,options)
        if self.pod_ready:
            identity = {s["name"]: dict(imageID=s["imageID"],containerID=s["containerID"]) for key in ("containerStatuses","initContainerStatuses") for s in pod["status"][key]}
            require(not hasattr(self,"identities") or self.identities==identity,"runtime identity changed")
            self.identities=identity
            self.save("runtime-identity.json",identity)
        else:
            self.job_events(self.job)
        return pod

    def monitor(self):
        require(self.current_pod() is not None and self.pod_ready,"runtime changed during trial")

    def exec_owned(self, container, command, timeout=25, *, monitor=False):
        pod = self.current_pod()
        require(pod is not None and self.pod_ready,"Pod not ready for exec")
        argv = ["kubectl","--context",self.target["context"],"--request-timeout=10s","--namespace",self.plan["namespace"],
                "exec",self.pod_entry["name"],"--container",container,"--"]+command
        self.last_exec_number = self.number+1
        return self.run(argv,timeout,monitor=monitor)

    def boundary(self):
        pod = self.current_pod()
        require(pod is not None and self.pod_ready,"runtime unavailable")
        entry = next(e for e in self.owned if e["kind"]=="ConfigMap" and e["name"]=="configuration")
        config = self.selected_object("ConfigMap","configuration")
        common.owner_check(config,entry)
        require(config["data"]==self.plan["objects"]["config"]["data"],"ConfigMap changed before load")
        address = ipaddress.ip_address(pod["status"]["podIP"])
        require(address.version==4 and not address.is_loopback and not address.is_unspecified,"own PodIP invalid")
        raw = self.exec_owned("elasticsearch",["/usr/bin/timeout","25","/bin/bash","--noprofile","--norc","/qualification/loopback-check.sh","main"])
        require(raw.endswith("loopback-check-complete\n") and "own-pod-ip="+str(address)+"\n" in raw,"loopback checks incomplete/PodIP drift")
        self.save(f"boundary-{self.number}.txt",raw)
        self.exec_owned("weir",["/weir","-probe","ready"])
        sample = json.loads(self.exec_owned("qualification",["/qualification","-mode","snapshot"]))
        self.save(f"client-snapshot-{self.number}.json",sample)
        network_check(sample["files"], require_listeners=True)
        return pod

    def diagnostics(self, name):
        metrics = self.exec_owned("elasticsearch",CURL+["http://127.0.0.1:7449/metrics"])
        why, owner, ingress = weir_metrics(metrics,True)
        self.save(name+"-metrics.txt",metrics)
        require(not why,"Weir Guard/ledger gate: "+str(why))
        stats = self.exec_owned("elasticsearch",CURL+["http://127.0.0.1:9200/_nodes/stats/process,jvm,os,fs,thread_pool,http?filter_path=nodes.*.process,nodes.*.jvm.mem,nodes.*.os.cpu,nodes.*.fs.io_stats,nodes.*.thread_pool.write,nodes.*.thread_pool.get,nodes.*.http.current_open,nodes.*.http.total_opened"])
        self.save(name+"-es-stats.json",stats)
        return dict(owner=owner,ingress=ingress)


def network_check(files, require_listeners=False):
    # The fixed helper captures TCP only; UDP is checked by the shell boundary.
    listeners = set()
    loop = {"0100007F","0000000000000000FFFF00000100007F"}
    for key in ("net/tcp","net/tcp6"):
        raw = files.get(key)
        evidence = dict(table=key, atomic=False, process="unknown", raw=raw[:65537] if isinstance(raw, str) else raw)
        context = json.dumps(evidence)
        require(isinstance(raw, str) and 0 < len(raw.encode()) <= 65536, "socket evidence missing/byte limit: "+context)
        lines=raw.splitlines()
        require(raw.endswith("\n") and len(lines) <= 256 and "local_address" in lines[0] and "inode" in lines[0], "socket evidence incomplete/header/line limit: "+context)
        for row in lines[1:]:
            if not row:
                continue
            parts=row.split(); require(len(parts)>=10,"short-row exit=33: "+context)
            local,remote,state=parts[1:4]
            details = json.dumps(dict(table=key, raw=row, local=local, remote=remote, state=state, uid=parts[7], inode=parts[9], process="unknown"))
            require(local.split(":")[0] in loop,"tcp-local-address exit=23: "+details+" snapshot="+context)
            require(remote.split(":")[0] in loop|{"00000000","0"*32},"tcp-peer-address exit=24: "+details+" snapshot="+context)
            if state=="0A": listeners.add(int(local.split(":")[1],16))
    require(listeners <= {7447,7449,9200,9300},"unexpected listener")
    require(not require_listeners or listeners=={7447,7449,9200,9300},"required listeners missing")


def trial_report(records, prefix):
    trials=[r for r in records if r["type"]=="trial"]
    audits=[r for r in records if r["type"]=="audit"]
    require(len(trials)==1 and trials[0]["run_error"]=="<nil>" and len(audits)==1,"trial/audit missing")
    t=trials[0]["trial"]
    wanted=dict(Rate=50,WarmSeconds=20,Seconds=20,Prefix=prefix,Workers=64,TimingOnly=False,LegacyExpiry=False)
    require(t["options"]==wanted and t["planned"]==2000,"trial contract")
    require(39 <= timestamp(t["end"])-timestamp(t["start"]) <= 43,"trial duration")
    reasons=[]
    for name in ("warm","measure"):
        reasons += [name+": "+r for r in validate_window(t[name])]
        require(t[name]["all"]["planned"]==1000 and t[name]["put"]["planned"]==100,"warm/measure count")
    windows=t["ten_second_windows"]
    require(len(windows)==2,"window count")
    for window in windows:
        reasons += validate_window(window)
        require(window["all"]["planned"]==500,"window offered count")
    for kind in ("all","read","put"):
        for count in COUNTS:
            require(t["measure"][kind][count]==sum(w[kind][count] for w in windows),"window counts")
        for h in HISTOGRAMS:
            combined={}
            for w in windows:
                for upper,n in bucket_map(w[kind][h]).items():combined[upper]=combined.get(upper,0)+n
            require(combined==bucket_map(t["measure"][kind][h]),"window histogram sum")
            require(t["measure"][kind][h]["max_ns"]==max(w[kind][h]["max_ns"] for w in windows),"window histogram maximum")
        for name in ("failures","drop_reasons"):
            merged={}
            for w in windows:
                for key,count in (w[kind][name] or {}).items():merged[key]=merged.get(key,0)+count
            require(merged==(t["measure"][kind][name] or {}),"window outcome sum")
    audit=audits[0]
    require(audit["error"]=="<nil>" and audit["prefix"]==prefix,"DB audit failure")
    require(audit["audit"]==dict(planned_writes=200,found_version1=200,applied=200,unknown_found=0,absent=0,pages=2),"DB audit counts")
    # Go []byte ledger is base64; its private applied encoding is 1.
    import base64
    require(base64.b64decode(audit["ledger"],validate=True)==bytes([1])*200,"mutation ledger")
    samples=[r["sample"] for r in records if r["type"] in ("client_start","client_sample")]
    select_samples(samples,t,warm=False)
    resource=resources(samples,network="loopback")
    for s in samples: network_check(s["files"],require_listeners=True)
    cpu=cpu_usage(samples,1)
    require(cpu["max_interval_fraction"]<.9,"client CPU limited")
    result=dict(prefix=prefix,passed=not reasons,reasons=reasons,resource=resource,client_cpu=cpu,audit=audit["audit"],
                warm=t["warm"],measure=t["measure"])
    return result


def prepare_context(run, owner):
    require(re.fullmatch(r"weir-qual-m26r-[a-z0-9-]{1,25}",owner) is not None,"owner syntax")
    cluster=json.loads(run.run(["aws","eks","describe-cluster","--name",TARGET["cluster"],"--region",TARGET["region"],
                               "--query","cluster.{arn:arn,name:name,version:version,status:status}","--output","json",
                               "--cli-connect-timeout","5","--cli-read-timeout","10","--no-cli-pager"]))
    require(cluster["arn"]==TARGET["context"] and cluster["status"]=="ACTIVE" and cluster["version"]=="1.36","cluster identity/version")
    require(not run.kube(["get","namespace",owner,"--ignore-not-found","-o","name"]).strip(),"namespace collision")
    nodes,used=run.nodes()
    candidates=[]; assessments=[]
    for node in sorted(nodes,key=lambda n:n["name"]):
        try:
            spare=node_gate(node,used.get(node["name"],dict(cpu=0,memory=0,pods=0,**{"ephemeral-storage":0})),minimum=MINIMUM)
            candidates.append((node,spare))
            assessments.append(dict(node=node,spare=spare,eligible=True))
        except (ValueError,KeyError) as exc:
            assessments.append(dict(node=node,eligible=False,error=str(exc)))
    run.save("node-assessment.json",assessments)
    require(candidates,"no suitable existing node under corrected resource model; no writes")
    selected,spare=candidates[0]
    for kind,verbs in dict(namespaces=("create","get","delete"),jobs=("create","get","list","delete"),pods=("create","get","list","delete"),
                           events=("list",),configmaps=("create","get","delete"),resourcequotas=("create","get","delete"),
                           networkpolicies=("create","get","delete"),**{"pods/log":("get",),"pods/exec":("create",)}).items():
        for verb in verbs:
            require(run.kube(["auth","can-i",verb,kind],None if kind=="namespaces" else owner).strip()=="yes","permission missing")
    cni_template=r'''{{range .spec.template.spec.containers}}{{.name}} {{.image}}{{range .args}}{{if or (eq . "--enable-network-policy=true") (eq . "--enable-network-policy=false")}} {{.}}{{end}}{{end}}{{"\n"}}{{end}}'''
    run.save("cni-selected.txt",run.kube(["get","daemonset","aws-node","-o","go-template="+cni_template],"kube-system"))
    require(not run.run(["git","status","--porcelain"]).strip(),"committed clean implementation required")
    run.run(["git","diff","--exit-code",SOURCE,"--","*.go","go.mod","go.sum","packaging/Dockerfile","scripts/qualification.Dockerfile"])
    source=run.run(["git","rev-parse","HEAD"]).strip()
    context = dict(node=selected, initial_spare=spare, cluster=cluster, source=source)
    return context


def prepare(run, owner):
    context = prepare_context(run, owner)
    selected, spare, cluster, source = (context[key] for key in ("node", "initial_spare", "cluster", "source"))
    plan=dict(schema_version=1,profile="m26r-single-pod-loopback-limited-functional",target=TARGET,namespace=owner,owner=owner,
              node=selected,initial_spare=spare,cluster=cluster,sampled_at=time.time(),atomic_snapshot=False,source=source,image_source=SOURCE,
              images=dict(IMAGES,es=ES),tool_inputs={name:common.digest(common.REPO/name) for name in FILES},minimum=MINIMUM,
              resource_peak=dict(cpu=6,memory_mib=4608,ephemeral_mib=2560),sequence=["elasticsearch-startup","empty-index-init","weir+client","through-weir","direct-es"],
              trials=dict(rate=50,warm=20,seconds=20,workers=64,connections=4,deadline_ms=1000,expiry_ms=20,catchup=8,corpus=1000,document_bytes=1024),
              budgets=dict(remote_seconds=900,cleanup_seconds=180,startup_seconds=150,planned=6000,load_planned=4000,document_mutations=2400,bootstrap_management=1,trial_management_each=3),
              thresholds=dict(dispatch_p99_us=5000,arrival_p95_us=100000,arrival_p99_us=250000,errors=0,drops=0,unknown=0,restarts=0),
              endpoints=dict(es="127.0.0.1:9200",transport="127.0.0.1:9300",weir="127.0.0.1:7447",diagnostics="127.0.0.1:7449",negative="verified own PodIP ports 9200/9300/7447/7449"),
              output=dict(stream_bytes=common.LIMIT,combined_bytes=common.LIMIT,total_bytes=common.TOTAL),network_isolation="unqualified",capacity_candidate=None,
              evidence_inputs=run.registry_evidence,
              product_inputs={name:common.digest(common.REPO/name) for name in run.run(["git","ls-files","*.go","go.mod","go.sum","packaging/Dockerfile","scripts/qualification.Dockerfile"]).splitlines()})
    plan["objects"]=objects(plan)
    plan["commands"]=dict(curl=CURL,trial_base=["/qualification","-mode","trial","-backend","http://127.0.0.1:9200","-rate","50","-warm","20","-seconds","20","-mutation-reservation","1200"],
                          config=["/qualification","-mode","config","-config","/config/node.json"],snapshot=["/qualification","-mode","snapshot"],version=["/weir","-version"],probe=["/weir","-probe","ready"])
    run.save("plan.json",plan); (run.root/"plan.json").chmod(0o400)
    print(json.dumps(dict(plan=str(run.root/"plan.json"),sha256=common.digest(run.root/"plan.json"),node_uid=selected["uid"])),flush=True)


def namespace_start(run):
    p=run.plan
    labels={common.LABEL:p["owner"],"pod-security.kubernetes.io/enforce":"restricted","pod-security.kubernetes.io/enforce-version":"v1.36",
            "pod-security.kubernetes.io/audit":"restricted","pod-security.kubernetes.io/warn":"restricted"}
    namespace=dict(apiVersion="v1",kind="Namespace",metadata=dict(name=p["namespace"],labels=labels))
    run.create(namespace)
    metadata=dict(name="budget",namespace=p["namespace"],labels={common.LABEL:p["owner"]})
    hard={"pods":"1","count/pods":"1","count/jobs.batch":"1","requests.cpu":"6","limits.cpu":"6","requests.memory":"4608Mi","limits.memory":"4608Mi",
          "requests.ephemeral-storage":"2560Mi","limits.ephemeral-storage":"2560Mi","services":"0","persistentvolumeclaims":"0","count/secrets":"0","count/configmaps":"2"}
    quota=dict(apiVersion="v1",kind="ResourceQuota",metadata=metadata,spec=dict(hard=hard))
    entry=run.create(quota)
    until=time.monotonic()+20
    while True:
        actual=run.selected_object("ResourceQuota","budget");common.owner_check(actual,entry)
        common.quota_check(actual["spec"],quota["spec"])
        if (actual.get("status") or {}).get("used",{}).get("count/secrets")=="0":break
        require(time.monotonic()<until,"quota not initialized");time.sleep(.5)
    policy=dict(apiVersion="networking.k8s.io/v1",kind="NetworkPolicy",metadata=dict(metadata,name="default-deny"),
                spec=dict(podSelector={},policyTypes=["Ingress","Egress"],ingress=[],egress=[]))
    run.create(policy)
    rows=run.inventory()
    for _,kind,name,uid,_,_,_ in rows:
        if (kind,name) in (("ServiceAccount","default"),("ConfigMap","kube-root-ca.crt")):
            run.defaults[kind,name]=uid
    run.foreign_check(rows)
    run.save("namespace-defaults.json",[dict(kind=k[0],name=k[1],uid=v) for k,v in run.defaults.items()])
    run.create(p["objects"]["config"])


def execute(run, options):
    plan=json.loads((run.root/"plan.json").read_text())
    require(common.digest(run.root/"plan.json")==options.plan_sha256 and plan["target"]==TARGET,"plan hash/context")
    require(plan["node"]["uid"]==options.node_uid and plan["minimum"]==MINIMUM,"node/minimum drift")
    require(plan["images"]==dict(IMAGES,es=ES) and plan["objects"]==objects(plan),"frozen artifacts/templates drift")
    require(plan["tool_inputs"]=={name:common.digest(common.REPO/name) for name in FILES},"frozen input drift")
    require(all(common.digest(common.REPO/name)==digest for name,digest in plan["product_inputs"].items()),"product input drift")
    proof=plan["evidence_inputs"]
    require(common.digest(proof["path"])==proof["sha256"],"registry evidence drift")
    require(all(common.digest(Path(proof["path"]).parent/name)==digest for name,digest in proof["proof"]["files"].items()),"registry raw evidence drift")
    require(run.run(["git","rev-parse","HEAD"]).strip()==plan["source"] and not run.run(["git","status","--porcelain"]).strip(),"source/worktree drift")
    run.plan=plan;run.pod_entry=None;run.pod_ready=False;run.runtime_evidence=False;run.job_create_attempted=False
    with (run.root/"invocation.json").open("x") as handle:json.dump(dict(start=time.time(),plan_sha256=options.plan_sha256),handle)
    result=dict(profile=plan["profile"],passed=False,trials=[],candidate=None,full_calibration="not-run",overload="not-run",recovery="not-run",soak="not-run",
                network_isolation="unqualified",resource_evidence="not-run",plan_sha256=options.plan_sha256)
    budget=Budget()
    try:
        run.check_node()
        require(not run.kube(["get","namespace",plan["namespace"],"--ignore-not-found","-o","name"]).strip(),"namespace collision")
        run.remote_started=time.monotonic();run.deadline=run.remote_started+900
        namespace_start(run)
        run.template=plan["objects"]["job"]
        # Reserve before creation, because the init may run before create returns.
        run.save("bootstrap-management.json",dict(reserved=1,started_lower_bound=0,started_upper_bound=1,completed=None,document_mutations=0))
        startup=time.monotonic();run.job=run.create(run.template)
        until=startup+150
        while time.monotonic()<until:
            pod=run.current_pod()
            if pod is not None and run.pod_ready:break
            time.sleep(1)
        require(run.pod_ready and time.monotonic()<until,"150 second startup budget")
        run.boundary()
        bootstrap=run.kube(["logs",run.pod_entry["name"],"--container=bootstrap","--limit-bytes="+str(common.LIMIT),"--tail=-1"],plan["namespace"])
        run.save("bootstrap.log",bootstrap)
        require(bootstrap.endswith('{"management":"create-empty-records","completed":1,"document_mutations":0}\n'),"bootstrap receipt")
        run.save("bootstrap-management.json",dict(reserved=1,started_lower_bound=1,started_upper_bound=1,completed=1,document_mutations=0))
        version=json.loads(run.exec_owned("weir",plan["commands"]["version"]))
        expected=dict(product="weir",revision=SOURCE,go="go1.27.1",target="linux/arm64",state="clean-commit",dirty="false",version="local-"+SOURCE)
        require(version==expected,"product version");run.save("version.json",version)
        effective=json.loads(run.exec_owned("qualification",plan["commands"]["config"]))
        require(effective["timing"]==dict(expiry_ms=20,max_catchup_per_wake=8,deadline_ms=1000),"helper timing drift")
        require(effective["store_effective"]["Concurrency"]==4 and effective["store_effective"]["BatchOperations"]==16,"effective Store limits")
        run.save("effective-config.json",effective)
        for prefix in ("through-weir","direct-es"):
            run.boundary();run.diagnostics(prefix+"-before")
            reservation=budget.reserve(prefix,planned=2000,seeds=1000,seconds=40)
            require(budget.snapshot()["reserved"]<=2400 and budget.snapshot()["planned"]<=6000,"M26 budget")
            run.save("mutation-budget.json",budget.snapshot())
            run.save(prefix+"-management.json",dict(reserved=3,operations=["DELETE records","PUT records","POST records/_refresh"],started_lower_bound=0,started_upper_bound=3,completed=None))
            command=plan["commands"]["trial_base"]+["-prefix",prefix]
            if prefix=="through-weir":command += ["-target","127.0.0.1:7447"]
            records=[];complete=False
            try:
                raw=run.exec_owned("qualification",command,110,monitor=True)
                run.save(prefix+".jsonl",raw)
                records=[json.loads(line) for line in raw.splitlines() if line]
                complete=True
            except BaseException:
                if hasattr(run,"last_exec_number"):
                    path=run.root/f"command-{run.last_exec_number:04d}.out"
                    raw=path.read_text() if path.is_file() else ""
                    run.save(prefix+"-incomplete.jsonl",raw)
                    for line in raw.splitlines():
                        try: records.append(json.loads(line))
                        except ValueError: break
                raise
            finally:
                # Failure retains the full reservation; no retry or budget release.
                try:
                    budget.reconcile(reservation,records,complete)
                except Exception as exc:
                    run.save(prefix+"-reconciliation-error.json",dict(error=str(exc)))
                    if complete: raise
                finally:
                    run.save("mutation-budget.json",budget.snapshot())
            run.save(prefix+"-management.json",dict(reserved=3,completed=3))
            run.boundary();run.diagnostics(prefix+"-after")
            report=trial_report(records,prefix);result["trials"].append(report);run.save("result.json",result)
            require(report["passed"],"trial gate failed; later load not run: "+str(report["reasons"]))
        result["passed"]=True
    except BaseException as exc:
        result["error"]=str(exc)
    finally:
        signal.signal(signal.SIGINT,signal.SIG_IGN);signal.signal(signal.SIGTERM,signal.SIG_IGN)
        # Capture owned container status/logs before deletion; failures are retained.
        if run.pod_entry:
            for name in ("elasticsearch","bootstrap","weir","qualification"):
                try:
                    pod=run.selected_object("Pod",run.pod_entry["name"])
                    identity=dict(job=run.job,template=run.template,pod_uid=run.pod_entry["uid"])
                    common.pod_identity(pod,identity)
                    run.save("final-pod.json",pod)
                    run.save("final-container-outcomes.json",container_outcomes(pod.get("status") or {}))
                    raw=run.kube(["logs",run.pod_entry["name"],"--container="+name,"--limit-bytes="+str(common.LIMIT),"--tail=-1"],plan["namespace"])
                    run.save("final-"+name+".log",raw)
                    require(len(raw.encode()) < common.LIMIT,"potentially truncated container log")
                except BaseException as exc:run.save("final-"+name+"-error.json",dict(error=str(exc)))
        result["cleanup"]=run.cleanup()
        result["remote_elapsed_seconds"]=time.monotonic()-run.remote_started if run.remote_started else 0
        result["passed"] &= result["cleanup"]["confirmed"]
        if run.runtime_evidence:
            result["resource_evidence"] = "partial: no full Weir/ES process sampling"
        reservation = run.root/"bootstrap-management.json"
        if reservation.exists():
            management = json.loads(reservation.read_text())
            result["bootstrap_management"] = dict(management)
            if not run.job_create_attempted:
                result["bootstrap_management"].update(started_upper_bound=0,completed=0)
        result["budget"]=budget.snapshot();run.save("result.json",result)
    print(json.dumps(dict(evidence=str(run.root),passed=result["passed"],error=result.get("error"),cleanup=result["cleanup"]["confirmed"])),flush=True)
    return 0 if result["passed"] else 1


def main():
    require(os.environ.get("WEIR_EKS_M26R")=="1","explicit WEIR_EKS_M26R=1 required")
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("prepare","run"));parser.add_argument("--evidence",type=Path,required=True)
    parser.add_argument("--owner");parser.add_argument("--plan-sha256");parser.add_argument("--node-uid")
    parser.add_argument("--registry-evidence",type=Path)
    args=parser.parse_args();root=args.evidence.absolute()
    require(root.parent.resolve()==(common.REPO/".testdata/m26r").resolve() and not root.is_symlink(),"controlled evidence path")
    if args.mode=="prepare":root.mkdir(mode=0o700)
    else:require(root.is_dir() and root.stat().st_mode & 0o077==0,"private prepared evidence")
    run=Run(root,TARGET);run.number=max([int(p.stem.split("-")[1]) for p in root.glob("command-*.json")]+[0])
    def interrupted(signum,frame):raise KeyboardInterrupt("signal "+str(signum))
    signal.signal(signal.SIGINT,interrupted);signal.signal(signal.SIGTERM,interrupted)
    if args.mode=="prepare":
        require(args.registry_evidence and args.registry_evidence.is_file(),"registry verification evidence required")
        proof=json.loads(args.registry_evidence.read_text())
        require(proof["manifest"]==ES["manifest"] and proof["config"]==ES["config"] and proof["architecture"]=="arm64" and proof["version"]=="8.19.22" and proof["user"]=="1000:0","ES registry identity")
        run.registry_evidence=dict(path=str(args.registry_evidence.absolute()),sha256=common.digest(args.registry_evidence),proof=proof)
        prepare(run,args.owner);return 0
    return execute(run,args)


if __name__=="__main__":raise SystemExit(main())
