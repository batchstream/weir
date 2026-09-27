"""Fixed M22 gates over raw counts and two-second samples; no configurable SLOs."""
import datetime
import json
import re


def timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def prom(raw):
    return {line.split()[0]:float(line.split()[-1]) for line in raw.splitlines() if line and not line.startswith("#")}


def metric(values, name):
    return sum(v for k,v in values.items() if k.split("{")[0]==name)


def counter(raw):
    return {line.split()[0]:int(line.split()[1]) for line in raw.splitlines() if len(line.split())==2}


def window_gate(window):
    reasons=[]
    for kind in ("all","read","put"):
        m=window[kind]
        if m["planned"]!=m["completed"]+m["client_drop"] or m["started"]!=m["completed"]:
            raise RuntimeError("count identity")
        if m["completed"]!=m["success"]+sum((m["failures"] or {}).values()):
            raise RuntimeError("outcome identity")
        if m["client_drop"] or m["client_late"] or m["unknown"] or m["success"]!=m["planned"]:
            reasons.append(kind+": errors/drop/unknown")
        for histogram in ("arrival","dispatch","lag"):
            h=m[histogram]
            if sum(b["count"] for b in h["buckets"])!=m["completed"]:
                raise RuntimeError("histogram count identity")
            for p in (50,95,99):
                want=(m["completed"]*p+99)//100
                accumulated=0;calculated=-1
                for bucket in h["buckets"]:
                    accumulated+=bucket["count"]
                    if accumulated>=want:
                        calculated=bucket["upper_us"];break
                if h["p"+str(p)+"_us"]!=calculated:
                    raise RuntimeError("histogram quantile mismatch")
        for histogram, quantile, bound in (("arrival","p95_us",100000),("arrival","p99_us",250000),("lag","p99_us",5000)):
            if not 0<=m[histogram][quantile]<=bound:
                reasons.append(kind+":"+histogram+"/"+quantile)
    return reasons


def resource_gate(samples, stable=True):
    reasons=[]
    if not samples:
        return ["no resource samples"],{}
    peak={"rss_bytes":0,"cgroup_bytes":0,"fd":0,"owner":0,"connections":0}
    for s in samples:
        if s.get("errors"):
            reasons.append("observer errors")
        current=int(s["files"].get("memory.current","0"))
        peak["rss_bytes"]=max(peak["rss_bytes"],s["rss_bytes"])
        peak["cgroup_bytes"]=max(peak["cgroup_bytes"],current)
        peak["fd"]=max(peak["fd"],s["fd"])
        if s["rss_bytes"]==0 or current==0 or current>int(s["files"].get("memory.max","0")) or s["fd"]>4096:
            raise RuntimeError("hard process/cgroup/handle boundary")
        events=counter(s["files"].get("memory.events",""))
        if events.get("oom",0) or events.get("oom_kill",0):
            raise RuntimeError("OOM observed")
        if s.get("metrics"):
            m=prom(s["metrics"])
            required={"weir_memory_sample_observed":1,"weir_memory_process_valid":1,"weir_memory_cgroup_valid":1,"weir_memory_unknown":0}
            if stable:
                required["weir_memory_latched"]=0
            for name,value in required.items():
                if metric(m,name)!=value:
                    reasons.append(name)
            for name,limit in (("pending_entries","pending_entries_limit"),("pending_reserved_bytes","pending_reserved_bytes_limit"),("result_reserved_entries","result_reserved_entries_limit"),("result_reserved_bytes","result_reserved_bytes_limit"),("active_executions","window_limit")):
                if metric(m,"weir_store_"+name)>metric(m,"weir_store_"+limit):
                    raise RuntimeError("ledger overrun: "+name)
            if metric(m,"weir_backend_connections_peak")>metric(m,"weir_backend_connections_limit"):
                raise RuntimeError("backend owner overrun")
            peak["owner"]=max(peak["owner"],metric(m,"weir_backend_connections_owned"))
            peak["connections"]=max(peak["connections"],metric(m,"weir_ingress_connections"))
    if stable and (peak["rss_bytes"]>614*1024**2 or peak["cgroup_bytes"]>819*1024**2):
        reasons.append("stable memory threshold")
    return sorted(set(reasons)),peak


def select_samples(samples, trial, warm=True):
    start=timestamp(trial["start"])+(trial["options"]["WarmSeconds"] if warm else 0)
    end=start+trial["options"]["Seconds"]
    return [s for s in samples if start<=timestamp(s["time"])<=end]


def cpu_usage(samples, cpus):
    if len(samples)<2:
        return None
    before,after=samples[0],samples[-1]
    seconds=timestamp(after["time"])-timestamp(before["time"])
    b=counter(before["files"].get("cpu.stat",""));a=counter(after["files"].get("cpu.stat",""))
    return {"quota_fraction":(a.get("usage_usec",0)-b.get("usage_usec",0))/1e6/seconds/cpus,"throttled_seconds":(a.get("throttled_usec",0)-b.get("throttled_usec",0))/1e6}


def evaluate(trial, samples, clients, db_samples):
    reasons=window_gate(trial["measure"])
    selected=select_samples(samples,trial)
    resource_reasons,peak=resource_gate(selected)
    reasons+=resource_reasons
    client_cpu=cpu_usage(select_samples(clients,trial),1)
    db_cpu=cpu_usage(select_samples(db_samples,trial),3)
    if client_cpu is None or db_cpu is None:
        reasons.append("CPU evidence unavailable")
    if client_cpu and client_cpu["quota_fraction"]>=.9:
        reasons.append("client CPU limited")
    if db_cpu and db_cpu["quota_fraction"]>=.9:
        reasons.append("DB CPU limited")
    for s in selected:
        if not s.get("db"):
            reasons.append("DB statistics unavailable")
            continue
        for node in json.loads(s["db"]).get("nodes",{}).values():
            for pool in node.get("thread_pool",{}).values():
                if pool.get("queue",0)>0 or pool.get("rejected",0)>0:
                    reasons.append("DB queue/rejection limited")
    opts=trial["options"];m=trial["measure"]
    return {"rate":opts["Rate"],"prefix":opts["Prefix"],"pass":not reasons,"reasons":sorted(set(reasons)),"measure":m,"warm":trial["warm"],"resource_peak":peak,"client_cpu":client_cpu,"db_cpu":db_cpu,
            "planned_rps":m["all"]["planned"]/opts["Seconds"],"started_rps":m["all"]["started"]/opts["Seconds"],"success_rps":m["all"]["success"]/opts["Seconds"],"reads_per_second":m["read"]["success"]/opts["Seconds"],"writes_per_second":m["put"]["success"]/opts["Seconds"]}
