"""Synthetic fixed-profile evidence. No Docker or historical measurement inputs."""
import copy
import datetime
import json
import unittest

from capacity_contract import PLAN, Budget
from capacity_report import resource_gate, evaluate, require_evidence, cpu_usage, covered_samples

BASE = 1800000000


def sample(role, second):
    limit = PLAN["resources"][role]
    m = dict(weir_memory_sample_observed=1, weir_memory_process_valid=1,
             weir_memory_cgroup_valid=1, weir_memory_cgroup_finite=1,
             weir_memory_unknown=0, weir_memory_latched=0,
             weir_memory_budget_bytes=768<<20, weir_memory_cgroup_limit_bytes=1<<30,
             weir_memory_cgroup_current_bytes=32<<20, weir_memory_cgroup_levels=1)
    m['weir_memory_sample_bytes{source="linux_rss"}'] = 32<<20
    m['weir_memory_cgroup_state{state="v2"}'] = 1
    m['weir_memory_cgroup_scope{scope="leaf"}'] = 1
    for kind,names in (("state",("unknown","profile_changed","not_applicable")),("scope",("ancestor","none"))):
        for name in names:m[f'weir_memory_cgroup_{kind}{{{kind}="{name}"}}']=0
    for name in ("darwin_phys_footprint","go_sys_minus_released","unobserved"):
        m[f'weir_memory_sample_bytes{{source="{name}"}}']=0
    for name, bound, maximum in (("pending_entries","pending_entries_limit",256),
                                ("pending_reserved_bytes","pending_reserved_bytes_limit",8<<20),
                                ("result_reserved_entries","result_reserved_entries_limit",128),
                                ("result_reserved_bytes","result_reserved_bytes_limit",16<<20),
                                ("active_executions","window_limit",4),("live_sessions","live_sessions_limit",1)):
        m['weir_store_'+name+'{store="records"}'] = 0
        m['weir_store_'+bound+'{store="records"}'] = maximum
    for name, maximum in (("ingress_connections",16),("ingress_sessions",16),
                          ("diagnostic_connections",4),("diagnostic_handlers",2)):
        m['weir_'+name] = 1
        m['weir_'+name+'_limit'] = maximum
    for key, value in (("owned",1),("peak",1),("limit",5)):
        m['weir_backend_connections_'+key+'{store="records"}'] = value
    for name in ("native_reserved_bytes", "native_sessions", "retained_result_reserved_bytes", "retained_results", "scan_cleanups", "scan_page_reserved_bytes", "scan_pages", "scan_sessions"):
        m['weir_store_'+name+'{store="records"}'] = 0
    raw = ''.join(k+' '+str(v)+'\n' for k,v in m.items())
    node = dict(process=dict(timestamp=(BASE+second)*1000, open_file_descriptors=30),
                thread_pool={p:dict(queue=0,rejected=0,active=0,completed=100) for p in ("write","get")},
                http=dict(current_open=2),jvm=dict(mem=dict(heap_used_in_bytes=1000)),fs=dict(io_stats={}))
    files = {"memory.current":str(32<<20),"memory.max":str(limit["memory_mib"]<<20),
             "memory.swap.max":"0", "pids.current":"8", "pids.max":str(limit["pids"]),
             "cpuset.cpus.effective":limit["cpuset"], "cpu.max":str(int(limit["cpu"]*100000))+" 100000",
             "memory.events":"low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
             "cpu.stat":f"usage_usec {100000+int(second*100)}\nuser_usec 100\nsystem_usec 100\nnr_periods 100\nnr_throttled 0\nthrottled_usec 0\n"}
    files.update(status="Cpus_allowed_list: "+limit["cpuset"]+"\n")
    for name in ("net/tcp","net/tcp6"):files[name]="sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
    result = dict(role=role,time=datetime.datetime.fromtimestamp(BASE+second,datetime.timezone.utc).isoformat(),
                  monotonic_ns=int((second+100)*1e9), duration_ns=1000000,
                  files=files, rss_bytes=32<<20, fd=10, goroutines=10, gomaxprocs=1)
    if role == "weir":
        result.update(metrics=raw,db=json.dumps(dict(nodes=dict(node=node))))
    if role == "es":
        result.update(db=json.dumps(dict(nodes=dict(node=node))))
    return result


def metrics(count):
    h = dict(buckets=[dict(upper_us=1000,count=count)] if count else [],p50_us=1000 if count else -1,
             p95_us=1000 if count else -1,p99_us=1000 if count else -1)
    result = dict(planned=count,started=count,completed=count,success=count,client_drop=0,
                  client_late=0,unknown=0,failures={},arrival=copy.deepcopy(h),dispatch=copy.deepcopy(h),lag=copy.deepcopy(h))
    return result


def trial():
    window = {k:metrics(n) for k,n in (("all",1000),("read",900),("put",100))}
    result = dict(start=sample("weir",0)["time"],options=dict(Rate=50,WarmSeconds=0,Seconds=20,Prefix="synthetic"),
                  warm={k:metrics(0) for k in window},measure=window)
    return result


class Evidence(unittest.TestCase):
    def observations(self):
        return [[sample(role,t) for t in range(-2,23,2)] for role in ("weir","client","es")]

    def test_complete_synthetic_evidence_can_pass(self):
        s,c,d = self.observations()
        observations=dict(weir=s,client=c,es=d)
        self.assertTrue(evaluate(trial(),observations)["pass"])
        self.assertFalse(resource_gate(d,role="es")[0])
        self.assertTrue(resource_gate(d)[0])

    def test_metrics_fail_closed(self):
        original=sample("weir",0)
        variants = [None,"","weir_memory_unknown 0",original["metrics"].replace("weir_memory_unknown 0","weir_memory_unknown nan"),
                    original["metrics"].replace("weir_memory_unknown 0","weir_memory_unknown inf"),
                    original["metrics"]+"weir_memory_unknown 0\n",
                    original["metrics"].replace('store="records"','store="foreign"'),
                    '\n'.join(line for line in original["metrics"].splitlines() if not line.startswith(("weir_store_","weir_backend_")))+'\n',
                    original["metrics"].replace("weir_memory_sample_observed 1","weir_memory_sample_observed invalid")]
        variants += [original["metrics"].replace('state="profile_changed"} 0','state="profile_changed"} 1'),
                     original["metrics"].replace('weir_store_pending_entries{store="records"} 0','weir_store_pending_entries{store="records"} 0.5')]
        for raw in variants:
            with self.subTest(raw=str(raw)[:60]):
                s=copy.deepcopy(original);s["metrics"]=raw
                why,peak=resource_gate([s]);self.assertTrue(why);self.assertIsNone(peak["owner"])
                w,c,d=self.observations()
                for x in w:x["metrics"]=raw
                observations=dict(weir=w,client=c,es=d)
                report=evaluate(trial(),observations)
                self.assertFalse(report["pass"])
                with self.assertRaises(RuntimeError):require_evidence(report)

    def test_resource_missing_nonfinite_reset_and_gap(self):
        for role in ("weir","es","client"):
            for key in ("cpu.stat","memory.current","memory.events","cpu.max","pids.max","status","net/tcp","net/tcp6"):
                with self.subTest(role=role,key=key):
                    s=sample(role,0);s["files"].pop(key)
                    self.assertTrue(resource_gate([s],role=role)[0])
            for field in ("rss_bytes","fd","duration_ns","monotonic_ns"):
                s=sample(role,0);s[field]=float('nan')
                self.assertTrue(resource_gate([s],role=role)[0])
            a,b=sample(role,0),sample(role,2)
            b["files"]["cpu.stat"]=b["files"]["cpu.stat"].replace("100200","1")
            self.assertTrue(resource_gate([a,b],role=role)[0])
            with self.assertRaises(ValueError):cpu_usage([a,b],1)
            self.assertTrue(resource_gate([a,sample(role,7)],role=role)[0])
            self.assertFalse(resource_gate([a,sample(role,6)],role=role)[0])
            self.assertTrue(resource_gate([a,a],role=role)[0])
            b=sample(role,2);b["duration_ns"]=2000000001
            self.assertTrue(resource_gate([a,b],role=role)[0])

    def test_boundary_and_interior_coverage(self):
        s,c,d=self.observations()
        for broken in ([s[4]],s[2:],s[:-2],s[:3]+s[7:]):
            observations=dict(weir=broken,client=c,es=d)
            report=evaluate(trial(),observations)
            self.assertFalse(report["pass"])
            with self.assertRaises(RuntimeError):require_evidence(report)
        self.assertEqual(len(covered_samples(s,BASE,BASE+20)),11)

    def test_hard_failures_are_fatal_even_during_overload(self):
        mutations=[("metrics",'weir_store_pending_entries{store="records"} 0','weir_store_pending_entries{store="records"} 257'),
                   ("metrics",'weir_backend_connections_peak{store="records"} 1','weir_backend_connections_peak{store="records"} 6')]
        for key,old,new in mutations:
            s,c,d=self.observations();s[5][key]=s[5][key].replace(old,new)
            observations=dict(weir=s,client=c,es=d)
            with self.assertRaises(RuntimeError):require_evidence(evaluate(trial(),observations,stable=False))
        s,c,d=self.observations();d[5]["files"]["memory.events"]=d[5]["files"]["memory.events"].replace("oom 0","oom 1")
        observations=dict(weir=s,client=c,es=d)
        with self.assertRaises(RuntimeError):require_evidence(evaluate(trial(),observations,stable=False))
        for raw in ('{}','{"nodes":{}}','{"nodes":{"x":{}}}'):
            s,c,d=self.observations();d[5]["db"]=raw
            observations=dict(weir=s,client=c,es=d)
            with self.assertRaises(RuntimeError):require_evidence(evaluate(trial(),observations))


class MutationBudget(unittest.TestCase):
    def test_exact_limit_plus_one_and_no_double_recovery_seed(self):
        b=Budget()
        b.reserve("bootstrap",seeds=1000)
        for n in range(4):b.reserve(str(n),planned=990000,seeds=1000)
        b.reserve("tail",planned=980000,seeds=1000)
        self.assertEqual(b.snapshot()["reserved"],500000)
        with self.assertRaises(RuntimeError):b.reserve("one-too-many",seeds=1)
        self.assertEqual(b.snapshot()["reserved"],500000)
        b=Budget();e=b.reserve("overload-recovery",planned=100*30+35*120,seeds=1000,seconds=150)
        self.assertEqual(e["reserved"],1720)
        with self.assertRaises(RuntimeError):b.reserve("overload-recovery",seeds=1000)

    def test_partial_setup_receipt_and_killed_process_stay_accounted(self):
        b=Budget();e=b.reserve("bootstrap",seeds=1000)
        records=[dict(type="setup_progress",started=n) for n in range(1,18)]
        b.reconcile(e,records,False)
        self.assertIsNone(e["actually_started"]);self.assertEqual(e["started_lower_bound"],17)
        records.append(dict(type="mutation_receipt",started=17))
        b.reconcile(e,records,False)
        self.assertEqual(e["actually_started"],17);self.assertEqual(e["reserved"],1000)


if __name__ == "__main__":unittest.main()
