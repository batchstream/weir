"""Synthetic UID resource accounting, including the actual kubectl template path."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import eks_pacing as entry
import eks_resources as resource
from eks_pacing_test import node
from eks_pacing_report import node_gate


def container(name, cpu="1", memory="1Gi", **extra):
    resources = dict(requests=dict(cpu=cpu, memory=memory))
    result = dict(name=name, restartPolicy=None, resources=resources, **extra)
    return result


def pod():
    result = dict(uid="synthetic-pod", nodeName="node", phase="Running", deleting=None,
                  resources=None, statusResources=None, allocatedResources=None, overhead=None,
                  resize=None, resizeConditions=[], unsupported=False, containers=[container("main")],
                  initContainers=[], containerStatuses=[], initContainerStatuses=[])
    return result


def account(p):
    return resource.allocated(json.dumps([p]))


class ResourceModel(unittest.TestCase):
    def test_identical_three_views_count_once_and_names_not_order(self):
        p = pod()
        p["containers"].append(container("second", "2"))
        p["containerStatuses"] = [dict(name=c["name"], resources=c["resources"], allocatedResources=c["resources"]["requests"]) for c in reversed(p["containers"])]
        used, detail = account(p)
        self.assertEqual(used["node"]["cpu"], 3)
        self.assertEqual(used["node"]["memory"], 2*1024**3)
        self.assertEqual(len(detail[0]["containers"]), 2)

    def test_resize_up_down_infeasible_and_missing_status(self):
        for spec, allocated, actual in (("2", "1", "1"), ("1", "2", "2"), ("1", "1", "2")):
            for reason in ("InProgress", "Infeasible", "Deferred"):
                p = pod()
                p["containers"] = [container("main", spec)]
                p["containerStatuses"] = [dict(name="main", resources=dict(requests=dict(cpu=actual)), allocatedResources=dict(cpu=allocated))]
                p["resizeConditions"] = [dict(type="PodResizePending", status="True", reason=reason)]
                used, _ = account(p)
                self.assertEqual(used["node"]["cpu"], 2)
        p["containerStatuses"] = []
        used, _ = account(p)
        self.assertTrue(used["node"]["errors"])

    def test_ordered_init_sidecars_completed_peak_and_independent_dimensions(self):
        p = pod()
        p["initContainers"] = [container("first", "4", "1Gi"), container("side", "2", "1Gi"), container("second", "1", "6Gi"), container("side2", "1", "2Gi")]
        p["initContainers"][1]["restartPolicy"] = "Always"
        p["initContainers"][3]["restartPolicy"] = "Always"
        used, detail = account(p)
        self.assertEqual(used["node"]["cpu"], 4)
        self.assertEqual(used["node"]["memory"], 7*1024**3)
        self.assertEqual(detail[0]["init_phases"][2]["requests"]["cpu"], 3)

    def test_partial_pod_override_overhead_once_and_requests_not_limits(self):
        p = pod()
        p["containers"][0]["resources"]["limits"] = dict(cpu="8", memory="8Gi")
        p["resources"] = dict(requests=dict(cpu="2"))
        p["statusResources"] = dict(requests=dict(cpu="3"))
        p["allocatedResources"] = dict(cpu="2")
        p["overhead"] = dict(cpu="100m", memory="1Mi")
        used, _ = account(p)
        self.assertEqual(str(used["node"]["cpu"]), "3.1")
        self.assertEqual(used["node"]["memory"], 1025*1024**2)

    def test_unknowns_reject_affected_node_not_zero(self):
        mutations = [lambda p: p.update(unsupported=True), lambda p: p.pop("overhead"),
                     lambda p: p.update(phase="Unknown"), lambda p: p.update(resources=dict(requests={"ephemeral-storage":"1Gi"})),
                     lambda p: p["containers"].append(container("main")),
                     lambda p: p.update(containerStatuses=[dict(name="foreign")]),
                     lambda p: p.update(containerStatuses=[dict(name="main"),dict(name="main")]),
                     lambda p: p["containers"][0].update(resources=dict(limits=dict(cpu="2"))),
                     lambda p: p["containers"][0].update(resources=dict(requests={"example.com/gpu":"1"})),
                     lambda p: p["containers"][0].update(restartPolicy="Always")]
        for quantity in ("-1", "NaN", "1e99999", "999999999999999999999", "1frogs", True):
            mutations.append(lambda p, q=quantity: p["containers"][0]["resources"]["requests"].update(cpu=q))
        for mutate in mutations:
            p = pod(); mutate(p)
            used, details = account(p)
            self.assertTrue(details[0].get("error"), details)
            with self.assertRaises(ValueError):
                node_gate(node(), used["node"])

    def test_empty_pending_terminal_deleting_and_duplicate_uid(self):
        p = pod()
        p["containers"][0]["resources"] = {}
        p["phase"] = "Pending"
        used, _ = account(p)
        self.assertFalse(used["node"]["errors"])
        self.assertEqual(used["node"]["cpu"], 0)
        for phase in ("Succeeded", "Failed"):
            p["phase"] = phase
            used, _ = account(p)
            self.assertEqual(used["node"]["pods"], 0)
            p["deleting"] = "synthetic-time"
            used, _ = account(p)
            self.assertEqual(used["node"]["pods"], 1)
            p["deleting"] = None
        with self.assertRaises(ValueError):
            resource.allocated(json.dumps([p,p]))

    def test_rounding_and_overflow(self):
        self.assertEqual(resource.resource_map(dict(cpu="1n", memory="0.1")), dict(cpu=resource.Decimal("0.001"), memory=1))
        with self.assertRaises(ValueError):
            resource.plus(dict(memory=resource.MAX), dict(memory=1))

    def test_real_go_projection_then_cli_node_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            c = container("main", "500m", "1Gi")
            c.update(env=[dict(name="hidden",value="BUSINESS_SECRET")],command=["BUSINESS_COMMAND"])
            observed = dict(name="main",resources=c["resources"],allocatedResources=c["resources"]["requests"])
            obj = dict(kind="PodList", apiVersion="v1", metadata={}, items=[dict(metadata=dict(uid="synthetic-pod", name="BUSINESS_NAME"),
                                   spec=dict(nodeName="node",containers=[c]),
                                   status=dict(phase="Running", containerStatuses=[observed]))])
            driver = root/"template.go"
            driver.write_text('package main\nimport("os";"encoding/json";"text/template")\nfunc main(){var p struct{Template string; Object any}; f,e:=os.Open(os.Args[1]);if e!=nil{panic(e)};defer f.Close();if e=json.NewDecoder(f).Decode(&p);e!=nil{panic(e)};t,e:=template.New("x").Parse(p.Template);if e!=nil{panic(e)};if e=t.Execute(os.Stdout,p.Object);e!=nil{panic(e)}}')
            payload = dict(Template=resource.projection(entry.JSON_TEMPLATE, scoped=True),Object=obj)
            (root/"input.json").write_text(json.dumps(payload))
            fixed = entry.REPO/".tools/go1.27.1"
            go = str(fixed/"bin/go") if (fixed/"bin/go").is_file() else shutil.which("go")
            self.assertIsNotNone(go, "Go compiler required for offline template regression")
            env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
            env.update(GOENV="off", GOWORK="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", CGO_ENABLED="0")
            if (fixed/"bin/go").is_file():
                env["GOROOT"] = str(fixed)
            result = subprocess.run([go,"run",str(driver),str(root/"input.json")],env=env,capture_output=True,text=True,timeout=45)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertNotIn("BUSINESS",result.stdout)
            (root/"pods.json").write_text(result.stdout)
            n = node(); n.update(kernel="offline", kubelet="offline"); n["allocatable"]["ephemeral-storage"] = "20Gi"
            (root/"nodes.json").write_text(json.dumps(n))
            cli = root/"kubectl"
            cli.write_text('#!'+sys.executable+'\nimport pathlib,sys\np=pathlib.Path(__file__).parent\na=sys.argv\nif "go-template=" not in " ".join(a): raise SystemExit(2)\nprint((p/("nodes.json" if "node" in a else "pods.json")).read_text())\n')
            cli.chmod(0o700)
            target = dict(context="offline",cluster="offline",region="offline")
            run = entry.Run(root,target)
            run.plan = dict(node=dict(name="node",uid="node-uid"))
            run.node_minimum = dict(cpu=7,memory=5632*1024**2,pods=3,**{"ephemeral-storage":5*1024**3})
            with patch.dict(os.environ,PATH=str(root)+os.pathsep+os.environ["PATH"]):
                check = run.check_node()
                self.assertEqual(check["spare"]["cpu"],"7.5")
                projected=json.loads(result.stdout);projected["items"][0]["unsupported"]=True
                (root/"pods.json").write_text(json.dumps(projected))
                with self.assertRaises(ValueError):
                    run.check_node()
            self.assertTrue(list(root.glob("resource-accounting-*.json")))


class ScopedReads(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.n = node()
        self.n.update(kernel="offline", kubelet="offline")
        self.n["allocatable"]["ephemeral-storage"] = "20Gi"
        self.p = pod()
        for c in self.p["containers"]:
            c.update(allocatedResources=None, claims=False, resizePolicy=None)
        self.envelope = dict(kind="PodList", apiVersion="v1", itemsType="[]interface {}",
                             **{"continue":None}, remainingItemCount=None, items=[self.p])
        self.scenario = dict(node=self.n, pods=self.envelope, failures={})
        cli = self.root/"kubectl"
        cli.write_text('''#!'''+sys.executable+'''
import json, pathlib, sys
root = pathlib.Path(__file__).parent
calls = root/'calls.jsonl'
number = len(calls.read_text().splitlines())+1 if calls.exists() else 1
args = sys.argv[1:]
with calls.open('a') as output: output.write(json.dumps(args)+'\\n')
cfg = json.loads((root/'scenario.json').read_text())
if 'get' in args:
    if 'node' in args:
        if args[args.index('node')+1] != 'node': raise SystemExit(7)
        value = cfg['node']
    elif 'pods' in args:
        if not all(a in args for a in ('--all-namespaces', '--field-selector=spec.nodeName=node', '--chunk-size=0')): raise SystemExit(8)
        if '--namespace' in args or '-l' in args: raise SystemExit(9)
        value = cfg['pods']
    else: raise SystemExit(10)
else: value = {}
failure = cfg['failures'].get(str(number))
if failure:
    sys.stdout.write(failure.get('stdout', '[]'))
    sys.stderr.write(failure['stderr'])
    raise SystemExit(failure.get('exit', 1))
sys.stdout.write(value if isinstance(value, str) else json.dumps(value))
''')
        cli.chmod(0o700)
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, PATH=str(self.root)+os.pathsep+os.environ["PATH"]).start()
        target = dict(context="offline", cluster="offline", region="offline")
        self.run = entry.Run(self.root, target)
        self.run.plan = dict(node=dict(name="node", uid="node-uid"))
        self.run.node_minimum = dict(cpu=7, memory=5632*1024**2, pods=3, **{"ephemeral-storage":5*1024**3})

    def check(self):
        (self.root/"scenario.json").write_text(json.dumps(self.scenario))
        return self.run.check_node()

    def calls(self):
        path = self.root/"calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_same_frozen_node_across_prepare_and_dispatch_all_namespaces(self):
        self.check()
        state = copy.deepcopy(self.run.resource_preflight)
        second = entry.Run(self.root, self.run.target)
        second.plan = self.run.plan
        second.number = self.run.number
        second.resource_preflight = state
        second.node_minimum = self.run.node_minimum
        second.check_node()
        self.assertEqual(self.calls()[:2], self.calls()[2:])
        self.assertEqual(state, second.resource_preflight)
        ledger = json.loads((self.root/'resource-accounting-4.json').read_text())
        self.assertEqual(set(ledger['nodes']), {'node'})
        self.assertEqual(len(ledger['pods']), 1)
        self.assertNotIn('BUSINESS', (self.root/'command-0002.out').read_text())

    def test_empty_success_and_legal_array_with_timeout_are_distinct(self):
        self.envelope['items'] = []
        self.assertEqual(self.check()['spare']['cpu'], '8')
        failure = dict(stdout='[]', stderr='context deadline exceeded (Client.Timeout or context cancellation while reading body)')
        self.scenario['failures'] = {'4':failure}
        self.check()
        self.assertEqual(len(self.calls()), 5)
        self.assertEqual(self.calls()[3], self.calls()[4])
        self.assertEqual((self.root/'command-0004.out').read_text(), '[]')
        self.assertEqual(json.loads((self.root/'command-0004.json').read_text())['exit'], 1)
        self.assertEqual(self.run.resource_preflight['recovery']['failed_command'], 4)

    def test_only_one_recovery_across_objects_and_calls(self):
        failure = dict(stderr='net/http: timeout awaiting response headers')
        self.scenario['failures'] = {'1':failure, '3':failure}
        with self.assertRaisesRegex(ValueError, 'recovery ineligible'):
            self.check()
        self.assertEqual(len(self.calls()), 3)
        self.assertEqual(self.calls()[0], self.calls()[1])
        self.assertEqual(self.run.resource_preflight['recovery']['failed_command'], 1)

    def test_no_recovery_after_any_mutation_or_for_permission_failure(self):
        for marker in ('permission', 'mutation'):
            with self.subTest(marker=marker):
                self.scenario['failures'] = {str(len(self.calls())+1):dict(stderr='Forbidden' if marker == 'permission' else 'net/http: timeout awaiting response headers')}
                self.run.mutation_attempted = marker == 'mutation'
                before = len(self.calls())
                with self.assertRaises(ValueError): self.check()
                self.assertEqual(len(self.calls()), before+1)
                self.assertIsNone(self.run.resource_preflight['recovery'])
        self.scenario['failures'] = {str(len(self.calls())+1):dict(stderr='net/http: timeout awaiting response headers')}
        (self.root/'scenario.json').write_text(json.dumps(self.scenario))
        before = len(self.calls())
        with self.assertRaises(entry.CommandFailure): self.run.kube(['create', '-f', 'unused'])
        self.assertEqual(len(self.calls()), before+1)
        self.assertTrue(self.run.mutation_attempted)

    def test_identity_change_and_capacity_failure_never_retry(self):
        self.n['uid'] = 'different'
        with self.assertRaisesRegex(ValueError, 'identity'): self.check()
        self.assertEqual(len(self.calls()), 1)
        self.n['uid'] = 'node-uid'
        self.p['containers'][0]['resources']['requests']['cpu'] = '2'
        with self.assertRaisesRegex(ValueError, 'capacity'): self.check()
        self.assertEqual(len(self.calls()), 3)
        self.assertIsNone(self.run.resource_preflight['recovery'])

    def test_unready_node_stops_before_pods_and_postwrite_uses_native_deadline(self):
        self.n['conditions'][0]['status'] = 'False'
        with self.assertRaisesRegex(ValueError, 'not Ready'): self.check()
        self.assertEqual(len(self.calls()), 1)
        self.n['conditions'][0]['status'] = 'True'
        self.run.resource_preflight['deadline'] = time.monotonic()-1
        self.run.mutation_attempted = True
        self.check()
        self.assertEqual(len(self.calls()), 3)
        self.scenario['failures'] = {'4':dict(stderr='net/http: timeout awaiting response headers')}
        with self.assertRaisesRegex(ValueError, 'recovery ineligible'): self.check()
        self.assertEqual(len(self.calls()), 4)
        self.assertIsNone(self.run.resource_preflight['recovery'])

    def test_wrong_node_duplicate_uid_incomplete_unknown_and_partial(self):
        mutations = [lambda e:e['items'][0].update(nodeName='other-node'),
                     lambda e:e['items'].append(copy.deepcopy(e['items'][0])),
                     lambda e:e['items'][0].pop('overhead'),
                     lambda e:e['items'][0]['containers'][0].pop('resources'),
                     lambda e:e['items'][0].update(uid=''),
                     lambda e:e['items'][0].update(unsupported=True),
                     lambda e:e['items'][0]['containers'][0]['resources']['requests'].update(gpu='1'),
                     lambda e:e.update(**{'continue':'next-page'}),
                     lambda e:e.update(remainingItemCount=1),
                     lambda e:e.update(itemsType='<nil>')]
        for change in mutations:
            value = copy.deepcopy(self.envelope)
            change(value)
            self.scenario['pods'] = value
            before = len(self.calls())
            with self.assertRaises(ValueError): self.check()
            self.assertEqual(len(self.calls()), before+2)
            self.assertIsNone(self.run.resource_preflight['recovery'])
        for raw in ('[]', '{', json.dumps(self.envelope)[:-2]):
            self.scenario['pods'] = raw
            with self.assertRaises(ValueError): self.check()

    def test_common_deadline_scope_and_output_bound(self):
        self.check()
        deadline = self.run.resource_preflight['deadline']
        self.run.plan['node']['uid'] = 'changed'
        with self.assertRaisesRegex(ValueError, 'scope drift'): self.check()
        self.assertEqual(self.run.resource_preflight['deadline'], deadline)
        self.run.plan['node']['uid'] = 'node-uid'
        self.run.resource_preflight['deadline'] = time.monotonic()
        with self.assertRaisesRegex(ValueError, 'deadline'): self.check()
        self.assertEqual(len(self.calls()), 2)
        self.run.resource_preflight['deadline'] = deadline
        self.scenario['pods'] = 'x'*(entry.LIMIT+1)
        with self.assertRaisesRegex(ValueError, 'overflow'): self.check()
        self.assertEqual(len(self.calls()), 4)
        self.assertIsNone(self.run.resource_preflight['recovery'])
        self.assertEqual((self.root/'command-0004.out').stat().st_size, entry.LIMIT)
        self.assertIsNotNone(json.loads((self.root/'command-0004.json').read_text())['exit'])


if __name__ == "__main__":
    unittest.main()
