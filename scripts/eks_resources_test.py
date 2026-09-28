"""Synthetic UID resource accounting, including the actual kubectl template path."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
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
            obj = dict(items=[dict(metadata=dict(uid="synthetic-pod", name="BUSINESS_NAME"),
                                   spec=dict(nodeName="node",containers=[c]),
                                   status=dict(phase="Running", containerStatuses=[observed]))])
            driver = root/"template.go"
            driver.write_text('package main\nimport("os";"encoding/json";"text/template")\nfunc main(){var p struct{Template string; Object any}; f,e:=os.Open(os.Args[1]);if e!=nil{panic(e)};defer f.Close();if e=json.NewDecoder(f).Decode(&p);e!=nil{panic(e)};t,e:=template.New("x").Parse(p.Template);if e!=nil{panic(e)};if e=t.Execute(os.Stdout,p.Object);e!=nil{panic(e)}}')
            payload = dict(Template=resource.projection(entry.JSON_TEMPLATE),Object=obj)
            (root/"input.json").write_text(json.dumps(payload))
            env = dict(os.environ, GOROOT=str(entry.REPO/".tools/go1.27.1"), GOENV="off", GOWORK="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off")
            result = subprocess.run([str(entry.REPO/".tools/go1.27.1/bin/go"),"run",str(driver),str(root/"input.json")],env=env,capture_output=True,text=True,timeout=45)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertNotIn("BUSINESS",result.stdout)
            (root/"pods.json").write_text(result.stdout)
            n = node(); n["allocatable"]["ephemeral-storage"] = "20Gi"
            (root/"nodes.json").write_text(json.dumps([n]))
            cli = root/"kubectl"
            cli.write_text('#!'+sys.executable+'\nimport pathlib,sys\np=pathlib.Path(__file__).parent\na=sys.argv\nif "go-template=" not in " ".join(a): raise SystemExit(2)\nprint((p/("nodes.json" if "nodes" in a else "pods.json")).read_text())\n')
            cli.chmod(0o700)
            target = dict(context="offline",cluster="offline",region="offline")
            run = entry.Run(root,target)
            run.plan = dict(node=dict(name="node",uid="node-uid"))
            run.node_minimum = dict(cpu=7,memory=5632*1024**2,pods=3,**{"ephemeral-storage":5*1024**3})
            with patch.dict(os.environ,PATH=str(root)+os.pathsep+os.environ["PATH"]):
                check = run.check_node()
                self.assertEqual(check["spare"]["cpu"],"7.5")
                projected=json.loads(result.stdout);projected[0]["unsupported"]=True
                (root/"pods.json").write_text(json.dumps(projected))
                with self.assertRaises(ValueError):
                    run.check_node()
            self.assertTrue(list(root.glob("resource-accounting-*.json")))


if __name__ == "__main__":
    unittest.main()
