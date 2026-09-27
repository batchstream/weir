"""Offline failure cleanup checks. No Docker, Kubernetes or backend is started."""
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from kubernetes_fixture import Fixture


class CleanupTests(unittest.TestCase):
    def test_failure_does_not_skip_other_nodes_volumes_or_access_file(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp),"weir-m21-unit")
            f.nodes={f.owner+"-worker":{"id":"worker-id","volumes":["owned-volume"]},
                     f.owner+"-worker2":{"id":"worker2-id","volumes":["other-owned-volume"]}}
            f.network="owned-network"
            access=f.root/"kubeconfig"
            access.touch()
            calls=[]
            def docker(args,**kwargs):
                calls.append(args)
                if args[1:3]==["inspect","--format"]:
                    name=args[-1];identity=f.nodes[name]["id"]
                    obj={"Id":identity,"Config":{"Labels":{"io.x-k8s.kind.cluster":f.owner}},"State":{"Paused":True}}
                    return subprocess.CompletedProcess(args,0,json.dumps(obj),"")
                if args[1:3]==["network","inspect"]:
                    return subprocess.CompletedProcess(args,0,json.dumps([{"Id":f.network,"Containers":{}}]),"")
                if args[1] in ("unpause","rm") and args[-1]=="worker-id":
                    return subprocess.CompletedProcess(args,1,"","injected failure")
                if args[1:3]==["volume","rm"] and args[-1]=="owned-volume":
                    return subprocess.CompletedProcess(args,1,"","volume in use")
                return subprocess.CompletedProcess(args,0,"","")
            with patch("subprocess.run",side_effect=docker):
                self.assertFalse(f.cleanup())
            self.assertFalse(access.exists())
            self.assertIn(["docker","rm","-f","worker2-id"],calls)
            self.assertIn(["docker","volume","rm","other-owned-volume"],calls)
            self.assertIn(["docker","network","rm","owned-network"],calls)
            results=json.loads((f.root/"cleanup.json").read_text())
            self.assertEqual(sum(not item["clean"] for item in results),3)

    def test_owner_mismatch_never_removes_container(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp),"weir-m21-unit")
            f.nodes={f.owner+"-worker":{"id":"expected","volumes":[]}}
            calls=[]
            def docker(args,**kwargs):
                calls.append(args)
                obj={"Id":"other","Config":{"Labels":{"io.x-k8s.kind.cluster":"unrelated"}},"State":{"Paused":True}}
                return subprocess.CompletedProcess(args,0,json.dumps(obj),"")
            with patch("subprocess.run",side_effect=docker):
                self.assertFalse(f.cleanup())
            self.assertFalse(any(args[1] in ("unpause","rm") for args in calls))


if __name__=="__main__":unittest.main()
