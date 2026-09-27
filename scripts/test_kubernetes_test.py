"""Offline failure cleanup checks. No Docker, Kubernetes or backend is started."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from kubernetes_fixture import Fixture, REPO

SPEC = importlib.util.spec_from_file_location("kube_entry", REPO / "scripts/test-kubernetes.py")
ENTRY = importlib.util.module_from_spec(SPEC)
if __debug__:
    SPEC.loader.exec_module(ENTRY)


class CleanupTests(unittest.TestCase):
    def test_failure_does_not_skip_other_nodes_volumes_or_access_file(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp)/"fresh","weir-m21-unit")
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
                    obj={"Id":identity,"Config":{"Labels":{"io.x-k8s.kind.cluster":f.owner}},"State":{"Paused":True},"Mounts":[{"Type":"volume","Name":v} for v in f.nodes[name]["volumes"]]}
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
            self.assertNotIn(["docker","volume","rm","owned-volume"],calls)
            results=json.loads((f.root/"cleanup.json").read_text())
            self.assertEqual(sum(not item["clean"] for item in results),3)

    def test_owner_mismatch_never_removes_container(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp)/"fresh","weir-m21-unit")
            f.nodes={f.owner+"-worker":{"id":"expected","volumes":["must-retain"]}}
            calls=[]
            def docker(args,**kwargs):
                calls.append(args)
                obj={"Id":"other","Config":{"Labels":{"io.x-k8s.kind.cluster":"unrelated"}},"State":{"Paused":True}}
                return subprocess.CompletedProcess(args,0,json.dumps(obj),"")
            with patch("subprocess.run",side_effect=docker):
                self.assertFalse(f.cleanup())
            self.assertFalse(any(args[1] in ("unpause","rm") for args in calls))
            self.assertFalse(any(args[1:3] == ["volume", "rm"] for args in calls))

    def test_preexisting_owner_is_rejected_before_bootstrap(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp)/"fresh","weir-m21-unit")
            calls=[]
            def docker(args,**kwargs):
                calls.append(args)
                output="existing-id" if "-aq" in args else ""
                return subprocess.CompletedProcess(args,0,output,"")
            with patch("subprocess.run",side_effect=docker):
                with self.assertRaisesRegex(RuntimeError, "owner already exists"):
                    f.preflight()
            self.assertFalse(f.cluster_started)
            self.assertEqual(len(calls),2)

    def test_fast_bootstrap_failure_discovers_unrecorded_owned_node(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Fixture(Path(temp)/"fresh","weir-m21-unit")
            f.cluster_started=True
            calls=[]
            def docker(args,**kwargs):
                calls.append(args)
                if args[1:3]==["inspect","--format"]:
                    if args[-1]!=f.owner+"-worker2":
                        return subprocess.CompletedProcess(args,1,"","absent")
                    if args[-2]=="{{.Id}}":
                        return subprocess.CompletedProcess(args,0,"retained-id","")
                    obj={"Id":"retained-id","Config":{"Labels":{"io.x-k8s.kind.cluster":f.owner}},"State":{"Paused":False},"Mounts":[{"Type":"volume","Name":"retained-volume"}]}
                    return subprocess.CompletedProcess(args,0,json.dumps(obj),"")
                if args[1:3]==["network","inspect"]:
                    return subprocess.CompletedProcess(args,1,"","absent")
                return subprocess.CompletedProcess(args,0,"","")
            with patch("subprocess.run",side_effect=docker):
                self.assertTrue(f.cleanup())
            self.assertIn(["docker","rm","-f","retained-id"],calls)
            self.assertIn(["docker","volume","rm","retained-volume"],calls)

    def test_changed_id_or_nonempty_network_is_never_removed(self):
        for changed in ("node-id", "network-id", "network-members"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                f = Fixture(Path(temp)/"fresh", "weir-m21-unit")
                f.nodes = {f.owner+"-worker": {"id": "expected", "volumes": []}}
                f.network = "expected-network"
                calls = []
                def docker(args, **kwargs):
                    calls.append(args)
                    if args[1] == "inspect":
                        obj = {"Id": "other" if changed == "node-id" else "expected", "Config": {"Labels": {"io.x-k8s.kind.cluster": f.owner}}, "State": {"Paused": True}, "Mounts": []}
                    elif args[1:3] == ["network", "inspect"]:
                        obj = [{"Id": "other" if changed == "network-id" else f.network, "Containers": {"foreign": {}} if changed == "network-members" else {}}]
                    else:
                        obj = ""
                    return subprocess.CompletedProcess(args, 0, json.dumps(obj), "")
                with patch("subprocess.run", side_effect=docker):
                    self.assertFalse(f.cleanup())
                if changed == "node-id":
                    self.assertFalse(any(args[1] in ("unpause", "rm") for args in calls))
                else:
                    self.assertNotIn(["docker", "network", "rm", f.network], calls)


@unittest.skipUnless(__debug__, "CLI explicitly rejects optimized Python; subprocess checks cover that refusal")
class EntryTests(unittest.TestCase):
    def invoke(self, root, owner="weir-m21-unit"):
        args = ["test-kubernetes.py", "--owner", owner, "--evidence", str(root)]
        with patch.object(sys, "argv", args), patch.dict(os.environ, {"WEIR_KUBE_INTEGRATION": "1"}):
            ENTRY.main()

    def test_existing_paths_refused_without_reading_or_external_commands(self):
        for kind in ("directory", "file", "root-link", "dangling-link", "home-link", "docker-link"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                base = Path(temp)
                root, target = base/"existing", base/"target"
                target.mkdir()
                sentinel = target/"synthetic"
                sentinel.write_bytes(b"synthetic sentinel, not a credential")
                if kind == "file":
                    root.write_bytes(b"synthetic root")
                elif kind in ("root-link", "dangling-link"):
                    root.symlink_to(target if kind == "root-link" else base/"absent", target_is_directory=True)
                else:
                    root.mkdir()
                    access = root/"kubeconfig"
                    access.write_bytes(b"synthetic access sentinel")
                    before_access = access.stat()
                    if kind in ("home-link", "docker-link"):
                        (root/kind.removesuffix("-link")).symlink_to(target, target_is_directory=True)
                before_root, before_target = root.lstat(), sentinel.stat()
                with patch("subprocess.run") as run, patch("subprocess.Popen") as start, patch.object(ENTRY, "prepare") as prepare, patch.object(Path, "read_text", side_effect=AssertionError("must not read an existing file")), patch.object(Path, "read_bytes", side_effect=AssertionError("must not read an existing file")):
                    with self.assertRaises(FileExistsError):
                        self.invoke(root)
                run.assert_not_called()
                start.assert_not_called()
                prepare.assert_not_called()
                self.assertEqual(root.lstat(), before_root)
                self.assertEqual(sentinel.stat(), before_target)
                self.assertEqual(sentinel.read_bytes(), b"synthetic sentinel, not a credential")
                if kind in ("directory", "home-link", "docker-link"):
                    self.assertEqual(access.stat(), before_access)
                    self.assertEqual(access.read_bytes(), b"synthetic access sentinel")

    def test_collisions_refused_before_prepare_or_cleanup(self):
        for collision in ("running", "owner", "node", "network", "tag"):
            with self.subTest(collision=collision), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)/"fresh"
                calls = []
                def docker(args, **kwargs):
                    calls.append(args)
                    self.assertEqual(kwargs["env"]["DOCKER_CONFIG"], str(root/"docker"))
                    hit = ((collision == "running" and args[1:] == ["ps", "-q"]) or
                           (collision == "owner" and args[-1].startswith("label=")) or
                           (collision == "node" and args[-1].startswith("name=^/")) or
                           (collision == "network" and args[1:3] == ["network", "ls"]) or
                           (collision == "tag" and args[-1] == "reference=weir-m21-unit-client:local"))
                    return subprocess.CompletedProcess(args, 0, "old-id" if hit else "", "")
                with patch("subprocess.run", side_effect=docker), patch("subprocess.Popen") as start, patch.object(ENTRY, "prepare") as prepare, patch.object(Fixture, "bootstrap") as bootstrap, patch.object(Fixture, "cleanup") as cleanup:
                    with self.assertRaisesRegex(RuntimeError, "exists"):
                        self.invoke(root)
                prepare.assert_not_called()
                bootstrap.assert_not_called()
                cleanup.assert_not_called()
                start.assert_not_called()
                self.assertTrue(calls)
                self.assertTrue(all(args[1] == "ps" or args[1:3] in (["network", "ls"], ["image", "ls"]) for args in calls))

    def test_fresh_root_prepare_failure_and_success_order(self):
        for fail_prepare in (True, False):
            with self.subTest(fail_prepare=fail_prepare), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)/"fresh"
                events = []
                def docker(args, **kwargs):
                    events.append(args[1])
                    return subprocess.CompletedProcess(args, 0, "", "")
                def prepare(f, artifact, profile):
                    self.assertTrue(f.preflight_complete)
                    self.assertEqual(events, ["ps", "ps", "ps", "network", "image"])
                    self.assertTrue((root/"home").is_dir() and (root/"docker").is_dir())
                    self.assertFalse((root/"kubeconfig").exists())
                    events.append("prepare")
                    if fail_prepare:
                        raise RuntimeError("injected prepare failure")
                    return "client", "backend"
                def bootstrap(f, profile):
                    self.assertEqual(events[-1], "prepare")
                    events.append("bootstrap")
                    (root/"kubeconfig").touch()  # new empty synthetic access file
                    raise RuntimeError("injected bootstrap failure")
                with patch("subprocess.run", side_effect=docker), patch("subprocess.Popen") as start, patch.object(ENTRY, "prepare", side_effect=prepare), patch.object(Fixture, "bootstrap", autospec=True, side_effect=bootstrap):
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        self.invoke(root)
                start.assert_not_called()
                self.assertFalse((root/"kubeconfig").exists())
                self.assertTrue(all(item["clean"] for item in json.loads((root/"cleanup.json").read_text())))
                self.assertFalse(any(event in ("import", "rm", "unpause") for event in events))

    def test_partial_directory_creation_cleans_only_created_empty_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)/"fresh"
            # A child process scopes the audit hook; no persistent monkeypatch of mkdir.
            code = "import sys; from pathlib import Path; from kubernetes_fixture import Fixture\n"
            code += "root = Path(sys.argv[1])\n"
            code += "def denied(event, args):\n if event == 'os.mkdir' and args[0] == str(root/'docker'): raise PermissionError('injected')\n"
            code += "sys.addaudithook(denied)\ntry: Fixture(root, 'weir-m21-unit')\nexcept PermissionError: pass\nelse: raise RuntimeError('expected refusal')\n"
            result = subprocess.run([sys.executable, "-c", code, str(root)], cwd=REPO/"scripts", capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(root.exists())

    def test_opt_in_and_owner_refused_before_directory_creation(self):
        for owner, opt_in in (("invalid", "1"), ("weir-m21-unit", "0")):
            with self.subTest(owner=owner), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)/"fresh"
                args = ["test-kubernetes.py", "--owner", owner, "--evidence", str(root)]
                with patch.object(sys, "argv", args), patch.dict(os.environ, {"WEIR_KUBE_INTEGRATION": opt_in}), patch("subprocess.run") as run:
                    with self.assertRaises((ValueError, RuntimeError)):
                        ENTRY.main()
                run.assert_not_called()
                self.assertFalse(root.exists())


class OptimizedEntryTests(unittest.TestCase):
    def test_optimized_entry_refuses_before_any_fixture_side_effect(self):
        code = """
import runpy, sys
def reject(event, args):
    if event in ('os.mkdir', 'os.remove', 'subprocess.Popen'):
        raise AssertionError('unauthorized side effect: ' + event)
sys.addaudithook(reject)
runpy.run_path(sys.argv[1], run_name='__main__')
"""
        for flags, optimize in ((["-O"], ""), ([], "1")):
            with self.subTest(flags=flags, optimize=optimize), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)/"fresh"
                env = dict(os.environ, PYTHONOPTIMIZE=optimize, PYTHONDONTWRITEBYTECODE="1", WEIR_KUBE_INTEGRATION="1")
                args = [sys.executable, *flags, "-B", "-c", code, str(REPO/"scripts/test-kubernetes.py"), "--owner", "weir-m21-unit", "--evidence", str(root)]
                result = subprocess.run(args, cwd=REPO/"scripts", env=env, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("optimized Python is unsupported", result.stderr)
                self.assertNotIn("unauthorized side effect", result.stderr)
                self.assertFalse(root.exists())


if __name__=="__main__":unittest.main()
