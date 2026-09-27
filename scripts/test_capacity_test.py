"""Offline safety boundaries; no Docker daemon or existing evidence access."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from capacity_fixture import Fixture, inventory_diff
from capacity_report import window_gate


class Safety(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{"WEIR_CAPACITY_INTEGRATION":"1"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_existing_roots_untouched(self):
        with tempfile.TemporaryDirectory() as base:
            root=Path(base)/"existing";root.mkdir();sentinel=root/"sentinel";sentinel.write_text("original")
            with self.assertRaises(FileExistsError):Fixture(root,"weir-m22-test")
            self.assertEqual(sentinel.read_text(),"original")
            link=Path(base)/"link";link.symlink_to(root)
            with self.assertRaises(FileExistsError):Fixture(link,"weir-m22-test")

    def test_optin_and_owner_before_mkdir(self):
        with tempfile.TemporaryDirectory() as base:
            root=Path(base)/"new"
            with patch.dict(os.environ,{"WEIR_CAPACITY_INTEGRATION":"0"}):
                with self.assertRaises(RuntimeError):Fixture(root,"weir-m22-test")
            with self.assertRaises(ValueError):Fixture(root,"arbitrary")
            self.assertFalse(root.exists())

    def test_partial_initialization_unwinds_owned_empty_directories(self):
        real=Path.mkdir
        def fail(path,*args,**kwargs):
            if path.name=="docker":raise OSError("injected")
            return real(path,*args,**kwargs)
        with tempfile.TemporaryDirectory() as base:
            root=Path(base)/"new"
            with patch.object(Path,"mkdir",fail):
                with self.assertRaises(OSError):Fixture(root,"weir-m22-test")
            self.assertFalse(root.exists())

    def test_real_preflight_collision_prevents_mutation(self):
        with tempfile.TemporaryDirectory() as base:
            f=Fixture(Path(base)/"new","weir-m22-test")
            commands=[]
            inventory={"containers":[],"networks":[],"volumes":[]}
            def run(args,*a,**kw):
                commands.append(args)
                return subprocess.CompletedProcess(args,0,"collision" if "reference="+f.tag in args else "","")
            with patch.object(f,"inventory",return_value=inventory),patch.object(f,"run",side_effect=run):
                with self.assertRaises(RuntimeError):f.preflight()
            self.assertFalse(f.preflight_complete)
            self.assertTrue(all(c[1] in ("ps","network","image") for c in commands))

    def test_cleanup_continues_after_owner_failure(self):
        with tempfile.TemporaryDirectory() as base:
            f=Fixture(Path(base)/"new","weir-m22-test")
            f.attempted=[f.owner+"-es",f.owner+"-weir"]
            def run(args,*a,**kw):return subprocess.CompletedProcess(args,0,"","")
            obj={"Id":"owned","State":{}}
            with patch.object(f,"run",side_effect=run),patch.object(f,"owned",side_effect=[RuntimeError("foreign owner"),obj,obj]):
                self.assertFalse(f.cleanup())
            result=json.loads((f.root/"cleanup.json").read_text())
            self.assertEqual([x["clean"] for x in result],[False,True])

    def test_optimized_entry_has_no_side_effects(self):
        with tempfile.TemporaryDirectory() as base:
            for extra,env in ((["-O"],os.environ.copy()),([],dict(os.environ,PYTHONOPTIMIZE="1"))):
                root=Path(base)/("root"+str(len(extra)))
                command=[sys.executable,*extra,str(Path(__file__).with_name("test-capacity.py")),"--evidence",str(root),"--owner","weir-m22-test"]
                r=subprocess.run(command,env=env,capture_output=True,text=True,timeout=5)
                self.assertNotEqual(r.returncode,0);self.assertFalse(root.exists())

    def test_bridge_change_separate_from_foreign_change(self):
        before={"containers":[],"volumes":[],"networks":[{"Name":"bridge","Id":"a"},{"Name":"user","Id":"x"}]}
        after={"containers":[],"volumes":[],"networks":[{"Name":"bridge","Id":"b"},{"Name":"user","Id":"x"}]}
        self.assertTrue(inventory_diff(before,after)["default_bridge_changed"])
        self.assertFalse(inventory_diff(before,after)["nondefault_changed"])
        after["networks"][1]["Id"]="changed"
        self.assertTrue(inventory_diff(before,after)["nondefault_changed"])


if __name__=="__main__":unittest.main()
