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


@unittest.skipUnless(__debug__, "ordinary fixture rejects optimized Python")
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
            self.assertEqual([x["clean"] for x in result["resources"]],[False,True,True,True])

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



class ArtifactSafety(unittest.TestCase):
    def test_archive_preserves_config_and_rejects_corrupt_layer(self):
        import gzip
        import hashlib
        import io
        import tarfile
        from capacity_artifact import docker_archive
        def digest(data):
            return "sha256:"+hashlib.sha256(data).hexdigest()
        layer=b"synthetic uncompressed layer"
        compressed=gzip.compress(layer)
        config=json.dumps({"rootfs":{"diff_ids":[digest(layer)]}}).encode()
        identity={"config":digest(config),"layers":[digest(compressed)]}
        with tempfile.TemporaryDirectory() as base:
            source=Path(base)/"oci.tar";dest=Path(base)/"docker.tar"
            with tarfile.open(source,"w") as a:
                for data in (config,compressed):
                    item=tarfile.TarInfo("blobs/sha256/"+digest(data)[7:]);item.size=len(data)
                    a.addfile(item,io.BytesIO(data))
            docker_archive(source,dest,identity)
            with tarfile.open(dest) as a:
                manifest=json.load(a.extractfile("manifest.json"))[0]
                self.assertEqual(manifest["RepoTags"],[])
                self.assertEqual(a.extractfile(manifest["Config"]).read(),config)
                self.assertEqual(a.extractfile(manifest["Layers"][0]).read(),layer)
            wrong=json.dumps({"rootfs":{"diff_ids":[digest(b"wrong")]}}).encode()
            with tarfile.open(source,"w") as a:
                for data in (wrong,compressed):
                    item=tarfile.TarInfo("blobs/sha256/"+digest(data)[7:]);item.size=len(data)
                    a.addfile(item,io.BytesIO(data))
            identity["config"]=digest(wrong)
            with self.assertRaises(ValueError):docker_archive(source,dest,identity)

    @unittest.skipUnless(__debug__, "entry rejects optimized Python before imports")
    def test_product_input_identity_uses_revision_file_set(self):
        spec=importlib.util.spec_from_file_location("capacity_entry",Path(__file__).with_name("test-capacity.py"))
        entry=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(entry)
        source="a"*40
        expected={"go.mod":"fixture", "deploy/docker/node.example.json":"fixture", "deploy/docker/routes.example.json":"fixture"}
        with tempfile.TemporaryDirectory() as base:
            artifact=Path(base)
            for inputs in ({name:digest for name,digest in expected.items() if "routes.example" not in name}, dict(expected, extra="fixture")):
                receipt=dict(source=source,inputs=inputs)
                (artifact/"receipt.json").write_text(json.dumps(receipt))
                plan=dict(artifact_source=source)
                with patch.object(entry.packaging,"source_files",return_value=[(name,"fixture") for name in expected]) as source_files, patch.object(entry,"sha") as digest:
                    with self.assertRaisesRegex(RuntimeError,"source/input identity"):
                        entry.prepare(None,plan,artifact)
                source_files.assert_called_once_with(entry.REPO,source)
                digest.assert_not_called()


class CountSafety(unittest.TestCase):
    def test_quantiles_are_recomputed_and_drop_is_not_success(self):
        histogram={"buckets":[{"upper_us":1000,"count":9}],"p50_us":1000,"p95_us":1000,"p99_us":1000}
        metrics={"planned":10,"started":9,"completed":9,"success":9,"client_drop":1,"client_late":1,"unknown":0,"failures":{},"arrival":histogram,"dispatch":histogram,"lag":histogram}
        from capacity_report_test import metrics as clean_metrics
        window={"all":metrics,"read":metrics,"put":clean_metrics(0)}
        self.assertTrue(window_gate(window))
        histogram["p99_us"]=100
        with self.assertRaises(RuntimeError):window_gate(window)


if __name__=="__main__":unittest.main()
