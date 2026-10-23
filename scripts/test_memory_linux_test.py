"""Offline failure injection at subprocess/socket boundaries; no Docker or DB."""
import contextlib
import importlib.util
import io
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class CleanupTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("memory_fixture", Path(__file__).with_name("test-memory-linux.py"))
        self.fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.fixture)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture.OUTPUT = Path(self.temp.name) / "owned"
        self.fixture.OWNER = "owned"
        self.resources = {}
        self.calls = []
        self.failure = ""
        self.cleanups = []

    def command(self, args, **kwargs):
        self.calls.append(args)
        if "--format" in args:
            template = args[args.index("--format")+1]
            self.assertNotIn("{{json .}}", template)
            json.loads(re.sub(r"{{.*?}}", "null", template))
        output, code = "", 0
        if args[:2] == ["docker", "info"]:
            meta = dict(OSType="linux", CgroupVersion="2", Architecture="aarch64")
            output = json.dumps(meta)
        elif args[:3] == ["docker", "image", "inspect"]:
            if self.failure == "image":
                raise subprocess.CalledProcessError(1, args, output="Error response from daemon: No such image")
            meta = dict(Id="pinned", Architecture="arm64", Os="linux", RepoDigests=[])
            output = json.dumps(meta if "--format" in args else [meta])
        elif args[0] == "go":
            Path(args[args.index("-o")+1]).write_text("owned binary")
            if self.failure == "build" and args[-1] == "./internal/app":
                raise subprocess.TimeoutExpired(args, 1)
        elif args[:3] == ["docker", "network", "create"]:
            self.resources[args[-1]] = "owned"
            if self.failure == "network_timeout":
                raise subprocess.TimeoutExpired(args, 1)
        elif args[:2] in (["docker", "create"], ["docker", "run"]):
            name = args[args.index("--name")+1]
            self.resources[name] = "other" if self.failure == "mismatch" else "owned"
            if self.failure == "container_timeout":
                raise subprocess.TimeoutExpired(args, 1)
        elif len(args) > 2 and args[2] == "inspect":
            name = args[-1]
            if self.failure == "inspect" and name.endswith("cli"):
                raise subprocess.TimeoutExpired(args, 1)
            if name not in self.resources:
                code, output = 1, f"Error response from daemon: No such {args[1]}: {name}"
                if args[1] == "network":
                    output = f"Error response from daemon: network {name} not found"
            else:
                owner = self.resources[name]
                state = dict(Running=False, ExitCode=0, OOMKilled=False, Status="exited", Pid=0)
                meta = dict(Id=name, Owner=owner, **state)
                if "--format" not in args:
                    labels = {"weir.owner": owner}
                    config = dict(Labels=labels)
                    minimal = dict(Id=name, Config=config, Labels=labels, State=state)
                    meta = [minimal]
                output = json.dumps(meta)
        elif args[:2] == ["docker", "start"]:
            if self.failure in ("mismatch", "wait", "marker", "absent") or self.failure in ("logs", "inspect") and args[-1].endswith("cli"):
                if self.failure == "marker":
                    (self.fixture.OUTPUT / "owner").write_text("other")
                if self.failure == "absent":
                    self.resources.pop(args[-1])
                raise RuntimeError("case failure")
        elif args[:2] == ["docker", "logs"]:
            if self.failure == "logs":
                raise subprocess.TimeoutExpired(args, 1)
        elif args[:2] == ["docker", "rm"] or args[:3] == ["docker", "network", "rm"]:
            self.resources.pop(args[-1])
        result = subprocess.CompletedProcess(args, code, output)
        return result

    def cleanup(self, resources):
        result = self.original_cleanup(resources)
        self.cleanups.append(result)
        return result

    def execute(self):
        env = dict(WEIR_MEMORY_INTEGRATION="1")
        self.original_cleanup = self.fixture.cleanup
        with patch.dict(os.environ, env), \
             patch.object(subprocess, "run", side_effect=self.command), \
             patch.object(self.fixture, "cleanup", side_effect=self.cleanup), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(Exception) as caught:
                self.fixture.main()
        return str(caught.exception)

    def test_logs_failure_preserves_case_failure_and_stops_all(self):
        self.failure = "logs"
        error = self.execute()
        self.assertIn("case failure", error)
        self.assertFalse(self.resources)
        self.assertTrue(self.cleanups[-1]["errors"])
        self.assertTrue(self.cleanups[-1]["all_stopped"])

    def test_inspect_failure_does_not_skip_network_and_database(self):
        # guard succeeds; cli inspect fails after create/start.
        self.failure = "inspect"
        self.execute()
        self.assertTrue(any(args[:3] == ["docker", "network", "rm"] for args in self.calls))
        self.assertIn("owned-cli", self.resources)
        self.assertNotIn("owned-guard", self.resources)
        self.assertNotIn("owned-mongo", self.resources)
        self.assertFalse(self.cleanups[-1]["all_stopped"])
        self.assertTrue((self.fixture.OUTPUT / "weir").exists())

    def test_partial_create_and_build_failures(self):
        for failure in ("network_timeout", "container_timeout", "build"):
            with self.subTest(failure=failure):
                self.failure = failure
                self.execute()
                self.assertFalse(self.resources)
                for name in ("weir", "app.test", "overload.test"):
                    self.assertFalse((self.fixture.OUTPUT / name).exists())
                # Fresh output path on the next run.
                self.fixture.OUTPUT = self.fixture.OUTPUT.with_name(failure)

    def test_owner_mismatch_is_not_modified(self):
        self.failure = "mismatch"
        self.execute()
        expected = {"owned-mongo": "other", "owned-guard": "other"}
        self.assertEqual(self.resources, expected)
        self.assertFalse(any(args[:2] in (["docker", "stop"], ["docker", "rm"]) for args in self.calls))

    def test_output_owner_mismatch_preserves_files(self):
        self.failure = "marker"
        self.execute()
        self.assertTrue((self.fixture.OUTPUT / "weir").exists())

    def test_absent_container_counts_as_stopped(self):
        self.failure = "absent"
        self.execute()
        self.assertFalse(self.resources)
        self.assertTrue(self.cleanups[-1]["all_stopped"])

    def test_missing_image_names_pull(self):
        self.failure = "image"
        error = self.execute()
        self.assertIn("docker pull " + self.fixture.IMAGE, error)
        self.assertFalse(any(args[0] == "go" for args in self.calls))

    def test_every_case_gets_its_own_bounded_container(self):
        cases = {case["name"]: self.fixture.container_args(case, "net") for case in self.fixture.CASES}
        self.assertEqual(len(cases), len(self.fixture.CASES))
        for args in cases.values():
            self.assertIn("--read-only", args)
            self.assertTrue(any(arg.startswith("--memory=") for arg in args))
        self.assertIn("--shm-size=240m", cases["shmem"])
        self.assertIn("type=volume,dst=/scratch", cases["page-cache"])
        self.assertIn("GOMEMLIMIT=off", cases["runtime-off"])
        self.assertNotIn("GOMEMLIMIT=off", cases["runtime-automatic"])


if __name__ == "__main__":
    unittest.main()
