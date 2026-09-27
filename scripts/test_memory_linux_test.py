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
from unittest.mock import MagicMock, patch


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
        self.process = MagicMock(pid=123, returncode=0)
        self.process.poll.return_value = None
        self.process.wait.return_value = 0

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
                    legacy = dict(Id=name, Config=config, Labels=labels, State=state)
                    meta = [legacy]
                output = json.dumps(meta)
        elif args[:2] == ["docker", "start"]:
            if self.failure in ("mismatch", "wait", "marker", "absent") or self.failure in ("logs", "inspect") and args[-1].endswith("cli"):
                if self.failure == "marker":
                    (self.fixture.OUTPUT / "owner").write_text("other")
                if self.failure == "absent":
                    self.resources.pop(args[-1])
                raise RuntimeError("original test failure")
        elif args[:2] == ["docker", "logs"]:
            if self.failure == "logs":
                raise subprocess.TimeoutExpired(args, 1)
        elif args[:2] == ["docker", "rm"] or args[:3] == ["docker", "network", "rm"]:
            self.resources.pop(args[-1])
        result = subprocess.CompletedProcess(args, code, output)
        return result

    def execute(self):
        candidate = MagicMock()
        candidate.__enter__.return_value.getsockname.return_value = ("127.0.0.1", 12345)
        connection = MagicMock()
        env = dict(WEIR_M14_INTEGRATION="1", WEIR_M14_HOST_MONGO="1")
        with patch.dict(os.environ, env), patch.object(self.fixture.sys, "platform", "darwin"), \
             patch.object(subprocess, "run", side_effect=self.command), \
             patch.object(subprocess, "Popen", return_value=self.process), \
             patch.object(self.fixture.socket, "socket", return_value=candidate), \
             patch.object(self.fixture.socket, "create_connection", return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(Exception) as caught:
                self.fixture.main()
        return str(caught.exception)

    def test_logs_failure_preserves_original_and_stops_all(self):
        self.failure = "logs"
        error = self.execute()
        self.assertIn("original test failure", error)
        self.process.send_signal.assert_called_once()
        self.process.wait.assert_called_once()
        self.assertFalse(self.resources)
        receipt = json.loads((self.fixture.OUTPUT / "cleanup.json").read_text())
        self.assertIn("original test failure", receipt["test_error"])
        self.assertTrue(receipt["errors"])
        self.assertTrue(receipt["all_stopped"])

    def test_inspect_failure_does_not_skip_network_and_host(self):
        # guard succeeds; cli inspect fails after create/start. Host must always Wait.
        self.failure = "inspect"
        error = self.execute()
        self.process.send_signal.assert_called_once()
        self.process.wait.assert_called_once()
        self.assertTrue(any(args[:3] == ["docker", "network", "rm"] for args in self.calls))
        self.assertIn("owned-cli", self.resources)
        self.assertNotIn("owned-guard", self.resources)
        receipt = json.loads((self.fixture.OUTPUT / "cleanup.json").read_text())
        self.assertFalse(receipt["all_stopped"])
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
        expected = {"owned-guard": "other"}
        self.assertEqual(self.resources, expected)
        self.assertFalse(any(args[:2] in (["docker", "stop"], ["docker", "rm"]) for args in self.calls))
        self.process.wait.assert_called_once()

    def test_wait_failure_preserves_data(self):
        self.failure = "wait"
        self.process.wait.side_effect = subprocess.TimeoutExpired("owned mongod", 10)
        self.execute()
        self.process.kill.assert_called_once()
        self.assertEqual(self.process.wait.call_count, 2)
        self.assertTrue((self.fixture.OUTPUT / "mongo-data").exists())
        receipt = json.loads((self.fixture.OUTPUT / "cleanup.json").read_text())
        self.assertFalse(receipt["all_stopped"])

    def test_output_owner_mismatch_preserves_files(self):
        self.failure = "marker"
        self.execute()
        self.process.wait.assert_called_once()
        self.assertTrue((self.fixture.OUTPUT / "mongo-data").exists())
        self.assertTrue((self.fixture.OUTPUT / "weir").exists())

    def test_absent_candidate_has_explicit_evidence(self):
        self.failure = "absent"
        self.execute()
        self.process.wait.assert_called_once()
        self.assertFalse(self.resources)
        receipt = json.loads((self.fixture.OUTPUT / "cleanup.json").read_text())
        self.assertTrue(receipt["all_stopped"])


if __name__ == "__main__":
    unittest.main()
