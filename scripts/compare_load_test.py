"""Offline paired-binary load orchestration; no Docker or network requests."""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, call, patch


spec = importlib.util.spec_from_file_location("compare_load", Path(__file__).with_name("compare-load.py"))
entry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entry)


class PairedComparison(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        binaries = {"client": b"same-client", "weir": b"current-weir", "baseline": b"baseline-weir"}
        for name, data in binaries.items():
            (root / name).write_bytes(data)
        values = {
            "client": root / "client", "weir": root / "weir", "baseline_weir": root / "baseline",
            "baseline_source": "b" * 40, "client_source": None,
            "weir_source": "c" * 40, "output": root / "output",
            "rates": "3200", "write_every": "10", "modes": "baseline,weir", "repetitions": 3,
            "seconds": 30, "warm": 20, "recovery_rate": 0, "recovery_seconds": 20,
            "pool": 4, "direct_pool": 0, "batch_operations": 16, "db_cpu": 0.25,
            "db_queue": 200, "prewarm": True, "es_image": "sha256:test-image",
        }
        self.args = argparse.Namespace(**values)

    def test_archive_and_provenance_include_each_production_binary(self):
        self.args.client_source = "d" * 40
        fixture = entry.Fixture(self.args)
        client_hash = hashlib.sha256(self.args.client.read_bytes()).hexdigest()
        probe = {"exe_sha256": client_hash,
                 "samples": [{"process": {"identity": {"exe_sha256": client_hash}}}],
                 "trial": {"measure": {"all": {"planned": 32000, "success": 32000,
                                                  "client_drop": 0, "lag": {"p99_us": 1000}}}}}

        def command(argv, **_options):
            stdout = ""
            if argv[:3] == ["docker", "import", "--platform"]:
                stdout = "sha256:owned-image"
            elif argv[:2] == ["docker", "port"]:
                stdout = "127.0.0.1:9000"
            elif argv == ["git", "rev-parse", "HEAD"]:
                stdout = "a" * 40
            elif argv[:2] == ["docker", "info"]:
                stdout = "test-engine"
                if "NCPU" in argv[-1]:
                    stdout = '{"cpus":8,"memory_bytes":8589934592,"kernel":"test","arch":"aarch64"}'
            elif "pace" in argv:
                stdout = json.dumps(probe)
            result = subprocess.CompletedProcess(argv, 0, stdout, "")
            return result

        identity = {"version": {"number": "8.19.22"}}
        inventory = {"containers": [], "networks": {}, "volumes": []}
        with patch.object(entry, "run", side_effect=command), \
                patch.object(entry, "http", return_value=json.dumps(identity)), \
                patch.object(fixture, "inventory", return_value=inventory), \
                patch.object(fixture, "create", side_effect=["db-id", "client-id"]):
            fixture.start()
        with tarfile.open(fixture.root / "binaries.tar") as archive:
            self.assertEqual(archive.getnames(), ["client", "weir", "baseline"])
            for name, path in fixture.binaries.items():
                self.assertEqual(archive.extractfile(name).read(), path.read_bytes())
                self.assertEqual(archive.getmember(name).mode, 0o555)
        provenance = fixture.summary["provenance"]
        expected_sources = {"client": "d" * 40, "weir": "c" * 40, "baseline": "b" * 40}
        self.assertEqual(provenance["declared_binary_sources"], expected_sources)
        expected_hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest()
                           for name, path in fixture.binaries.items()}
        self.assertEqual(provenance["binary_sha256"], expected_hashes)

    def test_provenance_hashes_imported_bytes_when_binary_paths_change_during_startup(self):
        fixture = entry.Fixture(self.args)
        imported_binaries = {}
        client_hash = hashlib.sha256(self.args.client.read_bytes()).hexdigest()
        probe = {"exe_sha256": client_hash,
                 "samples": [{"process": {"identity": {"exe_sha256": client_hash}}}],
                 "trial": {"measure": {"all": {"planned": 32000, "success": 32000,
                                                  "client_drop": 0, "lag": {"p99_us": 1000}}}}}

        def command(argv, **_options):
            stdout = ""
            if argv[:3] == ["docker", "import", "--platform"]:
                with tarfile.open(argv[-1]) as archive:
                    for name in archive.getnames():
                        imported_binaries[name] = archive.extractfile(name).read()
                stdout = "sha256:owned-image"
            elif argv[:2] == ["docker", "port"]:
                stdout = "127.0.0.1:9000"
            elif argv == ["git", "rev-parse", "HEAD"]:
                stdout = "a" * 40
            elif argv[:2] == ["docker", "info"]:
                stdout = "test-engine"
                if "NCPU" in argv[-1]:
                    stdout = '{"cpus":8,"memory_bytes":8589934592,"kernel":"test","arch":"aarch64"}'
            elif "pace" in argv:
                stdout = json.dumps(probe)
            result = subprocess.CompletedProcess(argv, 0, stdout, "")
            return result

        def startup(_url):
            self.assertEqual(set(imported_binaries), {"client", "weir", "baseline"})
            self.args.weir.write_bytes(b"replaced-current-weir")
            self.args.baseline_weir.write_bytes(b"replaced-baseline-weir")
            identity = {"version": {"number": "8.19.22"}}
            return json.dumps(identity)

        inventory = {"containers": [], "networks": {}, "volumes": []}
        with patch.object(entry, "run", side_effect=command), \
                patch.object(entry, "http", side_effect=startup), \
                patch.object(fixture, "inventory", return_value=inventory), \
                patch.object(fixture, "create", side_effect=["db-id", "client-id"]):
            fixture.start()
        expected_hashes = {name: hashlib.sha256(data).hexdigest()
                           for name, data in imported_binaries.items()}
        self.assertEqual(fixture.summary["provenance"]["binary_sha256"], expected_hashes)
        self.assertEqual(fixture.summary["provenance"]["declared_binary_sources"]["client"], "a" * 40)
        for name in ("weir", "baseline"):
            current_hash = hashlib.sha256(fixture.binaries[name].read_bytes()).hexdigest()
            self.assertNotEqual(expected_hashes[name], current_hash)

    def test_production_entrypoints_share_config_and_resource_limits(self):
        fixture = entry.Fixture(self.args)
        fixture.client = "client-id"
        commands = []

        def create(name, options, command):
            commands.append((name, options, command))
            return "node-" + name

        completed = subprocess.CompletedProcess([], 0, "127.0.0.1:9000", "")
        with patch.object(entry, "run", return_value=completed), \
                patch.object(entry, "http", return_value="metrics"), \
                patch.object(fixture, "stop_weir"), patch.object(fixture, "create", side_effect=create):
            fixture.start_weir("baseline")
            baseline_config = (fixture.root / "node.yaml").read_text()
            baseline_routes = (fixture.root / "routes.yaml").read_text()
            fixture.start_weir("weir")
            current_config = (fixture.root / "node.yaml").read_text()
            current_routes = (fixture.root / "routes.yaml").read_text()
        self.assertIn('"routing":\n  "file": "routes.yaml"\n', current_config)
        self.assertNotIn('"services":', current_config)
        self.assertNotIn('"routes":', current_config)
        self.assertEqual(baseline_config, current_config)
        self.assertEqual(baseline_routes, current_routes)
        self.assertEqual(commands[0][2], ["serve", "--config", "/node.yaml"])
        self.assertEqual(commands[1][2], commands[0][2])
        baseline_options = commands[0][1]
        current_options = commands[1][1]
        self.assertEqual(baseline_options[:-1], current_options[:-1])
        self.assertEqual(baseline_options[-1], "/baseline")
        self.assertEqual(current_options[-1], "/weir")
        for options in (baseline_options, current_options):
            self.assertEqual(options[options.index("--cpus") + 1], "2")
            self.assertEqual(options[options.index("--cpuset-cpus") + 1], "3,4")
            self.assertEqual(options[options.index("--memory") + 1], "768m")
            self.assertNotIn("WEIR_CAPACITY_INTEGRATION=1", options)
            for filename in ("node.yaml", "routes.yaml"):
                self.assertIn("type=bind,source=" + str(fixture.root / filename) + ",target=/" + filename + ",readonly", options)

    def test_shared_prewarm_covers_both_production_paths_and_restores_cpu(self):
        fixture = entry.Fixture(self.args)
        fixture.db = "db-id"
        fixture.client = "client-id"
        fixture.summary["provenance"] = {
            "binary_sha256": {"baseline": "baseline-hash", "weir": "current-hash"},
            "declared_binary_sources": {"baseline": "b" * 40, "weir": "c" * 40},
        }
        trial = {"type": "trial", "run_error": "<nil>", "trial": {"measure": {"all": {"success": 16000}}}}
        audit = {"type": "audit", "error": "<nil>"}
        output = json.dumps(trial) + "\n" + json.dumps(audit) + "\n"
        completed = subprocess.CompletedProcess([], 0, output, "")
        with patch.object(entry, "run", return_value=completed) as run, \
                patch.object(fixture, "start_weir") as start, patch.object(fixture, "stop_weir"):
            fixture.prewarm(10)
        self.assertEqual(run.call_args_list[0], call(["docker", "update", "--cpus", "1", "db-id"]))
        self.assertEqual(start.call_args_list, [call("baseline"), call("weir")])
        receipts = fixture.summary["prewarm"]
        self.assertEqual([r["mode"] for r in receipts], ["direct", "baseline", "weir"])
        self.assertTrue(all(r["database_cpu"] == 1 for r in receipts))
        self.assertEqual(receipts[1]["binary_sha256"], "baseline-hash")
        self.assertEqual(receipts[2]["declared_source"], "c" * 40)
        trials = [c.args[0] for c in run.call_args_list if "trial" in c.args[0]]
        self.assertNotIn("-target", trials[0])
        self.assertTrue(all(c[c.index("-target") + 1] == "weir:7447" for c in trials[1:]))

    def test_diagnostic_prewarm_uses_adaptive_path(self):
        self.args.modes = "adaptive,control"
        fixture = entry.Fixture(self.args)
        fixture.db = "db-id"
        fixture.client = "client-id"
        trial = {"type": "trial", "run_error": "<nil>", "trial": {"measure": {"all": {}}}}
        audit = {"type": "audit", "error": "<nil>"}
        completed = subprocess.CompletedProcess([], 0, json.dumps(trial) + "\n" + json.dumps(audit), "")
        with patch.object(entry, "run", return_value=completed), \
                patch.object(fixture, "start_weir") as start, patch.object(fixture, "stop_weir"):
            fixture.prewarm(1)
        self.assertEqual(start.call_args_list, [call("adaptive")])
        self.assertEqual([r["mode"] for r in fixture.summary["prewarm"]], ["direct", "adaptive"])

    def test_main_alternates_paired_trials_with_same_client(self):
        fixture = Mock()
        fixture.cleanup.return_value = True
        argv = ["compare-load.py", "--client", "client", "--weir", "weir", "--baseline-weir", "baseline",
                "--baseline-source", "old-ref", "--weir-source", "new-ref", "--output", "unused",
                "--modes", "baseline,weir", "--repetitions", "3", "--prewarm", "--rates", "3200",
                "--write-every", "10", "--warm", "20", "--seconds", "30"]

        def resolve(command):
            source = "b" * 40 if command[-1] == "old-ref^{commit}" else "c" * 40
            result = subprocess.CompletedProcess(command, 0, source, "")
            return result

        with patch.object(sys, "argv", argv), patch.object(entry, "run", side_effect=resolve), \
                patch.object(entry, "Fixture", return_value=fixture) as factory, patch.object(entry.signal, "signal"):
            entry.main()
        options = factory.call_args.args[0]
        self.assertEqual(options.client, Path("client"))
        self.assertEqual(options.baseline_source, "b" * 40)
        self.assertIsNone(options.client_source)
        self.assertEqual(options.weir_source, "c" * 40)
        self.assertEqual(fixture.prewarm.call_args_list, [call(10)])
        expected = [call("baseline", 3200, 0, 10), call("weir", 3200, 0, 10),
                    call("weir", 3200, 1, 10), call("baseline", 3200, 1, 10),
                    call("baseline", 3200, 2, 10), call("weir", 3200, 2, 10)]
        self.assertEqual(fixture.trial.call_args_list, expected)
        fixture.start.assert_called_once()
        fixture.cleanup.assert_called_once()

    def test_main_resolves_explicit_older_client_source(self):
        fixture = Mock()
        fixture.cleanup.return_value = True
        argv = ["compare-load.py", "--client", "client", "--client-source", "older-client",
                "--weir", "weir", "--output", "unused"]
        resolved = subprocess.CompletedProcess([], 0, "d" * 40, "")
        with patch.object(sys, "argv", argv), patch.object(entry, "run", return_value=resolved) as run, \
                patch.object(entry, "Fixture", return_value=fixture) as factory, patch.object(entry.signal, "signal"):
            entry.main()
        run.assert_called_once_with(["git", "rev-parse", "--verify", "--end-of-options", "older-client^{commit}"])
        self.assertEqual(factory.call_args.args[0].client_source, "d" * 40)
        fixture.start.assert_called_once()
        fixture.cleanup.assert_called_once()

    def test_incomplete_paired_options_fail_before_fixture_creation(self):
        base = ["compare-load.py", "--client", "client", "--weir", "weir", "--output", "unused",
                "--modes", "baseline,weir"]
        variants = [[], ["--baseline-weir", "baseline"],
                    ["--baseline-weir", "baseline", "--baseline-source", "old"],
                    ["--baseline-weir", "baseline", "--baseline-source", "old", "--prewarm", "--repetitions", "2"]]
        for extra in variants:
            with self.subTest(options=extra), patch.object(sys, "argv", base + extra), \
                    patch.object(entry, "Fixture") as fixture, patch.object(entry, "run") as run, \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                entry.main()
            fixture.assert_not_called()
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
