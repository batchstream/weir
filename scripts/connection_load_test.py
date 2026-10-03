"""Offline checks for connection budgets and real-process evidence accounting."""

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("connection_load", Path(__file__).with_name("connection-load.py"))
LOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOAD)
FIXTURE_SPEC = importlib.util.spec_from_file_location("connection_fixture", Path(__file__).with_name("connection-fixture.py"))
FIXTURE = importlib.util.module_from_spec(FIXTURE_SPEC)
FIXTURE_SPEC.loader.exec_module(FIXTURE)


class ConnectionLoadTests(unittest.TestCase):
    def test_fixture_rejects_memory_shortfall_before_inventory_or_directory_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "fresh"
            values = dict(max_sessions=32, max_connections=64, weir_memory_mib=2560, output=root)
            options = SimpleNamespace(**values)
            with patch.object(FIXTURE, "inventory") as inventory:
                with self.assertRaisesRegex(ValueError, "memory cannot cover"):
                    FIXTURE.Fixture(options)
            inventory.assert_not_called()
            self.assertFalse(root.exists())

    def test_fixture_configuration_uses_current_batch_fields_and_sufficient_memory(self):
        with tempfile.TemporaryDirectory() as temporary:
            values = dict(max_sessions=32, max_connections=64, weir_memory_mib=4096,
                          max_read_size=None, output=Path(temporary) / "fresh")
            options = SimpleNamespace(**values)
            completed = subprocess.CompletedProcess([], 0, "127.0.0.1:9000", "")
            with patch.object(FIXTURE, "inventory", return_value={}), patch.object(FIXTURE, "run", return_value=completed), \
                    patch.object(FIXTURE, "http", return_value="ready"), patch.object(FIXTURE.Fixture, "create", return_value="owned-node"):
                fixture = FIXTURE.Fixture(options)
                fixture.start_weir(0, 2)
            node = (fixture.root / "node.yaml").read_text()
            routes = (fixture.root / "routes.yaml").read_text()
            self.assertIn('"memory": "4096MiB"', node)
            self.assertNotIn("batch_collect", routes)

    def test_fixed_total_rate_and_nondivisible_distribution(self):
        rates = LOAD.split_rates(803, 16)
        self.assertEqual(sum(rates), 803)
        self.assertEqual(len(rates), 16)
        self.assertEqual(max(rates) - min(rates), 1)
        with self.assertRaises(ValueError):
            LOAD.split_rates(15, 16)

    def test_budget_counts_each_store_heartbeat_and_termination(self):
        options = SimpleNamespace(
            store_concurrency="4,2", max_replicas=4, max_surge=0,
            terminating_pods=4, other_connections=16, database_budget=80,
        )
        budget = LOAD.connection_budget(options)
        self.assertEqual(budget["raw_tcp_limit_per_replica"], 8)
        self.assertEqual(budget["steady_weir_raw_tcp_limit"], 32)
        self.assertEqual(budget["required_database_connection_budget"], 80)
        self.assertTrue(budget["fits"])
        options.database_budget = 79
        budget = LOAD.connection_budget(options)
        self.assertFalse(budget["fits"])
        self.assertEqual(budget["max_replicas_under_assumed_overlap"], 3)

    def test_evidence_requires_processes_balanced_work_and_phase_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = datetime(2026, 10, 2, tzinfo=timezone.utc)
            for n in range(2):
                report = {
                    "type": "connection_probe", "pid": 100 + n,
                    "start": start.isoformat(),
                    "end": (start + timedelta(seconds=4)).isoformat(),
                    "idle_end": (start + timedelta(seconds=6)).isoformat(),
                    "closed": (start + timedelta(seconds=6.1)).isoformat(),
                    "options": {"seconds": 4}, "rate": 20, "planned": 80,
                    "started": 80, "success": 80, "client_drop": 0, "failures": {},
                }
                (root / ("client-" + str(n) + ".jsonl")).write_text(json.dumps(report) + "\n")
            samples = []
            for n in range(-10, 41):
                when = start + timedelta(seconds=n / 5)
                connections = 0 if n < 0 or n >= 31 else 4
                samples.append({
                    "type": "connection_sample", "pid": "1", "start_ticks": 50,
                    "time": when.isoformat(), "established": connections,
                    "states": {"01": connections}, "lost_fds": 0,
                })
            (root / "observer.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
            result = LOAD.summarize(root)
            self.assertEqual(result["offered_rps_total"], 40)
            self.assertEqual(result["success"], 160)
            self.assertEqual(result["phases"]["baseline"]["median"], 0)
            self.assertEqual(result["phases"]["active"]["median"], 4)
            self.assertEqual(result["phases"]["clients_closed_weir_retained"]["max"], 0)
            self.assertTrue(result["qualified"])
            report["pid"] = 100
            (root / "client-1.jsonl").write_text(json.dumps(report) + "\n")
            with self.assertRaisesRegex(ValueError, "non-distinct"):
                LOAD.summarize(root)

    def test_executor_is_argv_not_shell(self):
        self.assertEqual(LOAD.argv('["docker","exec","owned","/client"]')[0], "docker")
        for raw in ['"sh -c something"', '[]', '["a",4]', '[""]']:
            with self.assertRaises(ValueError):
                LOAD.argv(raw)

    def test_raw_owner_bound_and_final_close_conservation(self):
        raw = "\n".join(
            "weir_backend_connections_" + name + '{store="database"} ' + str(value)
            for name, value in (("owned", 4), ("peak", 5), ("limit", 5), ("acquired", 9), ("released", 5))
        )
        self.assertEqual(FIXTURE.owner_snapshot(raw)["owned"], 4)
        with self.assertRaises(RuntimeError):
            FIXTURE.owner_snapshot(raw.replace("released{store=\"database\"} 5", "released{store=\"database\"} 4"))
        receipt = 'time=2026-10-02T00:00:00Z level=INFO msg=backend_connections_closed backend=search store=database owned=0 peak=5 limit=5 acquired=9 released=9'
        self.assertEqual(FIXTURE.closed_owner(receipt)["released"], 9)
        with self.assertRaises(RuntimeError):
            FIXTURE.closed_owner(receipt.replace("released=9", "released=8"))
        with self.assertRaises(RuntimeError):
            FIXTURE.closed_owner(receipt + "\n" + receipt)

    def test_active_cpu_and_rss_exclude_idle_samples(self):
        start = datetime(2026, 10, 2, tzinfo=timezone.utc)
        samples = []
        for n in range(-3, 25):
            samples.append({
                "time": (start + timedelta(seconds=n)).isoformat(), "monotonic": 100 + n,
                "database": {"cpu_ticks": 100 + 20*n, "ticks_per_second": 100,
                             "rss_bytes": 1000 + n, "cgroup_memory_current": 2000 + n,
                             "memory_events": {"oom_kill": 0}},
                "weir": [{"cpu_seconds": 10 + n/4, "rss_bytes": 4000 + n,
                          "admission_rejections": {"sessions": max(0, n)}}],
            })
        report = {"clients": [{"start": start.isoformat()}], "measurement_seconds": 20}
        summary = FIXTURE.resource_summary(samples, report)
        self.assertAlmostEqual(summary["database_cpu_cores"], 0.2)
        self.assertAlmostEqual(summary["weir"][0]["cpu_cores"], 0.25)
        self.assertEqual(summary["database_rss_max"], 1020)
        self.assertEqual(summary["weir"][0]["rss_max"], 4020)
        self.assertEqual(summary["weir"][0]["admission_rejections"]["sessions"], 20)
        with self.assertRaises(RuntimeError):
            FIXTURE.resource_summary(samples[:10], report)


if __name__ == "__main__":
    unittest.main()
