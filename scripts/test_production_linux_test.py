"""Offline finite executor failure injection. No services, Docker or host changes."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

SPEC = importlib.util.spec_from_file_location("production_linux_fixture", Path(__file__).with_name("test-production-linux.py"))
M = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = M
SPEC.loader.exec_module(M)
ROOT = Path(__file__).resolve().parent.parent


# Actual failed CI39333200153 command-0011/0013 inspect data; internal bridge, no published ports.
RECORDED_INTERNAL_BACKEND = json.loads('''
{
  "CPU": 2000000000,
  "Caps": [
    "ALL"
  ],
  "Cgroupns": "private",
  "Id": "74f73797d7a40c67c00c3f9d3079d28f695d6511d90fd8ec6f3ee1fdaa1dd9e0",
  "Image": "sha256:5d068400eb33be90f060467b36ba31612ae488d928672a2a83b78abf334283b1",
  "Memory": 1610612736,
  "Mounts": [
    {
      "Destination": "/observer",
      "Mode": "",
      "Propagation": "rprivate",
      "RW": false,
      "Source": "/home/runner/work/weir/weir/.testdata/native-linux-finite/bin/observer",
      "Type": "bind"
    }
  ],
  "Networks": {
    "weir-finite-d6da2c800fed443480909ff3c5d504b1": {
      "Aliases": null,
      "DNSNames": [
        "weir-finite-d6da2c800fed443480909ff3c5d504b1-backend",
        "74f73797d7a4"
      ],
      "DriverOpts": null,
      "EndpointID": "a4a1ba3b434a344f474be247fdda3f5ff7125884e05d04073d4aea3d6e21deb9",
      "Gateway": "",
      "GlobalIPv6Address": "",
      "GlobalIPv6PrefixLen": 0,
      "GwPriority": 0,
      "IPAMConfig": null,
      "IPAddress": "172.18.0.2",
      "IPPrefixLen": 16,
      "IPv6Gateway": "",
      "Links": null,
      "MacAddress": "be:d4:62:b2:29:bd",
      "NetworkID": "c498007cc074b17cc7ed101c72f3c3e2c2007f2b7054b64ed3c0a38f53c34545"
    }
  },
  "Owner": "weir-finite-d6da2c800fed443480909ff3c5d504b1",
  "PIDs": 512,
  "PidMode": "",
  "Ports": {},
  "Privileged": false,
  "Readonly": true,
  "Security": [
    "no-new-privileges:true"
  ],
  "State": {
    "Dead": false,
    "Error": "",
    "ExitCode": 0,
    "FinishedAt": "0001-01-01T00:00:00Z",
    "OOMKilled": false,
    "Paused": false,
    "Pid": 8275,
    "Restarting": false,
    "Running": true,
    "StartedAt": "2026-10-19T22:13:13.863990307Z",
    "Status": "running"
  },
  "Swap": 1610612736,
  "User": "1000:0"
}
''')


class Contracts(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.opts = M.Options(server_root=ROOT, harness_root=ROOT, revision="a" * 40, cli=self.root / "weir", driver=self.root / "app.test", observer=self.root / "observer", output=self.root / "evidence")
        for path in (self.opts.cli, self.opts.driver, self.opts.observer):
            path.write_bytes(b"compiled fixture")
        self.controller = M.Controller(self.opts)
        self.controller.owner = "weir-finite-" + "1" * 32
        self.calls = []
        self.resources = {}
        self.empty = {}
        self.failure = ""
        self.primary = False

    def command(self, args, timeout=15):
        self.calls.append(args)
        if "inspect" in args:
            template = args[args.index("--format") + 1]
            self.assertNotIn(".Env", template)
            self.assertNotIn("json .}}", template)
        if args[0] == "info":
            info = dict(OS="linux", Arch="x86_64", Cgroup="2", CPUs=4, Memory=7 << 30, Root=str(self.root), Kernel=M.platform.release())
            return json.dumps(info)
        if args[:2] == ["image", "inspect"]:
            ref = args[-1]
            info = dict(Id="sha256:" + "b" * 64, OS="linux", Arch="amd64", Digests=[ref])
            return json.dumps(info)
        if args[1] == "create":
            name = args[-1] if args[0] == "network" else args[args.index("--name") + 1]
            identity = str(len(self.resources) + 1) * 64
            if args[0] == "network":
                containers = {}
                driver = next(a.split("=", 1)[1] for a in args if a.startswith("--driver="))
                info = dict(Id=identity, Owner=self.controller.owner, Driver=driver, Internal="--internal" in args, Containers=containers)
            else:
                flags = {a.split("=", 1)[0]: a.split("=", 1)[1] for a in args if a.startswith("--") and "=" in a}
                net = self.resources[self.controller.owner]
                ports = {}
                for i, arg in enumerate(args):
                    # The real internal-only bridge did not program host publications.
                    if arg == "--publish" and not net["Internal"]:
                        host, hostport, port = args[i + 1].split(":")
                        ports[port + "/tcp"] = [dict(HostIp=host, HostPort=hostport)]
                mounts = [dict(Destination="/observer", Source=str(self.opts.observer), RW=False)]
                state = dict(Running=False, ExitCode=0, OOMKilled=False, Pid=100 + len(self.resources))
                membership = dict(NetworkID=net["Id"])
                networks = {self.controller.owner: membership}
                info = dict(Id=identity, Owner=self.controller.owner, Image="sha256:" + "b" * 64, State=state, Memory=int(flags["--memory"]), Swap=int(flags["--memory-swap"]), CPU=2_000_000_000, PIDs=int(flags["--pids-limit"]), Cgroupns="private", Readonly=True, User=flags["--user"], Caps=["ALL"], Security=["no-new-privileges:true"], Privileged=False, PidMode="", Networks=networks, Ports=ports, Mounts=mounts)
            self.resources[name] = info
            if self.failure == "partial-" + args[0]:
                self.failure = ""
                raise RuntimeError("primary partial create timeout")
            return identity + "\n"
        resource = args[-1]
        pair = next(((name, info) for name, info in self.resources.items() if name == resource or info["Id"] == resource), None)
        if "inspect" in args:
            if self.failure == "inspect":
                raise RuntimeError("daemon unavailable")
            if pair is None:
                raise M.CommandError("Error: No such " + args[0] + ": " + resource, 1)
            info = pair[1]
            if pair[0] == self.controller.owner:
                if self.failure == "network-driver":
                    info = dict(info, Driver="overlay")
                if self.failure == "network-internal":
                    info = dict(info, Internal=True)
                if self.failure == "network-populated":
                    containers = dict(foreign="endpoint")
                    info = dict(info, Containers=containers)
            if self.failure == "owner" and "-cli" in pair[0]:
                info = dict(info, Owner="someone-else")
            if self.failure == "id" and "-cli" in pair[0]:
                info = dict(info, Id="f" * 64)
            return json.dumps(info)
        if args[0] == "exec":
            identity = args[2]
            pair = next((name, info) for name, info in self.resources.items() if info["Id"] == identity)
            is_cli = pair[0].endswith("-cli")
            sha = self.controller.hashes["cli"] if is_cli else "d" * 64
            cpu = dict(usage_usec=10, nr_periods=1, nr_throttled=0, throttled_usec=0)
            memory_events = dict(low=0, high=0, max=0, oom=0, oom_kill=0)
            sample = dict(pid=1, start_ticks=123, executable="/weir" if is_cli else "/bin/tini", sha256=sha, observer_sha256=self.controller.hashes["observer"], membership="0::/", mount_root="/", cpu_max="200000 100000", memory_max=pair[1]["Memory"], memory_swap_max=0, pids_max=pair[1]["PIDs"], cpu=cpu, memory_current=1024, memory_peak=2048, memory_events=memory_events)
            if self.failure == "actual-controls" and is_cli:
                sample["memory_max"] = 2 << 30
            if self.failure == "observer-bytes":
                sample["observer_sha256"] = "0" * 64
            if self.failure == "pid1-bytes" and is_cli:
                sample["sha256"] = "0" * 64
            return json.dumps(sample)
        if args[:2] == ["container", "start"]:
            pair[1]["State"]["Running"] = True
        elif args[0] in ("kill", "stop"):
            pair[1]["State"]["Running"] = False
        elif args[0] == "wait":
            if self.failure == "shutdown":
                pair[1]["State"]["ExitCode"] = 1
            return "0\n"
        elif args[0] == "logs" and self.failure == "logs":
            raise RuntimeError("logs unavailable")
        elif args[1] == "rm":
            if self.failure != "remains":
                del self.resources[pair[0]]
        return ""

    def host_snapshot(self, pid, executable=True):
        info = next(info for info in self.resources.values() if info.get("State", self.empty).get("Pid") == pid)
        snapshot = dict(pid=pid, start_ticks=123, executable="", sha256="", membership="0::/system.slice/docker-" + info["Id"] + ".scope", namespace_pid=1)
        return snapshot

    def driver(self, stage, environment):
        if stage == "preflight":
            self.assertEqual(self.resources, self.empty, "preflight must finish before fixture creation")
            if self.failure == "preflight":
                raise RuntimeError("missing preflight structured completion")
        if stage == "finite" and self.failure == "driver":
            raise RuntimeError("primary driver timeout")
        result = {}
        return result

    def execute(self):
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(M, "process_snapshot", side_effect=self.host_snapshot), patch.object(self.controller, "docker", side_effect=self.command), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http"):
            self.controller.execute()

    def test_success_owned_bounded_path_and_verified_cleanup(self):
        self.execute()
        self.assertEqual(self.resources, self.empty)
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertTrue(result["finite_complete"])
        self.assertFalse(result["qualification"])
        network_creates = [a for a in self.calls if a[:2] == ["network", "create"]]
        self.assertEqual(len(network_creates), 1)
        self.assertIn("--driver=bridge", network_creates[0])
        self.assertNotIn("--internal", network_creates[0])
        network_inspects = [a for a in self.calls if a[:2] == ["network", "inspect"]]
        self.assertTrue(all("json .Driver" in a[a.index("--format") + 1] for a in network_inspects))
        creates = [a for a in self.calls if a[:2] == ["container", "create"]]
        self.assertEqual(len(creates), 2)
        for args in creates:
            self.assertIn("--pull=never", args)
            self.assertIn("--read-only", args)
            self.assertIn("--cgroupns=private", args)
            self.assertFalse(any(a == "--privileged" or a.startswith("--pid=") for a in args))
        for args in self.calls:
            self.assertNotIn("pull", args)
            self.assertNotIn("build", args)
            if args[0] in ("kill", "wait", "stop", "logs") or len(args) > 1 and args[1] == "rm":
                self.assertRegex(args[-1], "^[0-9a-f]{64}$")
        receipts = json.loads((self.opts.output / "cleanup.json").read_text())
        self.assertEqual(len(receipts["resources"]), 3)
        self.assertTrue(all(r["absent"] for r in receipts["resources"]))

    def recorded_backend(self):
        info = copy.deepcopy(RECORDED_INTERNAL_BACKEND)
        self.controller.owner = info["Owner"]
        self.opts.observer = Path(info["Mounts"][0]["Source"])
        self.opts.output.mkdir()
        network_id = next(iter(info["Networks"].values()))["NetworkID"]
        network = M.Resource("network", self.controller.owner, network_id)
        resource = M.Resource("container", self.controller.owner + "-backend", info["Id"])
        ports = {9200: 19200}
        spec = dict(role="backend", image_id=info["Image"], memory=1536 << 20, pids=512, user="1000:0", network=network, ports=ports)
        result = resource, spec, info
        return result

    def test_recorded_internal_backend_missing_publication_is_still_rejected(self):
        resource, spec, info = self.recorded_backend()
        self.assertEqual(info["Ports"], self.empty)
        with patch.object(self.controller, "inspect", return_value=info), patch.object(self.controller, "docker") as docker:
            with self.assertRaisesRegex(RuntimeError, "unexpected published ports"):
                self.controller.check_container(resource, spec)
            docker.assert_not_called()

    def test_recorded_backend_requires_exact_loopback_ports_and_owned_network(self):
        resource, spec, info = self.recorded_backend()
        binding = dict(HostIp="127.0.0.1", HostPort="19200")
        info["Ports"] = {"9200/tcp": [binding], "9300/tcp": None}
        with patch.object(self.controller, "inspect", return_value=info), patch.object(self.controller, "docker") as docker:
            self.assertEqual(self.controller.check_container(resource, spec), info)
            docker.assert_not_called()
        for failure in ("missing", "unbound", "wildcard", "ipv6", "wrong-port", "extra-port", "multiple-bindings", "foreign-network", "extra-network"):
            with self.subTest(failure=failure):
                invalid = copy.deepcopy(info)
                ports = invalid["Ports"]
                if failure == "missing":
                    ports.clear()
                elif failure == "unbound":
                    ports["9200/tcp"] = None
                elif failure in ("wildcard", "ipv6"):
                    ports["9200/tcp"][0]["HostIp"] = "0.0.0.0" if failure == "wildcard" else "::"
                elif failure == "wrong-port":
                    ports["9200/tcp"][0]["HostPort"] = "19201"
                elif failure == "extra-port":
                    ports["9300/tcp"] = [binding]
                elif failure == "multiple-bindings":
                    ports["9200/tcp"].append(binding)
                elif failure == "foreign-network":
                    invalid["Networks"][self.controller.owner]["NetworkID"] = "f" * 64
                else:
                    invalid["Networks"]["foreign"] = dict(NetworkID="f" * 64)
                message = "foreign/shared network" if "network" in failure else "unexpected published ports" if failure in ("missing", "unbound", "extra-port") else "non-loopback/wrong port publication"
                with patch.object(self.controller, "inspect", return_value=invalid), patch.object(self.controller, "docker") as docker:
                    with self.assertRaisesRegex(RuntimeError, message):
                        self.controller.check_container(resource, spec)
                    docker.assert_not_called()

    def test_wrong_network_mode_is_not_used_or_removed(self):
        for failure in ("network-driver", "network-internal"):
            with self.subTest(failure=failure):
                self.resources = {}
                self.calls = []
                self.failure = failure
                self.opts.output = self.root / failure
                self.controller = M.Controller(self.opts)
                with self.assertRaisesRegex(RuntimeError, "owned network must be a standard bridge"):
                    self.execute()
                self.assertFalse(any(a[:2] == ["container", "create"] or a[:2] == ["network", "rm"] for a in self.calls))
                self.assertEqual(len(self.resources), 1)
                result = json.loads((self.opts.output / "finite-result.json").read_text())
                self.assertIn("owned network must be a standard bridge", result["primary_error"])
                self.assertTrue(result["cleanup_errors"])
                self.assertFalse(result["finite_complete"])

    def test_populated_owned_bridge_is_preserved_during_cleanup(self):
        self.failure = "network-populated"
        with self.assertRaises(RuntimeError):
            self.execute()
        self.assertEqual(list(self.resources), [self.controller.owner])
        self.assertFalse(any(a[:2] == ["network", "rm"] for a in self.calls))
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIsNone(result["primary_error"])
        self.assertTrue(result["cleanup_errors"])
        self.assertFalse(result["finite_complete"])

    def test_failures_cleanup_and_primary_error_retention(self):
        for failure in ("preflight", "partial-network", "partial-container", "driver", "shutdown", "actual-controls", "observer-bytes", "pid1-bytes"):
            with self.subTest(failure=failure):
                self.failure = failure
                self.opts.output = self.root / failure
                self.controller = M.Controller(self.opts)
                with self.assertRaises(RuntimeError):
                    self.execute()
                self.assertEqual(self.resources, self.empty)
                result = json.loads((self.opts.output / "finite-result.json").read_text())
                self.assertTrue(result["primary_error"])
                self.assertFalse(result["finite_complete"])
                self.assertEqual(result["cleanup_errors"], [])

    def test_preflight_missing_never_creates_services(self):
        self.failure = "preflight"
        with self.assertRaises(RuntimeError):
            self.execute()
        self.assertFalse(any(a[1] in ("create", "start") for a in self.calls if len(a) > 1))

    def test_log_failure_does_not_prevent_cleanup_or_hide_errors(self):
        self.failure = "logs"
        with self.assertRaises(RuntimeError):
            self.execute()
        self.assertEqual(self.resources, self.empty)
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIsNone(result["primary_error"])
        self.assertEqual(len(result["cleanup_errors"]), 2)
        self.assertFalse(result["finite_complete"])

    def test_changed_owner_id_inspect_and_remaining_are_not_absence(self):
        for failure in ("owner", "id", "inspect", "remains"):
            with self.subTest(failure=failure):
                self.resources = {}
                self.calls = []
                self.failure = failure
                self.opts.output = self.root / failure
                self.controller = M.Controller(self.opts)
                with self.assertRaises(RuntimeError):
                    self.execute()
                result = json.loads((self.opts.output / "finite-result.json").read_text())
                self.assertTrue(result["cleanup_errors"])
                self.assertFalse(result["finite_complete"])
                self.assertTrue(self.resources)
                if failure in ("owner", "id", "inspect"):
                    self.assertFalse(any(a[:2] == ["network", "rm"] for a in self.calls))

    def test_platform_rejection_zero_callbacks(self):
        with patch.object(M.platform, "system", return_value="Darwin"), patch.object(self.controller, "docker") as docker:
            with self.assertRaisesRegex(RuntimeError, "native Linux"):
                self.controller.execute()
            docker.assert_not_called()
        self.assertFalse(self.opts.output.exists())

    def test_remote_and_emulated_rejection_zero_callbacks(self):
        for environment in ({"DOCKER_HOST": "tcp://remote:2375"}, {"DOCKER_CONTEXT": "desktop-linux"}, {"DOCKER_TLS_VERIFY": "1"}, {"DOCKER_CERT_PATH": "/private"}):
            with self.subTest(environment=environment), patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="x86_64"), patch.dict(os.environ, environment, clear=True):
                with self.assertRaises(RuntimeError):
                    M.native_preflight(self.opts)
        with patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="riscv64"):
            with self.assertRaises(RuntimeError):
                M.native_preflight(self.opts)

    def test_inspect_failure_only_exact_not_found_means_absence(self):
        resource = M.Resource("container", "owned", "c" * 64)
        for output in ("permission denied", "daemon unavailable", "Error: No such container: other", "Error: No such container: " + resource.identity + "\nextra"):
            with patch.object(self.controller, "docker", side_effect=M.CommandError(output, 1)):
                with self.assertRaises(M.CommandError):
                    self.controller.inspect(resource)
        with patch.object(self.controller, "docker", side_effect=M.CommandError("Error: No such container: " + resource.identity, 1)):
            self.assertIsNone(self.controller.inspect(resource))

    def test_http_wrong_identity_does_not_retry_or_mutate(self):
        class Response:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self, limit):
                return b'{"cluster_name":"unrelated","version":{"number":"8.19.22"}}'
        self.opts.output.mkdir()
        with patch.object(self.controller.http, "open", return_value=Response()):
            with self.assertRaisesRegex(RuntimeError, "wrong backend"):
                self.controller.wait_http("http://127.0.0.1:1/", 1, identity=True)
        self.assertEqual(self.calls, [])

    def test_readiness_timeout_and_redirects_fail_closed(self):
        with patch.object(self.controller.http, "open", side_effect=TimeoutError), patch.object(M.time, "sleep"), patch.object(M.time, "monotonic", side_effect=[0, 0, 0, 2]):
            with self.assertRaisesRegex(RuntimeError, "readiness timeout"):
                self.controller.wait_http("http://127.0.0.1:1/", 1)
        class RedirectResponse:
            code = 302
        redirect = M.NoRedirect()
        response = RedirectResponse()
        with self.assertRaises(RuntimeError):
            redirect.http_response(None, response)

    def test_run_timeout_preserves_failed_output(self):
        self.opts.output.mkdir()
        def timeout(args, **kwargs):
            kwargs["stdout"].write(b"sanitized partial failure\n")
            raise subprocess.TimeoutExpired(args, 1)
        with patch.object(M.subprocess, "run", side_effect=timeout):
            with self.assertRaisesRegex(RuntimeError, "bounded command timeout"):
                self.controller.run(["fixture"], timeout=1)
        self.assertIn("partial failure", (self.opts.output / "command-0001.log").read_text())

    def test_driver_timeout_kills_only_its_owned_process_group_and_keeps_log(self):
        self.opts.output.mkdir()
        self.controller.hashes = {role: M.digest(getattr(self.opts, role)) for role in ("cli", "driver", "observer")}
        child = MagicMock(pid=31337, returncode=-9)
        child.poll.return_value = None
        expired = subprocess.TimeoutExpired(["owned-app.test"], 60)
        child.wait.side_effect = [expired, 0]
        snapshot = dict(sha256=self.controller.hashes["driver"], membership="0::/host", pid=31337)
        environment = {}
        with patch.object(M.subprocess, "Popen", return_value=child) as launch, patch.object(M, "process_snapshot", return_value=snapshot), patch.object(M.os, "killpg") as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                self.controller.driver("finite", environment)
            kill.assert_called_once_with(31337, M.signal.SIGKILL)
            self.assertTrue(launch.call_args.kwargs["start_new_session"])
            self.assertEqual(launch.call_args.kwargs["env"]["WEIR_FINITE_NATIVE"], "1")
        self.assertTrue((self.opts.output / "finite-driver.log").exists())
        self.assertFalse((self.opts.output / "finite-completion.json").exists())

    def test_wrong_prepared_engine_never_creates_services(self):
        for field, value in (("Cgroup", "1"), ("Arch", "aarch64"), ("Kernel", "remote"), ("CPUs", 1), ("Memory", 1), ("OS", "darwin")):
            with self.subTest(field=field):
                self.opts.output = self.root / ("wrong-" + field)
                self.controller = M.Controller(self.opts)
                original = self.command
                def command(args, timeout=15):
                    text = original(args, timeout)
                    if args[0] == "info":
                        info = json.loads(text)
                        info[field] = value
                        return json.dumps(info)
                    return text
                with patch.object(M, "native_preflight", return_value="amd64"), patch.object(self.controller, "docker", side_effect=command):
                    with self.assertRaises(RuntimeError):
                        self.controller.execute()
                self.assertEqual(self.resources, self.empty)
        self.assertFalse(any(a[1] in ("create", "start") for a in self.calls if len(a) > 1))

    def test_primary_and_cleanup_errors_retained_together(self):
        self.failure = "driver"
        original = self.command
        def command(args, timeout=15):
            if args[0] == "logs":
                raise RuntimeError("cleanup log failed")
            return original(args, timeout)
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(M, "process_snapshot", side_effect=self.host_snapshot), patch.object(self.controller, "docker", side_effect=command), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http"):
            with self.assertRaises(RuntimeError):
                self.controller.execute()
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIn("driver timeout", result["primary_error"])
        self.assertTrue(result["cleanup_errors"])
        self.assertEqual(self.resources, self.empty)


class CompletionTests(unittest.TestCase):
    def test_completion_is_not_exit_zero_or_test_log_alone(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "completion.json"
            text = "=== RUN   TestProductionLinuxFinite\n--- PASS: TestProductionLinuxFinite (0.01s)\nPASS\n"
            opts = dict(path=path, stage="finite", revision="a" * 40, pid=123, stdout=text)
            with self.assertRaises(FileNotFoundError):
                M.completion(opts)
            record = dict(schema=1, stage="finite", profile=M.PROFILE, revision="a" * 40, driver_pid=123, cases=M.FINITE_CASES, samples=2, documents=6, qualification=False)
            path.write_text(json.dumps(record))
            self.assertEqual(M.completion(opts), record)
            for key, value in (("cases", M.FINITE_CASES[:-1]), ("cases", M.FINITE_CASES + ["Read"]), ("samples", 0), ("documents", 17), ("qualification", True), ("driver_pid", 124), ("profile", "linux-native-load-v1"), ("revision", "b" * 40), ("schema", 2), ("passed", True)):
                with self.subTest(key=key, value=value):
                    invalid = dict(record)
                    invalid[key] = value
                    path.write_text(json.dumps(invalid))
                    with self.assertRaises(RuntimeError):
                        M.completion(opts)
            path.write_text(json.dumps(record))
            for stdout in ("PASS\n", "testing: warning: no tests to run\nPASS\n", text + "--- SKIP: other\nPASS\n", text + text, text.replace("PASS:", "FAIL:")):
                invalid = dict(opts, stdout=stdout)
                with self.assertRaises(RuntimeError):
                    M.completion(invalid)

    def test_sanitized_evidence_removes_controls_and_userinfo(self):
        self.assertEqual(M.sanitized(b"\x1b[31mhttp://user:secret@example/x\x00\n"), "http://[redacted]@example/x\n")


if __name__ == "__main__":
    unittest.main()
