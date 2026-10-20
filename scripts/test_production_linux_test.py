"""Offline finite executor failure injection. No services, Docker or host changes."""
import copy
import importlib.util
import json
import os
import shlex
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


# Actual CI39335210270 read-only config bootstrap failure; never executed.
RECORDED_KEYSTORE_ERROR = 'java.nio.file.FileSystemException: /usr/share/elasticsearch/config/elasticsearch.keystore.tmp: Read-only file system\n\tat java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:102)\n\tat java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:108)\n\tat java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:113)\n\tat java.base/sun.nio.fs.UnixFileSystemProvider.newFileChannel(UnixFileSystemProvider.java:214)\n\tat java.base/sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:245)\n\tat java.base/java.nio.file.spi.FileSystemProvider.newOutputStream(FileSystemProvider.java:426)\n\tat java.base/java.nio.file.Files.newOutputStream(Files.java:215)\n\tat org.apache.lucene.store.FSDirectory$FSIndexOutput.<init>(FSDirectory.java:394)\n\tat org.apache.lucene.store.FSDirectory$FSIndexOutput.<init>(FSDirectory.java:387)\n\tat org.apache.lucene.store.FSDirectory.createOutput(FSDirectory.java:220)\n\tat org.elasticsearch.common.settings.KeyStoreWrapper.save(KeyStoreWrapper.java:454)\n\tat org.elasticsearch.common.settings.KeyStoreWrapper.save(KeyStoreWrapper.java:444)\n\tat org.elasticsearch.common.settings.KeyStoreWrapper.bootstrap(KeyStoreWrapper.java:230)\n\tat org.elasticsearch.server.cli.KeyStoreLoader.bootstrap(KeyStoreLoader.java:38)\n\tat org.elasticsearch.server.cli.ServerCli.execute(ServerCli.java:94)\n\tat org.elasticsearch.common.cli.EnvironmentAwareCommand.execute(EnvironmentAwareCommand.java:55)\n\tat org.elasticsearch.cli.Command.mainWithoutErrorHandling(Command.java:101)\n\tat org.elasticsearch.cli.Command.main(Command.java:54)\n\tat org.elasticsearch.launcher.CliToolLauncher.main(CliToolLauncher.java:65)\n'
RECORDED_KEYSTORE_EXIT = json.loads('''
{
  "Dead": false,
  "Error": "",
  "ExitCode": 74,
  "FinishedAt": "2026-10-19T22:29:56.682287834Z",
  "OOMKilled": false,
  "Paused": false,
  "Pid": 0,
  "Restarting": false,
  "Running": false,
  "StartedAt": "2026-10-19T22:29:55.571636034Z",
  "Status": "exited"
}
''')


class Contracts(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        # These lifecycle tests mock every listener/container. Real ephemeral
        # sockets can reuse a just-closed port and make the mock nondeterministic.
        ports = iter(range(20000, 21000))
        port_patch = patch.object(M, "free_port", side_effect=ports)
        self.free_ports = port_patch.start()
        self.addCleanup(port_patch.stop)
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

    def command(self, args, timeout=15, deadline=None):
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
            info = dict(Id="sha256:" + "b" * 64, OS="linux", Arch="amd64", Digests=[ref], Volumes=None)
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
                network_mode = args[args.index("--network") + 1]
                if network_mode == "host":
                    networks = {"host": dict(NetworkID="")}
                if self.opts.profile == M.LOAD_PROFILE and name.endswith("-backend"):
                    mounts.append(dict(Destination="/usr/share/elasticsearch/data", Source=str(self.opts.output / "es-data"), RW=True))
                info = dict(Id=identity, Name="/" + name, Owner=self.controller.owner, Image="sha256:" + "b" * 64, State=state, Memory=int(flags["--memory"]), Swap=int(flags["--memory-swap"]), CPU=2_000_000_000, PIDs=int(flags["--pids-limit"]), Cgroupns="private", Readonly="--read-only" in args, User=flags["--user"], Caps=["ALL"], Security=["no-new-privileges:true"], Privileged=False, PidMode="", Networks=networks, Ports=ports, Mounts=mounts)
                info["NetworkMode"] = network_mode
                image_index = next(i for i, arg in enumerate(args) if arg in (self.controller.es_ref, self.controller.base_ref))
                info["Cmd"] = args[image_index + 1:]
                info["Entrypoint"] = ["/weir"] if name.endswith("-cli") else ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
                if name.endswith("-backend") and self.opts.profile == M.OS_PROFILE:
                    info["Entrypoint"] = ["./opensearch-docker-entrypoint.sh"]
                    info["Cmd"] = ["opensearch"]
                info["Tmpfs"] = dict(arg[len("--tmpfs="):].split(":", 1) for arg in args if arg.startswith("--tmpfs="))
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
            if pair[0].endswith("-backend") and not pair[1]["Cmd"]:
                # Do not invent a working read-only keystore: this is the actual
                # terminal state reached without configuration staging.
                pair[1]["State"] = copy.deepcopy(RECORDED_KEYSTORE_EXIT)
        elif args[0] in ("kill", "stop"):
            pair[1]["State"]["Running"] = False
        elif args[0] == "wait":
            if self.failure == "shutdown":
                pair[1]["State"]["ExitCode"] = 1
            return "0\n"
        elif args[0] == "logs":
            if self.failure == "logs":
                raise RuntimeError("logs unavailable")
            if pair[1]["State"]["ExitCode"] == 74:
                return RECORDED_KEYSTORE_ERROR
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

    def test_formal_load_uses_owned_host_cli_and_preserves_finite_budget(self):
        self.opts.profile = M.LOAD_PROFILE
        self.controller = M.Controller(self.opts)
        self.controller.owner = "weir-finite-" + "1" * 32
        self.assertGreater(self.controller.deadline - M.time.monotonic(), 950)
        stages = []

        def mongo():
            self.controller.mongo_uri = "mongodb://127.0.0.1:27099/?replicaSet=" + self.controller.owner

        def driver(stage, environment):
            stages.append(stage)
            self.driver(stage, environment)
            if stage == "load":
                basic = (self.opts.output / "weir.yaml").read_text()
                self.assertNotIn("0.0.0.0", basic)
                self.assertIn('application: "127.0.0.1:', basic)
                self.assertIn('address: "127.0.0.1:', basic)
                self.assertIn(self.controller.mongo_uri, (self.opts.output / "routes.yaml").read_text())

        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(M, "process_snapshot", side_effect=self.host_snapshot), patch.object(self.controller, "docker", side_effect=self.command), patch.object(self.controller, "driver", side_effect=driver), patch.object(self.controller, "wait_http"), patch.object(self.controller, "start_mongo", side_effect=mongo), patch.object(self.controller, "mongo_guard"):
            self.controller.execute()
        self.assertEqual(stages, ["preflight", "load"])
        profile = json.loads((self.opts.output / "profile.json").read_text())
        self.assertEqual((profile["suite"], profile["intent"]), ("load", "formal"))
        self.assertEqual([b["product"] for b in profile["backends"]], ["mongodb", "elasticsearch"])
        cli = json.loads((self.opts.output / "cli-container.json").read_text())
        self.assertEqual(cli["NetworkMode"], "host")
        self.assertEqual(cli["Ports"], {})
        self.assertEqual((cli["Memory"], cli["Swap"], cli["CPU"], cli["PIDs"]), (1 << 30, 1 << 30, 2_000_000_000, 96))
        result = json.loads((self.opts.output / "load-result.json").read_text())
        self.assertTrue(result["load_complete"])
        self.assertFalse(result["qualification"])
        self.assertEqual(self.resources, {})
        self.opts.profile = M.PROFILE
        finite = M.Controller(self.opts)
        self.assertLessEqual(finite.deadline - M.time.monotonic(), 240)

    def test_formal_host_mode_requires_exact_capability_and_actual_mode(self):
        resource, spec, info = self.recorded_backend()
        spec.update(role="cli", host_network=True, ports={})
        info.update(Ports={}, Networks={"host": dict(NetworkID="")}, NetworkMode="host")
        with patch.object(self.controller, "guard", return_value=info):
            with self.assertRaisesRegex(RuntimeError, "host networking only"):
                self.controller.check_container(resource, spec)
            self.opts.profile = M.LOAD_PROFILE
            info["NetworkMode"] = "bridge"
            with self.assertRaisesRegex(RuntimeError, "actual host network mode"):
                self.controller.check_container(resource, spec)

    def test_native_mongo_lifecycle_is_owned_bounded_and_not_docker(self):
        self.opts.output.mkdir()
        self.controller.prepare_load_storage()
        self.opts.mongod = self.root / "mongod"
        self.opts.mongod.write_bytes(b"pinned-stock")
        sha = M.hashlib.sha256(b"pinned-stock").hexdigest()
        child = MagicMock(pid=4242, returncode=0)
        child.poll.return_value = None
        snapshot = dict(pid=4242, start_ticks=123, membership="0::/host")
        connection = MagicMock()
        with patch.object(M, "MONGO_SHA256", sha), patch.object(M.subprocess, "Popen", return_value=child) as launch, patch.object(self.controller, "mongo_guard", return_value=snapshot), patch.object(M.socket, "create_connection", return_value=connection), patch.object(self.controller, "driver") as driver:
            self.controller.start_mongo()
            args = launch.call_args.args[0]
            self.assertEqual(args[0], str(self.opts.mongod))
            self.assertIn("--replSet", args)
            self.assertIn("--bind_ip", args)
            self.assertIn("127.0.0.1", args)
            self.assertNotIn("--fork", args)
            self.assertTrue(launch.call_args.kwargs["start_new_session"])
            self.assertEqual(driver.call_args.args[0], "mongo")
            self.controller.cleaning = True
            self.controller.cleanup_deadline = M.time.monotonic() + 60
            with patch.object(M.Path, "exists", return_value=False):
                self.controller.cleanup_mongo()
            child.terminate.assert_called_once()
            child.wait.assert_called_once()
            child.kill.assert_not_called()
        receipt = json.loads((self.opts.output / "mongo-cleanup.json").read_text())
        self.assertTrue(receipt["absent"])
        self.assertFalse(self.calls)

    def test_native_mongo_identity_failure_refuses_signal(self):
        self.opts.output.mkdir()
        child = MagicMock(pid=4242)
        child.poll.return_value = None
        self.controller.mongo = child
        self.controller.cleaning = True
        self.controller.cleanup_deadline = M.time.monotonic() + 60
        with patch.object(self.controller, "mongo_guard", side_effect=RuntimeError("changed process identity")):
            with self.assertRaisesRegex(RuntimeError, "changed process"):
                self.controller.cleanup_mongo()
        child.terminate.assert_not_called()
        child.kill.assert_not_called()

    def test_formal_platform_rejection_precedes_docker_and_native_process(self):
        self.opts.profile = M.LOAD_PROFILE
        bad_release = dict(ID="ubuntu", VERSION_ID="22.04")
        with patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="x86_64"), patch.object(M.platform, "freedesktop_os_release", return_value=bad_release), patch.object(M.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(RuntimeError, "Ubuntu24.04"):
                M.native_preflight(self.opts)
            launch.assert_not_called()
        good_release = dict(ID="ubuntu", VERSION_ID="24.04")
        with patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="aarch64"), patch.object(M.platform, "freedesktop_os_release", return_value=good_release):
            with self.assertRaisesRegex(RuntimeError, "Ubuntu24.04"):
                M.native_preflight(self.opts)
        with patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="x86_64"), patch.object(M.platform, "freedesktop_os_release", return_value=good_release), patch.dict(M.os.environ, GITHUB_ACTIONS="true", RUNNER_ENVIRONMENT="self-hosted"):
            with self.assertRaisesRegex(RuntimeError, "disposable GitHub-hosted"):
                M.native_preflight(self.opts)
        self.assertFalse(self.calls)

    def test_load_storage_bounds_marker_containment_and_verified_removal(self):
        self.opts.output.mkdir()
        self.controller.prepare_load_storage()
        data = self.controller.storage_paths[0]
        payload = data / "owned-payload"
        payload.write_bytes(b"x" * 64)
        self.controller.monitor_load_storage()
        self.assertGreaterEqual(self.controller.storage_last_sample["bytes"]["es-data"], 64)
        with patch.object(M, "LOAD_DATA_LIMIT", 32):
            with self.assertRaisesRegex(RuntimeError, "abort threshold"):
                self.controller.monitor_load_storage()
        usage = MagicMock(free=0)
        with patch.object(M.shutil, "disk_usage", return_value=usage):
            with self.assertRaisesRegex(RuntimeError, "free space"):
                self.controller.monitor_load_storage()
        outside = self.root / "outside"
        outside.write_text("preserve")
        link = data / "escape"
        link.symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            self.controller.monitor_load_storage()
        link.unlink()
        marker = self.opts.output / "load-storage-owner.json"
        original = marker.read_text()
        marker.write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "owner marker"):
            self.controller.cleanup_load_storage()
        self.assertTrue(payload.exists())
        marker.write_text(original)
        self.controller.cleanup_load_storage()
        self.assertTrue(outside.exists())
        self.assertTrue(marker.exists())
        self.assertTrue(all(not path.exists() for path in self.controller.storage_paths))

    def test_load_data_cleanup_failure_reports_retention_and_fails_result(self):
        self.opts.output.mkdir()
        self.controller.prepare_load_storage()
        self.controller.monitor_load_storage()
        with patch.object(M.shutil, "rmtree", side_effect=PermissionError("retained owned data")):
            errors = self.controller.cleanup()
        self.assertTrue(any("load data cleanup" in error for error in errors))
        retained = json.loads((self.opts.output / "load-storage-retained.json").read_text())
        self.assertFalse(retained["cleanup_complete"])
        self.assertEqual(len(retained["paths"]), 2)
        self.assertIn("bytes", retained["last_observed"])

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
        info["Name"] = "/" + resource.name
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
        info.update(Entrypoint=M.ES_ENTRYPOINT, Cmd=M.ES_COMMAND, Tmpfs=M.ES_TMPFS)
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

    def test_injected_duplicate_ports_fail_before_workload_and_clean_network(self):
        self.free_ports.side_effect = [20000, 20000, 20001]
        with self.assertRaisesRegex(RuntimeError, "ephemeral port collision"):
            self.execute()
        self.assertEqual(self.free_ports.call_count, 3, "port collision must not silently retry")
        self.assertFalse(any(a[:2] == ["container", "create"] or a[:1] == ["exec"] for a in self.calls))
        self.assertEqual(sum(a[:2] == ["network", "create"] for a in self.calls), 1)
        self.assertEqual(sum(a[:2] == ["network", "rm"] for a in self.calls), 1)
        self.assertEqual(self.resources, self.empty)
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIn("ephemeral port collision", result["primary_error"])
        self.assertEqual(result["cleanup_errors"], [])
        self.assertFalse(result["finite_complete"])
        cleanup = json.loads((self.opts.output / "cleanup.json").read_text())
        self.assertEqual(len(cleanup["resources"]), 1)
        self.assertEqual(cleanup["resources"][0]["kind"], "network")
        self.assertTrue(cleanup["resources"][0]["absent"])

    def test_backend_staging_contract_rejects_missing_unbounded_or_unsafe_configuration(self):
        resource, spec, info = self.recorded_backend()
        binding = dict(HostIp="127.0.0.1", HostPort="19200")
        info["Ports"] = {"9200/tcp": [binding]}
        info.update(Entrypoint=M.ES_ENTRYPOINT, Cmd=M.ES_COMMAND, Tmpfs=M.ES_TMPFS)
        for failure in ("missing-command", "wrong-copy", "wrong-config-path", "missing-export", "missing-fail-fast", "bypass-entrypoint", "wrong-pid1", "missing-tmpfs", "unbounded-tmpfs", "wrong-uid", "writable-bind", "config-bind"):
            with self.subTest(failure=failure):
                invalid = copy.deepcopy(info)
                if failure == "missing-command":
                    invalid["Cmd"] = ["eswrapper"]
                elif failure == "wrong-copy":
                    invalid["Cmd"][2] = invalid["Cmd"][2].replace("config/.", "config/*")
                elif failure == "wrong-config-path":
                    invalid["Cmd"][2] = invalid["Cmd"][2].replace("/tmp/weir-es-config", "/usr/share/elasticsearch/config")
                elif failure == "missing-export":
                    invalid["Cmd"][2] = invalid["Cmd"][2].replace("export ES_PATH_CONF", "ES_PATH_CONF")
                elif failure == "missing-fail-fast":
                    invalid["Cmd"][1] = "-uc"
                elif failure == "bypass-entrypoint":
                    invalid["Cmd"][2] = invalid["Cmd"][2].replace("/usr/local/bin/docker-entrypoint.sh eswrapper", "/usr/share/elasticsearch/bin/elasticsearch")
                elif failure == "wrong-pid1":
                    invalid["Entrypoint"] = ["/bin/bash"]
                elif failure == "missing-tmpfs":
                    invalid["Tmpfs"].pop("/tmp")
                elif failure == "unbounded-tmpfs":
                    invalid["Tmpfs"]["/tmp"] = "rw,nosuid,uid=1000,gid=0"
                elif failure == "wrong-uid":
                    invalid["Tmpfs"]["/tmp"] = invalid["Tmpfs"]["/tmp"].replace("uid=1000", "uid=0")
                elif failure == "writable-bind":
                    invalid["Mounts"][0]["Destination"] = "/tmp"
                else:
                    mount = dict(Destination="/usr/share/elasticsearch/config", Source="/foreign-config", RW=True)
                    invalid["Mounts"].append(mount)
                with patch.object(self.controller, "inspect", return_value=invalid), patch.object(self.controller, "docker") as docker:
                    with self.assertRaises(RuntimeError):
                        self.controller.check_container(resource, spec)
                    docker.assert_not_called()

    def test_staging_copies_complete_config_and_preserves_source_before_stock_startup(self):
        source = self.root / "image-config"
        source.mkdir()
        nested = source / "jvm.options.d"
        nested.mkdir()
        files = ("elasticsearch.yml", "jvm.options", "log4j2.properties", "jvm.options.d/example.options", ".hidden-fixture")
        for name in files:
            (source / name).write_text("immutable fixture " + name)
        before = {name: (source / name).read_bytes() for name in files}
        destination = self.root / "staged-config"
        entrypoint = self.root / "stock-entrypoint-stub"
        # This is our inert test stub, not downloaded Elasticsearch code.
        entrypoint.write_text('#!/bin/bash\nset -eu\ntest "$1" = eswrapper\ntest -d "$ES_PATH_CONF"\nprintf staged > "$ES_PATH_CONF/elasticsearch.keystore.tmp"\n')
        entrypoint.chmod(0o755)
        script = M.ES_COMMAND[2].replace("/usr/share/elasticsearch/config/.", shlex.quote(str(source)) + "/.")
        script = script.replace("/tmp/weir-es-config", shlex.quote(str(destination)))
        script = script.replace("/usr/local/bin/docker-entrypoint.sh", shlex.quote(str(entrypoint)))
        command = ["/bin/bash", "-euc", script]
        result = subprocess.run(command, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        staged = {name: (destination / name).read_bytes() for name in files}
        unchanged = {name: (source / name).read_bytes() for name in files}
        self.assertEqual(staged, before)
        self.assertEqual(unchanged, before)
        self.assertEqual((destination / "elasticsearch.keystore.tmp").read_text(), "staged")
        self.assertFalse((source / "elasticsearch.keystore.tmp").exists())
        self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
        # Existing destination must not be reused, even if it contains valid data.
        again = subprocess.run(command, capture_output=True, check=False)
        self.assertNotEqual(again.returncode, 0)
        missing = script.replace(shlex.quote(str(source)), shlex.quote(str(self.root / "missing-image-config")))
        missing = missing.replace(shlex.quote(str(destination)), shlex.quote(str(self.root / "failed-stage")))
        failed = subprocess.run(["/bin/bash", "-euc", missing], capture_output=True, check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertFalse((self.root / "failed-stage/elasticsearch.keystore.tmp").exists())

    def test_readiness_detects_recorded_backend_exit_before_http(self):
        backend = M.Resource("container", self.controller.owner + "-backend", "a" * 64)
        info = dict(Id=backend.identity, Name="/" + backend.name, Owner=self.controller.owner, State=RECORDED_KEYSTORE_EXIT)
        with patch.object(self.controller, "inspect", return_value=info), patch.object(self.controller.http, "open") as http, patch.object(M.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "backend exited during readiness: exit=74 oom=False"):
                self.controller.wait_http("http://127.0.0.1:1/", 90, backend=backend)
            http.assert_not_called()
            sleep.assert_not_called()
        self.assertIn("elasticsearch.keystore.tmp: Read-only file system", RECORDED_KEYSTORE_ERROR)

    def test_readiness_terminal_error_keeps_logs_and_verified_cleanup(self):
        original_wait = self.controller.wait_http
        logged = []
        def readiness(url, timeout, backend=None):
            self.assertIsNotNone(backend)
            info = next(info for info in self.resources.values() if info["Id"] == backend.identity)
            info["State"] = copy.deepcopy(RECORDED_KEYSTORE_EXIT)
            original_wait(url, timeout, backend=backend)
        def command(args, timeout=15, deadline=None):
            result = self.command(args, timeout, deadline=deadline)
            if args[0] == "logs":
                logged.append(result)
            return result
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(self.controller, "docker", side_effect=command), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http", side_effect=readiness), patch.object(self.controller.http, "open") as http:
            with self.assertRaisesRegex(RuntimeError, "backend exited during readiness: exit=74 oom=False"):
                self.controller.execute()
            http.assert_not_called()
        self.assertEqual(logged, [RECORDED_KEYSTORE_ERROR])
        self.assertEqual(self.resources, self.empty)
        self.assertEqual(len([a for a in self.calls if a[:2] == ["container", "create"]]), 1)
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIn("backend exited during readiness: exit=74 oom=False", result["primary_error"])
        self.assertEqual(result["cleanup_errors"], [])
        self.assertFalse(result["finite_complete"])
        cleanup = json.loads((self.opts.output / "cleanup.json").read_text())
        self.assertTrue(all(resource["absent"] for resource in cleanup["resources"]))

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
        backend = M.Resource("container", self.controller.owner + "-backend", "a" * 64)
        state = dict(Running=True, ExitCode=0, OOMKilled=False)
        info = dict(Id=backend.identity, Name="/" + backend.name, Owner=self.controller.owner, State=state)
        with patch.object(self.controller, "inspect", return_value=info), patch.object(self.controller.http, "open", return_value=Response()):
            with self.assertRaisesRegex(RuntimeError, "wrong backend"):
                self.controller.wait_http("http://127.0.0.1:1/", 1, backend=backend)
        self.assertEqual(self.calls, [])

    def readiness_deadline_fixture(self):
        self.opts.output.mkdir()
        self.controller.deadline = 240
        clock = dict(now=0.0)
        backend = M.Resource("container", self.controller.owner + "-backend", "a" * 64)
        state = dict(Running=True, ExitCode=0, OOMKilled=False)
        info = dict(Id=backend.identity, Name="/" + backend.name, Owner=self.controller.owner, State=state)
        version = dict(number="8.19.22", build_flavor="default", build_hash="fixture")
        identity = dict(cluster_name=self.controller.owner, version=version)
        response = MagicMock(status=200)
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.read.return_value = json.dumps(identity).encode()
        result = clock, backend, info, response
        return result

    def test_readiness_guard_89_to_100_cannot_start_http_after_90(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        clock["now"] = 89
        initial = iter([0])
        def now():
            return next(initial, clock["now"])
        deadlines = []
        def guard(resource, deadline=None):
            self.assertEqual(clock["now"], 89)
            deadlines.append(deadline)
            clock["now"] = 100
            return info
        with patch.object(M.time, "monotonic", side_effect=now), patch.object(self.controller, "guard", side_effect=guard), patch.object(self.controller.http, "open", return_value=response) as http:
            with self.assertRaisesRegex(RuntimeError, "bounded readiness timeout"):
                self.controller.wait_http("http://127.0.0.1:1/", 90, backend=backend)
            http.assert_not_called()
        self.assertEqual(deadlines, [90])

    def test_inspect_remaining_deadline_reaches_subprocess_without_floor(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        clock["now"] = 89
        observed = []
        def inspect(args, **kwargs):
            observed.append(kwargs["timeout"])
            kwargs["stdout"].write(json.dumps(info).encode())
            result = subprocess.CompletedProcess(args, 0)
            return result
        with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M.subprocess, "run", side_effect=inspect) as execute:
            self.assertEqual(self.controller.guard(backend, deadline=90), info)
            self.assertEqual(observed, [1, 1])
            self.controller.deadline = 89.5
            self.controller.guard(backend, deadline=90)
            self.assertEqual(observed, [1, 1, .5, .5])
            self.controller.deadline = 240
            clock["now"] = 90
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                self.controller.guard(backend, deadline=90)
            self.assertEqual(execute.call_count, 4)
        # Budget must also be recomputed after command setup, not before it.
        clock["now"] = 89
        original = M.tempfile.TemporaryFile
        def setup(*args, **kwargs):
            clock["now"] = 90
            return original(*args, **kwargs)
        with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M.tempfile, "TemporaryFile", side_effect=setup), patch.object(M.subprocess, "run") as execute:
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                self.controller.guard(backend, deadline=90)
            execute.assert_not_called()

    def test_readiness_http_open_read_close_and_acceptance_deadlines(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        body = response.read.return_value
        for timeout, owned in ((90, backend), (20, None)):
            for phase in ("open", "read", "close", "evidence"):
                if phase == "evidence" and owned is None:
                    continue
                with self.subTest(timeout=timeout, phase=phase):
                    clock["now"] = 0
                    response.read.reset_mock()
                    def open_http(url, timeout):
                        if phase == "open":
                            clock["now"] = 100
                        return response
                    def read(limit):
                        if phase == "read":
                            clock["now"] = 100
                        return body
                    def close(*args):
                        if phase == "close":
                            clock["now"] = 100
                        return False
                    def evidence(name, value):
                        if phase == "evidence":
                            clock["now"] = 100
                    response.read.side_effect = read
                    response.__exit__.side_effect = close
                    with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(self.controller, "inspect", return_value=info), patch.object(self.controller.http, "open", side_effect=open_http), patch.object(self.controller, "evidence", side_effect=evidence):
                        with self.assertRaisesRegex(RuntimeError, "bounded readiness timeout"):
                            self.controller.wait_http("http://127.0.0.1:1/", timeout, backend=owned)
                    if phase == "open":
                        response.read.assert_not_called()

    def test_readiness_predeadline_success_uses_actual_fractional_http_budget(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        clock["now"] = 89.5
        initial = iter([0])
        body = response.read.return_value
        def now():
            return next(initial, clock["now"])
        def read(limit):
            clock["now"] = 89.9
            return body
        response.read.side_effect = read
        with patch.object(M.time, "monotonic", side_effect=now), patch.object(self.controller, "inspect", return_value=info) as inspect, patch.object(self.controller.http, "open", return_value=response) as http:
            self.controller.wait_http("http://127.0.0.1:1/", 90, backend=backend)
            self.assertEqual(inspect.call_count, 2)
            expected_deadline = dict(deadline=90)
            self.assertEqual(inspect.call_args_list[0].kwargs, expected_deadline)
            self.assertEqual(http.call_args.kwargs["timeout"], .5)
        identity = json.loads((self.opts.output / "backend-identity.json").read_text())
        self.assertEqual(identity["cluster_name"], self.controller.owner)

    def test_readiness_expiration_starts_no_inspect_or_http(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        for controller_deadline, stage_timeout in ((0, 90), (240, 0)):
            with self.subTest(controller_deadline=controller_deadline, stage_timeout=stage_timeout):
                self.controller.deadline = controller_deadline
                with patch.object(M.time, "monotonic", return_value=0), patch.object(self.controller, "inspect") as inspect, patch.object(self.controller.http, "open") as http:
                    with self.assertRaisesRegex(RuntimeError, "bounded readiness timeout"):
                        self.controller.wait_http("http://127.0.0.1:1/", stage_timeout, backend=backend)
                    inspect.assert_not_called()
                    http.assert_not_called()

    def test_readiness_retry_sleep_does_not_extend_deadline(self):
        clock, backend, info, response = self.readiness_deadline_fixture()
        clock["now"] = 89.95
        initial = iter([0])
        def now():
            return next(initial, clock["now"])
        def sleep(seconds):
            self.assertGreater(seconds, 0)
            self.assertAlmostEqual(seconds, .05)
            clock["now"] = 90
        with patch.object(M.time, "monotonic", side_effect=now), patch.object(self.controller.http, "open", side_effect=TimeoutError) as http, patch.object(M.time, "sleep", side_effect=sleep) as pause:
            with self.assertRaisesRegex(RuntimeError, "bounded readiness timeout"):
                self.controller.wait_http("http://127.0.0.1:1/", 90)
            self.assertEqual(http.call_count, 1)
            self.assertAlmostEqual(http.call_args.kwargs["timeout"], .05)
            self.assertEqual(pause.call_count, 1)

    def test_readiness_deadline_failure_keeps_primary_logs_and_independent_cleanup(self):
        for failure in ("", "logs"):
            with self.subTest(failure=failure):
                self.resources = {}
                self.calls = []
                self.failure = failure
                self.opts.output = self.root / ("late-" + (failure or "clean"))
                self.controller = M.Controller(self.opts)
                self.controller.deadline = 240
                clock = dict(now=0, readiness=False)
                original_wait = self.controller.wait_http
                def readiness(url, timeout, backend=None):
                    clock["readiness"] = True
                    try:
                        original_wait(url, timeout, backend=backend)
                    finally:
                        clock["readiness"] = False
                def command(args, timeout=15, deadline=None):
                    result = self.command(args, timeout, deadline=deadline)
                    if clock["readiness"] and args[:2] == ["container", "inspect"]:
                        self.assertEqual(deadline, 90)
                        clock["now"] = 100
                    elif not clock["readiness"] and deadline is not None:
                        self.assertGreater(deadline, clock["now"])
                        self.assertLessEqual(deadline - clock["now"], 60)
                    return result
                with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M, "native_preflight", return_value="amd64"), patch.object(self.controller, "docker", side_effect=command), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http", side_effect=readiness), patch.object(self.controller.http, "open") as http:
                    with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                        self.controller.execute()
                    http.assert_not_called()
                self.assertEqual(self.resources, self.empty)
                self.assertTrue(any(args[0] == "logs" for args in self.calls))
                result = json.loads((self.opts.output / "finite-result.json").read_text())
                self.assertEqual(result["primary_error"], "absolute controller/phase deadline exceeded")
                self.assertEqual(bool(result["cleanup_errors"]), failure == "logs")
                self.assertFalse(result["finite_complete"])
                cleanup = json.loads((self.opts.output / "cleanup.json").read_text())
                self.assertTrue(all(resource["absent"] for resource in cleanup["resources"]))

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
                def command(args, timeout=15, deadline=None):
                    text = original(args, timeout, deadline=deadline)
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
        def command(args, timeout=15, deadline=None):
            if args[0] == "logs":
                raise RuntimeError("cleanup log failed")
            return original(args, timeout, deadline=deadline)
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(M, "process_snapshot", side_effect=self.host_snapshot), patch.object(self.controller, "docker", side_effect=command), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http"):
            with self.assertRaises(RuntimeError):
                self.controller.execute()
        result = json.loads((self.opts.output / "finite-result.json").read_text())
        self.assertIn("driver timeout", result["primary_error"])
        self.assertTrue(result["cleanup_errors"])
        self.assertEqual(self.resources, self.empty)


    def test_opensearch_profile_fixture_and_provenance_agree(self):
        self.opts.profile = M.OS_PROFILE
        self.execute()
        profile = json.loads((self.opts.output / "profile.json").read_text())
        provenance = json.loads((self.opts.output / "provenance.json").read_text())
        self.assertEqual(profile["id"], M.OS_PROFILE)
        self.assertEqual(provenance["profileID"], M.OS_PROFILE)
        self.assertEqual(profile["backends"][0]["product"], "opensearch")
        self.assertEqual(profile["backends"][0]["version"], "3.3.0")
        creates = [a for a in self.calls if a[:2] == ["container", "create"]]
        self.assertEqual(len(creates), 2)
        backend, cli = creates
        self.assertNotIn("--read-only", backend)
        self.assertIn("--read-only", cli)
        self.assertIn("--pids-limit=96", cli)
        self.assertIn("--memory=1073741824", cli)
        self.assertEqual(backend[-1], M.OS_REPOSITORY + "@" + M.OS_PINS["amd64"])
        self.assertNotIn("--entrypoint", " ".join(backend))
        self.assertFalse(any("type=volume" in a for a in backend))
        self.assertNotIn("bootstrap", " ".join(backend))
        self.assertNotIn("jna", " ".join(backend))
        self.assertEqual(M.OS_TMPFS["/tmp"], "rw,nosuid,nodev,size=134217728,uid=1000,gid=1000")
        self.assertEqual(set(M.OS_TMPFS), {"/tmp", "/usr/share/opensearch/data", "/usr/share/opensearch/logs"})
        self.assertFalse(any(a[:1] in (["cp"], ["volume"]) for a in self.calls))
        # Database internals are not Weir admission gates or observer prerequisites.
        backend_id = json.loads((self.opts.output / "backend-container.json").read_text())["Id"]
        self.assertFalse(any(a[0] == "exec" and backend_id in a for a in self.calls))
        self.assertEqual(self.resources, self.empty)

    def test_opensearch_wrong_product_readiness_fails_before_workload(self):
        self.opts.profile = M.OS_PROFILE
        clock, backend, info, response = self.readiness_deadline_fixture()
        self.controller.deadline = M.time.monotonic() + 240
        with patch.object(self.controller, "guard", return_value=info), patch.object(self.controller.http, "open", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "wrong backend cluster/version/product"):
                self.controller.wait_http("http://127.0.0.1:1/", 90, backend)
        version = dict(number="3.3.0", distribution="opensearch", build_hash="fixture")
        identity_record = dict(cluster_name=self.controller.owner, version=version)
        response.read.return_value = json.dumps(identity_record).encode()
        with patch.object(self.controller, "guard", return_value=info), patch.object(self.controller.http, "open", return_value=response):
            self.controller.wait_http("http://127.0.0.1:1/", 90, backend)

    def test_opensearch_foreign_mount_and_anonymous_volume_rejected(self):
        self.opts.profile = M.OS_PROFILE
        command = self.command
        def foreign(args, timeout=15, deadline=None):
            raw = command(args, timeout, deadline)
            if args[:2] == ["container", "inspect"]:
                info = json.loads(raw)
                if info["Name"].endswith("-backend"):
                    foreign_mount = dict(Destination="/foreign", Source="other", RW=True)
                    info["Mounts"].append(foreign_mount)
                return json.dumps(info)
            return raw
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(self.controller, "docker", side_effect=foreign), patch.object(self.controller, "driver", side_effect=self.driver):
            with self.assertRaisesRegex(RuntimeError, "foreign mounts"):
                self.controller.execute()
        self.assertEqual(self.resources, self.empty)
        self.controller.es_ref = "test@sha256:" + "a" * 64
        self.controller.arch = "amd64"
        image = dict(Id="sha256:" + "b" * 64, OS="linux", Arch="amd64", Digests=[self.controller.es_ref], Volumes={"/foreign": {}})
        with patch.object(self.controller, "docker", return_value=json.dumps(image)):
            with self.assertRaisesRegex(RuntimeError, "anonymous volumes"):
                self.controller.image(self.controller.es_ref)

    def test_backend_cleanup_preserves_actual_nonzero_exit(self):
        self.opts.profile = M.OS_PROFILE
        command = self.command
        def term(args, timeout=15, deadline=None):
            raw = command(args, timeout, deadline)
            if args[:1] == ["kill"]:
                for name, info in self.resources.items():
                    if name.endswith("-backend") and info["Id"] == args[-1]:
                        info["State"]["ExitCode"] = 143
            return raw
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(M, "process_snapshot", side_effect=self.host_snapshot), patch.object(self.controller, "docker", side_effect=term), patch.object(self.controller, "driver", side_effect=self.driver), patch.object(self.controller, "wait_http"):
            self.controller.execute()
        receipts = json.loads((self.opts.output / "cleanup.json").read_text())["resources"]
        backend = next(r for r in receipts if r["name"].endswith("-backend"))
        self.assertTrue(backend["absent"])
        self.assertEqual(backend["exit_code"], 143)

    def test_driver_absolute_deadline_crossings_reject_late_success(self):
        for horizon in ("phase", "overall"):
            for boundary in ("setup", "launch", "snapshot", "evidence", "wait", "log", "parse"):
                with self.subTest(boundary=boundary):
                    self.opts.output = self.root / (horizon + "-" + boundary)
                    self.opts.output.mkdir()
                    self.controller.hashes = {role: M.digest(getattr(self.opts, role)) for role in ("cli", "driver", "observer")}
                    self.controller.deadline = 90 if horizon == "overall" else 240
                    self.controller.phase_deadline = 90 if horizon == "phase" else None
                    clock = dict(now=89)
                    child = MagicMock(pid=31337, returncode=0)
                    child.poll.return_value = 0
                    original_temp = M.tempfile.TemporaryFile
                    def setup(*args, **kwargs):
                        if boundary == "setup": clock["now"] = 100
                        return original_temp(*args, **kwargs)
                    def launch(*args, **kwargs):
                        if boundary == "launch": clock["now"] = 100
                        return child
                    def snapshot(pid):
                        if boundary == "snapshot": clock["now"] = 100
                        driver_snapshot = dict(sha256=self.controller.hashes["driver"], membership="0::/host")
                        return driver_snapshot
                    def evidence(*args):
                        if boundary == "evidence": clock["now"] = 100
                    def wait(timeout):
                        self.assertGreater(timeout, 0)
                        self.assertLessEqual(timeout, 1)
                        if boundary == "wait": clock["now"] = 100
                    def sanitize(raw):
                        if boundary == "log": clock["now"] = 100
                        return "retained log"
                    def parse(opts):
                        if boundary == "parse": clock["now"] = 100
                        completion_record = {}
                        return completion_record
                    child.wait.side_effect = wait
                    with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M.tempfile, "TemporaryFile", side_effect=setup), patch.object(M.subprocess, "Popen", side_effect=launch) as popen, patch.object(M, "process_snapshot", side_effect=snapshot) as process, patch.object(self.controller, "evidence", side_effect=evidence), patch.object(M, "sanitized", side_effect=sanitize), patch.object(M, "completion", side_effect=parse) as completion:
                        with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                            environment = {}
                            self.controller.driver("finite", environment)
                        if boundary == "setup": popen.assert_not_called()
                        if boundary == "launch": process.assert_not_called()
                        if boundary != "parse": completion.assert_not_called()
                        if boundary in ("launch", "snapshot", "evidence"): child.wait.assert_not_called()
                    if boundary != "setup":
                        self.assertEqual((self.opts.output / "finite-driver.log").read_text(), "retained log")

    def test_driver_fractional_budget_and_independent_cleanup_failure(self):
        self.opts.output.mkdir()
        self.controller.hashes = {role: M.digest(getattr(self.opts, role)) for role in ("cli", "driver", "observer")}
        self.controller.deadline = 90
        self.controller.phase_deadline = 89.75
        clock = dict(now=89.5)
        child = MagicMock(pid=31337, returncode=0)
        child.poll.return_value = 0
        snapshot = dict(sha256=self.controller.hashes["driver"], membership="0::/host")
        completion_record = {}
        environment = {}
        with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M.subprocess, "Popen", return_value=child), patch.object(M, "process_snapshot", return_value=snapshot), patch.object(M, "completion", return_value=completion_record):
            self.controller.driver("finite", environment)
        child.wait.assert_called_once_with(timeout=.25)
        child.poll.return_value = None
        def timeout(*args, **kwargs):
            clock["now"] = 100
            raise subprocess.TimeoutExpired("primary owned driver", 60)
        child.wait.side_effect = timeout
        clock["now"] = 89.5
        def launch(*args, **kwargs):
            kwargs["stdout"].write(b"\x1b[31mhttps://user:password@host/failure\x00")
            return child
        with patch.object(M.time, "monotonic", side_effect=lambda: clock["now"]), patch.object(M.subprocess, "Popen", side_effect=launch), patch.object(M, "process_snapshot", return_value=snapshot), patch.object(M.os, "killpg") as kill:
            with self.assertRaisesRegex(RuntimeError, "primary owned driver.*owned driver cleanup"):
                self.controller.driver("finite", environment)
        kill.assert_called_once_with(31337, M.signal.SIGKILL)
        expected_cleanup_budget = dict(timeout=3)
        self.assertEqual(child.wait.call_args_list[-1].kwargs, expected_cleanup_budget)
        text = (self.opts.output / "finite-driver.log").read_text()
        self.assertIn("[redacted]@host/failure", text)
        self.assertNotIn("password", text)
        self.assertNotIn("\x00", text)
        receipt = json.loads((self.opts.output / "finite-driver-cleanup-error.json").read_text())
        self.assertIn("primary owned driver", receipt["primary_error"])
        self.assertIn("timed out", receipt["cleanup_error"])


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

    def test_formal_completion_rejects_shortened_missing_and_false_policy(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "completion.json"
            text = "=== RUN   TestProductionLinuxLoad\n--- PASS: TestProductionLinuxLoad (631.00s)\nPASS\n"
            opts = dict(path=path, stage="load", revision="a" * 40, pid=123, stdout=text, profile=M.LOAD_PROFILE, elapsed=631)
            record = dict(schema=1, stage="load", profile=M.LOAD_PROFILE, revision="a" * 40, driver_pid=123, cases=M.LOAD_CASES, samples=0, documents=1000, qualification=False, warmup_seconds=30, measurement_seconds=600, arrival_rpc=200)
            path.write_text(json.dumps(record))
            self.assertEqual(M.completion(opts), record)
            for key, value in (("warmup_seconds", 2), ("measurement_seconds", 120), ("arrival_rpc", 20), ("documents", 264), ("cases", M.LOAD_CASES[:-1]), ("qualification", True), ("profile", M.PROFILE)):
                invalid = dict(record)
                invalid[key] = value
                path.write_text(json.dumps(invalid))
                with self.subTest(key=key), self.assertRaises(RuntimeError):
                    M.completion(opts)
            path.write_text(json.dumps(record))
            premature = dict(opts, elapsed=629.999)
            with self.assertRaisesRegex(RuntimeError, "premature"):
                M.completion(premature)
            skipped = dict(opts, stdout=text.replace("PASS:", "SKIP:"))
            with self.assertRaises(RuntimeError):
                M.completion(skipped)

    def test_formal_ci_has_own_full_duration_envelope_and_no_lock_promotion(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        job = workflow.split("  native-linux-formal-load:", 1)[1].split("  native-linux-finite:", 1)[0]
        for required in ("ubuntu-24.04", "timeout-minutes: 25", "test --mongo", "timeout 18m", "--profile=linux-native-load-v1", '--mongod="$binary"', "if: always()"):
            self.assertIn(required, job)
        self.assertNotIn("promote", job)
        self.assertNotIn("server.lock", job)
        self.assertEqual(M.driver_name(M.LOAD_PROFILE, "preflight"), "TestProductionLinuxLoadPreflight")
        self.assertEqual(M.driver_name(M.LOAD_PROFILE, "mongo"), "TestProductionLinuxLoadMongo")
        with self.assertRaises(KeyError):
            M.driver_name(M.LOAD_PROFILE, "finite")

    def test_completion_profiles_cannot_be_substituted(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "completion.json"
            for profile in (M.PROFILE, M.OS_PROFILE):
                text = "=== RUN   TestProductionLinuxFinite\n--- PASS: TestProductionLinuxFinite (0.01s)\nPASS\n"
                opts = dict(path=path, stage="finite", revision="a" * 40, pid=123, stdout=text, profile=profile)
                record = dict(schema=1, stage="finite", profile=profile, revision="a" * 40, driver_pid=123, cases=M.FINITE_CASES, samples=2, documents=6, qualification=False)
                path.write_text(json.dumps(record))
                self.assertEqual(M.completion(opts), record)
                other = M.OS_PROFILE if profile == M.PROFILE else M.PROFILE
                substituted_record = dict(record, profile=other)
                path.write_text(json.dumps(substituted_record))
                with self.assertRaisesRegex(RuntimeError, "completion identity mismatch"):
                    M.completion(opts)

    def test_opensearch_ci_builds_only_three_finite_binaries(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        job = workflow.split("  native-linux-opensearch-finite:", 1)[1].split("  native-linux-volume-copy:", 1)[0]
        self.assertIn("--profile=" + M.OS_PROFILE, job)
        self.assertIn("./cmd/weir", job)
        self.assertIn("./internal/app", job)
        self.assertIn("./internal/testutil/testresource", job)
        self.assertNotIn("helper", job)
        self.assertNotIn("testvolume", job)
        self.assertNotIn("volume-helper", job)
        self.assertIn("github.event.pull_request.head.sha || github.sha", job)
        self.assertIn("if: always()", job)

    def test_sanitized_evidence_removes_controls_and_userinfo(self):
        self.assertEqual(M.sanitized(b"\x1b[31mhttp://user:secret@example/x\x00\n"), "http://[redacted]@example/x\n")


if __name__ == "__main__":
    unittest.main()
