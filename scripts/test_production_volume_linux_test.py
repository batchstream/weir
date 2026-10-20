"""Offline lifecycle regressions. Fake tmpfs loses data on its last unmount."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

SPEC = importlib.util.spec_from_file_location("production_volume_linux", Path(__file__).with_name("test-production-volume-linux.py"))
M = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = M
SPEC.loader.exec_module(M)
ROOT = Path(__file__).resolve().parent.parent
# Actual CI39343925588 Docker28.0.4 selected-inspect failure, not fabricated absence.
RECORDED_TMPFS_ERROR = ('\ntemplate parsing error: template: :1:952: executing "" at '
                        '<.HostConfig.Tmpfs>: map has no entry for key "Tmpfs"\n')


def specs():
    result = {}
    for role in ("holder", "probe"):
        result[role] = dict(role=role, reference="public@sha256:" + "a" * 64, image="sha256:" + ("b" if role == "holder" else "c") * 64,
                            memory=(64 if role == "holder" else 128) << 20, cpu=100_000_000 if role == "holder" else 500_000_000,
                            ttl="300s" if role == "holder" else "55s", destination="/held" if role == "holder" else M.CONFIG)
    return result


class Fake(M.Controller):
    def __init__(self, opts):
        super().__init__(opts)
        self.arch = "amd64"
        self.specs = specs()
        self.calls = []
        self.volumes = {}
        self.containers = {}
        self.data = False
        self.mounts = set()
        self.lost_copyup = False
        self.fail_after_create = ""
        self.foreign = {}
        self.fail_logs = False
        self.fail_rm = False
        self.remain = False

    def prepare(self):
        self.calls.append(["preflight-complete"])

    def process(self, pid):
        info = next(v for v in self.containers.values() if v["State"]["Pid"] == pid)
        return dict(pid=pid, start_ticks=100 + pid, membership="0::/docker/" + info["Id"], namespace_pid=1)

    def proof(self, info, stage):
        role = info["Name"].rsplit("-", 1)[-1]
        spec = self.specs[role]
        pid = info["State"]["Pid"]
        process = dict(pid=1, start_ticks=100 + pid, sha256=self.opts.helper_sha256, executable=M.HELPER, uid=1000, gid=1000,
                       membership="0::/", cpu_max=("10000" if role == "holder" else "50000") + " 100000", memory_max=spec["memory"],
                       memory_swap_max=0, pids_max=32, cap_eff="0000000000000000", no_new_privs="1")
        filesystem = dict(magic=0x01021994, capacity=M.CAPACITY, mount=spec["destination"], device="0:123", options=["ro" if role == "holder" else "rw", "noexec", "nosuid", "nodev"], root_readonly=True, helper_readonly=True, no_shared_memory=True)
        effect = stage == "probe"
        files = [dict(name=n, size=123, sha256="d" * 64) for n in ("opensearch.yml", "jvm.options", "log4j2.properties")] if (effect or stage == "copy") and self.data else []
        return dict(schema=1, stage=stage, role=role, owner=self.owner, revision=self.opts.revision, helper_sha256=self.opts.helper_sha256,
                    image=spec["image"], arch=self.arch, process=process, filesystem=filesystem, files=files, fill_bytes=1024 if effect else 0,
                    fill_attempts=1 if effect else 0, enospc=effect, round_trip=effect, preserved=effect, qualification=False)

    def unmount(self, identity):
        self.mounts.discard(identity)
        if not self.mounts:
            self.data = False

    def docker(self, args, timeout=15, deadline=None):
        self.remaining(deadline)
        self.calls.append(args[:])
        if args[:2] == ["volume", "create"]:
            name = args[-1]
            self.volumes[name] = dict(Name=name, CreatedAt="2026-10-20T00:00:00Z", Driver="local", Scope="local", Options=copy.deepcopy(M.VOLUME_OPTIONS), Labels={"weir.owner": self.owner})
            if self.fail_after_create == "volume":
                raise RuntimeError("injected partial create timeout")
            return name + "\n"
        if len(args) > 1 and args[1] == "inspect":
            kind, name = args[0], args[-1]
            source = self.volumes if kind == "volume" else self.containers
            info = source.get(name)
            if info is None and kind == "container":
                info = next((v for v in source.values() if v["Name"] == "/" + name), None)
            if info is None:
                raise M.CommandError("Error: No such " + kind + ": " + name + "\n", 1)
            selected = info.copy()
            if kind == "container":
                # Keep the raw optional key omitted as the actual daemon does.
                # Safe Go index emits a REQUIRED selected key with null instead.
                raw_host = selected.pop("HostConfig")
                selected["Tmpfs"] = raw_host.get("Tmpfs")
            return json.dumps(selected)
        if args[:2] == ["container", "create"]:
            name = args[args.index("--name") + 1]
            role = name.rsplit("-", 1)[-1]
            spec = self.specs[role]
            identity = ("1" if role == "holder" else "2") * 64
            if role == "probe":
                # Creation mounts temporarily and copies only into an empty
                # volume. Without a live holder, final unmount LOSES copy-up.
                self.data = True
                if not self.mounts:
                    self.data = False
                    self.lost_copyup = True
            helper = dict(Type="bind", Destination=M.HELPER, Source=str(self.opts.helper), RW=False)
            volume = dict(Type="volume", Destination=spec["destination"], Name=self.volume.name, Driver="local", RW=role == "probe")
            self.containers[identity] = dict(Id=identity, Name="/" + name, Owner=self.owner, Image=spec["image"],
                State=dict(Running=False, Status="created", OOMKilled=False, ExitCode=0, Pid=101 if role == "holder" else 102, Dead=False, Paused=False, Restarting=False),
                Memory=spec["memory"], Swap=spec["memory"], CPU=spec["cpu"], PIDs=32, Cgroupns="private", Readonly=True,
                User="1000:1000", Workdir="/", Caps=["ALL"], Security=["no-new-privileges:true"], Privileged=False, PidMode="", IPC="none", Mounts=[helper, volume],
                NetworkMode="none", Ports={}, Bindings={}, HostConfig={}, Restart="no", Entrypoint=[M.HELPER], Cmd=self.helper_args(spec, role))
            if self.fail_after_create == role:
                raise RuntimeError("injected interrupted create")
            return identity + "\n"
        if args[:2] == ["container", "start"]:
            info = self.containers[args[-1]]
            info["State"]["Running"] = True
            info["State"]["Status"] = "running"
            self.mounts.add(args[-1])
            return args[-1] + "\n"
        if args[0] == "exec":
            info = self.containers[args[2]]
            return json.dumps(self.proof(info, "copy" if "--mode=copy" in args else "observe")) + "\n"
        if args[0] == "logs":
            if self.fail_logs:
                raise RuntimeError("injected logs failure")
            info = self.containers[args[-1]]
            role = info["Name"].rsplit("-", 1)[-1]
            return json.dumps(self.proof(info, role)) + "\n"
        if args[0] == "kill":
            identity = args[-1]
            self.containers[identity]["State"]["Running"] = False
            self.unmount(identity)
            return identity + "\n"
        if args[0] == "wait":
            return "0\n"
        if args[:2] == ["container", "rm"]:
            if self.fail_rm:
                raise RuntimeError("injected removal failure")
            if not self.remain:
                self.unmount(args[-1])
                del self.containers[args[-1]]
            return args[-1] + "\n"
        if args[:2] == ["container", "ls"]:
            if "--all" not in args:
                raise AssertionError("stopped attachments omitted")
            return "\n".join(list(self.foreign) + list(self.containers)) + ("\n" if self.foreign or self.containers else "")
        if args[:2] == ["volume", "rm"]:
            if self.containers or self.foreign or self.mounts:
                raise AssertionError("removed attached volume")
            if not self.remain:
                del self.volumes[args[-1]]
            return args[-1] + "\n"
        raise AssertionError("unexpected Docker command: " + repr(args))


class VolumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "evidence"
        self.output.mkdir()
        self.helper = self.root / "helper"
        self.helper.write_bytes(b"known test helper")
        opts = M.Options(ROOT, "a" * 40, self.helper, M.digest(self.helper), self.output)
        self.c = Fake(opts)
        self.patcher = patch.object(M, "host_process", side_effect=self.c.process)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def held(self):
        c = self.c
        c.volume = M.Resource("volume", c.owner + "-config")
        c.create(c.volume, ["volume", "create", c.volume.name])
        c.holder = c.create_container("holder")
        c.observe(c.holder, "holder")
        return c

    def test_held_copyup_and_exact_reverse_cleanup(self):
        c = self.c
        c.fixture()
        self.assertFalse(c.lost_copyup)
        self.assertFalse(c.data)  # Last holder unmount really loses tmpfs data.
        self.assertEqual(c.cleanup(), [])
        self.assertFalse(c.volumes or c.containers or c.mounts)
        mutations = [a for a in c.calls if a[:2] in (["container", "rm"], ["volume", "rm"])]
        self.assertEqual([a[-1] for a in mutations], ["2" * 64, "1" * 64, c.volume.name])
        target_create = next(i for i, a in enumerate(c.calls) if a[:2] == ["container", "create"] and c.owner + "-probe" in a)
        earlier = c.calls[:target_create]
        self.assertTrue(any(a[:2] == ["container", "start"] and a[-1] == "1" * 64 for a in earlier))
        self.assertTrue(any(a[0] == "exec" and a[2] == "1" * 64 for a in earlier))
        self.assertFalse(any(a[0] in ("pull", "build", "run") for a in c.calls))
        for a in c.calls:
            if a[:2] == ["container", "create"]:
                self.assertIn("--pull=never", a)
                self.assertIn("--network=none", a)
                self.assertNotIn("volume-nocopy", " ".join(a))
        result = json.loads((self.output / "probe-completion.json").read_text())
        copied = json.loads((self.output / "create-time-copy.json").read_text())
        self.assertFalse(copied["target_started"])
        self.assertEqual(copied["helper"]["files"], result["files"])
        copy_command = next(i for i, a in enumerate(c.calls) if a[0] == "exec" and "--mode=copy" in a)
        target_start = next(i for i, a in enumerate(c.calls) if a[:2] == ["container", "start"] and a[-1] == "2" * 64)
        self.assertLess(target_create, copy_command)
        self.assertLess(copy_command, target_start)
        self.assertTrue(result["enospc"] and result["preserved"])

    def test_empty_create_copy_rejects_before_start_and_changed_after_start(self):
        c = self.held()
        original = c.proof
        def empty_copy(info, stage):
            proof = original(info, stage)
            if stage == "copy":
                proof["files"] = []
            return proof
        with patch.object(c, "proof", side_effect=empty_copy), self.assertRaises(RuntimeError):
            c.create_container("probe")
        self.assertFalse(any(a[:2] == ["container", "start"] and a[-1] == "2" * 64 for a in c.calls))
        self.assertEqual(c.cleanup(), [])
        self.c = Fake(c.opts)
        with patch.object(M, "host_process", side_effect=self.c.process):
            c = self.held()
            target = c.create_container("probe")
            proof = c.proof(c.containers[target.identity], "probe")
            proof["files"][0]["sha256"] = "e" * 64
            with self.assertRaisesRegex(RuntimeError, "CREATE-time"):
                c.record(json.dumps(proof), "probe", "probe")
            self.assertEqual(c.cleanup(), [])

    def test_fake_does_not_invent_last_unmount_persistence(self):
        c = self.held()
        c.mounts.clear()
        c.data = False
        c.containers[c.holder.identity]["State"]["Running"] = False
        with self.assertRaises(RuntimeError):
            c.create_container("probe")
        self.assertFalse(any(a[:2] == ["container", "create"] and c.owner + "-probe" in a for a in c.calls))
        # Directly model the old wrong topology, bypassing ONLY our admission.
        args = ["container", "create", "--name", c.owner + "-probe"]
        c.docker(args)
        self.assertTrue(c.lost_copyup)
        proof = c.proof(c.containers["2" * 64], "probe")
        with self.assertRaises(RuntimeError):
            c.record(json.dumps(proof), "probe", "probe")

    def test_interruption_at_every_registered_create_boundary(self):
        for boundary in ("volume", "holder", "probe"):
            with self.subTest(boundary=boundary):
                self.c = Fake(self.c.opts)
                self.c.fail_after_create = boundary
                with patch.object(M, "host_process", side_effect=self.c.process):
                    with self.assertRaises(RuntimeError):
                        self.c.fixture()
                    self.assertEqual(self.c.cleanup(), [])
                self.assertFalse(self.c.volumes or self.c.containers)

    def test_interruption_before_each_create_takes_effect(self):
        for boundary in ("volume", "holder", "probe"):
            c = Fake(self.c.opts)
            original = c.docker
            def fail_before(args, timeout=15, deadline=None):
                match = args[:2] == ["volume", "create"] if boundary == "volume" else args[:2] == ["container", "create"] and c.owner + "-" + boundary in args
                if match:
                    raise RuntimeError("injected create did not take effect")
                return original(args, timeout, deadline)
            with self.subTest(boundary=boundary), patch.object(M, "host_process", side_effect=c.process), patch.object(c, "docker", side_effect=fail_before):
                with self.assertRaises(RuntimeError):
                    c.fixture()
                self.assertEqual(c.cleanup(), [])
            self.assertEqual(len(c.resources), {"volume": 1, "holder": 2, "probe": 3}[boundary])
            self.assertFalse(c.containers or c.volumes)

    def test_changed_volume_and_attachment_query_errors_never_remove_volume(self):
        for failure in ("recreated", "inspect", "attachments"):
            c = Fake(self.c.opts)
            with patch.object(M, "host_process", side_effect=c.process):
                previous = self.c
                self.c = c
                self.held()
                self.c = previous
            original = c.docker
            if failure == "recreated":
                c.volumes[c.volume.name]["CreatedAt"] = "2026-10-20T00:00:01Z"
            def fault(args, timeout=15, deadline=None):
                if failure == "inspect" and args[:2] == ["volume", "inspect"]:
                    raise M.CommandError("daemon unavailable", 1)
                if failure == "attachments" and args[:2] == ["container", "ls"]:
                    raise M.CommandError("permission denied", 1)
                return original(args, timeout, deadline)
            with self.subTest(failure=failure), patch.object(c, "docker", side_effect=fault):
                errors = c.cleanup()
            self.assertTrue(errors)
            self.assertIn(c.volume.name, c.volumes)
            self.assertFalse(any(a[:2] == ["volume", "rm"] for a in c.calls))

    def test_volume_tuple_changes_rejected(self):
        c = self.held()
        original = copy.deepcopy(c.volumes[c.volume.name])
        edits = {"Name": "foreign", "CreatedAt": "2026-10-20T00:00:01Z", "Driver": "plugin", "Scope": "global", "Options": {}, "Labels": {"weir.owner": "foreign"}}
        for key, value in edits.items():
            with self.subTest(key=key):
                c.volumes[c.volume.name] = copy.deepcopy(original)
                c.volumes[c.volume.name][key] = value
                with self.assertRaises(RuntimeError):
                    c.guard(c.volume)
                del c.volumes[c.volume.name][key]
                with self.assertRaises(RuntimeError):
                    c.guard(c.volume)
        c.volumes[c.volume.name] = original
        self.assertEqual(c.cleanup(), [])

    def test_tmpfs_raw_omission_null_and_empty_map_allow_full_lifecycle(self):
        for value in ("omitted", None, {}):
            c = Fake(self.c.opts)
            original = c.docker
            def raw_value(args, timeout=15, deadline=None):
                reply = original(args, timeout, deadline)
                if args[:2] == ["container", "create"]:
                    raw_host = c.containers[reply.strip()]["HostConfig"]
                    if value == "omitted":
                        self.assertNotIn("Tmpfs", raw_host)
                    else:
                        raw_host["Tmpfs"] = value
                return reply
            with self.subTest(value=value), patch.object(M, "host_process", side_effect=c.process), patch.object(c, "docker", side_effect=raw_value):
                c.fixture()
                selected = c.inspect(c.holder)
                self.assertIn("Tmpfs", selected)
                self.assertEqual(selected["Tmpfs"], None if value == "omitted" else value)
                self.assertEqual(c.cleanup(), [])
            self.assertFalse(c.volumes or c.containers)

    def test_tmpfs_wrong_types_and_nonempty_map_rejected_before_target_create(self):
        c = self.held()
        raw_host = c.containers[c.holder.identity]["HostConfig"]
        for value in (False, 0, "", [], ["/tmp"], {"/tmp": "rw,size=1"}, True, 1):
            raw_host["Tmpfs"] = value
            before = len(c.calls)
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                c.create_container("probe")
            self.assertFalse(any(a[:2] == ["container", "create"] for a in c.calls[before:]))
        raw_host.pop("Tmpfs")
        self.assertEqual(c.cleanup(), [])

    def test_missing_selected_tmpfs_key_is_not_defaulted(self):
        c = self.held()
        selected = c.inspect(c.holder)
        del selected["Tmpfs"]
        with patch.object(c, "guard", return_value=selected), self.assertRaises(KeyError):
            c.check_container(c.holder, "holder")
        self.assertEqual(c.cleanup(), [])

    def test_actual_template_failure_remains_unknown_and_preserves_resources(self):
        c = self.held()
        original = c.docker
        def inspect_failure(args, timeout=15, deadline=None):
            if args[:2] == ["container", "inspect"]:
                raise M.CommandError(RECORDED_TMPFS_ERROR, 1)
            return original(args, timeout, deadline)
        before = len(c.calls)
        with patch.object(c, "docker", side_effect=inspect_failure):
            errors = c.cleanup()
        self.assertEqual(len(errors), 4)
        self.assertIn(c.holder.identity, c.containers)
        self.assertIn(c.volume.name, c.volumes)
        self.assertFalse(any(a[0] == "kill" or a[:2] in (["container", "rm"], ["volume", "rm"]) for a in c.calls[before:]))
        receipt = json.loads((self.output / "cleanup.json").read_text())
        self.assertTrue(all(not item["absent"] for item in receipt["resources"]))

    def test_holder_identity_mount_and_security_admission(self):
        c = self.held()
        original = copy.deepcopy(c.containers[c.holder.identity])
        variants = []
        for key, value in (("Owner", "foreign"), ("Id", "e" * 64), ("Name", "/foreign"), ("Entrypoint", ["/wrong"]), ("Readonly", False), ("Memory", 0), ("CPU", 500_000_000), ("Swap", -1), ("PIDs", 0), ("Cgroupns", "host"), ("User", "0"), ("NetworkMode", "bridge"), ("Security", [])):
            changed = copy.deepcopy(original)
            changed[key] = value
            variants.append(changed)
        for key, value in (("Running", False), ("OOMKilled", True), ("Pid", 0)):
            changed = copy.deepcopy(original)
            changed["State"][key] = value
            variants.append(changed)
        for key, value in (("RW", True), ("Name", "foreign"), ("Destination", "/wrong")):
            changed = copy.deepcopy(original)
            changed["Mounts"][1][key] = value
            variants.append(changed)
        changed = copy.deepcopy(original)
        changed["Mounts"][0]["RW"] = True
        variants.append(changed)
        for changed in variants:
            c.containers[c.holder.identity] = changed
            before = len(c.calls)
            with self.assertRaises((RuntimeError, StopIteration)):
                c.create_container("probe")
            self.assertFalse(any(a[:2] == ["container", "create"] for a in c.calls[before:]))
        c.containers[c.holder.identity] = original
        self.assertEqual(c.cleanup(), [])

    def test_absent_holder_rejects_target(self):
        c = self.held()
        del c.containers[c.holder.identity]
        with self.assertRaises(RuntimeError):
            c.create_container("probe")

    def test_wrong_actual_executable_or_mount_rejects_before_target_create(self):
        c = self.held()
        original = c.proof
        for kind in ("executable", "sha256", "device", "capacity", "options", "start"):
            def altered(info, stage):
                proof = original(info, stage)
                if kind in ("executable", "sha256"):
                    proof["process"][kind] = "/wrong" if kind == "executable" else "e" * 64
                elif kind == "start":
                    proof["process"]["start_ticks"] += 1
                elif kind == "device":
                    proof["filesystem"]["device"] = "0:999"
                elif kind == "capacity":
                    proof["filesystem"]["capacity"] = 32 << 20
                else:
                    proof["filesystem"]["options"] = ["rw", "noexec", "nodev", "nosuid"]
                return proof
            before = len(c.calls)
            with self.subTest(kind=kind), patch.object(c, "proof", side_effect=altered), self.assertRaises(RuntimeError):
                c.create_container("probe")
            self.assertFalse(any(a[:2] == ["container", "create"] for a in c.calls[before:]))
        self.assertEqual(c.cleanup(), [])

    def test_missing_late_duplicate_and_mismatched_completion(self):
        c = self.held()
        target = c.create_container("probe")
        observation = c.observe(target, "probe")
        valid = json.dumps(c.proof(c.containers[target.identity], "probe")) + "\n"
        with patch.object(c, "logs", return_value=""), patch.object(c, "check_container"), patch.object(M.time, "sleep"), patch.object(M.time, "monotonic", side_effect=[10, 10, 21]):
            with self.assertRaises(RuntimeError):
                c.completion(target, "probe", observation)
        with patch.object(c, "logs", return_value=valid), patch.object(M.time, "monotonic", side_effect=[10, 10, 21]):
            with self.assertRaisesRegex(RuntimeError, "late"):
                c.completion(target, "probe", observation)
        with patch.object(c, "logs", return_value=valid + valid):
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                c.completion(target, "probe", observation)
        changed = copy.deepcopy(observation)
        changed["process"]["start_ticks"] += 1
        with self.assertRaisesRegex(RuntimeError, "identity differ"):
            c.completion(target, "probe", changed)
        self.assertEqual(c.cleanup(), [])

    def test_foreign_running_and_stopped_attachments_preserved(self):
        for state in ("running", "stopped"):
            with self.subTest(state=state):
                c = self.held() if not self.c.resources else self.c
                c.foreign = {"f" * 64: state}
                errors = c.cleanup()
                self.assertTrue(any("attachments" in e for e in errors))
                self.assertIn(c.volume.name, c.volumes)
                self.assertFalse(any(a[:2] == ["container", "rm"] and a[-1] == "f" * 64 for a in c.calls))
        self.assertTrue(any("--all" in a for a in c.calls if a[:2] == ["container", "ls"]))

    def test_cleanup_failures_not_success(self):
        for attr in ("fail_logs", "fail_rm", "remain"):
            with self.subTest(attr=attr):
                self.c = Fake(self.c.opts)
                with patch.object(M, "host_process", side_effect=self.c.process):
                    c = self.held()
                    setattr(c, attr, True)
                    self.assertTrue(c.cleanup())
                    receipt = json.loads((self.output / "cleanup.json").read_text())
                    self.assertTrue(receipt["errors"])
                    if attr != "fail_logs":
                        self.assertIn(c.volume.name, c.volumes)

    def test_expired_or_failed_helper_removed_but_never_cleanup_success(self):
        c = self.held()
        state = c.containers[c.holder.identity]["State"]
        state["Running"] = False
        state["ExitCode"] = 1
        c.unmount(c.holder.identity)
        errors = c.cleanup()
        self.assertTrue(any("TTL" in value for value in errors))
        self.assertFalse(c.volumes or c.containers)
        receipt = json.loads((self.output / "cleanup.json").read_text())
        self.assertTrue(all(r["absent"] for r in receipt["resources"]))
        self.assertTrue(receipt["errors"])

    def test_unknown_inspection_is_not_absence(self):
        c = M.Controller(self.c.opts)
        resource = M.Resource("volume", "owned")
        for error in (M.CommandError("permission denied", 1), RuntimeError("timeout"), M.CommandError("daemon unavailable", 1)):
            with patch.object(c, "docker", side_effect=error):
                with self.assertRaises(RuntimeError):
                    c.inspect(resource)
        missing = M.CommandError("Error response from daemon: get owned: no such volume\n", 1)
        with patch.object(c, "docker", side_effect=missing):
            self.assertIsNone(c.inspect(resource))

    def test_reused_name_never_counts_as_absence(self):
        c = self.held()
        original = c.containers.pop(c.holder.identity)
        original["Id"] = "e" * 64
        original["Owner"] = "foreign"
        c.containers[original["Id"]] = original
        errors = c.cleanup()
        self.assertTrue(any("replacement" in e for e in errors))
        self.assertIn(c.volume.name, c.volumes)
        self.assertIn("e" * 64, c.containers)

    def test_completion_strict_identity_effects_and_controls(self):
        c = self.held()
        target = c.create_container("probe")
        proof = c.proof(c.containers[target.identity], "probe")
        c.record(json.dumps(proof), "probe", "probe")
        for key, value in (("owner", "foreign"), ("revision", "e" * 40), ("helper_sha256", "e" * 64), ("image", "sha256:" + "e" * 64), ("stage", "holder"), ("files", []), ("enospc", False), ("preserved", False), ("fill_bytes", M.CAPACITY + 1), ("fill_attempts", 274), ("qualification", True), ("schema", True)):
            changed = copy.deepcopy(proof)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                c.record(json.dumps(changed), "probe", "probe")
        for group, key, value in (("process", "sha256", "e" * 64), ("process", "memory_max", 0), ("process", "cpu_max", "max 100000"), ("process", "cap_eff", "1"), ("filesystem", "magic", 0), ("filesystem", "capacity", 2*M.CAPACITY), ("filesystem", "options", ["rw"])):
            changed = copy.deepcopy(proof)
            changed[group][key] = value
            with self.subTest(key=key), self.assertRaises((RuntimeError, ValueError)):
                c.record(json.dumps(changed), "probe", "probe")
        for text in ("", "{}", json.dumps(proof) + "\n" + json.dumps(proof), '{"schema":1,"schema":1}', "x" * 16385):
            with self.assertRaises((RuntimeError, json.JSONDecodeError)):
                c.record(text, "probe", "probe")
        c.cleanup()

    def test_deadlines_have_no_positive_floor(self):
        self.c.deadline = time.monotonic() - 1
        with self.assertRaises(RuntimeError):
            self.c.remaining()
        self.c.deadline = time.monotonic() + 1
        with self.assertRaises(RuntimeError):
            self.c.remaining(time.monotonic() - 1)

    def test_primary_and_cleanup_errors_retained(self):
        c = self.c
        c.opts.output = self.root / "new-output"
        def fail():
            raise RuntimeError("primary failure")
        with patch.object(M, "native_preflight", return_value="amd64"), patch.object(c, "fixture", side_effect=fail), patch.object(c, "cleanup", return_value=["cleanup failure"]):
            with self.assertRaises(RuntimeError):
                c.execute()
        result = json.loads((c.opts.output / "result.json").read_text())
        self.assertEqual(result["primary_error"], "primary failure")
        self.assertEqual(result["cleanup_errors"], ["cleanup failure"])
        self.assertFalse(result["filesystem_prerequisite_complete"])

    def test_unsupported_host_touches_no_paths_or_docker(self):
        with patch.object(M.platform, "system", return_value="Darwin"), patch.object(M.Path, "is_socket", side_effect=AssertionError("path touched")), patch.object(M.subprocess, "Popen", side_effect=AssertionError("command started")):
            with self.assertRaisesRegex(RuntimeError, "native Linux"):
                M.native_preflight(self.c.opts)

    def test_real_prepare_validation_never_mutates_or_prepares_images(self):
        build = "helper: go1.27.1\n\tpath\tgithub.com/batchstream/weir/internal/testutil/testvolume\n"
        for key, value in (("GOOS", "linux"), ("GOARCH", "amd64"), ("CGO_ENABLED", "0"), ("-trimpath", "true")):
            build += "\tbuild\t" + key + "=" + value + "\n"
        for failure in ("", "revision", "dirty", "compiler", "dependencies", "stamp", "engine", "image", "anonymous-volume", "config-id"):
            c = M.Controller(self.c.opts)
            c.arch = "amd64"
            commands = []
            def command(args, timeout=15, deadline=None):
                commands.append(args)
                if args[0] == "git":
                    if args[-1] == "HEAD":
                        return ("e" * 40 if failure == "revision" else c.opts.revision) + "\n"
                    return " M changed-source\n" if failure == "dirty" else ""
                if args == ["go", "version"]:
                    return "go version go1.27.1 linux/amd64\n"
                if args[0] == str(c.opts.helper):
                    identity = dict(schema=1, stage="identity", revision="e" * 40 if failure == "stamp" else c.opts.revision,
                                    helper_sha256=c.opts.helper_sha256, os="linux", arch="amd64", go="go1.27.1")
                    return json.dumps(identity) + "\n"
                return build.replace("go1.27.1", "go1.26.0") if failure == "compiler" else build + ("\tdep\tforeign/package\tv1\n" if failure == "dependencies" else "")
            def docker(args, timeout=15, deadline=None):
                commands.append(args)
                if args[0] == "info":
                    engine = dict(OS="linux", Arch="amd64", Cgroup="1" if failure == "engine" else "2", CPUs=2, Memory=4 << 30,
                                  Root=str(self.root), Kernel=M.platform.release(), Version="known-test-engine", ID="known-test-daemon")
                    return json.dumps(engine)
                self.assertEqual(args[:2], ["image", "inspect"])
                reference = args[-1]
                target = reference.startswith(M.REPOSITORY + "@")
                image = dict(OS="linux", Arch="arm64" if failure == "image" else "amd64", Digests=[reference],
                             Id=("sha256:" + "e" * 64 if failure == "config-id" else M.CONFIG_IDS["amd64"]) if target else "sha256:" + "b" * 64,
                             Volumes={"/data": {}} if failure == "anonymous-volume" else None)
                return json.dumps(image)
            usage = SimpleNamespace(free=8 << 30)
            with patch.object(c, "run", side_effect=command), patch.object(c, "docker", side_effect=docker), patch.object(M.shutil, "disk_usage", return_value=usage):
                if failure:
                    with self.subTest(failure=failure), self.assertRaises(RuntimeError):
                        c.prepare()
                else:
                    c.prepare()
            self.assertEqual(c.resources, [])
            self.assertFalse(any(a[0] in ("pull", "build", "volume", "container") for a in commands))

    def test_remote_and_context_overrides_fail_before_paths(self):
        for override in ({"DOCKER_HOST": "tcp://remote.invalid:2375"}, {"DOCKER_CONTEXT": "foreign"}, {"DOCKER_TLS_VERIFY": "1"}, {"DOCKER_API_VERSION": "1.1"}):
            with patch.object(M.platform, "system", return_value="Linux"), patch.object(M.platform, "machine", return_value="x86_64"), patch.dict(os.environ, override, clear=True), patch.object(M.Path, "is_socket", side_effect=AssertionError("path touched")):
                with self.assertRaisesRegex(RuntimeError, "overrides"):
                    M.native_preflight(self.c.opts)

    def test_bounded_command_timeout_output_and_no_inherited_values(self):
        c = M.Controller(self.c.opts)
        output = c.run([sys.executable, "-I", "-c", "print('public')"])
        self.assertEqual(output, "public\n")
        with self.assertRaisesRegex(RuntimeError, "exceeds64KiB"):
            c.run([sys.executable, "-I", "-c", "print('x'*100000)"])
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            c.run([sys.executable, "-I", "-c", "import time; time.sleep(1)"], .02)
        output = c.run([sys.executable, "-I", "-c", "import os; print(any(k.startswith('DOCKER_') for k in os.environ))"])
        self.assertEqual(output, "False\n")


if __name__ == "__main__":
    unittest.main()
