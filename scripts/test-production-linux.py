#!/usr/bin/env python3
"""Owned native Linux finite Search correctness or exact formal backend load/fault.

Execution observations are not build-origin or promotion receipts.

Image preparation is an explicit, separate CI step. This executor never pulls,
builds, installs, changes host controls, discovers credentials or uses contexts.
"""

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import ssl
import stat
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

PROFILE = "linux-native-es-functional-v1"
OS_PROFILE = "linux-native-opensearch-functional-v1"
LOAD_PROFILE = "linux-native-load-v1"
FAULT_PROFILE = "linux-native-fault-v1"
FORMAL_PROFILES = (LOAD_PROFILE, FAULT_PROFILE)
FAULT_CASES = ["Resolve", "healthy", "lost-ack-native", "lost-ack-typed", "cancel-after-ack", "slow-IO", "unavailable-recovery", "ownership-zero", "SIGTERM", "oracle", "no-replay"]
PROFILES = {FAULT_PROFILE: ("elasticsearch", "8.19.22"), PROFILE: ("elasticsearch", "8.19.22"), OS_PROFILE: ("opensearch", "3.3.0"), LOAD_PROFILE: ("elasticsearch", "8.19.22")}
MONGO_SHA256 = "15c7caf565edeaca228c76fbe6c4503b1cff7ae6517f344cc555552423b3209a"
MONGO_BYTES = 222890240
LOAD_DATA_LIMIT = 2 << 30
LOAD_DISK_ADMISSION = 8 << 30
LOAD_DISK_FREE = 2 << 30
LOAD_CASES = ["Resolve", "fixed-arrival", "slow-consumers", "burst", "latency", "memory", "ownership-zero", "oracle"]
ES_REPOSITORY = "docker.elastic.co/elasticsearch/elasticsearch"
# Verified public 8.19.22 index sha256:e98f9c3b09beb2fbb9eaf667d602df3f0e00bd3644138b8458dc17ba1a675595.
# The old server fixture pin is its arm64 child, NOT an interchangeable index.
ES_PINS = {
    "amd64": "sha256:f1e3d88299cb0db2131fc2ab9060faefccd88fd684d11a49256ae00d7dc3c857",
    "arm64": "sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237",
}
# The pinned image's tini/stock entrypoint supports a custom CMD. Stage only
# in the existing bounded /tmp, then re-enter its normal eswrapper startup.
# cp -R includes dotfiles via /. without preserving root-owned file ownership.
ES_ENTRYPOINT = ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
ES_COMMAND = ["/bin/bash", "-euc", 'export ES_PATH_CONF=/tmp/weir-es-config\n'
              'mkdir -m 0700 "$ES_PATH_CONF"\n'
              'cp -R /usr/share/elasticsearch/config/. "$ES_PATH_CONF/"\n'
              'exec /usr/local/bin/docker-entrypoint.sh eswrapper']
ES_LOAD_COMMAND = ES_COMMAND[:2] + ["umask 0002\n" + ES_COMMAND[2]]
ES_TMPFS = {
    "/usr/share/elasticsearch/data": "rw,noexec,nosuid,size=268435456,uid=1000,gid=0",
    "/usr/share/elasticsearch/logs": "rw,noexec,nosuid,size=33554432,uid=1000,gid=0",
    "/tmp": "rw,nosuid,size=134217728,uid=1000,gid=0",
}
OS_REPOSITORY = "opensearchproject/opensearch"
OS_PINS = {
    "amd64": "sha256:59e9e49ac00f19c2af9094e367c8e6586f7d07e6c06465c3f4c43b40934309f2",
    "arm64": "sha256:98db5375d94a62ee3d9073681bb40fe1087657d036f3dfa79053b0b1f83f5577",
}
OS_LOAD_COMMAND = ["/bin/bash", "-euc", "umask 0002\nexec ./opensearch-docker-entrypoint.sh opensearch"]
OS_TMPFS = {
    "/tmp": "rw,nosuid,nodev,size=134217728,uid=1000,gid=1000",
    "/usr/share/opensearch/data": "rw,noexec,nosuid,nodev,size=268435456,uid=1000,gid=1000",
    "/usr/share/opensearch/logs": "rw,noexec,nosuid,nodev,size=33554432,uid=1000,gid=1000",
}
FULL_ID = re.compile(r"[0-9a-f]{64}\Z")
FINITE_CASES = ["Resolve", "Put", "Read", "Native", "Lua", "Batch4", "Update", "Delete", "oracle", "SDK-close", "ownership-zero"]
CONTAINER_FORMAT = ('{"Id":{{json .Id}},"Name":{{json .Name}},"Owner":{{json (index .Config.Labels "weir.owner")}},'
                    '"Image":{{json .Image}},"State":{{json .State}},'
                    '"Memory":{{json .HostConfig.Memory}},"Swap":{{json .HostConfig.MemorySwap}},'
                    '"CPU":{{json .HostConfig.NanoCpus}},"PIDs":{{json .HostConfig.PidsLimit}},'
                    '"Cgroupns":{{json .HostConfig.CgroupnsMode}},"Readonly":{{json .HostConfig.ReadonlyRootfs}},'
                    '"User":{{json .Config.User}},"Caps":{{json .HostConfig.CapDrop}},'
                    '"Security":{{json .HostConfig.SecurityOpt}},"Privileged":{{json .HostConfig.Privileged}},'
                    '"PidMode":{{json .HostConfig.PidMode}},"Mounts":{{json .Mounts}},'
                    '"NetworkMode":{{json .HostConfig.NetworkMode}},'
                    '"Networks":{{json .NetworkSettings.Networks}},"Ports":{{json .NetworkSettings.Ports}},'
                    '"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},"Tmpfs":{{json (index .HostConfig "Tmpfs")}}}')
NETWORK_FORMAT = '{"Id":{{json .Id}},"Owner":{{json (index .Labels "weir.owner")}},"Driver":{{json .Driver}},"Internal":{{json .Internal}},"Containers":{{json .Containers}}}'
IMAGE_FORMAT = '{"Id":{{json .Id}},"OS":{{json .Os}},"Arch":{{json .Architecture}},"Digests":{{json .RepoDigests}},"Volumes":{{json (index .Config "Volumes")}}}'
MAX_OUTPUT = 4 << 20


@dataclass
class Options:
    server_root: Path
    harness_root: Path
    revision: str
    cli: Path
    driver: Path
    observer: Path
    output: Path
    profile: str = PROFILE
    mongod: Path | None = None
    product: str = "elasticsearch"
    transport: str = "plain"


def backend_product(opts):
    require(opts.product in ("elasticsearch", "opensearch"), "unsupported formal backend product")
    if opts.profile in FORMAL_PROFILES:
        version = "3.3.0" if opts.product == "opensearch" else "8.19.22"
        return opts.product, version
    require(opts.product == "elasticsearch", "product option is only supported for formal profiles")
    return PROFILES[opts.profile]


@dataclass
class Resource:
    kind: str
    name: str
    identity: str = ""


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as stream:
        while data := stream.read(1 << 20):
            total += len(data)
            require(total <= 128 << 20, "executable exceeds128MiB bound")
            h.update(data)
    return h.hexdigest()


def sanitized(raw):
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    text = re.sub(r"(https?://)[^\s/@]+:[^\s/@]+@", r"\1[redacted]@", text)
    return "".join(c for c in text if c in "\n\t" or ord(c) >= 32 and ord(c) != 127)


def native_preflight(opts):
    require(opts.profile in PROFILES, "exact implemented profile required")
    require(opts.transport in ("plain", "tls") and (opts.transport == "plain" or opts.profile in FORMAL_PROFILES), "TLS requires an exact formal profile")
    backend_product(opts)
    # No Docker command or fixture action is reachable on unsupported hosts.
    require(platform.system() == "Linux", "native Linux required; Darwin/Desktop unsupported")
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    require(arch in ES_PINS, "unsupported native host architecture")
    if opts.profile in FORMAL_PROFILES:
        release = platform.freedesktop_os_release()
        require(arch == "amd64" and release.get("ID") == "ubuntu" and release.get("VERSION_ID") == "24.04", "formal load requires Ubuntu24.04 amd64")
        require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "formal host-network CLI requires disposable GitHub-hosted CI runner")
        require(opts.mongod is not None and opts.mongod.is_absolute() and opts.mongod.resolve() == opts.mongod and opts.mongod.is_file() and os.access(opts.mongod, os.X_OK), "explicit canonical pinned native mongod required")
        require(opts.mongod.stat().st_size == MONGO_BYTES, "native Mongo binary size mismatch")
        with opts.mongod.open("rb") as stream:
            require(hashlib.file_digest(stream, "sha256").hexdigest() == MONGO_SHA256, "native Mongo binary hash mismatch")
    else:
        require(opts.mongod is None, "finite profiles do not accept Mongo fixtures")
    for key in ("DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        require(not os.environ.get(key), "Docker context/TLS overrides unsupported")
    require(os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock") == "unix:///var/run/docker.sock", "remote/custom Docker unsupported")
    require(shutil.which("docker") is not None and shutil.which("git") is not None, "prepared Docker/Git required")
    require(Path("/var/run/docker.sock").is_socket(), "local Docker socket required")
    require(re.fullmatch(r"[0-9a-f]{40}", opts.revision), "exact source revision required")
    for path in (opts.server_root, opts.harness_root, opts.output):
        require(not any(c in str(path) for c in (",", "\n", "\r")), "source/evidence path contains delimiters")
    for root in (opts.server_root, opts.harness_root):
        require(root.is_absolute() and root.resolve() == root and root.is_dir(), "canonical explicit source roots required")
    for path in (opts.cli, opts.driver, opts.observer):
        require(path.is_absolute() and path.resolve() == path and path.is_file() and os.access(path, os.X_OK), "canonical compiled executable required")
        require(0 < path.stat().st_size <= 128 << 20, "executable exceeds bounded input size")
        if path in (opts.cli, opts.observer):
            require(path.stat().st_mode & 0o005 == 0o005, "container-mounted executable must be readable/executable by its nonroot UID")
        require(not any(c in str(path) for c in (",", "\n", "\r")), "executable mount path contains delimiters")
        with path.open("rb") as stream:
            header = stream.read(64)
        machine = {"amd64": 62, "arm64": 183}[arch]
        require(len(header) == 64 and header[:6] == b"\x7fELF\x02\x01" and struct.unpack_from("<H", header, 18)[0] == machine, "native Linux ELF required; no emulation")
    require(len({opts.cli, opts.driver, opts.observer}) == 3, "distinct executable roles required")
    require((os.cpu_count() or 0) >= 2, "two host CPUs required")
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    require(int(memory["MemAvailable"].split()[0]) >= 4 * 1024 * 1024, "at least4GiB actual available host memory required")
    required_disk = LOAD_DISK_ADMISSION if opts.profile in FORMAL_PROFILES else 2 << 30
    require(shutil.disk_usage(opts.output.parent).free >= required_disk, "insufficient disposable host storage admission")
    return arch


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate completion field")
        result[key] = value
    return result


def completion(opts):
    with opts["path"].open("rb") as stream:
        raw = stream.read(16385)
    require(len(raw) <= 16384, "completion exceeds bound")
    record = json.loads(raw, object_pairs_hook=unique_fields)
    require(isinstance(record, dict), "structured completion object required")
    expected_keys = {"schema", "stage", "profile", "revision", "driver_pid", "cases", "samples", "documents", "qualification"}
    if opts["stage"] == "load":
        expected_keys |= {"warmup_seconds", "measurement_seconds", "arrival_rpc"}
    require(set(record) == expected_keys, "missing/unknown completion fields")
    require(type(record["schema"]) is int and record["schema"] == 1 and record["stage"] == opts["stage"] and record["profile"] == opts.get("profile", PROFILE) and record["revision"] == opts["revision"], "completion identity mismatch")
    require(type(record["driver_pid"]) is int and record["driver_pid"] == opts["pid"] and record["qualification"] is False, "completion driver identity/qualification mismatch")
    cases = {"tls": ["exact-loopback-certificates", "stock-security-configuration"], "preflight": ["source", "cli", "kernel-driver"], "mongo": ["replica-init", "version", "advertised-loopback"], "load": LOAD_CASES, "fault": FAULT_CASES, "finite": FINITE_CASES}[opts["stage"]]
    require(record["cases"] == cases, "missing, duplicate or reordered finite cases")
    require(type(record["samples"]) is int and type(record["documents"]) is int, "invalid sample/document counts")
    if opts["stage"] == "finite":
        require(record["samples"] >= 2 and record["documents"] == 6, "missing samples or finite persisted effects")
    elif opts["stage"] == "load":
        require(opts.get("profile") == LOAD_PROFILE and record["samples"] == 0 and record["documents"] > 264, "missing formal load persisted effects")
        require(all(type(record[key]) is int and record[key] == value for key, value in (("warmup_seconds", 30), ("measurement_seconds", 600), ("arrival_rpc", 200))), "formal workload policy changed")
        require(opts.get("elapsed", 0) >= 630, "premature formal completion")
    elif opts["stage"] == "fault":
        require(opts.get("profile") == FAULT_PROFILE and record["samples"] == 0 and record["documents"] == 1, "missing fault oracle/completion")
    else:
        require(record["samples"] == 0 and record["documents"] == 0, "preflight performed workload")
    name = driver_name(opts.get("profile", PROFILE), opts["stage"])
    text = opts["stdout"]
    require(text.count("=== RUN   " + name + "\n") == 1 and len(re.findall(r"^--- PASS: " + name + r" \(", text, re.M)) == 1, "selected test did not execute/pass exactly once")
    require(not re.search(r"SKIP|--- FAIL|no tests to run", text) and text.rstrip().endswith("PASS"), "skips/failures/no-test execution rejected")
    return record


def process_snapshot(pid, executable=True):
    root = Path("/proc") / str(pid)
    before = (root / "stat").read_text()
    start = int(before[before.rfind(")") + 2:].split()[19])
    exe = os.readlink(root / "exe") if executable else ""
    sha = digest(root / "exe") if executable else ""
    member = (root / "cgroup").read_text().strip()
    status = (root / "status").read_text()
    after = (root / "stat").read_text()
    require(int(after[after.rfind(")") + 2:].split()[19]) == start, "process replaced during snapshot")
    result = dict(pid=pid, start_ticks=start, executable=exe, sha256=sha, membership=member)
    ns = re.search(r"^NSpid:\s+(.+)$", status, re.M)
    result["namespace_pid"] = int(ns.group(1).split()[-1]) if ns else None
    return result


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def driver_name(profile, stage):
    if stage == "tls":
        require(profile in FORMAL_PROFILES, "TLS preparation requires formal profile")
        return "TestProductionLinuxTLS"
    if profile in FORMAL_PROFILES:
        prefix = "TestProductionLinuxLoad" if profile == LOAD_PROFILE else "TestProductionLinuxFault"
        final_stage = "load" if profile == LOAD_PROFILE else "fault"
        return {"preflight": prefix + "Preflight", "mongo": prefix + "Mongo", final_stage: prefix}[stage]
    return {"preflight": "TestProductionLinuxPreflight", "finite": "TestProductionLinuxFinite"}[stage]


class Controller:
    def __init__(self, opts):
        self.opts = opts
        self.owner = "weir-finite-" + uuid.uuid4().hex
        self.resources = []
        self.deadline = time.monotonic() + (960 if opts.profile == LOAD_PROFILE else 360 if opts.profile == FAULT_PROFILE else 240)
        self.mongo = None
        self.mongo_snapshot = None
        self.mongo_stream = None
        self.cleaning = False
        self.sequence = 0
        self.arch = ""
        self.hashes = {}
        proxy_settings = {}
        proxy_handler = urllib.request.ProxyHandler(proxy_settings)
        redirect_handler = NoRedirect()
        self.http = urllib.request.build_opener(proxy_handler, redirect_handler)
        self.docker_prefix = []
        self.cli_resource = None
        self.phase_deadline = None

    def remaining(self, deadline=None):
        active = self.cleanup_deadline if self.cleaning else self.deadline
        for boundary in (deadline, None if self.cleaning else self.phase_deadline):
            if boundary is not None:
                active = min(active, boundary)
        value = active - time.monotonic()
        require(value > 0, "absolute controller/phase deadline exceeded")
        return value

    def evidence(self, name, value):
        path = self.opts.output / name
        raw = json.dumps(value, sort_keys=True, indent=2) + "\n"
        require(len(raw.encode()) <= MAX_OUTPUT, "evidence exceeds output cap")
        path.write_text(raw)

    def run(self, args, timeout=15, deadline=None):
        self.sequence += 1
        active_deadline = self.cleanup_deadline if self.cleaning else self.deadline
        if self.phase_deadline is not None and not self.cleaning:
            active_deadline = min(active_deadline, self.phase_deadline)
        if deadline is not None:
            active_deadline = min(active_deadline, deadline)
        path = self.opts.output / ("command-%04d.log" % self.sequence)
        with tempfile.TemporaryFile() as stream:
            limit = min(timeout, active_deadline - time.monotonic())
            require(limit > 0, "controller overall deadline exceeded" if deadline is None else "command deadline exceeded")
            try:
                result = subprocess.run(args, cwd=self.opts.harness_root, stdout=stream, stderr=subprocess.STDOUT, timeout=limit, check=False)
            except subprocess.TimeoutExpired:
                stream.seek(0)
                path.write_text(sanitized(stream.read(MAX_OUTPUT)))
                raise RuntimeError("bounded command timeout; retained " + path.name) from None
            stream.seek(0)
            raw = stream.read(MAX_OUTPUT + 1)
        path.write_text(sanitized(raw[:MAX_OUTPUT]))
        require(len(raw) <= MAX_OUTPUT, "command output exceeds evidence bound")
        require(time.monotonic() < active_deadline, "command returned beyond absolute deadline")
        text = raw.decode("utf-8")
        if result.returncode:
            raise CommandError(text, result.returncode)
        return text

    def docker(self, args, timeout=15, deadline=None):
        if args[:1] in (["kill"], ["stop"]) or len(args) > 1 and args[1] in ("create", "start", "rm"):
            self.remaining(deadline)
            item = dict(monotonic=time.monotonic(), mutation=args, resources=[vars(r) for r in self.resources])
            with (self.opts.output / "lifecycle.jsonl").open("a") as stream:
                stream.write(json.dumps(item) + "\n")
            self.remaining(deadline)
        return self.run(self.docker_prefix + args, timeout, deadline=deadline)

    def inspect(self, resource, deadline=None):
        name = resource.identity or resource.name
        template = CONTAINER_FORMAT if resource.kind == "container" else NETWORK_FORMAT
        try:
            return json.loads(self.docker([resource.kind, "inspect", "--format", template, name], deadline=deadline))
        except CommandError as error:
            # Only exact not-found text is absence. Daemon failure, permission,
            # malformed output and timeout mean UNKNOWN and must fail cleanup.
            absent = {f"Error: No such {resource.kind}: {name}", f"Error response from daemon: No such {resource.kind}: {name}"}
            if resource.kind == "network":
                absent.add(f"Error response from daemon: network {name} not found")
            if error.output.strip() in absent:
                return None
            raise

    def guard(self, resource, deadline=None):
        self.remaining(deadline)
        info = self.inspect(resource, deadline=deadline)
        self.remaining(deadline)
        require(info is not None and info["Owner"] == self.owner and FULL_ID.fullmatch(info["Id"]), "resource owner/full-ID missing or changed; refuse mutation")
        require(not resource.identity or info["Id"] == resource.identity, "captured resource ID changed; refuse mutation")
        resource.identity = info["Id"]
        if resource.kind == "container":
            require(info["Name"] == "/" + resource.name, "container exact name changed")
            named = Resource("container", resource.name)
            self.remaining(deadline)
            same = self.inspect(named, deadline)
            self.remaining(deadline)
            require(same is not None and same["Id"] == resource.identity and same["Owner"] == self.owner, "container name reused")
        if resource.kind == "network":
            require(info.get("Driver") == "bridge" and info.get("Internal") is False, "owned network must be a standard bridge (non-internal)")
        return info

    def create(self, resource, args):
        # Guard is registered before create; partial success/timeout resolves the
        # unique name and owner once, then every action uses the full captured ID.
        self.resources.append(resource)
        self.evidence("registered-resources.json", [vars(r) for r in self.resources])
        identity = self.docker(args, 30).strip()
        require(FULL_ID.fullmatch(identity), "create did not return full ID")
        resource.identity = identity
        self.guard(resource)
        return resource

    def image(self, reference):
        info = json.loads(self.docker(["image", "inspect", "--format", IMAGE_FORMAT, reference]))
        require(info["OS"] == "linux" and info["Arch"] == self.arch and reference in (info["Digests"] or []) and re.fullmatch(r"sha256:[0-9a-f]{64}", info["Id"]), "cached immutable native image mismatch")
        require(info["Volumes"] is None or type(info["Volumes"]) is dict and not info["Volumes"], "image-declared anonymous volumes unsupported")
        return info

    def driver(self, stage, environment):
        started = time.monotonic()
        seconds = 800 if stage == "load" else 210 if stage == "fault" else 45 if stage == "mongo" else 22 if stage == "preflight" else 60
        end = started + min(seconds, self.remaining())
        def budget():
            value = min(end - time.monotonic(), self.remaining())
            require(value > 0, "absolute driver deadline exceeded")
            return value
        name = driver_name(self.opts.profile, stage)
        target = self.opts.output / (stage + "-completion.json")
        env = os.environ.copy()
        env.update(environment)
        env.pop("WEIR_NATIVE_TLS", None)
        if self.opts.transport == "tls":
            env["WEIR_NATIVE_TLS"] = str(self.opts.output / "tls-material")
        env.update(WEIR_FINITE_NATIVE="1", WEIR_FINITE_COMPLETION=str(target),
                   WEIR_EXECUTION_PROFILE=str(self.opts.output / "profile.json"),
                   WEIR_PROVENANCE_INPUTS=str(self.opts.output / "provenance.json"))
        if self.opts.profile in FORMAL_PROFILES:
            env.update(WEIR_NATIVE_LOAD="1", WEIR_NATIVE_LOAD_COMPLETION=str(target))
        if self.opts.profile == FAULT_PROFILE:
            env.update(WEIR_NATIVE_FAULT="1", WEIR_NATIVE_FAULT_CONTROL=str(self.opts.output))
        args = [str(self.opts.driver), "-test.run=^" + name + "$", "-test.v", "-test.count=1", "-test.timeout=" + str(seconds - 2) + "s"]
        log = self.opts.output / (stage + "-driver.log")
        budget()
        with tempfile.TemporaryFile() as stream:
            budget()
            child = subprocess.Popen(args, cwd=self.opts.harness_root, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            primary = None
            cleanup_error = None
            try:
                budget()
                snapshot = process_snapshot(child.pid)
                budget()
                require(snapshot["sha256"] == self.hashes["driver"], "actual running driver SHA mismatch")
                if self.cli_resource:
                    require(self.cli_resource.identity not in snapshot["membership"], "driver must remain outside CLI cgroup")
                self.evidence(stage + "-driver-runtime.json", snapshot)
                budget()
                if stage in ("load", "fault"):
                    next_storage = time.monotonic()
                    while True:
                        if stage == "fault":
                            self.fault_progress(child.pid)
                        if stage == "load" or time.monotonic() >= next_storage:
                            self.monitor_load_storage()
                            next_storage = time.monotonic() + 2
                        try:
                            child.wait(timeout=min(.02 if stage == "fault" else 2, budget()))
                            break
                        except subprocess.TimeoutExpired:
                            budget()
                else:
                    child.wait(timeout=budget())
                budget()
            except Exception as error:
                primary = error
            finally:
                try:
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGKILL)
                        if not hasattr(self, "cleanup_deadline"):
                            self.cleanup_deadline = time.monotonic() + 60
                        left = self.cleanup_deadline - time.monotonic()
                        require(left > 0, "owned driver cleanup deadline expired")
                        child.wait(timeout=min(3, left))
                except Exception as error:
                    cleanup_error = error
                finally:
                    stream.seek(0)
                    raw = stream.read(MAX_OUTPUT + 1)
                    log.write_text(sanitized(raw[:MAX_OUTPUT]))
            if cleanup_error is not None:
                details = dict(primary_error=str(primary), cleanup_error=str(cleanup_error))
                self.evidence(stage + "-driver-cleanup-error.json", details)
                if primary is not None:
                    raise RuntimeError(str(primary) + "; owned driver cleanup: " + str(cleanup_error)) from primary
                raise cleanup_error
            if primary is not None:
                raise primary
        budget()
        require(child.returncode == 0 and len(raw) <= MAX_OUTPUT, "driver failed/timed out; retained sanitized driver log")
        options = dict(path=target, stage=stage, revision=self.opts.revision, pid=child.pid, stdout=raw.decode("utf-8"), profile=self.opts.profile, elapsed=time.monotonic() - started)
        result = completion(options)
        budget()
        return result

    def wait_http(self, url, timeout, backend=None):
        deadline = min(self.deadline, time.monotonic() + timeout)

        def remaining():
            active = self.deadline
            if self.phase_deadline is not None:
                active = min(active, self.phase_deadline)
            budget = min(deadline, active) - time.monotonic()
            require(budget > 0, "bounded readiness timeout")
            return budget

        while True:
            remaining()
            if backend:
                info = self.guard(backend, deadline=deadline)
                remaining()
                state = info["State"]
                require(state["Running"] and not state["OOMKilled"], "backend exited during readiness: exit=" + str(state["ExitCode"]) + " oom=" + str(state["OOMKilled"]))
            try:
                opener = self.secure_http if url.startswith("https://") else self.http
                request = self.backend_request(url) if backend else url
                with opener.open(request, timeout=min(2, remaining())) as response:
                    remaining()
                    raw = response.read((64 << 10) + 1)
                    remaining()
                    require(response.status == 200 and len(raw) <= 64 << 10, "readiness response bound/status")
                remaining()
                if backend:
                    body = json.loads(raw)
                    version = body.get("version")
                    require(isinstance(version, dict), "wrong backend version identity")
                    product, number = backend_product(self.opts)
                    identity = version.get("distribution") == "opensearch" if product == "opensearch" else version.get("build_flavor") == "default" and not version.get("distribution")
                    require(body.get("cluster_name") == self.owner and version.get("number") == number and identity and version.get("build_hash"), "wrong backend cluster/version/product")
                    self.evidence("backend-identity.json", body)
                remaining()
                return
            except urllib.error.HTTPError as error:
                error.close()
                if error.code == 503 and backend and getattr(self, "tls_bootstrap_pending", False):
                    remaining()
                    return
                if error.code not in (401, 503):
                    raise
                time.sleep(min(.2, remaining()))
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(min(.2, remaining()))

    def container(self, spec):
        if spec.get("host_network"):
            require(self.opts.profile in FORMAL_PROFILES and spec["role"] == "cli" and not spec["ports"], "host networking only for owned formal CLI")
        resource = Resource("container", self.owner + "-" + spec["role"])
        mem, pids = spec["memory"], spec["pids"]
        args = ["container", "create", "--pull=never", "--platform=linux/" + self.arch, "--name", resource.name,
                "--label", "weir.owner=" + self.owner, "--network", "host" if spec.get("host_network") else spec["network"].identity,
                "--cgroupns=private", "--cpus=2", "--memory=" + str(mem), "--memory-swap=" + str(mem),
                "--pids-limit=" + str(pids), "--user=" + spec["user"], "--cap-drop=ALL",
                "--security-opt=no-new-privileges:true", "--log-driver=json-file", "--log-opt=max-size=2m", "--log-opt=max-file=1",
                "--mount", "type=bind,src=" + str(self.opts.observer) + ",dst=/observer,readonly"]
        if spec.get("readonly", True):
            args += ["--read-only"]
        for port, host in spec["ports"].items():
            args += ["--publish", "127.0.0.1:" + str(host) + ":" + str(port)]
        args += spec["extra"] + [spec["image"]] + spec.get("command", [])
        self.create(resource, args)
        self.guard(resource)
        self.docker(["container", "start", resource.identity])
        self.check_container(resource, spec)
        return resource

    def check_container(self, resource, spec):
        info = self.guard(resource)
        require(info["Image"] == spec["image_id"] and info["State"]["Running"] and not info["State"]["OOMKilled"], "running container/image identity mismatch")
        require(info["Memory"] == spec["memory"] and info["Swap"] == spec["memory"] and info["CPU"] == 2_000_000_000 and info["PIDs"] == spec["pids"], "Docker resource settings mismatch")
        require(info["Cgroupns"] == "private" and info["Readonly"] is spec.get("readonly", True) and not info["Privileged"] and not info["PidMode"] and info["User"] == spec["user"], "container isolation mismatch")
        require(info["Caps"] == ["ALL"] and any(x in ("no-new-privileges", "no-new-privileges:true") for x in info["Security"]), "container security controls missing")
        networks = info["Networks"]
        if spec.get("host_network"):
            require(self.opts.profile in FORMAL_PROFILES and spec["role"] == "cli" and not spec["ports"], "host networking only for owned formal CLI")
            require(info.get("NetworkMode") == "host" and set(networks) == {"host"}, "actual host network mode missing")
        else:
            require(info.get("NetworkMode") != "host" and len(networks) == 1 and next(iter(networks.values()))["NetworkID"] == spec["network"].identity, "foreign/shared network")
        published = {name for name, bindings in (info["Ports"] or {}).items() if bindings}
        require(published == {str(p) + "/tcp" for p in spec["ports"]}, "unexpected published ports")
        for port, host in spec["ports"].items():
            require(info["Ports"][str(port) + "/tcp"] == [dict(HostIp="127.0.0.1", HostPort=str(host))], "non-loopback/wrong port publication")
        observer = [m for m in info["Mounts"] if m["Destination"] == "/observer"]
        require(len(observer) == 1 and observer[0]["Source"] == str(self.opts.observer) and not observer[0]["RW"], "unknown/writable observer mount")
        if spec["role"] == "backend":
            if backend_product(self.opts)[0] == "opensearch":
                expected_command = OS_LOAD_COMMAND if self.opts.profile in FORMAL_PROFILES else ["opensearch"]
                require(info.get("Entrypoint") == ["./opensearch-docker-entrypoint.sh"] and info.get("Cmd") == expected_command, "backend stock entrypoint changed")
                expected_tmpfs = OS_TMPFS
            else:
                expected_command = ES_LOAD_COMMAND if self.opts.profile in FORMAL_PROFILES else ES_COMMAND
                require(info.get("Entrypoint") == ES_ENTRYPOINT and info.get("Cmd") == expected_command, "backend stock entrypoint/config staging mismatch")
                expected_tmpfs = ES_TMPFS
            if self.opts.profile in FORMAL_PROFILES:
                data_destination = "/usr/share/" + backend_product(self.opts)[0] + "/data"
                expected_tmpfs = {path: options for path, options in expected_tmpfs.items() if path != data_destination}
                data_mounts = [m for m in info["Mounts"] if m["Destination"] == data_destination]
                require(len(data_mounts) == 1 and data_mounts[0]["Source"] == str(spec["data_directory"]) and data_mounts[0]["RW"], "owned load dependency storage mismatch")
            mounts = spec.get("tls_mounts", [])
            for source, destination in mounts:
                actual = [m for m in info["Mounts"] if m["Destination"] == destination]
                require(len(actual) == 1 and actual[0]["Source"] == str(source) and not actual[0]["RW"], "new TLS fixture mount mismatch")
            require(info.get("Tmpfs") == expected_tmpfs and len(info["Mounts"]) == (2 if self.opts.profile in FORMAL_PROFILES else 1) + len(mounts), "backend bounded tmpfs/foreign mounts mismatch")
        self.evidence(spec["role"] + "-container.json", info)
        return info

    def observe(self, resource, spec, phase):
        info = self.check_container(resource, spec)
        if spec["role"] == "backend" and backend_product(self.opts)[0] == "opensearch":
            # External dependency context, not database process/JVM qualification.
            context = dict(pid=info["State"]["Pid"], image=info["Image"])
            self.evidence("backend-" + phase + "-context.json", context)
            return context
        pid = info["State"]["Pid"]
        host = process_snapshot(pid, executable=False)
        require(resource.identity in host["membership"] and host["namespace_pid"] == 1, "host PID membership/namespace does not identify owned PID1")
        require(digest(self.opts.observer) == self.hashes["observer"], "observer bytes changed")
        self.guard(resource)
        # An unprivileged runner cannot read another UID's /proc/PID/exe.
        # Establish the backend's measured identity through the exact owned ID
        # with the known helper, then bind the controls read to that identity.
        identity_raw = self.docker(["exec", "--user=" + spec["user"], resource.identity, "/observer", "--once", "--pid=1", "--identity-only"], 5)
        identity = json.loads(identity_raw)
        require(identity["observer_sha256"] == self.hashes["observer"] and identity["start_ticks"] == host["start_ticks"] and re.fullmatch(r"[0-9a-f]{64}", identity["sha256"]), "PID1 baseline/observer identity mismatch")
        if spec["role"] == "cli":
            require(identity["sha256"] == self.hashes["cli"] and identity["executable"] == "/weir", "actual CLI PID1 bytes differ")
        elif self.opts.profile == PROFILE:
            require(identity["executable"] in ("/bin/tini", "/usr/bin/tini"), "unexpected backend PID1 executable")
        self.guard(resource)
        raw = self.docker(["exec", "--user=" + spec["user"], resource.identity, "/observer", "--once", "--pid=1", "--expected-sha256=" + identity["sha256"]], 5)
        sample = json.loads(raw)
        after = process_snapshot(pid, executable=False)
        require(host == after, "target PID1 identity changed during observation")
        require(sample["pid"] == 1 and sample["start_ticks"] == host["start_ticks"] and sample["sha256"] == identity["sha256"] and sample["executable"] == identity["executable"] and sample["observer_sha256"] == self.hashes["observer"], "observed process/known-helper identity mismatch")
        require(sample["membership"] == "0::/" and sample["mount_root"] == "/", "invalid observer cgroup mapping")
        cpu = sample["cpu_max"].split()
        require(len(cpu) == 2 and int(cpu[0]) == 2 * int(cpu[1]) and int(cpu[1]) > 0, "actual two-CPU quota missing")
        require(sample["memory_max"] == spec["memory"] and sample["memory_swap_max"] == 0 and sample["pids_max"] == spec["pids"], "actual finite memory/swap/PID controls mismatch")
        for group, names in (("cpu", ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec")), ("memory_events", ("low", "high", "max", "oom", "oom_kill"))):
            require(all(type(sample[group].get(n)) is int and sample[group][n] >= 0 for n in names), "missing/malformed observed counters")
        require(sample["memory_events"]["oom"] == 0 and sample["memory_events"]["oom_kill"] == 0 and 0 < sample["memory_current"] <= sample["memory_peak"] <= spec["memory"], "OOM/unbounded memory observation")
        self.evidence(spec["role"] + "-" + phase + "-controls.json", sample)
        self.evidence(spec["role"] + "-" + phase + "-host-process.json", host)
        self.guard(resource)
        return sample

    def prepare_load_storage(self):
        self.storage_paths = [self.opts.output / "es-data", self.opts.output / "mongo-data"]
        if self.opts.transport == "tls":
            self.storage_paths.append(self.opts.output / "tls-material")
        self.storage_owner = dict(owner=self.owner, paths=[])
        for path in self.storage_paths:
            path.mkdir(mode=0o700)
            info = path.stat()
            identity = dict(path=str(path), device=info.st_dev, inode=info.st_ino)
            self.storage_owner["paths"].append(identity)
        # Inherit the runner group and group-writable descendants so the runner
        # can remove its own plain dependency data after the container is gone.
        self.storage_paths[0].chmod(0o2777)
        if self.opts.transport == "tls":
            self.storage_paths[2].chmod(0o755)
        self.evidence("load-storage-owner.json", self.storage_owner)

    def guard_load_storage(self):
        marker = self.opts.output / "load-storage-owner.json"
        require(not marker.is_symlink() and marker.stat().st_size <= 4096 and json.loads(marker.read_text()) == self.storage_owner, "load storage owner marker changed; preserve data")
        for path, identity in zip(self.storage_paths, self.storage_owner["paths"]):
            require(path.parent == self.opts.output and path.resolve() == path and path.is_dir(), "load storage path escaped/changed; preserve data")
            info = path.stat()
            require((info.st_dev, info.st_ino) == (identity["device"], identity["inode"]), "owned load storage inode changed; preserve data")

    def monitor_load_storage(self):
        self.guard_load_storage()
        end = time.monotonic() + min(3, self.remaining())
        def walk_error(error):
            missing = Path(error.filename) if error.filename else None
            if isinstance(error, FileNotFoundError) and missing is not None and any(missing != root and missing.is_relative_to(root) for root in self.storage_paths):
                return
            raise error

        sizes = {path.name: None for path in self.storage_paths}
        self.storage_last_sample = dict(bytes=sizes, free_bytes=None, per_dependency_abort_bytes=LOAD_DATA_LIMIT)
        for path in self.storage_paths:
            total, entries = 0, 0
            for root, directories, files in os.walk(path, onerror=walk_error):
                for name in directories + files:
                    self.remaining(end)
                    entry = Path(root) / name
                    try:
                        info = entry.lstat()
                    except FileNotFoundError:
                        # Compaction/retirement may remove an enumerated entry.
                        continue
                    require(not stat.S_ISLNK(info.st_mode), "load data contains symlink; refuse traversal")
                    entries += 1
                    total += max(info.st_size, info.st_blocks * 512)
                    sizes[path.name] = total
                    require(entries <= 100000 and total <= LOAD_DATA_LIMIT, "load dependency data exceeded2GiB monitored abort threshold")
            sizes[path.name] = total
        free = shutil.disk_usage(self.opts.output).free
        require(free >= LOAD_DISK_FREE, "load storage free space fell below2GiB abort threshold")
        log = self.opts.output / "mongo-process.log"
        require(not log.exists() or log.stat().st_size <= 64 << 20, "native Mongo log exceeded64MiB abort threshold")
        sample = dict(bytes=sizes, free_bytes=free, per_dependency_abort_bytes=LOAD_DATA_LIMIT)
        self.storage_last_sample = sample
        self.evidence("load-storage.json", sample)

    def cleanup_load_storage(self):
        if not hasattr(self, "storage_paths"):
            return
        self.remaining()
        self.guard_load_storage()

        def remove_directory(parent, name, identity=None):
            self.remaining()
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            descriptor = os.open(name, flags, dir_fd=parent)
            try:
                self.remaining()
                if identity is not None:
                    actual = os.fstat(descriptor)
                    require((actual.st_dev, actual.st_ino) == (identity["device"], identity["inode"]), "owned load storage inode changed before removal")
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        self.remaining()
                        info = entry.stat(follow_symlinks=False)
                        self.remaining()
                        require(not stat.S_ISLNK(info.st_mode), "owned load data contains symlink; preserve data")
                        if stat.S_ISDIR(info.st_mode):
                            remove_directory(descriptor, entry.name)
                        else:
                            os.unlink(entry.name, dir_fd=descriptor)
                        self.remaining()
            finally:
                os.close(descriptor)
            self.remaining()
            os.rmdir(name, dir_fd=parent)
            self.remaining()

        # FD-relative, no-follow removal is checked against the same independent
        # cleanup deadline before/after each entry. A late syscall is not success.
        self.remaining()
        parent = os.open(self.opts.output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for path, identity in zip(self.storage_paths, self.storage_owner["paths"]):
                remove_directory(parent, path.name, identity)
                require(not path.exists(), "owned load data remains")
                self.remaining()
        finally:
            os.close(parent)
        receipt = dict(owner=self.owner, removed=[str(path) for path in self.storage_paths])
        self.evidence("load-storage-cleanup.json", receipt)
        self.remaining()

    def mongo_guard(self, verify_bytes=False):
        require(self.mongo is not None and self.mongo.poll() is None, "owned native Mongo exited")
        snapshot = process_snapshot(self.mongo.pid, executable=False)
        if self.mongo_snapshot is not None:
            require(snapshot == self.mongo_snapshot, "native Mongo process identity changed")
        actual = os.stat("/proc/" + str(self.mongo.pid) + "/exe")
        expected = self.opts.mongod.stat()
        require((actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino) and actual.st_size == MONGO_BYTES, "native Mongo executable inode changed")
        self.mongo_snapshot = snapshot
        if verify_bytes:
            with open("/proc/" + str(self.mongo.pid) + "/exe", "rb") as stream:
                require(hashlib.file_digest(stream, "sha256").hexdigest() == MONGO_SHA256, "actual native Mongo bytes differ from pin")
            require(process_snapshot(self.mongo.pid, executable=False) == snapshot, "native Mongo identity changed during hash")
        if self.cli_resource:
            require(self.cli_resource.identity not in snapshot["membership"], "Mongo dependency charged to CLI")
        return snapshot

    def start_mongo(self):
        self.guard_load_storage()
        directory = self.storage_paths[1]
        self.mongo_port = free_port()
        self.mongo_uri = "mongodb://127.0.0.1:" + str(self.mongo_port) + "/?replicaSet=" + self.owner
        with self.opts.mongod.open("rb") as stream:
            require(hashlib.file_digest(stream, "sha256").hexdigest() == MONGO_SHA256, "native Mongo bytes changed before start")
        args = [str(self.opts.mongod), "--dbpath", str(directory), "--bind_ip", "127.0.0.1", "--port", str(self.mongo_port), "--replSet", self.owner]
        if self.opts.transport == "tls":
            material = self.opts.output / "tls-material"
            self.mongo_uri += "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
            args += ["--auth", "--keyFile", str(material / "mongo-cluster"), "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", str(material / "mongo.pem"), "--tlsCAFile", str(material / "ca.pem"), "--tlsAllowConnectionsWithoutCertificates"]
        self.mongo_stream = (self.opts.output / "mongo-process.log").open("wb")
        environment = dict(PATH="/usr/bin:/bin", LANG="C", HOME=str(directory), TMPDIR=str(directory))
        self.mongo = subprocess.Popen(args, cwd=directory, env=environment, stdout=self.mongo_stream, stderr=subprocess.STDOUT, start_new_session=True)
        snapshot = self.mongo_guard(verify_bytes=True)
        process_evidence = dict(process=snapshot, sha256=MONGO_SHA256, argv=args, outside_cli_quota=True)
        self.evidence("mongo-process.json", process_evidence)
        end = time.monotonic() + min(30, self.remaining())
        while True:
            self.mongo_guard()
            try:
                with socket.create_connection(("127.0.0.1", self.mongo_port), timeout=min(1, self.remaining(end))):
                    break
            except (ConnectionError, TimeoutError):
                time.sleep(min(.1, self.remaining(end)))
        environment = dict(WEIR_NATIVE_LOAD_MONGO=self.mongo_uri, WEIR_NATIVE_LOAD_OWNER=self.owner)
        self.driver("mongo", environment)

    def cleanup_mongo(self):
        if self.mongo is None:
            return
        try:
            if self.mongo.poll() is None:
                self.mongo_guard()
                self.mongo.terminate()
                try:
                    self.mongo.wait(timeout=min(15, self.remaining()))
                except subprocess.TimeoutExpired:
                    self.mongo_guard()
                    self.mongo.kill()
                    self.mongo.wait(timeout=min(3, self.remaining()))
                    raise RuntimeError("native Mongo required forced retirement")
            require(self.mongo.returncode == 0 and not Path("/proc/" + str(self.mongo.pid)).exists(), "native Mongo exit/absence unverified")
            cleanup_evidence = dict(pid=self.mongo.pid, exit_code=self.mongo.returncode, absent=True)
            self.evidence("mongo-cleanup.json", cleanup_evidence)
        finally:
            if self.mongo_stream:
                self.mongo_stream.close()

    def fault_message(self, name, pid):
        path = self.opts.output / name
        if not path.exists():
            return None
        require(not path.is_symlink() and path.stat().st_size <= 4096, "bounded owned fault message required")
        message = json.loads(path.read_text(), object_pairs_hook=unique_fields)
        require(set(message) == {"owner", "driver_pid", "mongo", "search", "pid", "start_ticks", "exit", "deadline_ns"} and message["owner"] == self.owner and type(message["driver_pid"]) is int and message["driver_pid"] == pid, "fault message identity/schema mismatch")
        return message

    def fault_publish(self, name, message):
        path = self.opts.output / name
        require(not path.exists(), "fault message already published")
        temporary = path.with_suffix(".tmp")
        with temporary.open("x") as stream:
            json.dump(message, stream)
        temporary.replace(path)

    def fault_progress(self, pid):
        if self.cli_resource is None:
            message = self.fault_message("proxies.json", pid)
            if message is None:
                return
            mongo = urllib.parse.urlsplit(message["mongo"])
            search = urllib.parse.urlsplit(message["search"])
            for endpoint in (mongo, search):
                require(endpoint.hostname == "127.0.0.1" and endpoint.port and not endpoint.username and not endpoint.password and not endpoint.fragment, "fault proxies must be exact owned loopback")
            expected_query = {"directConnection": ["true"], "serverMonitoringMode": ["poll"]}
            if self.opts.transport == "tls":
                expected_query.update(tls=["true"], authSource=["admin"], authMechanism=["SCRAM-SHA-256"])
            scheme = "https" if self.opts.transport == "tls" else "http"
            require(mongo.scheme == "mongodb" and mongo.path == "/" and urllib.parse.parse_qs(mongo.query, strict_parsing=True) == expected_query and search.scheme == scheme and not search.path and not search.query, "wrong proxy protocol/topology")
            # These listeners must belong to the measured driver process, never an
            # unrelated loopback service. Bind the selected ports to its socket FDs.
            inodes = set()
            for descriptor in Path("/proc", str(pid), "fd").iterdir():
                try:
                    target = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                if target.startswith("socket:["):
                    inodes.add(target[8:-1])
            listeners = set()
            for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
                fields = line.split()
                if fields[3] == "0A" and fields[9] in inodes:
                    listeners.add(fields[1])
            require(all("0100007F:%04X" % endpoint.port in listeners for endpoint in (mongo, search)), "proxy listeners not owned by actual driver")
            routing = 'stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: "%s"\n  - name: search\n    backend:\n      search:\n        url: "%s"\n' % (message["mongo"], message["search"])
            if self.opts.transport == "tls":
                routing = self.tls_routing(message["mongo"], message["search"])
            getattr(self, "routing_path", self.opts.output / "routes.yaml").write_text(routing)
            cli = self.container(self.fault_cli_spec)
            self.cli_resource = cli
            driver = process_snapshot(pid)
            require(driver["sha256"] == self.hashes["driver"] and cli.identity not in driver["membership"], "fault driver/proxies charged to CLI or replaced")
            self.wait_http(self.fault_diagnostics + "/readyz", 20)
            self.fault_before = self.observe(cli, self.fault_cli_spec, "before")
            identity = process_snapshot(self.guard(cli)["State"]["Pid"], executable=False)
            self.fault_identity = identity
            reply = dict(message, pid=identity["pid"], start_ticks=str(identity["start_ticks"]))
            self.fault_publish("running.json", reply)
        elif not hasattr(self, "fault_observed"):
            message = self.fault_message("observe.json", pid)
            if message is None:
                return
            preparation_deadline = time.monotonic() + min(8, self.remaining())
            self.phase_deadline = preparation_deadline
            try:
                after = self.observe(self.cli_resource, self.fault_cli_spec, "after")
                require(after["start_ticks"] == self.fault_before["start_ticks"] and after["sha256"] == self.fault_before["sha256"], "fault CLI replaced")
                for group in ("cpu", "memory_events"):
                    require(all(after[group][key] >= value for key, value in self.fault_before[group].items()), "fault controls regressed")
                self.remaining(preparation_deadline)
                self.fault_publish("observed.json", message)
                self.fault_observed = True
            finally:
                self.phase_deadline = None
        elif not hasattr(self, "fault_stopped"):
            message = self.fault_message("shutdown.json", pid)
            if message is None:
                return
            def signaled(deadline_ns):
                message["deadline_ns"] = deadline_ns
                self.fault_publish("signaled.json", message)
            self.shutdown_cli(self.cli_resource, signaled)
            self.fault_publish("stopped.json", message)
            self.fault_stopped = True

    def shutdown_cli(self, resource, on_signal=None):
        self.guard(resource)
        started = time.monotonic()
        deadline_ns = time.time_ns() + 8_000_000_000
        end = started + min(8, self.remaining())
        self.docker(["kill", "--signal=TERM", resource.identity], 2, end)
        if on_signal is not None:
            on_signal(deadline_ns)
        self.guard(resource, end)
        self.docker(["wait", resource.identity], self.remaining(end), end)
        info = self.guard(resource)
        elapsed = time.monotonic() - started
        require(elapsed <= 8 and not info["State"]["Running"] and info["State"]["ExitCode"] == 0 and not info["State"]["OOMKilled"], "CLI SIGTERM/exit/OOM bound failed")
        receipt = dict(elapsed_seconds=elapsed, exit_code=0, oom=False)
        self.evidence("cli-shutdown.json", receipt)

    def cleanup(self):
        self.cleaning = True
        self.phase_deadline = None
        if not hasattr(self, "cleanup_deadline"):
            self.cleanup_deadline = time.monotonic() + 60
        errors, receipts = [], []
        # Retire/reap the owned native child before slow Docker calls can consume
        # the shared 60s cleanup budget. Never extend that independent deadline.
        try:
            self.cleanup_mongo()
        except Exception as error:
            errors.append("native Mongo: " + str(error))
        all_absent = True
        for resource in reversed(self.resources):
            receipt = dict(kind=resource.kind, name=resource.name, id=resource.identity, absent=False)
            receipts.append(receipt)
            try:
                info = self.inspect(resource)
                if info is None:
                    # A known ID cannot make a changed/reused unique name absent.
                    if resource.identity:
                        by_name = Resource(resource.kind, resource.name)
                        require(self.inspect(by_name) is None, "resource name reused; absence uncertain")
                    receipt["absent"] = True
                    continue
                self.guard(resource)
                if resource.kind == "container":
                    try:
                        self.docker(["logs", "--tail=200", resource.identity], 5)
                    except Exception as error:
                        errors.append(resource.name + " logs: " + str(error))
                    info = self.guard(resource)
                    if info["State"]["Running"]:
                        end = time.monotonic() + min(8, self.remaining())
                        self.guard(resource, end)
                        self.docker(["kill", "--signal=TERM", resource.identity], 2, end)
                        self.guard(resource, end)
                        self.docker(["wait", resource.identity], self.remaining(end), end)
                    terminal = self.guard(resource)["State"]
                    require(not terminal["Running"], "container still running; refuse removal")
                    receipt["exit_code"] = terminal["ExitCode"]
                    receipt["oom"] = terminal["OOMKilled"]
                    self.guard(resource)
                    self.docker(["container", "rm", resource.identity], 5)
                else:
                    require(all_absent, "container absence unverified; preserve network")
                    require(not info["Containers"], "owned standard bridge is not empty")
                    self.guard(resource)
                    self.docker(["network", "rm", resource.identity], 5)
                require(self.inspect(resource) is None, "resource remains after removal")
                by_name = Resource(resource.kind, resource.name)
                require(self.inspect(by_name) is None, "resource name reused after removal")
                receipt["id"] = resource.identity
                receipt["absent"] = True
            except Exception as error:
                errors.append(resource.name + ": " + str(error))
                all_absent = False
        if hasattr(self, "storage_paths"):
            try:
                require(all_absent and (self.mongo is None or self.mongo.poll() is not None), "load processes/resources not fully joined; preserve data")
                self.cleanup_load_storage()
            except Exception as error:
                errors.append("load data cleanup: " + str(error))
                retained = dict(owner=self.owner, paths=[str(path) for path in self.storage_paths if path.exists()], last_observed=self.storage_last_sample if hasattr(self, "storage_last_sample") else None, cleanup_complete=False)
                self.evidence("load-storage-retained.json", retained)
        receipt = dict(resources=receipts, errors=errors)
        self.evidence("cleanup.json", receipt)
        try:
            self.remaining()
        except Exception as error:
            errors.append("cleanup deadline: " + str(error))
        return errors

    def execute(self):
        self.arch = native_preflight(self.opts)
        self.opts.output.mkdir(mode=0o755)  # new, task-owned directory only
        self.hashes = {role: digest(getattr(self.opts, role)) for role in ("cli", "driver", "observer")}
        self.evidence("executable-digests.json", self.hashes)
        config = self.opts.output / "empty-docker-config"
        config.mkdir(mode=0o700)
        self.docker_prefix = ["docker", "--config", str(config), "--host=unix:///var/run/docker.sock"]
        primary = None
        try:
            self.prepare_inputs()
            # Strict merged Go source/CLI/actual-loaded-driver validation finishes
            # structurally BEFORE any network, backend or CLI creation.
            environment = {}
            self.driver("preflight", environment)
            self.run_fixture()
        except Exception as error:
            primary = str(error)
        finally:
            cleanup = self.cleanup()
            if time.monotonic() >= self.cleanup_deadline:
                cleanup.append("cleanup deadline exceeded before result acceptance")
            complete = primary is None and not cleanup
            result = dict(profile=self.opts.profile, qualification=False, revision=self.opts.revision, arch=self.arch, primary_error=primary, cleanup_errors=cleanup)
            stage = "fault" if self.opts.profile == FAULT_PROFILE else "load" if self.opts.profile == LOAD_PROFILE else "finite"
            completion_key = stage + "_complete"
            result_path = stage + "-result.json"
            result[completion_key] = complete
            self.evidence(result_path, result)
            if time.monotonic() >= self.cleanup_deadline and complete:
                cleanup.append("cleanup deadline exceeded during result acceptance")
                result[completion_key] = False
                self.evidence(result_path, result)
        require(primary is None and not cleanup, "finite execution failed: " + str(primary) + "; cleanup=" + str(cleanup))

    def prepare_inputs(self):
        info_format = '{"OS":{{json .OSType}},"Arch":{{json .Architecture}},"Cgroup":{{json .CgroupVersion}},"CPUs":{{json .NCPU}},"Memory":{{json .MemTotal}},"Root":{{json .DockerRootDir}},"Kernel":{{json .KernelVersion}}}'
        info = json.loads(self.docker(["info", "--format", info_format]))
        aliases = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}
        require(info["OS"] == "linux" and aliases.get(info["Arch"]) == self.arch and info["Cgroup"] == "2" and info["Kernel"] == platform.release() and info["CPUs"] >= 2 and info["Memory"] >= 4 << 30, "local native Docker/cgroupv2/resources required")
        required_disk = LOAD_DISK_ADMISSION if self.opts.profile in FORMAL_PROFILES else 2 << 30
        require(shutil.disk_usage(info["Root"]).free >= required_disk, "insufficient Docker data free space")
        self.evidence("engine.json", info)
        target = dict(os="linux", arch=self.arch)
        roles = dict(driver="owned-local", cli="owned-local")
        product, version = backend_product(self.opts)
        backend = dict(product=product, version=version, transport=self.opts.transport, endpoints=roles)
        backends = [backend]
        if self.opts.profile in FORMAL_PROFILES:
            mongo = dict(product="mongodb", version="8.0.32", transport=self.opts.transport, endpoints=roles)
            backends.insert(0, mongo)
        profile = dict(schemaVersion=1, id=self.opts.profile, suite="fault" if self.opts.profile == FAULT_PROFILE else "load" if self.opts.profile == LOAD_PROFILE else "functional", intent="formal" if self.opts.profile in FORMAL_PROFILES else "finite", policy="production-v1", executor="linux-native-v1", driver=target, cli=target, backends=backends)
        self.evidence("profile.json", profile)
        server = dict(root=str(self.opts.server_root), revision=self.opts.revision)
        harness = dict(root=str(self.opts.harness_root), revision=self.opts.revision)
        cli = dict(role="cli", path=str(self.opts.cli), sha256=self.hashes["cli"])
        driver = dict(role="driver", path=str(self.opts.driver), sha256=self.hashes["driver"])
        provenance = dict(schemaVersion=1, profileID=self.opts.profile, server=server, harness=harness, cli=cli, driver=driver)
        self.evidence("provenance.json", provenance)
        base = json.loads((self.opts.harness_root / "deploy/docker/base.json").read_text())
        self.base_ref = base["image"] + "@" + base["platforms"][self.arch]
        self.es_ref = (OS_REPOSITORY + "@" + OS_PINS[self.arch]) if product == "opensearch" else ES_REPOSITORY + "@" + ES_PINS[self.arch]
        self.base_image, self.es_image = self.image(self.base_ref), self.image(self.es_ref)
        images = dict(base=self.base_image, backend=self.es_image, backend_reference=self.es_ref, base_reference=self.base_ref)
        self.evidence("images.json", images)

    def prepare_tls(self):
        environment = dict(WEIR_NATIVE_LOAD_OWNER=self.owner)
        self.driver("tls", environment)
        self.guard_load_storage()
        material = self.opts.output / "tls-material"
        state_path = material / "state.json"
        require(not state_path.is_symlink() and state_path.stat().st_size <= 4096, "bounded new TLS metadata required")
        self.tls_state = json.loads(state_path.read_text(), object_pairs_hook=unique_fields)
        require(set(self.tls_state) == {"username", "password", "mongo_username", "mongo_password"} and self.tls_state["mongo_username"] == "weir_native" and re.fullmatch("[0-9a-f]{48}", self.tls_state["mongo_password"]) and self.tls_state["username"] == "weir_admin" and re.fullmatch("[0-9a-f]{48}", self.tls_state["password"]), "invalid generated TLS fixture metadata")
        context = ssl.create_default_context(cafile=str(material / "ca.pem"))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        proxy_settings = {}
        proxy = urllib.request.ProxyHandler(proxy_settings)
        handlers = [proxy, NoRedirect(), urllib.request.HTTPSHandler(context=context)]
        self.secure_http = urllib.request.build_opener(*handlers)

    def tls_backend_spec(self, spec):
        product, _ = backend_product(self.opts)
        material = self.opts.output / "tls-material"
        base = "/usr/share/" + product + "/config"
        extra = [arg for arg in spec["extra"] if arg not in ("--env=xpack.security.enabled=false", "--env=DISABLE_SECURITY_PLUGIN=true")]
        mounts = [(material, base + "/weir"), (material / (product + ".yml"), base + "/" + product + ".yml")]
        if product == "elasticsearch":
            mounts += [(material / name, base + "/" + name) for name in ("users", "users_roles", "roles.yml")]
        for source, destination in mounts:
            extra += ["--mount", "type=bind,src=" + str(source) + ",dst=" + destination + ",readonly"]
        spec.update(extra=extra, tls_mounts=mounts)

    def tls_routing(self, mongo, search):
        trust = '      tls:\n        ca_file: "/fixture-ca.pem"\n'
        def authentication(username, password):
            return '      authentication:\n        username: "%s"\n        password: "%s"\n' % (username, password)
        return ('stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: "%s"\n' % mongo + trust +
                authentication(self.tls_state["mongo_username"], self.tls_state["mongo_password"]) +
                '  - name: search\n    backend:\n      search:\n        url: "%s"\n' % search + trust +
                authentication(self.tls_state["username"], self.tls_state["password"]))

    def backend_request(self, url):
        request = urllib.request.Request(url)
        if self.opts.transport == "tls":
            import base64
            pair = self.tls_state["username"] + ":" + self.tls_state["password"]
            request.add_header("Authorization", "Basic " + base64.b64encode(pair.encode()).decode())
        return request

    def run_fixture(self):
        if self.opts.profile in FORMAL_PROFILES:
            self.prepare_load_storage()
        if self.opts.transport == "tls":
            self.prepare_tls()
        network = Resource("network", self.owner)
        # Standard owned bridge permits the required loopback publications;
        # it does not provide outbound network isolation.
        self.create(network, ["network", "create", "--driver=bridge", "--label", "weir.owner=" + self.owner, network.name])
        self.guard(network)
        oracle_port, app_port, diag_port = free_port(), free_port(), free_port()
        require(len({oracle_port, app_port, diag_port}) == 3, "ephemeral port collision; retry explicitly with fresh invocation")
        es_extra = ["--tmpfs=" + path + ":" + options for path, options in ES_TMPFS.items()]
        es_extra += ["--env=cluster.name=" + self.owner, "--env=discovery.type=single-node", "--env=xpack.security.enabled=false", "--env=action.auto_create_index=false", "--env=ES_JAVA_OPTS=-Xms512m -Xmx512m"]
        es_ports = {9200: oracle_port}
        es_spec = dict(role="backend", memory=1536 << 20, pids=512, user="1000:0", network=network, ports=es_ports, extra=es_extra, image=self.es_ref, image_id=self.es_image["Id"], command=ES_COMMAND)
        if self.opts.profile in FORMAL_PROFILES:
            self.guard_load_storage()
            data = self.storage_paths[0]
            es_extra = [arg for arg in es_extra if not arg.startswith("--tmpfs=/usr/share/elasticsearch/data:")]
            es_extra += ["--mount", "type=bind,src=" + str(data) + ",dst=/usr/share/elasticsearch/data"]
            es_spec.update(extra=es_extra, data_directory=data, command=ES_LOAD_COMMAND)
        if backend_product(self.opts)[0] == "opensearch":
            extra = ["--tmpfs=" + path + ":" + options for path, options in OS_TMPFS.items()]
            extra += ["--env=DISABLE_SECURITY_PLUGIN=true", "--env=DISABLE_INSTALL_DEMO_CONFIG=true",
                      "--env=cluster.name=" + self.owner, "--env=discovery.type=single-node", "--env=action.auto_create_index=false",
                      "--env=OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"]
            es_spec.update(user="1000:1000", extra=extra, command=[], readonly=False)
            if self.opts.profile in FORMAL_PROFILES:
                extra = [arg for arg in extra if not arg.startswith("--tmpfs=/usr/share/opensearch/data:")]
                extra += ["--mount", "type=bind,src=" + str(data) + ",dst=/usr/share/opensearch/data"]
                es_spec.update(extra=extra, command=OS_LOAD_COMMAND)
        if self.opts.transport == "tls":
            self.tls_backend_spec(es_spec)
        self.phase_deadline = min(self.deadline, time.monotonic() + 90)
        backend = self.container(es_spec)
        oracle = ("https" if self.opts.transport == "tls" else "http") + "://127.0.0.1:" + str(oracle_port)
        self.tls_bootstrap_pending = self.opts.transport == "tls" and backend_product(self.opts)[0] == "opensearch"
        self.wait_http(oracle + "/", 90, backend=backend)
        if self.tls_bootstrap_pending:
            self.guard(backend)
            base = "/usr/share/opensearch/config/weir/"
            self.docker(["exec", "--user=1000:1000", backend.identity,
                         "/usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh",
                         "-h", "127.0.0.1", "-p", "9200", "-cn", self.owner,
                         "-cacert", base + "ca.pem", "-cert", base + "admin.pem", "-key", base + "admin.key", "-cd", base + "security/"], 45)
            self.tls_bootstrap_pending = False
            self.wait_http(oracle + "/", 20, backend=backend)
        backend_before = self.observe(backend, es_spec, "before")
        self.phase_deadline = None
        if self.opts.profile in FORMAL_PROFILES:
            self.start_mongo()
        basic = 'listeners:\n  application: "0.0.0.0:7447"\ndiagnostics:\n  address: "0.0.0.0:7449"\n  allow_intranet: true\ndiscovery:\n  advertise: ["127.0.0.1:%d"]\n' % app_port
        routing = 'stores:\n  - name: search\n    backend:\n      search:\n        url: "http://%s:9200"\n' % backend.name
        if self.opts.profile in FORMAL_PROFILES:
            basic = 'listeners:\n  application: "127.0.0.1:%d"\ndiagnostics:\n  address: "127.0.0.1:%d"\ndiscovery:\n  advertise: ["127.0.0.1:%d"]\n' % (app_port, diag_port, app_port)
            routing = 'stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: "%s"\n  - name: search\n    backend:\n      search:\n        url: "%s"\n' % (self.mongo_uri, oracle)
        basic_path, routing_path = self.opts.output / "weir.yaml", self.opts.output / "routes.yaml"
        if self.opts.transport == "tls":
            routing = self.tls_routing(self.mongo_uri, oracle)
            routing_path = self.opts.output / "tls-material" / "routes.yaml"
        self.routing_path = routing_path
        basic_path.write_text(basic)
        routing_path.write_text(routing)
        # Only these newly-created, credential-free fixture files are exposed
        # read-only to UID65532, independently of the invoking shell's umask.
        basic_path.chmod(0o644)
        routing_path.chmod(0o644)
        cli_extra = ["--tmpfs=/tmp:rw,noexec,nosuid,size=16777216,uid=65532,gid=65532", "--entrypoint=/weir"]
        if self.opts.transport == "tls":
            cli_extra += ["--mount", "type=bind,src=" + str(self.opts.output / "tls-material" / "ca.pem") + ",dst=/fixture-ca.pem,readonly"]
        for source, destination in ((self.opts.cli, "/weir"), (basic_path, "/weir.yaml"), (routing_path, "/routes.yaml")):
            cli_extra += ["--mount", "type=bind,src=" + str(source) + ",dst=" + destination + ",readonly"]
        cli_ports = {7447: app_port, 7449: diag_port}
        cli_spec = dict(role="cli", memory=1 << 30, pids=96, user="65532:65532", network=network, ports=cli_ports, extra=cli_extra, image=self.base_ref, image_id=self.base_image["Id"], command=["serve", "--config=/weir.yaml", "--routes=/routes.yaml"])
        if self.opts.profile in FORMAL_PROFILES:
            cli_spec.update(host_network=True, ports={})
        if self.opts.profile == FAULT_PROFILE:
            cli_spec.update(memory=512 << 20)
            self.fault_cli_spec = cli_spec
            self.fault_diagnostics = "http://127.0.0.1:" + str(diag_port)
            environment = dict(WEIR_NATIVE_LOAD_APP="127.0.0.1:" + str(app_port), WEIR_NATIVE_LOAD_DIAGNOSTICS="127.0.0.1:" + str(diag_port), WEIR_NATIVE_LOAD_ORACLE=oracle, WEIR_NATIVE_LOAD_OWNER=self.owner, WEIR_NATIVE_LOAD_MONGO=self.mongo_uri)
            self.driver("fault", environment)
            require(getattr(self, "fault_stopped", False), "fault shutdown did not execute")
            self.mongo_guard(verify_bytes=True)
            self.observe(backend, es_spec, "after")
            return
        cli = self.container(cli_spec)
        self.cli_resource = cli
        self.wait_http("http://127.0.0.1:" + str(diag_port) + "/readyz", 20)
        before = self.observe(cli, cli_spec, "before")
        environment = dict(WEIR_FINITE_APP="127.0.0.1:" + str(app_port), WEIR_FINITE_DIAGNOSTICS="127.0.0.1:" + str(diag_port), WEIR_FINITE_ORACLE=oracle, WEIR_FINITE_OWNER=self.owner)
        if self.opts.profile in FORMAL_PROFILES:
            self.mongo_guard()
            environment = dict(WEIR_NATIVE_LOAD_APP="127.0.0.1:" + str(app_port), WEIR_NATIVE_LOAD_DIAGNOSTICS="127.0.0.1:" + str(diag_port), WEIR_NATIVE_LOAD_ORACLE=oracle, WEIR_NATIVE_LOAD_OWNER=self.owner, WEIR_NATIVE_LOAD_MONGO=self.mongo_uri)
            self.driver("load", environment)
            self.mongo_guard(verify_bytes=True)
        else:
            self.driver("finite", environment)
        after = self.observe(cli, cli_spec, "after")
        for group in ("cpu", "memory_events"):
            require(all(after[group][k] >= v for k, v in before[group].items()), "cgroup counters regressed/replaced")
        backend_after = self.observe(backend, es_spec, "after")
        require(before["start_ticks"] == after["start_ticks"] and before["sha256"] == after["sha256"], "CLI PID1 replaced between observations")
        if self.opts.profile == PROFILE:
            require(backend_before["start_ticks"] == backend_after["start_ticks"] and backend_before["sha256"] == backend_after["sha256"], "backend PID1 replaced between observations")
        self.shutdown_cli(cli)


class NoRedirect(urllib.request.HTTPErrorProcessor):
    def http_response(self, request, response):
        require(not 300 <= response.code < 400, "fixture redirects forbidden")
        return super().http_response(request, response)

    https_response = http_response


class CommandError(RuntimeError):
    def __init__(self, output, code):
        super().__init__("command failed exit=" + str(code))
        self.output = output


def interrupted(signum, frame):
    raise RuntimeError("controller interrupted by signal=" + str(signum))


def main():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("server-root", "harness-root", "revision", "cli", "driver", "observer", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--profile", required=True, choices=tuple(PROFILES))
    parser.add_argument("--mongod")
    parser.add_argument("--product", choices=("elasticsearch", "opensearch"), default="elasticsearch", help="exact formal search product; finite profiles select their own product")
    parser.add_argument("--transport", choices=("plain", "tls"), default="plain", help="backend and fault relay transport; application listener unchanged")
    args = parser.parse_args()
    opts = Options(server_root=Path(args.server_root), harness_root=Path(args.harness_root), revision=args.revision, cli=Path(args.cli), driver=Path(args.driver), observer=Path(args.observer), output=Path(args.output), profile=args.profile, mongod=Path(args.mongod) if args.mongod else None, product=args.product, transport=args.transport)
    require(opts.output.is_absolute() and opts.output.parent.resolve() == opts.output.parent and not opts.output.exists(), "new canonical task-owned evidence directory required")
    controller = Controller(opts)
    controller.execute()


if __name__ == "__main__":
    main()
