#!/usr/bin/env python3
"""Owned native Linux search/plain finite correctness; never qualification/promotion.

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
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

PROFILE = "linux-native-es-functional-v1"
OS_PROFILE = "linux-native-opensearch-functional-v1"
PROFILES = {PROFILE: ("elasticsearch", "8.19.22"), OS_PROFILE: ("opensearch", "3.3.0")}
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
    require(opts.profile in PROFILES, "exact finite profile required")
    # No Docker command or fixture action is reachable on unsupported hosts.
    require(platform.system() == "Linux", "native Linux required; Darwin/Desktop unsupported")
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    require(arch in ES_PINS, "unsupported native host architecture")
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
    require(shutil.disk_usage(opts.output.parent).free >= 2 << 30, "at least2GiB free disposable storage required")
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
    require(set(record) == expected_keys, "missing/unknown completion fields")
    require(type(record["schema"]) is int and record["schema"] == 1 and record["stage"] == opts["stage"] and record["profile"] == opts.get("profile", PROFILE) and record["revision"] == opts["revision"], "completion identity mismatch")
    require(type(record["driver_pid"]) is int and record["driver_pid"] == opts["pid"] and record["qualification"] is False, "completion driver identity/qualification mismatch")
    cases = ["source", "cli", "kernel-driver"] if opts["stage"] == "preflight" else FINITE_CASES
    require(record["cases"] == cases, "missing, duplicate or reordered finite cases")
    require(type(record["samples"]) is int and type(record["documents"]) is int, "invalid sample/document counts")
    if opts["stage"] == "finite":
        require(record["samples"] >= 2 and record["documents"] == 6, "missing samples or finite persisted effects")
    else:
        require(record["samples"] == 0 and record["documents"] == 0, "preflight performed workload")
    name = "TestProductionLinuxPreflight" if opts["stage"] == "preflight" else "TestProductionLinuxFinite"
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


class Controller:
    def __init__(self, opts):
        self.opts = opts
        self.owner = "weir-finite-" + uuid.uuid4().hex
        self.resources = []
        self.deadline = time.monotonic() + 240
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
        end = time.monotonic() + min(22 if stage == "preflight" else 60, self.remaining())
        def budget():
            value = min(end - time.monotonic(), self.remaining())
            require(value > 0, "absolute driver deadline exceeded")
            return value
        name = "TestProductionLinuxPreflight" if stage == "preflight" else "TestProductionLinuxFinite"
        target = self.opts.output / (stage + "-completion.json")
        env = os.environ.copy()
        env.update(environment)
        env.update(WEIR_FINITE_NATIVE="1", WEIR_FINITE_COMPLETION=str(target),
                   WEIR_EXECUTION_PROFILE=str(self.opts.output / "profile.json"),
                   WEIR_PROVENANCE_INPUTS=str(self.opts.output / "provenance.json"))
        args = [str(self.opts.driver), "-test.run=^" + name + "$", "-test.v", "-test.count=1", "-test.timeout=" + ("20s" if stage == "preflight" else "58s")]
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
        options = dict(path=target, stage=stage, revision=self.opts.revision, pid=child.pid, stdout=raw.decode("utf-8"), profile=self.opts.profile)
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
                with self.http.open(url, timeout=min(2, remaining())) as response:
                    remaining()
                    raw = response.read((64 << 10) + 1)
                    remaining()
                    require(response.status == 200 and len(raw) <= 64 << 10, "readiness response bound/status")
                remaining()
                if backend:
                    body = json.loads(raw)
                    version = body.get("version")
                    require(isinstance(version, dict), "wrong backend version identity")
                    product, number = PROFILES[self.opts.profile]
                    identity = version.get("distribution") == "opensearch" if product == "opensearch" else version.get("build_flavor") == "default" and not version.get("distribution")
                    require(body.get("cluster_name") == self.owner and version.get("number") == number and identity and version.get("build_hash"), "wrong backend cluster/version/product")
                    self.evidence("backend-identity.json", body)
                remaining()
                return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(min(.2, remaining()))

    def container(self, spec):
        resource = Resource("container", self.owner + "-" + spec["role"])
        mem, pids = spec["memory"], spec["pids"]
        args = ["container", "create", "--pull=never", "--platform=linux/" + self.arch, "--name", resource.name,
                "--label", "weir.owner=" + self.owner, "--network", spec["network"].identity,
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
        require(len(networks) == 1 and next(iter(networks.values()))["NetworkID"] == spec["network"].identity, "foreign/shared network")
        published = {name for name, bindings in info["Ports"].items() if bindings}
        require(published == {str(p) + "/tcp" for p in spec["ports"]}, "unexpected published ports")
        for port, host in spec["ports"].items():
            require(info["Ports"][str(port) + "/tcp"] == [dict(HostIp="127.0.0.1", HostPort=str(host))], "non-loopback/wrong port publication")
        observer = [m for m in info["Mounts"] if m["Destination"] == "/observer"]
        require(len(observer) == 1 and observer[0]["Source"] == str(self.opts.observer) and not observer[0]["RW"], "unknown/writable observer mount")
        if spec["role"] == "backend":
            if self.opts.profile == OS_PROFILE:
                require(info.get("Entrypoint") == ["./opensearch-docker-entrypoint.sh"] and info.get("Cmd") == ["opensearch"], "backend stock entrypoint changed")
                expected_tmpfs = OS_TMPFS
            else:
                require(info.get("Entrypoint") == ES_ENTRYPOINT and info.get("Cmd") == ES_COMMAND, "backend stock entrypoint/config staging mismatch")
                expected_tmpfs = ES_TMPFS
            require(info.get("Tmpfs") == expected_tmpfs and len(info["Mounts"]) == 1, "backend bounded tmpfs/foreign mounts mismatch")
        self.evidence(spec["role"] + "-container.json", info)
        return info

    def observe(self, resource, spec, phase):
        info = self.check_container(resource, spec)
        if spec["role"] == "backend" and self.opts.profile == OS_PROFILE:
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

    def shutdown_cli(self, resource):
        started = time.monotonic()
        end = started + min(8, self.remaining())
        self.guard(resource, end)
        self.docker(["kill", "--signal=TERM", resource.identity], 2, end)
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
        receipt = dict(resources=receipts, errors=errors)
        self.evidence("cleanup.json", receipt)
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
            result = dict(profile=self.opts.profile, qualification=False, revision=self.opts.revision, arch=self.arch, primary_error=primary, cleanup_errors=cleanup, finite_complete=primary is None and not cleanup)
            self.evidence("finite-result.json", result)
        require(primary is None and not cleanup, "finite execution failed: " + str(primary) + "; cleanup=" + str(cleanup))

    def prepare_inputs(self):
        info_format = '{"OS":{{json .OSType}},"Arch":{{json .Architecture}},"Cgroup":{{json .CgroupVersion}},"CPUs":{{json .NCPU}},"Memory":{{json .MemTotal}},"Root":{{json .DockerRootDir}},"Kernel":{{json .KernelVersion}}}'
        info = json.loads(self.docker(["info", "--format", info_format]))
        aliases = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}
        require(info["OS"] == "linux" and aliases.get(info["Arch"]) == self.arch and info["Cgroup"] == "2" and info["Kernel"] == platform.release() and info["CPUs"] >= 2 and info["Memory"] >= 4 << 30, "local native Docker/cgroupv2/resources required")
        require(shutil.disk_usage(info["Root"]).free >= 2 << 30, "insufficient Docker data free space")
        self.evidence("engine.json", info)
        target = dict(os="linux", arch=self.arch)
        roles = dict(driver="owned-local", cli="owned-local")
        product, version = PROFILES[self.opts.profile]
        backend = dict(product=product, version=version, transport="plain", endpoints=roles)
        profile = dict(schemaVersion=1, id=self.opts.profile, suite="functional", intent="finite", policy="production-v1", executor="linux-native-v1", driver=target, cli=target, backends=[backend])
        self.evidence("profile.json", profile)
        server = dict(root=str(self.opts.server_root), revision=self.opts.revision)
        harness = dict(root=str(self.opts.harness_root), revision=self.opts.revision)
        cli = dict(role="cli", path=str(self.opts.cli), sha256=self.hashes["cli"])
        driver = dict(role="driver", path=str(self.opts.driver), sha256=self.hashes["driver"])
        provenance = dict(schemaVersion=1, profileID=self.opts.profile, server=server, harness=harness, cli=cli, driver=driver)
        self.evidence("provenance.json", provenance)
        base = json.loads((self.opts.harness_root / "deploy/docker/base.json").read_text())
        self.base_ref = base["image"] + "@" + base["platforms"][self.arch]
        self.es_ref = (OS_REPOSITORY + "@" + OS_PINS[self.arch]) if self.opts.profile == OS_PROFILE else ES_REPOSITORY + "@" + ES_PINS[self.arch]
        self.base_image, self.es_image = self.image(self.base_ref), self.image(self.es_ref)
        images = dict(base=self.base_image, backend=self.es_image, backend_reference=self.es_ref, base_reference=self.base_ref)
        self.evidence("images.json", images)

    def run_fixture(self):
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
        if self.opts.profile == OS_PROFILE:
            extra = ["--tmpfs=" + path + ":" + options for path, options in OS_TMPFS.items()]
            extra += ["--env=DISABLE_SECURITY_PLUGIN=true", "--env=DISABLE_INSTALL_DEMO_CONFIG=true",
                      "--env=cluster.name=" + self.owner, "--env=discovery.type=single-node", "--env=action.auto_create_index=false",
                      "--env=OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"]
            es_spec.update(user="1000:1000", extra=extra, command=[], readonly=False)
        self.phase_deadline = min(self.deadline, time.monotonic() + 90)
        backend = self.container(es_spec)
        oracle = "http://127.0.0.1:" + str(oracle_port)
        self.wait_http(oracle + "/", 90, backend=backend)
        backend_before = self.observe(backend, es_spec, "before")
        self.phase_deadline = None
        basic = 'listeners:\n  application: "0.0.0.0:7447"\ndiagnostics:\n  address: "0.0.0.0:7449"\n  allow_intranet: true\ndiscovery:\n  advertise: ["127.0.0.1:%d"]\n' % app_port
        routing = 'stores:\n  - name: search\n    backend:\n      search:\n        url: "http://%s:9200"\n' % backend.name
        basic_path, routing_path = self.opts.output / "weir.yaml", self.opts.output / "routes.yaml"
        basic_path.write_text(basic)
        routing_path.write_text(routing)
        # Only these newly-created, credential-free fixture files are exposed
        # read-only to UID65532, independently of the invoking shell's umask.
        basic_path.chmod(0o644)
        routing_path.chmod(0o644)
        cli_extra = ["--tmpfs=/tmp:rw,noexec,nosuid,size=16777216,uid=65532,gid=65532", "--entrypoint=/weir"]
        for source, destination in ((self.opts.cli, "/weir"), (self.opts.output / "weir.yaml", "/weir.yaml"), (self.opts.output / "routes.yaml", "/routes.yaml")):
            cli_extra += ["--mount", "type=bind,src=" + str(source) + ",dst=" + destination + ",readonly"]
        cli_ports = {7447: app_port, 7449: diag_port}
        cli_spec = dict(role="cli", memory=1 << 30, pids=96, user="65532:65532", network=network, ports=cli_ports, extra=cli_extra, image=self.base_ref, image_id=self.base_image["Id"], command=["serve", "--config=/weir.yaml", "--routes=/routes.yaml"])
        cli = self.container(cli_spec)
        self.cli_resource = cli
        self.wait_http("http://127.0.0.1:" + str(diag_port) + "/readyz", 20)
        before = self.observe(cli, cli_spec, "before")
        environment = dict(WEIR_FINITE_APP="127.0.0.1:" + str(app_port), WEIR_FINITE_DIAGNOSTICS="127.0.0.1:" + str(diag_port), WEIR_FINITE_ORACLE=oracle, WEIR_FINITE_OWNER=self.owner)
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
    args = parser.parse_args()
    opts = Options(server_root=Path(args.server_root), harness_root=Path(args.harness_root), revision=args.revision, cli=Path(args.cli), driver=Path(args.driver), observer=Path(args.observer), output=Path(args.output), profile=args.profile)
    require(opts.output.is_absolute() and opts.output.parent.resolve() == opts.output.parent and not opts.output.exists(), "new canonical task-owned evidence directory required")
    controller = Controller(opts)
    controller.execute()


if __name__ == "__main__":
    main()
