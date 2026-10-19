#!/usr/bin/env python3
"""Owned held-tmpfs config copy-up probe; no backend startup or qualification.

Public image preparation and clean native helper compilation are separate CI
steps. Execution never pulls/builds/installs or uses Docker contexts/auth config.
"""

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import signal
import struct
import subprocess
import time
import uuid

PINS = {
    "amd64": "sha256:59e9e49ac00f19c2af9094e367c8e6586f7d07e6c06465c3f4c43b40934309f2",
    "arm64": "sha256:98db5375d94a62ee3d9073681bb40fe1087657d036f3dfa79053b0b1f83f5577",
}
REPOSITORY = "opensearchproject/opensearch"
# Public config blobs verified against each pinned manifest, not tag metadata.
CONFIG_IDS = {
    "amd64": "sha256:40d48c3e342ca437b89b733ce2062c2502c9ce4a3ea9ba69601671d4dfe08519",
    "arm64": "sha256:94b68af021602f06323f5e454881f5f8ec276ca9e4eadc403c108f584fdf1742",
}
FULL_ID = re.compile(r"[0-9a-f]{64}\Z")
CONFIG = "/usr/share/opensearch/config"
HELPER = "/weir-volume-helper"
CAPACITY = 16 << 20
VOLUME_OPTIONS = dict(device="tmpfs", type="tmpfs", o="rw,noexec,nosuid,nodev,size=16777216,uid=1000,gid=1000")
VOLUME_FORMAT = ('{"Name":{{json .Name}},"CreatedAt":{{json .CreatedAt}},'
                 '"Driver":{{json .Driver}},"Scope":{{json .Scope}},'
                 '"Options":{{json .Options}},"Labels":{{json .Labels}}}')
CONTAINER_FORMAT = ('{"Id":{{json .Id}},"Name":{{json .Name}},"Owner":{{json (index .Config.Labels "weir.owner")}},'
                    '"Image":{{json .Image}},"State":{{json .State}},'
                    '"Memory":{{json .HostConfig.Memory}},"Swap":{{json .HostConfig.MemorySwap}},'
                    '"CPU":{{json .HostConfig.NanoCpus}},"PIDs":{{json .HostConfig.PidsLimit}},'
                    '"Cgroupns":{{json .HostConfig.CgroupnsMode}},"Readonly":{{json .HostConfig.ReadonlyRootfs}},'
                    '"User":{{json .Config.User}},"Workdir":{{json .Config.WorkingDir}},"Caps":{{json .HostConfig.CapDrop}},'
                    '"Security":{{json .HostConfig.SecurityOpt}},"Privileged":{{json .HostConfig.Privileged}},'
                    '"PidMode":{{json .HostConfig.PidMode}},"IPC":{{json .HostConfig.IpcMode}},"Mounts":{{json .Mounts}},'
                    '"NetworkMode":{{json .HostConfig.NetworkMode}},"Ports":{{json .NetworkSettings.Ports}},'
                    '"Bindings":{{json .HostConfig.PortBindings}},"Restart":{{json .HostConfig.RestartPolicy.Name}},'
                    '"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},"Tmpfs":{{json .HostConfig.Tmpfs}}}')
IMAGE_FORMAT = '{"Id":{{json .Id}},"OS":{{json .Os}},"Arch":{{json .Architecture}},"Digests":{{json .RepoDigests}},"Volumes":{{json .Config.Volumes}}}'
MAX_OUTPUT = 64 << 10


@dataclass
class Options:
    root: Path
    revision: str
    helper: Path
    helper_sha256: str
    output: Path


@dataclass
class Resource:
    kind: str
    name: str
    identity: str = ""
    captured: dict | None = None


class CommandError(RuntimeError):
    def __init__(self, output, code):
        super().__init__("command failed exit=" + str(code))
        self.output = output


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate structured proof field")
        result[key] = value
    return result


def decode(text):
    return json.loads(text, object_pairs_hook=unique_fields)


def digest(path):
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as stream:
        while data := stream.read(1 << 20):
            total += len(data)
            require(total <= 128 << 20, "helper exceeds128MiB")
            h.update(data)
    return h.hexdigest()


def sanitized(raw):
    text = raw.decode("utf-8", errors="replace")
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    return "".join(c for c in text if c in "\n\t" or ord(c) >= 32 and ord(c) != 127)


def native_preflight(opts):
    # Unsupported execution must fail before touching source/helper/output paths.
    require(platform.system() == "Linux", "native Linux required; unsupported host is not a skip/PASS")
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    require(arch in PINS, "native supported architecture required; no emulation")
    for key, value in os.environ.items():
        if key.startswith("DOCKER_") and value:
            require(key == "DOCKER_HOST" and value == "unix:///var/run/docker.sock", "Docker overrides/context/TLS unsupported")
    require(Path("/var/run/docker.sock").is_socket(), "explicit local Docker socket required")
    require(all(shutil.which(name) for name in ("docker", "git", "go")), "prepared tools required")
    require(re.fullmatch(r"[0-9a-f]{40}", opts.revision), "full exact revision required")
    require(FULL_ID.fullmatch(opts.helper_sha256), "independent build SHA256 required")
    for path in (opts.root, opts.helper, opts.output):
        require(path.is_absolute() and path.resolve() == path and not any(c in str(path) for c in (",", "\n", "\r")), "canonical delimiter-free paths required")
    require(opts.root.is_dir() and not opts.output.exists() and opts.output.parent.is_dir(), "explicit source/new evidence directory required")
    require(opts.helper.is_file() and opts.helper.stat().st_mode & 0o005 == 0o005 and os.access(opts.helper, os.X_OK), "readable executable helper required")
    require(0 < opts.helper.stat().st_size <= 128 << 20 and digest(opts.helper) == opts.helper_sha256, "helper differs from independent build digest")
    with opts.helper.open("rb") as stream:
        header = stream.read(64)
    machine = {"amd64": 62, "arm64": 183}[arch]
    require(len(header) == 64 and header[:6] == b"\x7fELF\x02\x01" and struct.unpack_from("<H", header, 18)[0] == machine, "native64bit Linux ELF required")
    require((os.cpu_count() or 0) >= 2 and shutil.disk_usage(opts.output.parent).free >= 2 << 30, "bounded host CPU/storage prerequisites missing")
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    require(int(memory["MemAvailable"].split()[0]) >= 4 * 1024 * 1024, "4GiB actual available memory required")
    return arch


def host_process(pid):
    require(type(pid) is int and pid > 0, "invalid target host PID")
    root = Path("/proc") / str(pid)
    raw = (root / "stat").read_text()
    start = int(raw[raw.rfind(")") + 2:].split()[19])
    membership = (root / "cgroup").read_text().strip()
    status = (root / "status").read_text()
    ns = re.search(r"^NSpid:\s+(.+)$", status, re.M)
    after = (root / "stat").read_text()
    require(int(after[after.rfind(")") + 2:].split()[19]) == start, "host process replaced during observation")
    result = dict(pid=pid, start_ticks=start, membership=membership, namespace_pid=int(ns.group(1).split()[-1]) if ns else None)
    return result


class Controller:
    def __init__(self, opts):
        self.opts = opts
        self.owner = "weir-volume-" + uuid.uuid4().hex
        self.deadline = time.monotonic() + 240
        self.cleaning = False
        self.resources = []
        self.sequence = 0
        self.prefix = []
        self.arch = ""
        self.specs = {}
        self.holder = None
        self.volume = None
        self.holder_observation = None
        self.create_copy = None

    def evidence(self, name, value):
        raw = json.dumps(value, sort_keys=True, indent=2) + "\n"
        require(len(raw.encode()) <= MAX_OUTPUT, "evidence exceeds64KiB")
        (self.opts.output / name).write_text(raw)

    def remaining(self, deadline=None):
        active = self.cleanup_deadline if self.cleaning else self.deadline
        if deadline is not None:
            active = min(active, deadline)
        value = active - time.monotonic()
        require(value > 0, "absolute controller/phase deadline exceeded")
        return value

    def run(self, args, timeout=15, deadline=None):
        end = time.monotonic() + min(timeout, self.remaining(deadline))
        require(timeout > 0, "nonpositive command budget")
        self.sequence += 1
        path = self.opts.output / ("command-%04d.log" % self.sequence)
        # Do not inherit Git/Docker contexts, auth helpers, API overrides or tools'
        # user config. Only prepared public commands run in this empty home.
        env = dict(PATH=os.environ.get("PATH", ""), HOME=str(self.opts.output / "empty-home"), LANG="C", LC_ALL="C",
                   GOENV="off", GOWORK="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off",
                   GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null")
        raw = bytearray()
        process = subprocess.Popen(args, cwd=self.opts.root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            while selector.get_map():
                budget = end - time.monotonic()
                require(budget > 0, "bounded command timeout")
                for key, _ in selector.select(min(.1, budget)):
                    chunk = os.read(key.fd, 8192)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        raw.extend(chunk)
                        require(len(raw) <= MAX_OUTPUT, "command output exceeds64KiB")
            budget = end - time.monotonic()
            require(budget > 0, "command returned after absolute deadline")
            process.wait(timeout=budget)
        finally:
            if process.poll() is None:
                process.kill()
                # Reap the killed local CLI within the still-positive enclosing
                # controller/cleanup budget, never an unbounded wait or floor.
                active = self.cleanup_deadline if self.cleaning else self.deadline
                budget = active - time.monotonic()
                if budget > 0:
                    process.wait(timeout=min(1, budget))
                else:
                    process.poll()
            selector.close()
            process.stdout.close()
            path.write_text(sanitized(bytes(raw[:MAX_OUTPUT])))
        text = bytes(raw).decode("utf-8")
        if process.returncode:
            raise CommandError(text, process.returncode)
        self.remaining(deadline)
        return text

    def docker(self, args, timeout=15, deadline=None):
        return self.run(self.prefix + args, timeout, deadline)

    def inspect(self, resource, deadline=None):
        name = resource.identity or resource.name
        template = VOLUME_FORMAT if resource.kind == "volume" else CONTAINER_FORMAT
        try:
            return decode(self.docker([resource.kind, "inspect", "--format", template, name], deadline=deadline))
        except CommandError as error:
            absent = {f"Error: No such {resource.kind}: {name}", f"Error response from daemon: No such {resource.kind}: {name}"}
            if resource.kind == "volume":
                absent.add(f"Error response from daemon: get {name}: no such volume")
            if error.output.strip() in absent:
                return None
            raise

    def guard(self, resource, deadline=None):
        info = self.inspect(resource, deadline)
        require(info is not None, "registered resource unexpectedly absent")
        if resource.kind == "volume":
            require(set(info) == {"Name", "CreatedAt", "Driver", "Scope", "Options", "Labels"}, "volume tuple incomplete/unknown")
            expected_labels = {"weir.owner": self.owner}
            require(info["Name"] == resource.name and info["Driver"] == "local" and info["Scope"] == "local" and info["Options"] == VOLUME_OPTIONS and info["Labels"] == expected_labels, "volume tuple/owner mismatch; refuse mutation")
            require(isinstance(info["CreatedAt"], str) and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)", info["CreatedAt"]), "volume CreatedAt missing/invalid")
            require(resource.captured is None or resource.captured == info, "volume recreated/tuple changed; refuse mutation")
            resource.captured = info.copy()
        else:
            require(info.get("Owner") == self.owner and info.get("Name") == "/" + resource.name and FULL_ID.fullmatch(info.get("Id", "")), "container fullID/name/owner mismatch; refuse mutation")
            require(not resource.identity or resource.identity == info["Id"], "container replaced; refuse mutation")
            resource.identity = info["Id"]
            by_name = Resource("container", resource.name)
            named = self.inspect(by_name, deadline)
            require(named is not None and named.get("Id") == resource.identity and named.get("Owner") == self.owner, "container name replaced; refuse mutation")
        return info

    def absent(self, resource):
        require(self.inspect(resource) is None, "resource remains")
        if resource.identity:
            by_name = Resource(resource.kind, resource.name)
            require(self.inspect(by_name) is None, "same-name replacement remains; absence uncertain")

    def create(self, resource, args):
        # Register BEFORE mutation, including timeout/partial-create recovery.
        self.resources.append(resource)
        reply = self.docker(args, 30).strip()
        if resource.kind == "container":
            require(FULL_ID.fullmatch(reply), "create requires full containerID")
            resource.identity = reply
        else:
            require(reply == resource.name, "volume create name mismatch")
        self.guard(resource)
        return resource

    def prepare(self):
        git = ["git", "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null"]
        require(self.run(git + ["rev-parse", "HEAD"]).strip() == self.opts.revision, "requested source revision differs")
        require(not self.run(git + ["status", "--porcelain=v1", "--untracked-files=all"]).strip(), "exact source must be clean before mutation")
        version = self.run(["go", "version"]).strip()
        require(version == "go version go1.27.1 linux/" + self.arch, "fixed native Go1.27.1 required")
        build = self.run(["go", "version", "-m", str(self.opts.helper)])
        fields = [line.strip().split("\t") for line in build.splitlines()[1:]]
        require(build.splitlines()[0].endswith(": go1.27.1"), "helper compiler mismatch")
        require(["path", "github.com/batchstream/weir/internal/testutil/testvolume"] in fields and not any(row[0] == "dep" for row in fields if row), "unknown/non-stdlib helper package")
        settings = dict(row[1].split("=", 1) for row in fields if len(row) == 2 and row[0] == "build" and "=" in row[1])
        require(settings.get("GOOS") == "linux" and settings.get("GOARCH") == self.arch and settings.get("CGO_ENABLED") == "0" and settings.get("-trimpath") == "true", "native static trimmed helper build required")
        # -trimpath deliberately omits -ldflags from Go build information.
        # Admit the embedded stamp and actual loaded bytes with the known
        # helper itself, bounded and BEFORE any Docker resource mutation.
        identity_raw = self.run([str(self.opts.helper), "--mode=identity", "--revision=" + self.opts.revision,
                                 "--helper-sha256=" + self.opts.helper_sha256], 5)
        identity = decode(identity_raw)
        expected_identity = dict(schema=1, stage="identity", revision=self.opts.revision, helper_sha256=self.opts.helper_sha256,
                                 os="linux", arch=self.arch, go="go1.27.1")
        require(identity == expected_identity and type(identity.get("schema")) is int, "loaded helper/source admission mismatch")
        self.evidence("helper-admission.json", identity)
        engine_format = ('{"OS":{{json .OSType}},"Arch":{{json .Architecture}},"Cgroup":{{json .CgroupVersion}},'
                         '"CPUs":{{json .NCPU}},"Memory":{{json .MemTotal}},"Root":{{json .DockerRootDir}},'
                         '"Kernel":{{json .KernelVersion}},"Version":{{json .ServerVersion}},"ID":{{json .ID}}}')
        engine = decode(self.docker(["info", "--format", engine_format]))
        aliases = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}
        require(engine["OS"] == "linux" and aliases.get(engine["Arch"]) == self.arch and engine["Kernel"] == platform.release() and engine["Cgroup"] == "2", "native local unified Docker engine required")
        require(engine["CPUs"] >= 2 and engine["Memory"] >= 4 << 30 and engine["ID"] and engine["Version"], "engine resources/identity missing")
        require(shutil.disk_usage(engine["Root"]).free >= 2 << 30, "engine free storage insufficient")
        self.evidence("engine.json", engine)
        inputs = dict(revision=self.opts.revision, helper_sha256=self.opts.helper_sha256, helper_buildinfo=build, owner=self.owner, arch=self.arch, qualification=False)
        self.evidence("inputs.json", inputs)
        base = decode((self.opts.root / "deploy/docker/base.json").read_text())
        self.specs = {}
        for role, reference in (("holder", base["image"] + "@" + base["platforms"][self.arch]), ("probe", REPOSITORY + "@" + PINS[self.arch])):
            image = decode(self.docker(["image", "inspect", "--format", IMAGE_FORMAT, reference]))
            require(image["OS"] == "linux" and image["Arch"] == self.arch and reference in (image["Digests"] or []) and re.fullmatch(r"sha256:[0-9a-f]{64}", image["Id"]), "prepared exact native image mismatch")
            require(not image["Volumes"], "image-declared anonymous volumes unsupported")
            if role == "probe":
                require(image["Id"] == CONFIG_IDS[self.arch], "pinned target config identity mismatch")
            self.specs[role] = dict(role=role, reference=reference, image=image["Id"], memory=(64 if role == "holder" else 128) << 20, cpu=100_000_000 if role == "holder" else 500_000_000, ttl="300s" if role == "holder" else "55s", destination="/held" if role == "holder" else CONFIG)
            image_record = dict(reference=reference, metadata=image)
            self.evidence(role + "-image.json", image_record)

    def helper_args(self, spec, mode):
        return ["--mode=" + mode, "--role=" + spec["role"], "--owner=" + self.owner, "--revision=" + self.opts.revision,
                "--helper-sha256=" + self.opts.helper_sha256, "--image=" + spec["image"], "--ttl=" + spec["ttl"]]

    def create_container(self, role):
        spec = self.specs[role]
        resource = Resource("container", self.owner + "-" + role)
        args = ["container", "create", "--pull=never", "--platform=linux/" + self.arch, "--name", resource.name,
                "--label", "weir.owner=" + self.owner, "--network=none", "--ipc=none", "--cgroupns=private", "--cpus=" + ("0.1" if role == "holder" else "0.5"),
                "--memory=" + str(spec["memory"]), "--memory-swap=" + str(spec["memory"]), "--pids-limit=32", "--user=1000:1000",
                "--read-only", "--workdir=/", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "--restart=no", "--entrypoint=" + HELPER,
                "--log-driver=json-file", "--log-opt=max-size=64k", "--log-opt=max-file=1",
                "--mount", "type=bind,src=" + str(self.opts.helper) + ",dst=" + HELPER + ",readonly",
                "--mount", "type=volume,src=" + self.volume.name + ",dst=" + spec["destination"] + (",readonly" if role == "holder" else ""),
                spec["reference"]] + self.helper_args(spec, role)
        # Recheck mounted holder immediately BEFORE target CREATE, not just start.
        self.guard(self.volume)
        if role == "probe":
            self.observe(self.holder, "holder")
            self.attachments({self.holder.identity})
        else:
            self.attachments(set())
        self.create(resource, args)
        self.check_container(resource, role, running=False)
        expected_consumers = {resource.identity}
        if role == "probe":
            expected_consumers.add(self.holder.identity)
        self.attachments(expected_consumers)
        if role == "probe":
            self.capture_copy(resource)
        self.guard(resource)
        require(digest(self.opts.helper) == self.opts.helper_sha256, "helper changed before start")
        self.docker(["container", "start", resource.identity])
        return resource

    def check_container(self, resource, role, running=True):
        spec = self.specs[role]
        info = self.guard(resource)
        state = info["State"]
        require(state["Running"] is running and not state["OOMKilled"] and not state.get("Dead") and not state.get("Paused") and not state.get("Restarting"), "container state mismatch")
        require(info["Image"] == spec["image"] and info["Memory"] == spec["memory"] and info["Swap"] == spec["memory"] and info["CPU"] == spec["cpu"] and info["PIDs"] == 32, "container image/resource controls mismatch")
        require(info["Cgroupns"] == "private" and info["Readonly"] is True and not info["Privileged"] and not info["PidMode"] and info["IPC"] == "none" and info["User"] == "1000:1000" and info["Workdir"] == "/", "container root/namespace/user/working-directory controls mismatch")
        security = info["Security"] or []
        require(info["Caps"] == ["ALL"] and len(security) == 1 and security[0] in ("no-new-privileges", "no-new-privileges:true"), "capability/security controls mismatch")
        require(info["NetworkMode"] == "none" and not info["Ports"] and not info["Bindings"] and not info["Tmpfs"] and info["Restart"] == "no", "unexpected network/ports/writable tmpfs/restart")
        require(info["Entrypoint"] == [HELPER] and info["Cmd"] == self.helper_args(spec, role), "unexpected entrypoint/command")
        mounts = info["Mounts"]
        require(len(mounts) == 2, "unexpected filesystem mounts")
        helper = [m for m in mounts if m["Destination"] == HELPER]
        volume = [m for m in mounts if m["Destination"] == spec["destination"]]
        require(len(helper) == 1 and helper[0]["Type"] == "bind" and helper[0]["Source"] == str(self.opts.helper) and helper[0]["RW"] is False, "helper mount identity/access mismatch")
        require(len(volume) == 1 and volume[0]["Type"] == "volume" and volume[0]["Name"] == self.volume.name and volume[0]["Driver"] == "local" and volume[0]["RW"] is (role == "probe"), "volume mount identity/access mismatch")
        self.evidence(role + "-container.json", info)
        return info

    def record(self, text, role, stage):
        require(len(text.encode()) <= 16384 and len(text.splitlines()) == 1, "missing/duplicate/unbounded helper completion")
        proof = decode(text)
        expected = {"schema", "stage", "role", "owner", "revision", "helper_sha256", "image", "arch", "process", "filesystem", "files", "fill_bytes", "fill_attempts", "enospc", "round_trip", "preserved", "qualification"}
        require(isinstance(proof, dict) and set(proof) == expected, "completion missing/unknown fields")
        spec = self.specs[role]
        require(type(proof["schema"]) is int and proof["schema"] == 1 and proof["stage"] == stage and proof["role"] == role and proof["owner"] == self.owner and proof["revision"] == self.opts.revision and proof["helper_sha256"] == self.opts.helper_sha256 and proof["image"] == spec["image"] and proof["arch"] == self.arch and proof["qualification"] is False, "helper completion identity mismatch")
        process = proof["process"]
        process_keys = {"pid", "uid", "gid", "sha256", "executable", "membership", "start_ticks", "cpu_max", "memory_max", "memory_swap_max", "pids_max", "cap_eff", "no_new_privs"}
        require(isinstance(process, dict) and set(process) == process_keys, "incomplete/unknown process fields")
        require(all(type(process[k]) is int for k in ("pid", "uid", "gid", "start_ticks", "memory_max", "memory_swap_max", "pids_max")), "invalid process integer fields")
        require(process["pid"] == 1 and process["uid"] == 1000 and process["gid"] == 1000 and process["sha256"] == self.opts.helper_sha256 and process["executable"] == HELPER and process["membership"] == "0::/" and type(process["start_ticks"]) is int and process["start_ticks"] > 0, "PID1 helper process identity mismatch")
        cpu = process["cpu_max"].split()
        require(len(cpu) == 2 and int(cpu[1]) > 0 and int(cpu[0]) * 1_000_000_000 == spec["cpu"] * int(cpu[1]), "actual CPU quota mismatch")
        require(process["memory_max"] == spec["memory"] and process["memory_swap_max"] == 0 and process["pids_max"] == 32 and process["cap_eff"] == "0000000000000000" and process["no_new_privs"] == "1", "actual memory/swap/PID/cap/privilege controls mismatch")
        fs = proof["filesystem"]
        require(isinstance(fs, dict) and set(fs) == {"magic", "capacity", "mount", "device", "options", "root_readonly", "helper_readonly", "no_shared_memory"}, "incomplete/unknown filesystem fields")
        require(type(fs["magic"]) is int and type(fs["capacity"]) is int and isinstance(fs["options"], list) and len(fs["options"]) == len(set(fs["options"])), "invalid filesystem facts")
        require(fs["root_readonly"] is True and fs["helper_readonly"] is True and fs["no_shared_memory"] is True, "actual root/helper/shared-memory mount isolation mismatch")
        require(fs["magic"] == 0x01021994 and fs["capacity"] == CAPACITY and fs["mount"] == spec["destination"] and re.fullmatch(r"\d+:\d+", fs["device"]), "actual tmpfs type/capacity/mount mismatch")
        access = "ro" if role == "holder" else "rw"
        require({access, "noexec", "nosuid", "nodev"}.issubset(fs["options"]) and ("rw" if role == "holder" else "ro") not in fs["options"], "actual mount flags mismatch")
        if stage in ("probe", "copy"):
            files = proof["files"]
            require([f["name"] for f in files] == ["opensearch.yml", "jvm.options", "log4j2.properties"], "public config proof missing/reordered/duplicated")
            for file in files:
                require(set(file) == {"name", "size", "sha256"} and type(file["size"]) is int and 0 < file["size"] <= 1 << 20 and FULL_ID.fullmatch(file["sha256"]), "invalid bounded public file proof")
        else:
            require(proof["files"] == [], "holder/observer unexpectedly reported config reads")
        if stage == "probe":
            require(self.create_copy is not None and proof["files"] == self.create_copy, "public config differs from held CREATE-time copy-up")
            require(proof["enospc"] is True and proof["round_trip"] is True and proof["preserved"] is True and type(proof["fill_bytes"]) is int and 0 < proof["fill_bytes"] <= CAPACITY and type(proof["fill_attempts"]) is int and 1 <= proof["fill_attempts"] <= 273, "missing bounded writable/ENOSPC/preservation effects")
        else:
            require(proof["fill_bytes"] == 0 and proof["fill_attempts"] == 0 and proof["enospc"] is False and proof["round_trip"] is False and proof["preserved"] is False, "holder/observer unexpectedly wrote config")
        return proof

    def capture_copy(self, target):
        # The target is CREATED, not started. Only Docker's create-time copy-up
        # could populate the previously verified empty, readonly-held volume.
        created = self.check_container(target, "probe", running=False)
        require(created["State"].get("Status") == "created", "target started before create-time copy proof")
        observation = self.observe(self.holder, "holder")
        self.guard(self.holder)
        raw = self.docker(["exec", "--user=1000:1000", self.holder.identity, HELPER] + self.helper_args(self.specs["holder"], "copy"), 5)
        proof = self.record(raw, "holder", "copy")
        require(proof["process"] == observation["process"] and proof["filesystem"] == observation["filesystem"], "CREATE-time copy observer changed helper/mount identity")
        after = self.check_container(target, "probe", running=False)
        require(after["State"].get("Status") == "created", "target started during create-time copy proof")
        self.create_copy = proof["files"]
        receipt = dict(target_id=target.identity, target_image=self.specs["probe"]["image"], holder_id=self.holder.identity,
                       target_started=False, volume=self.volume.captured, helper=proof)
        self.evidence("create-time-copy.json", receipt)

    def observe(self, resource, role):
        info = self.check_container(resource, role)
        self.guard(self.volume)
        before = host_process(info["State"]["Pid"])
        require(resource.identity in before["membership"] and before["namespace_pid"] == 1, "host process is not owned container PID1")
        self.guard(resource)
        require(digest(self.opts.helper) == self.opts.helper_sha256, "known helper bytes changed")
        raw = self.docker(["exec", "--user=1000:1000", resource.identity, HELPER] + self.helper_args(self.specs[role], "observe"), 5)
        proof = self.record(raw, role, "observe")
        after = host_process(info["State"]["Pid"])
        require(before == after and proof["process"]["start_ticks"] == before["start_ticks"], "host/actual-loaded helper PID1 start mismatch")
        self.check_container(resource, role)
        if role == "holder":
            if self.holder_observation:
                require(self.holder_observation["process"] == proof["process"] and self.holder_observation["filesystem"] == proof["filesystem"], "holder process/mount replaced")
            self.holder_observation = proof
        else:
            require(self.holder_observation is not None and self.holder_observation["filesystem"]["device"] == proof["filesystem"]["device"], "target did not retain the held filesystem")
        observation = dict(host=before, helper=proof)
        self.evidence(role + "-observation.json", observation)
        return proof

    def logs(self, resource, deadline=None):
        self.guard(resource, deadline)
        return self.docker(["logs", "--tail=8", resource.identity], 5, deadline)

    def completion(self, resource, role, observation):
        end = time.monotonic() + min(10, self.remaining())
        raw = self.logs(resource, end)
        while not raw.strip():
            self.check_container(resource, role)
            budget = end - time.monotonic()
            require(budget > 0, "missing helper completion before deadline")
            time.sleep(min(.05, budget))
            raw = self.logs(resource, end)
        require(time.monotonic() < end, "late helper completion")
        proof = self.record(raw, role, role)
        require(proof["process"] == observation["process"] and proof["filesystem"] == observation["filesystem"], "completion and observed helper/mount identity differ")
        self.evidence(role + "-completion.json", proof)
        return proof

    def shutdown(self, resource):
        started = time.monotonic()
        end = min(self.cleanup_deadline if self.cleaning else self.deadline, started + 8)
        self.guard(resource, end)
        self.docker(["kill", "--signal=TERM", resource.identity], 2, end)
        self.guard(resource, end)
        reply = self.docker(["wait", resource.identity], 8, end).strip()
        info = self.guard(resource, end)
        require(reply == "0" and not info["State"]["Running"] and info["State"]["ExitCode"] == 0 and not info["State"]["OOMKilled"], "controlled TERM/wait exit0 bound failed")
        elapsed = time.monotonic() - started
        require(elapsed <= 8, "shutdown finished after absolute8s bound")
        receipt = dict(id=resource.identity, exit_code=0, elapsed_seconds=elapsed, within_seconds=8)
        self.evidence(resource.name.rsplit("-", 1)[-1] + "-shutdown.json", receipt)

    def attachments(self, expected=None):
        # -a is essential: stopped/created consumers still reference volumes.
        raw = self.docker(["container", "ls", "--all", "--no-trunc", "--filter", "volume=" + self.volume.name, "--format", "{{.ID}}"])
        ids = raw.splitlines()
        require(all(FULL_ID.fullmatch(value) for value in ids) and len(ids) == len(set(ids)), "ambiguous volume attachments")
        expected = set() if expected is None else expected
        require(set(ids) == expected, "volume has missing/running/stopped/foreign attachments; refuse mutation/removal")

    def cleanup(self):
        self.cleaning = True
        self.cleanup_deadline = time.monotonic() + 60
        errors, receipts = [], []
        consumers_absent = True
        for resource in reversed(self.resources):
            receipt = dict(kind=resource.kind, name=resource.name, id=resource.identity, absent=False)
            receipts.append(receipt)
            try:
                info = self.inspect(resource)
                if info is None:
                    self.absent(resource)
                    receipt["absent"] = True
                    continue
                info = self.guard(resource)
                if resource.kind == "container":
                    try:
                        self.logs(resource)
                    except Exception as error:
                        errors.append(resource.name + " logs: " + str(error))
                    info = self.guard(resource)
                    if info["State"]["Running"]:
                        try:
                            self.shutdown(resource)
                        except Exception as error:
                            errors.append(resource.name + " shutdown: " + str(error))
                    info = self.guard(resource)
                    require(not info["State"]["Running"], "consumer remains running")
                    if info["State"]["ExitCode"] != 0 or info["State"]["OOMKilled"]:
                        errors.append(resource.name + " nonzero/TTL/OOM exit; not controlled shutdown")
                    self.guard(resource)
                    self.docker(["container", "rm", resource.identity], 5)
                else:
                    require(consumers_absent, "consumer absence not proven; preserve volume")
                    for consumer in self.resources:
                        if consumer.kind == "container":
                            self.absent(consumer)
                    self.attachments()
                    self.guard(resource)
                    self.docker(["volume", "rm", resource.name], 5)
                self.absent(resource)
                receipt["id"] = resource.identity
                receipt["tuple"] = resource.captured
                receipt["absent"] = True
            except Exception as error:
                errors.append(resource.name + ": " + str(error))
                if resource.kind == "container":
                    consumers_absent = False
        for resource, item in zip(reversed(self.resources), receipts):
            try:
                self.absent(resource)
            except Exception as error:
                item["absent"] = False
                errors.append(resource.name + " final absence: " + str(error))
        receipt = dict(resources=receipts, errors=errors)
        self.evidence("cleanup.json", receipt)
        return errors

    def fixture(self):
        self.volume = Resource("volume", self.owner + "-config")
        args = ["volume", "create", "--driver=local", "--label", "weir.owner=" + self.owner]
        for key, value in VOLUME_OPTIONS.items():
            args += ["--opt", key + "=" + value]
        self.create(self.volume, args + [self.volume.name])
        self.evidence("volume.json", self.volume.captured)
        self.holder = self.create_container("holder")
        observation = self.observe(self.holder, "holder")
        self.completion(self.holder, "holder", observation)
        # Holder's empty read-only mount is now alive before copy-up at CREATE.
        target_deadline = min(self.deadline, time.monotonic() + 60)
        saved = self.deadline
        self.deadline = target_deadline
        try:
            target = self.create_container("probe")
            observation = self.observe(target, "probe")
            self.completion(target, "probe", observation)
            self.observe(self.holder, "holder")
            self.shutdown(target)
            # Reject a duplicate/late completion emitted during termination too.
            self.completion(target, "probe", observation)
            self.remaining()
        finally:
            self.deadline = saved
        self.shutdown(self.holder)
        self.completion(self.holder, "holder", self.holder_observation)

    def execute(self):
        self.arch = native_preflight(self.opts)
        self.opts.output.mkdir(mode=0o755)
        for name in ("empty-home", "empty-docker-config"):
            (self.opts.output / name).mkdir(mode=0o700)
        self.prefix = ["docker", "--config", str(self.opts.output / "empty-docker-config"), "--host=unix:///var/run/docker.sock"]
        primary = None
        try:
            self.prepare()
            self.fixture()
        except Exception as error:
            primary = str(error)
        finally:
            cleanup = self.cleanup()
            result = dict(schema=1, revision=self.opts.revision, owner=self.owner, arch=self.arch, qualification=False,
                          filesystem_prerequisite_complete=primary is None and not cleanup, primary_error=primary, cleanup_errors=cleanup)
            self.evidence("result.json", result)
        require(primary is None and not cleanup, "filesystem prerequisite failed: " + str(primary) + "; cleanup=" + str(cleanup))


def interrupted(signum, frame):
    raise RuntimeError("controller interrupted signal=" + str(signum))


def main():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "revision", "helper", "helper-sha256", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    opts = Options(Path(args.root), args.revision, Path(args.helper), args.helper_sha256, Path(args.output))
    controller = Controller(opts)
    controller.execute()


if __name__ == "__main__":
    main()
