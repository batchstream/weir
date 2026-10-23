#!/usr/bin/env python3
"""Runs the Linux memory Guard and CLI tests in owned, memory-limited containers.

Requires a Linux cgroup-v2 Docker engine of the host architecture and IMAGE
present locally (`docker pull` it once); the script pulls nothing, needs no
credentials and never writes host cgroups.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parent.parent
# Official MongoDB 8.0.32 multi-architecture index. It runs the database and,
# with the test binary as entrypoint, every test container. MongoDB 8.0 refuses
# to start on kernels 6.19 and newer (SERVER-121912), which blocks only the cli
# case.
IMAGE = "mongo:8.0@sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2"
OWNER = "weir-memory-" + uuid.uuid4().hex[:12]
OUTPUT = ROOT / ".testdata" / OWNER

# Each case runs in its own container so its cgroup and process are fresh. Only
# the cli case uses the database; it runs after every Guard case.
CASES = [
    dict(name="guard", binary="overload.test", run="^TestLinuxMemoryNative$", memory=512, count=3,
         env=["WEIR_MEMORY_NATIVE=1"]),
    dict(name="cli", binary="app.test", run="^TestLinuxMemoryCLI$", memory=512, count=3, database=True,
         env=["WEIR_MEMORY_NATIVE=1", "WEIR_MEMORY_MONGO_ADDR=memory-mongo:27017"]),
    # Page cache must be disk-backed, so TMPDIR is an anonymous volume, not tmpfs.
    dict(name="page-cache", binary="overload.test", run="^TestLinuxReclaimablePageCacheDoesNotOverload$", memory=256,
         env=["WEIR_MEMORY_PAGE_CACHE=1", "TMPDIR=/scratch"], volume="/scratch"),
    dict(name="released-heap", binary="overload.test", run="^TestLinuxReleasedGoHeapRecoversAfterScavenge$", memory=256,
         env=["WEIR_MEMORY_PAGE_CACHE=1"]),
    dict(name="shmem", binary="overload.test", run="^TestLinuxSharedMemoryDoesOverload$", memory=256, shm=240,
         env=["WEIR_MEMORY_SHMEM=1"]),
    dict(name="runtime-automatic", binary="overload.test", run="^TestLinuxAutomaticRuntimeMemoryRecoversReleasedHeap$",
         memory=256, env=["WEIR_MEMORY_RECOVERY=1"]),
    dict(name="runtime-off", binary="overload.test", run="^TestLinuxExplicitOffKeepsReleasedHeapBaseline$",
         memory=256, env=["WEIR_MEMORY_RECOVERY=1", "GOMEMLIMIT=off"]),
] + [
    dict(name=f"external-{mib}", binary="overload.test", run="^TestLinuxRuntimeMemoryExternalPressure$", memory=512,
         shm=384, env=[f"WEIR_MEMORY_EXTERNAL_PRESSURE_MIB={mib}"])
    for mib in (344, 352, 368)
]


def run(args, timeout=60, env=None, log=None, echo=True):
    result = subprocess.run(
        args,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    if log:
        (OUTPUT / log).write_text(result.stdout)
    elif echo:
        print(result.stdout, end="", flush=True)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, args, output=result.stdout)
    return result


def inspect(kind, name):
    # Select metadata in Docker itself; never collect Env, proxy config or full JSON.
    formats = {
        "image": '{"Id":{{json .Id}},"Architecture":{{json .Architecture}},"Os":{{json .Os}},"RepoDigests":{{json .RepoDigests}}}',
        "container": '{"Id":{{json .Id}},"Owner":{{json (index .Config.Labels "weir.owner")}},"Running":{{json .State.Running}},"Status":{{json .State.Status}},"ExitCode":{{json .State.ExitCode}},"OOMKilled":{{json .State.OOMKilled}},"Pid":{{json .State.Pid}},"Memory":{{json .HostConfig.Memory}},"MemorySwap":{{json .HostConfig.MemorySwap}},"NanoCpus":{{json .HostConfig.NanoCpus}},"PidsLimit":{{json .HostConfig.PidsLimit}}}',
        "network": '{"Id":{{json .Id}},"Owner":{{json (index .Labels "weir.owner")}}}',
    }
    try:
        result = run(["docker", kind, "inspect", "--format", formats[kind], name], echo=False)
    except subprocess.CalledProcessError as error:
        # Only an exact daemon not-found response proves absence. Engine failures
        # and timeouts leave ownership/existence unknown and must not permit rm.
        absent = f"Error response from daemon: No such {kind}: {name}"
        if kind == "network":
            absent = f"Error response from daemon: network {name} not found"
        if kind != "image" and error.output.strip() == absent:
            return None
        if kind == "image":
            raise RuntimeError(f"image {name} is not present locally; run `docker pull {name}`") from error
        raise
    return json.loads(result.stdout)


def container_args(case, network):
    args = [
        "docker",
        "create",
        "--name",
        OWNER + "-" + case["name"],
        "--label",
        "weir.owner=" + OWNER,
        "--network",
        network,
        f"--memory={case['memory']}m",
        f"--memory-swap={case['memory']}m",
        "--cpus=2",
        "--pids-limit=96",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,size=32m",
        "--mount",
        f"type=bind,src={OUTPUT},dst=/fixture,readonly",
    ]
    if "shm" in case:
        args.append(f"--shm-size={case['shm']}m")
    if "volume" in case:
        args += ["--mount", "type=volume,dst=" + case["volume"]]
    for variable in case["env"]:
        args += ["--env", variable]
    args += [
        "--entrypoint",
        "/fixture/" + case["binary"],
        IMAGE,
        "-test.run=" + case["run"],
        f"-test.count={case.get('count', 1)}",
        "-test.timeout=120s",
        "-test.v",
    ]
    return args


KERNEL_REFUSAL = "Linux kernel versions 6.19 and newer has a known incompatibility"


def run_case(case, network, resources):
    container = OWNER + "-" + case["name"]
    resources["containers"].append(container)
    run(container_args(case, network))
    result = run(["docker", "start", "--attach", container], timeout=150, log=case["name"] + ".log")
    state = inspect("container", container)
    if result.returncode or state["ExitCode"] or state["OOMKilled"]:
        raise RuntimeError(f"{case['name']} failed; preserve {OUTPUT / (case['name'] + '.log')}")
    print(f"{case['name']}: ok", flush=True)


def start_database(resources, network, database):
    # Register owned names before commands which can partially succeed.
    resources["network"] = network
    run(["docker", "network", "create", "--internal", "--label", "weir.owner=" + OWNER, network])
    resources["containers"].append(database)
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            database,
            "--label",
            "weir.owner=" + OWNER,
            "--network",
            network,
            "--network-alias",
            "memory-mongo",
            "--memory=768m",
            "--memory-swap=768m",
            "--cpus=1",
            "--pids-limit=128",
            "--tmpfs",
            "/data/db:rw,size=256m",
            "--entrypoint",
            "mongod",
            IMAGE,
            "--bind_ip_all",
            "--replSet",
            "weir_memory",
            "--wiredTigerCacheSizeGB=0.25",
            "--setParameter",
            "enableTestCommands=1",
        ]
    )
    bootstrap = '''for(let i=0;i<100;i++){try{db.adminCommand({ping:1});break}catch(e){sleep(100)}};
    rs.initiate({_id:"weir_memory",members:[{_id:0,host:"memory-mongo:27017"}]});
    for(let i=0;i<100;i++){if(db.hello().isWritablePrimary)break;sleep(100)};
    if(!db.hello().isWritablePrimary)throw new Error("not primary"); print(db.version());'''
    try:
        run(
            ["docker", "exec", database, "mongosh", "--quiet", "--norc", "--eval", bootstrap],
            timeout=30,
            log="mongo-bootstrap.log",
        )
    except subprocess.CalledProcessError as error:
        logs = run(["docker", "logs", database], timeout=15, echo=False).stdout
        if KERNEL_REFUSAL in logs:
            raise RuntimeError(
                "the cli case needs a Docker engine kernel below 6.19: MongoDB 8.0 refuses newer "
                "kernels (SERVER-121912); the Guard cases above passed"
            ) from error
        raise


def cleanup(resources):
    errors = []
    containers_stopped = True
    for name in reversed(resources["containers"]):
        try:
            info = inspect("container", name)
            if info is None:
                continue
            if info["Owner"] != OWNER:
                raise RuntimeError("container owner mismatch; refusing modification")
        except Exception as error:
            errors.append(f"{name} inspect: {error}")
            containers_stopped = False
            continue
        # A logs failure must not prevent stop, and a stop failure must not hide
        # a later independent exit/absence check. Never force rm a live container.
        commands = [
            (["docker", "logs", name], name + ".log"),
            (["docker", "stop", "--timeout=6", name], None),
        ]
        for args, log in commands:
            try:
                run(args, timeout=15, log=log)
            except Exception as error:
                errors.append(f"{name} {args[1]}: {error}")
        try:
            info = inspect("container", name)
            if info is not None:
                if info["Owner"] != OWNER or info["Running"]:
                    raise RuntimeError("owner changed or container still running")
                run(["docker", "rm", "-v", name], timeout=15)
                if inspect("container", name) is not None:
                    raise RuntimeError("container still present after rm")
        except Exception as error:
            errors.append(f"{name} removal: {error}")
            containers_stopped = False
    network_removed = True
    if resources["network"] is not None:
        name = resources["network"]
        try:
            info = inspect("network", name)
            if info is not None:
                if info["Owner"] != OWNER:
                    raise RuntimeError("network owner mismatch; refusing modification")
                run(["docker", "network", "rm", name], timeout=15)
                if inspect("network", name) is not None:
                    raise RuntimeError("network still present after rm")
        except Exception as error:
            errors.append(f"{name} cleanup: {error}")
            network_removed = False
    try:
        if (OUTPUT / "owner").read_text().strip() != OWNER:
            raise RuntimeError("output owner mismatch; preserving all files")
        if containers_stopped:
            for name in ("weir", "app.test", "overload.test"):
                try:
                    (OUTPUT / name).unlink(missing_ok=True)
                except Exception as error:
                    errors.append(f"{name} cleanup: {error}")
    except Exception as error:
        errors.append(str(error))
    result = {
        "errors": errors,
        "all_stopped": containers_stopped and network_removed,
    }
    return result


def main():
    if os.environ.get("WEIR_MEMORY_INTEGRATION") != "1":
        sys.exit("requires WEIR_MEMORY_INTEGRATION=1; starts bounded owned containers")
    OUTPUT.mkdir(parents=True)
    (OUTPUT / "owner").write_text(OWNER + "\n")
    print(f"memory logs: {OUTPUT}", flush=True)
    resources = {"containers": [], "network": None}
    network, database = OWNER, OWNER + "-mongo"
    failed = False
    try:
        engine = json.loads(
            run(
                [
                    "docker",
                    "info",
                    "--format",
                    '{"OSType":{{json .OSType}},"Architecture":{{json .Architecture}},"KernelVersion":{{json .KernelVersion}},"CgroupVersion":{{json .CgroupVersion}},"NCPU":{{json .NCPU}},"MemTotal":{{json .MemTotal}}}',
                ],
                echo=False,
            ).stdout
        )
        image = inspect("image", IMAGE)
        architecture = {"aarch64": "arm64", "x86_64": "amd64"}.get(
            engine["Architecture"], engine["Architecture"]
        )
        if (
            engine["OSType"] != "linux"
            or str(engine["CgroupVersion"]) != "2"
            or image["Architecture"] != architecture
            or image["Os"] != "linux"
        ):
            raise RuntimeError(
                "requires a Linux cgroup-v2 engine and a same-architecture local image; no QEMU"
            )
        env = dict(
            os.environ,
            GOPROXY="off",
            CGO_ENABLED="0",
            GOOS="linux",
            GOARCH=architecture,
        )
        commands = [
            (["go", "build", "-o", str(OUTPUT / "weir"), "./cmd/weir"], "build-cli.log"),
            (
                [
                    "go",
                    "test",
                    "-tags",
                    "integration",
                    "-c",
                    "-o",
                    str(OUTPUT / "overload.test"),
                    "./internal/overload",
                ],
                "build-overload.log",
            ),
            (
                [
                    "go",
                    "test",
                    "-tags",
                    "integration",
                    "-c",
                    "-o",
                    str(OUTPUT / "app.test"),
                    "./internal/app",
                ],
                "build-app.log",
            ),
        ]
        for args, log in commands:
            run(args, timeout=120, env=env, log=log)
        # Guard cases need no network or database, so a database failure
        # cannot hide their results.
        for case in CASES:
            if not case.get("database"):
                run_case(case, "none", resources)
        start_database(resources, network, database)
        for case in CASES:
            if case.get("database"):
                run_case(case, network, resources)
    except BaseException:
        failed = True
        raise
    finally:
        result = cleanup(resources)
        if result["errors"] or not result["all_stopped"]:
            print(
                f"Fixture cleanup has errors or unresolved resources: {result}; logs: {OUTPUT}",
                file=sys.stderr,
                flush=True,
            )
            if not failed:
                raise RuntimeError("fixture cleanup incomplete; see logs")
        else:
            print(f"Stopped owned fixtures; logs: {OUTPUT}", flush=True)


if __name__ == "__main__":
    main()
