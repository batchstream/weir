#!/usr/bin/env python3
"""Explicit opt-in, native Linux memory fixture. No image pulls, credentials or host cgroup writes."""

import shutil
import signal
import socket
import time
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "sha256:997ed65ff26fc20107e799f0fd1477e5af782b42bd651d870c5436b95fd0323c"
OWNER = "weir-memory-" + uuid.uuid4().hex[:12]
OUTPUT = ROOT / ".testdata" / OWNER


def run(args, timeout=60, env=None, log=None):
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
    else:
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
        result = run(
            ["docker", kind, "inspect", "--format", formats[kind], name], log=kind + "-inspect.json"
        )
    except subprocess.CalledProcessError as error:
        # Only an exact daemon not-found response proves absence. Engine failures
        # and timeouts leave ownership/existence unknown and must not permit rm.
        absent = f"Error response from daemon: No such {kind}: {name}"
        if kind == "network":
            absent = f"Error response from daemon: network {name} not found"
        if kind != "image" and error.output.strip() == absent:
            return None
        raise
    return json.loads(result.stdout)


def cleanup(resources):
    receipts, errors = [], []
    containers_stopped = True
    for name in reversed(resources["containers"]):
        receipt = {"container": name, "steps": []}
        receipts.append(receipt)
        try:
            info = inspect("container", name)
            receipt["inspection"] = info
            if info is None:
                receipt["absent"] = True
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
                step = {"action": args[1], "ok": True}
                receipt["steps"].append(step)
            except Exception as error:
                message = f"{name} {args[1]}: {error}"
                step = {"action": args[1], "error": str(error)}
                receipt["steps"].append(step)
                errors.append(message)
        try:
            info = inspect("container", name)
            receipt["after_stop"] = info
            if info is not None:
                if info["Owner"] != OWNER or info["Running"]:
                    raise RuntimeError("owner changed or container still running")
                run(["docker", "rm", "-v", name], timeout=15)
                if inspect("container", name) is not None:
                    raise RuntimeError("container still present after rm")
            receipt["absent"] = True
        except Exception as error:
            errors.append(f"{name} removal: {error}")
            containers_stopped = False
    network_removed = True
    if resources["network"] is not None:
        name = resources["network"]
        receipt = {"network": name}
        receipts.append(receipt)
        try:
            info = inspect("network", name)
            receipt["inspection"] = info
            if info is not None:
                if info["Owner"] != OWNER:
                    raise RuntimeError("network owner mismatch; refusing modification")
                run(["docker", "network", "rm", name], timeout=15)
                if inspect("network", name) is not None:
                    raise RuntimeError("network still present after rm")
            receipt["absent"] = True
        except Exception as error:
            errors.append(f"{name} cleanup: {error}")
            network_removed = False
    host = resources["host_mongo"]
    host_stopped = host is None
    if host is not None:
        receipt = {"host_mongo_pid": host.pid, "bind": "127.0.0.1"}
        receipts.append(receipt)
        try:
            host.send_signal(signal.SIGTERM)
        except Exception as error:
            errors.append(f"host SIGTERM: {error}")
        try:
            host.wait(timeout=10)
            host_stopped = True
        except Exception as error:
            errors.append(f"host Wait: {error}")
            try:
                host.kill()
            except Exception as error:
                errors.append(f"host kill: {error}")
            try:
                host.wait(timeout=3)
                host_stopped = True
            except Exception as error:
                errors.append(f"host final Wait: {error}")
        receipt.update(waited=host_stopped, returncode=host.returncode)
    if resources["host_output"] is not None:
        try:
            resources["host_output"].close()
        except Exception as error:
            errors.append(f"host log close: {error}")
    try:
        if (OUTPUT / "owner").read_text().strip() != OWNER:
            raise RuntimeError("output owner mismatch; preserving all files")
        files = []
        if host_stopped:
            files.append(OUTPUT / "mongo-data")
        if containers_stopped:
            files.extend(OUTPUT / name for name in ("weir", "app.test", "overload.test"))
        for item in files:
            try:
                if item.name == "mongo-data":
                    if item.exists():
                        shutil.rmtree(item)
                else:
                    item.unlink(missing_ok=True)
                file_receipt = {"file": item.name, "absent": True}
                receipts.append(file_receipt)
            except Exception as error:
                errors.append(f"{item.name} cleanup: {error}")
    except Exception as error:
        errors.append(str(error))
    result = {
        "resources": receipts,
        "errors": errors,
        "all_stopped": containers_stopped and network_removed and host_stopped,
    }
    return result


def main():
    if os.environ.get("WEIR_MEMORY_INTEGRATION") != "1":
        sys.exit("requires WEIR_MEMORY_INTEGRATION=1; starts bounded owned containers")
    OUTPUT.mkdir(parents=True)
    (OUTPUT / "owner").write_text(OWNER + "\n")
    print(f"memory logs: {OUTPUT}", flush=True)
    resources = {"containers": [], "network": None, "host_mongo": None, "host_output": None}
    network, database = OWNER, OWNER + "-mongo"
    host_db = os.environ.get("WEIR_MEMORY_HOST_MONGO") == "1"
    database_address = "memory-mongo:27017"
    test_error = None
    try:
        engine = json.loads(
            run(
                [
                    "docker",
                    "info",
                    "--format",
                    '{"OSType":{{json .OSType}},"Architecture":{{json .Architecture}},"KernelVersion":{{json .KernelVersion}},"CgroupVersion":{{json .CgroupVersion}},"NCPU":{{json .NCPU}},"MemTotal":{{json .MemTotal}}}',
                ],
                log="engine.json",
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
                "requires actual Linux cgroup-v2 engine and same-architecture local pinned image; no QEMU"
            )
        (OUTPUT / "image.json").write_text(json.dumps(image, indent=2))
        env = dict(
            os.environ,
            GOPROXY="off",
            GOSUMDB="off",
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
        # Register exact candidate names before commands which can partially succeed.
        resources["network"] = network
        run(
            ["docker", "network", "create"]
            + ([] if host_db else ["--internal"])
            + ["--label", "weir.owner=" + OWNER, network]
        )
        if host_db:
            if sys.platform != "darwin":
                raise RuntimeError("host fallback is explicit Darwin fixture only")
            with socket.socket() as candidate:
                candidate.bind(("127.0.0.1", 0))
                port = candidate.getsockname()[1]
            data = OUTPUT / "mongo-data"
            data.mkdir()
            mongo_binary = ROOT / ".tools/mongodb-macos-aarch64--8.0.32/bin/mongod"
            resources["host_output"] = (OUTPUT / "mongo-host.log").open("w")
            resources["host_mongo"] = subprocess.Popen(
                [
                    str(mongo_binary),
                    "--dbpath",
                    str(data),
                    "--bind_ip",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--replSet",
                    "weir_memory",
                    "--wiredTigerCacheSizeGB=0.25",
                ],
                stdout=resources["host_output"],
                stderr=subprocess.STDOUT,
            )
            (OUTPUT / "mongo-host-pid").write_text(str(resources["host_mongo"].pid))
            for _ in range(50):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if resources["host_mongo"].poll() is not None:
                        raise RuntimeError("host fixture startup failed")
                    time.sleep(0.1)
            bootstrap = f'rs.initiate({{_id:"weir_memory",members:[{{_id:0,host:"127.0.0.1:{port}"}}]}}); for(let i=0;i<100;i++){{if(db.hello().isWritablePrimary)break;sleep(100)}}; if(!db.hello().isWritablePrimary)throw new Error("not primary"); print(db.version());'
            run(
                [
                    "mongosh",
                    "--quiet",
                    "--norc",
                    f"mongodb://127.0.0.1:{port}/?directConnection=true",
                    "--eval",
                    bootstrap,
                ],
                timeout=30,
                log="mongo-bootstrap.log",
            )
            database_address = f"host.docker.internal:{port}"
        else:
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
            run(
                ["docker", "exec", database, "mongosh", "--quiet", "--norc", "--eval", bootstrap],
                timeout=30,
                log="mongo-bootstrap.log",
            )
        for name, binary, selector in [
            ("guard", "overload.test", "^TestLinuxMemoryNative$"),
            ("cli", "app.test", "^TestLinuxMemoryCLI$"),
        ]:
            container = OWNER + "-" + name
            resources["containers"].append(container)
            run(
                [
                    "docker",
                    "create",
                    "--name",
                    container,
                    "--label",
                    "weir.owner=" + OWNER,
                    "--network",
                    network,
                    "--memory=512m",
                    "--memory-swap=512m",
                    "--cpus=2",
                    "--pids-limit=96",
                    "--read-only",
                    "--tmpfs",
                    "/tmp:rw,size=32m",
                    "--mount",
                    f"type=bind,src={OUTPUT},dst=/fixture,readonly",
                    "--env",
                    "WEIR_MEMORY_NATIVE=1",
                    "--env",
                    "WEIR_MEMORY_MONGO_ADDR=" + database_address,
                    "--entrypoint",
                    "/fixture/" + binary,
                    IMAGE,
                    "-test.run=" + selector,
                    "-test.count=3",
                    "-test.timeout=120s",
                    "-test.v",
                ]
            )
            result = run(["docker", "start", "--attach", container], timeout=150, log=name + ".log")
            state = inspect("container", container)
            (OUTPUT / (name + "-state.json")).write_text(json.dumps(state, indent=2))
            if result.returncode or state["ExitCode"] or state["OOMKilled"]:
                raise RuntimeError(f"{name} failed; preserve {OUTPUT / (name + '.log')}")
    except BaseException as error:
        test_error = f"{type(error).__name__}: {error}"
        raise
    finally:
        receipt = cleanup(resources)
        receipt["test_error"] = test_error
        try:
            (OUTPUT / "cleanup.json").write_text(json.dumps(receipt, indent=2))
        except Exception as error:
            receipt["errors"].append(f"cleanup receipt write: {error}")
        if receipt["errors"] or not receipt["all_stopped"]:
            print(
                f"Fixture cleanup has errors or unresolved resources: {receipt}; logs: {OUTPUT}",
                file=sys.stderr,
                flush=True,
            )
            if test_error is None:
                raise RuntimeError("fixture cleanup incomplete; see cleanup receipt")
        else:
            print(f"Stopped owned fixtures; logs: {OUTPUT}", flush=True)


if __name__ == "__main__":
    main()
