#!/usr/bin/env python3
"""Explicit opt-in, native Linux M14 fixture. No image pulls, credentials or host cgroup writes."""
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
OWNER = "weir-m14-" + uuid.uuid4().hex[:12]
OUTPUT = ROOT / ".testdata" / OWNER


def run(args, timeout=60, env=None, log=None):
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if log:
        (OUTPUT / log).write_text(result.stdout)
    else:
        print(result.stdout, end="", flush=True)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args}; log={log}")
    return result


def inspect(kind, name):
    return json.loads(run(["docker", kind, "inspect", name], log="last-inspect.json").stdout)[0]


def main():
    if os.environ.get("WEIR_M14_INTEGRATION") != "1":
        sys.exit("requires WEIR_M14_INTEGRATION=1; starts bounded owned containers")
    OUTPUT.mkdir(parents=True)
    (OUTPUT / "owner").write_text(OWNER + "\n")
    print(f"M14 logs: {OUTPUT}", flush=True)
    engine = json.loads(run(["docker", "info", "--format", "{{json .}}"], log="engine.json").stdout)
    image = inspect("image", IMAGE)
    architecture = {"aarch64": "arm64", "x86_64": "amd64"}.get(engine["Architecture"], engine["Architecture"])
    if engine["OSType"] != "linux" or str(engine["CgroupVersion"]) != "2" or image["Architecture"] != architecture:
        raise RuntimeError("requires actual Linux cgroup-v2 engine and same-architecture local pinned image; no QEMU")
    # Store only selected image metadata; never inspect Docker credential files.
    image_metadata = {key: image[key] for key in ["Id", "Architecture", "Os", "RepoDigests"]}
    (OUTPUT / "image.json").write_text(json.dumps(image_metadata, indent=2))
    env = dict(os.environ, GOPROXY="off", GOSUMDB="off", CGO_ENABLED="0", GOOS="linux", GOARCH=architecture)
    commands = [(["go", "build", "-o", str(OUTPUT / "weir"), "./cmd/weir"], "build-cli.log"),
                (["go", "test", "-tags", "integration", "-c", "-o", str(OUTPUT / "overload.test"), "./internal/overload"], "build-overload.log"),
                (["go", "test", "-tags", "integration", "-c", "-o", str(OUTPUT / "app.test"), "./internal/app"], "build-app.log")]
    for args, log in commands:
        run(args, timeout=120, env=env, log=log)
    network, database = OWNER, OWNER + "-mongo"
    containers = []
    network_created = False
    host_mongo = None
    host_output = None
    host_db = os.environ.get("WEIR_M14_HOST_MONGO") == "1"
    database_address = "m14mongo:27017"
    try:
        run(["docker", "network", "create"] + ([] if host_db else ["--internal"]) + ["--label", "weir.owner=" + OWNER, network])
        network_created = True
        if host_db:
            if sys.platform != "darwin":
                raise RuntimeError("host fallback is explicit Darwin fixture only")
            with socket.socket() as candidate:
                candidate.bind(("127.0.0.1", 0))
                port = candidate.getsockname()[1]
            data = OUTPUT / "mongo-data"
            data.mkdir()
            mongo_binary = ROOT / ".tools/mongodb-macos-aarch64--8.0.32/bin/mongod"
            host_output = (OUTPUT / "mongo-host.log").open("w")
            host_mongo = subprocess.Popen([str(mongo_binary), "--dbpath", str(data), "--bind_ip", "127.0.0.1",
                                          "--port", str(port), "--replSet", "m14", "--wiredTigerCacheSizeGB=0.25"],
                                         stdout=host_output, stderr=subprocess.STDOUT)
            (OUTPUT / "mongo-host-pid").write_text(str(host_mongo.pid))
            for _ in range(50):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    if host_mongo.poll() is not None:
                        raise RuntimeError("host fixture startup failed")
                    time.sleep(.1)
            bootstrap = f'rs.initiate({{_id:"m14",members:[{{_id:0,host:"127.0.0.1:{port}"}}]}}); for(let i=0;i<100;i++){{if(db.hello().isWritablePrimary)break;sleep(100)}}; if(!db.hello().isWritablePrimary)throw new Error("not primary"); print(db.version());'
            run(["mongosh", "--quiet", "--norc", f"mongodb://127.0.0.1:{port}/?directConnection=true", "--eval", bootstrap], timeout=30, log="mongo-bootstrap.log")
            database_address = f"host.docker.internal:{port}"
        else:
            run(["docker", "run", "-d", "--name", database, "--label", "weir.owner=" + OWNER,
                 "--network", network, "--network-alias", "m14mongo", "--memory=768m", "--memory-swap=768m",
                 "--cpus=1", "--pids-limit=128", "--tmpfs", "/data/db:rw,size=256m", "--entrypoint", "mongod", IMAGE,
                 "--bind_ip_all", "--replSet", "m14", "--wiredTigerCacheSizeGB=0.25", "--setParameter", "enableTestCommands=1"])
            containers.append(database)
            bootstrap = '''for(let i=0;i<100;i++){try{db.adminCommand({ping:1});break}catch(e){sleep(100)}};
    rs.initiate({_id:"m14",members:[{_id:0,host:"m14mongo:27017"}]});
    for(let i=0;i<100;i++){if(db.hello().isWritablePrimary)break;sleep(100)};
    if(!db.hello().isWritablePrimary)throw new Error("not primary"); print(db.version());'''
            run(["docker", "exec", database, "mongosh", "--quiet", "--norc", "--eval", bootstrap], timeout=30, log="mongo-bootstrap.log")
        for name, binary, selector in [("guard", "overload.test", "^TestLinuxMemoryNative$"), ("cli", "app.test", "^TestLinuxMemoryCLI$")]:
            container = OWNER + "-" + name
            run(["docker", "create", "--name", container, "--label", "weir.owner=" + OWNER,
                 "--network", network, "--memory=512m", "--memory-swap=512m", "--cpus=2", "--pids-limit=96",
                 "--read-only", "--tmpfs", "/tmp:rw,size=32m", "--mount", f"type=bind,src={OUTPUT},dst=/fixture,readonly",
                 "--env", "WEIR_M14_NATIVE=1", "--env", "WEIR_M14_MONGO_ADDR=" + database_address, "--entrypoint", "/fixture/" + binary, IMAGE,
                 "-test.run=" + selector, "-test.count=3", "-test.timeout=120s", "-test.v"])
            containers.append(container)
            result = run(["docker", "start", "--attach", container], timeout=150, log=name + ".log")
            state = inspect("container", container)["State"]
            (OUTPUT / (name + "-state.json")).write_text(json.dumps(state, indent=2))
            if result.returncode or state["ExitCode"] or state["OOMKilled"]:
                raise RuntimeError(f"{name} failed; preserve {OUTPUT / (name + '.log')}")
    finally:
        cleanup = []
        for container in reversed(containers):
            info = inspect("container", container)
            if info["Config"]["Labels"].get("weir.owner") != OWNER:
                raise RuntimeError("container owner mismatch; refusing cleanup")
            run(["docker", "logs", container], log=container + ".log")
            run(["docker", "stop", "--timeout=6", container], timeout=15)
            state = inspect("container", container)["State"]
            receipt = {"name": container, "state": state}
            cleanup.append(receipt)
            run(["docker", "rm", "-v", container])
        if network_created:
            info = inspect("network", network)
            if info["Labels"].get("weir.owner") != OWNER:
                raise RuntimeError("network owner mismatch; refusing cleanup")
            run(["docker", "network", "rm", network])
        if host_mongo is not None:
            host_mongo.send_signal(signal.SIGTERM)
            try:
                host_mongo.wait(timeout=10)
            except subprocess.TimeoutExpired:
                host_mongo.kill()
                host_mongo.wait(timeout=3)
                raise RuntimeError("host mongod exceeded shutdown bound")
            host_output.close()
            receipt = {"host_mongo_pid": host_mongo.pid, "returncode": host_mongo.returncode, "bind": "127.0.0.1"}
            cleanup.append(receipt)
            if (OUTPUT / "owner").read_text().strip() != OWNER:
                raise RuntimeError("host fixture owner mismatch")
            shutil.rmtree(OUTPUT / "mongo-data")
        (OUTPUT / "cleanup.json").write_text(json.dumps(cleanup, indent=2))
        # Delete only the three known generated binaries after fixture shutdown.
        if (OUTPUT / "owner").read_text().strip() != OWNER:
            raise RuntimeError("output owner mismatch")
        for binary in ["weir", "app.test", "overload.test"]:
            (OUTPUT / binary).unlink(missing_ok=True)
        print(f"Stopped owned fixtures; logs: {OUTPUT}", flush=True)


if __name__ == "__main__":
    main()
