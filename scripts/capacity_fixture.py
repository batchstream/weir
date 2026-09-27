"""Owned standalone OCI lifecycle. Only used by explicit M22 calibration entry."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

REPO = Path(__file__).resolve().parent.parent
LABEL = "weir.capacity"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Fixture:
    def __init__(self, root, owner):
        if not __debug__ or os.environ.get("WEIR_CAPACITY_INTEGRATION") != "1":
            raise RuntimeError("unoptimized explicit opt-in required")
        if not re.fullmatch(r"weir-m22-[a-z0-9-]{1,32}", owner):
            raise ValueError("invalid owner")
        self.root, self.owner = Path(root).absolute(), owner
        self.root.mkdir(mode=0o700)
        made = []
        try:
            for name in ("home", "docker"):
                directory = self.root / name
                directory.mkdir(mode=0o700)
                made.append(directory)
        except OSError:
            for directory in reversed(made):
                directory.rmdir()
            self.root.rmdir()
            raise
        self.env = {k: os.environ[k] for k in ("PATH", "TMPDIR") if k in os.environ}
        self.env.update(HOME=str(self.root/"home"), DOCKER_CONFIG=str(self.root/"docker"),
                        DOCKER_HOST="unix:///var/run/docker.sock")
        self.started = time.monotonic()
        self.deadline = self.started + 2520
        self.number = 0
        self.containers = {}
        self.attempted = []
        self.network = None
        self.image = None
        self.preflight_complete = False
        self.mutations_started = False
        self.before = None
        self.first = None
        self.tag = owner+"-client:local"

    def save(self, name, value):
        data = value if isinstance(value, str) else json.dumps(value, indent=2)+"\n"
        if len(data.encode()) > 16<<20:
            raise RuntimeError("individual evidence bound")
        (self.root/name).write_text(data)
        self.check_size()

    def check_size(self):
        if sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file()) > 256<<20:
            raise RuntimeError("total evidence bound")

    def run(self, args, timeout=30, check=True, env=None):
        timeout = min(timeout, self.deadline-time.monotonic())
        if timeout <= 0:
            raise TimeoutError("invocation/cleanup budget")
        self.number += 1
        record = {"number":self.number, "start":time.time(), "argv":args}
        with (self.root/"commands.jsonl").open("a") as log:
            log.write(json.dumps(record)+"\n")
        # Disk files bound retained output, with a separate polling cap for noisy children.
        stdout = self.root/f"command-{self.number:04d}.out"
        stderr = self.root/f"command-{self.number:04d}.err"
        child = None
        try:
            with stdout.open("wb") as out, stderr.open("wb") as err:
                child = subprocess.Popen(args, cwd=REPO, env=env or self.env, stdout=out, stderr=err, start_new_session=True)
                until = time.monotonic()+timeout
                while child.poll() is None:
                    if time.monotonic() >= until or stdout.stat().st_size+stderr.stat().st_size > 16<<20:
                        raise TimeoutError("command time/output bound")
                    time.sleep(.05)
                result = subprocess.CompletedProcess(args, child.returncode, stdout.read_text(), stderr.read_text())
            if check and result.returncode:
                raise RuntimeError(f"command {self.number} exit {result.returncode}: {result.stderr[:1000]}")
            return result
        finally:
            if child is not None and child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=2)
            record.update(end=time.time(), exit=child.returncode if child else None)
            with (self.root/"commands.jsonl").open("a") as log:
                log.write(json.dumps(record)+"\n")

    def inventory(self):
        containers = self.run(["docker","ps","-a","--no-trunc","--format",'{{json .}}']).stdout
        # ps metadata only: never inspect foreign environment variables or mounts.
        cs = [{k:c[k] for k in ("ID","Names","State","Image")} for c in map(json.loads,filter(None,containers.splitlines()))]
        nets = []
        for identity in self.run(["docker","network","ls","-q","--no-trunc"]).stdout.split():
            obj = json.loads(self.run(["docker","network","inspect",identity]).stdout)[0]
            nets.append({k:obj[k] for k in ("Id","Name","Created","Driver","IPAM")})
        vols = self.run(["docker","volume","ls","--format",'{{json .}}']).stdout
        return {"containers":sorted(cs,key=lambda c:c["ID"]),"networks":sorted(nets,key=lambda n:n["Name"]),"volumes":sorted(vols.splitlines())}

    def preflight(self):
        self.before = self.inventory()
        self.save("inventory-initial.json", self.before)
        checks = [(["docker","ps","-q"],"running containers"),
                  (["docker","ps","-aq","--filter","label="+LABEL+"="+self.owner],"owner collision"),
                  (["docker","ps","-aq","--filter","name=^/"+self.owner+"-(es|weir|client|observer-weir|observer-es)$"],"name collision"),
                  (["docker","network","ls","-q","--filter","name=^"+self.owner+"$"],"network collision"),
                  (["docker","image","ls","-q","--filter","reference="+self.tag],"image tag collision")]
        for args, reason in checks:
            if self.run(args).stdout.strip():
                raise RuntimeError(reason)
        info = json.loads(self.run(["docker","info","--format",'{{json .}}']).stdout)
        selected = {k:info[k] for k in ("NCPU","MemTotal","KernelVersion","Architecture","ServerVersion","OSType")}
        if selected["NCPU"] != 8 or selected["MemTotal"] != 8319770624 or selected["Architecture"] != "aarch64" or selected["KernelVersion"] != "7.0.12-linuxkit":
            raise RuntimeError("frozen VM profile unavailable")
        self.save("vm.json",selected)
        self.preflight_complete = True
        return selected

    def first_mutation(self):
        if not self.preflight_complete:
            raise RuntimeError("preflight required")
        self.first = self.inventory()
        self.save("inventory-before-first-mutation.json",self.first)
        difference = inventory_diff(self.before,self.first)
        self.save("pre-mutation-difference.json",difference)
        if difference["nondefault_changed"]:
            raise RuntimeError("nondefault inventory changed during preparation")
        self.mutations_started = True

    def create_network(self):
        result = self.run(["docker","network","create","--internal","--label",LABEL+"="+self.owner,self.owner])
        self.network = result.stdout.strip()
        self.save("owned-network.json",{"id":self.network,"owner":self.owner})

    def create(self, role, spec):
        image,limits,extra,command=(spec[k] for k in ("image","limits","extra","command"))
        name = self.owner+"-"+role
        self.attempted.append(name)
        args = ["docker","create","--name",name,"--label",LABEL+"="+self.owner,"--cpus",str(limits["cpu"]),"--cpuset-cpus",limits["cpuset"],"--memory",str(limits["memory_mib"])+"m","--memory-swap",str(limits["memory_mib"])+"m","--pids-limit",str(limits["pids"]),"--ulimit","nofile=4096:4096","--log-driver","local","--log-opt","max-size=4m","--log-opt","max-file=1","--restart","no","--security-opt","no-new-privileges","--cap-drop","ALL"]
        args += extra+[image]+command
        cid = self.run(args).stdout.strip()
        self.containers[name] = cid
        self.save("owned-containers.json",self.containers)
        obj = self.owned(name)
        self.save(role+"-identity.json",{"id":cid,"image":obj["Image"],"host_config":obj["HostConfig"]})
        self.run(["docker","start",cid])
        return cid

    def owned(self, name):
        obj = json.loads(self.run(["docker","inspect",name]).stdout)[0]
        if obj["Name"] != "/"+name or obj["Config"]["Labels"].get(LABEL)!=self.owner or (name in self.containers and obj["Id"]!=self.containers[name]):
            raise RuntimeError("container owner/ID mismatch")
        return obj

    def cleanup(self):
        self.deadline = min(self.started+2700,time.monotonic()+180)
        result = []
        def attempt(name, action):
            try:
                action()
                result.append({"resource":name,"clean":True})
            except BaseException as exc:
                result.append({"resource":name,"clean":False,"error":str(exc)[:1000]})
        # Candidates are recorded before create. Only matching owner/name may be recovered.
        for name in reversed(self.attempted):
            def remove(n=name):
                inspect = self.run(["docker","inspect",n],check=False)
                if inspect.returncode:
                    # A failed query is not proof of absence; verify exact name listing.
                    if self.run(["docker","ps","-aq","--filter","name=^/"+n+"$"]).stdout.strip():
                        raise RuntimeError("container absence unconfirmed")
                    return
                obj = self.owned(n)
                cid = obj["Id"]
                self.run(["docker","stop","--time","8",cid],timeout=15)
                self.save(n+"-final-state.json",self.owned(n)["State"])
                log = self.run(["docker","logs","--tail","10000",cid],check=False)
                self.save(n+"-final.log",log.stdout+log.stderr)
                self.run(["docker","rm",cid])
                if self.run(["docker","ps","-aq","--filter","id="+cid]).stdout.strip():
                    raise RuntimeError("container removal unconfirmed")
            attempt(name,remove)
        if self.mutations_started:
            def remove_network():
                found = self.run(["docker","network","ls","-q","--no-trunc","--filter","name=^"+self.owner+"$"]).stdout.strip()
                if not found:
                    return
                obj = json.loads(self.run(["docker","network","inspect",found]).stdout)[0]
                if obj["Name"]!=self.owner or obj["Labels"].get(LABEL)!=self.owner or (self.network and obj["Id"]!=self.network) or obj["Containers"]:
                    raise RuntimeError("network owner/ID/empty check")
                self.run(["docker","network","rm",obj["Id"]])
            attempt("network",remove_network)
            def remove_image():
                found = self.run(["docker","image","ls","-q","--filter","reference="+self.tag]).stdout.strip()
                if not found:
                    return
                obj = json.loads(self.run(["docker","image","inspect",self.tag]).stdout)[0]
                if obj["Config"]["Labels"].get(LABEL)!=self.owner or (self.image and obj["Id"]!=self.image):
                    raise RuntimeError("image owner mismatch")
                self.run(["docker","image","rm",self.tag])
            attempt("client image tag",remove_image)
        self.save("cleanup.json",result)
        return all(r["clean"] for r in result)


def inventory_diff(before, after):
    b = {n["Name"]:n for n in before["networks"]}
    a = {n["Name"]:n for n in after["networks"]}
    return {"default_bridge_changed":b.get("bridge")!=a.get("bridge"),
            "bridge_before":b.get("bridge"),"bridge_after":a.get("bridge"),
            "nondefault_changed":before["containers"]!=after["containers"] or before["volumes"]!=after["volumes"] or {k:v for k,v in b.items() if k!="bridge"}!={k:v for k,v in a.items() if k!="bridge"}}
