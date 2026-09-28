"""Owned standalone OCI lifecycle. Only used by explicit M22 calibration entry."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import selectors
import subprocess
import time

REPO = Path(__file__).resolve().parent.parent
LABEL = "weir.capacity"
from capacity_contract import PLAN

STREAM_LIMIT = PLAN["output"]["stream_bytes"]
OUTPUT_LIMIT = PLAN["output"]["combined_bytes"]
EVIDENCE_LIMIT = PLAN["budgets"]["evidence_bytes"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class FixtureInterrupted(BaseException):
    pass


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
        self.cleaning = False
        self.diagnostic_errors = []

    def evidence_size(self):
        return sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())

    def save(self, name, value):
        data = value if isinstance(value, str) else json.dumps(value, indent=2)+"\n"
        raw = data.encode()
        target = self.root/name
        old = target.stat().st_size if target.exists() else 0
        if len(raw) > OUTPUT_LIMIT or self.evidence_size()-old+len(raw) > EVIDENCE_LIMIT:
            raise RuntimeError("individual/total evidence bound before write")
        target.write_bytes(raw)

    def check_size(self):
        if self.evidence_size() > EVIDENCE_LIMIT:
            raise RuntimeError("total evidence bound")

    def command_record(self, record):
        raw = (json.dumps(record)+"\n").encode()
        target = self.root/"commands.jsonl"
        if self.evidence_size()+len(raw) > EVIDENCE_LIMIT:
            raise RuntimeError("command evidence bound")
        with target.open("ab") as log:
            log.write(raw)

    def diagnostic_failure(self, exc):
        self.diagnostic_errors.append(str(exc)[:1000])
        if not self.cleaning:
            raise exc

    def run(self, args, timeout=30, check=True, env=None, monitor=None):
        timeout = min(timeout, self.deadline-time.monotonic())
        if timeout <= 0:
            raise TimeoutError("invocation/cleanup budget")
        self.number += 1
        command_number = self.number
        record = {"number": command_number, "start": time.time(), "argv": args}
        try:
            self.command_record(record)
        except (OSError, RuntimeError) as exc:
            self.diagnostic_failure(exc)
        child = None
        streams = [bytearray(), bytearray()]
        retained = 0
        truncated = False
        available = max(0, min(OUTPUT_LIMIT, EVIDENCE_LIMIT-self.evidence_size()-4096))
        if self.cleaning:
            available = OUTPUT_LIMIT
        # Pipes cap kernel buffering. Only bounded chunks and retained prefixes enter
        # memory; the child never receives an evidence-file descriptor.
        try:
            child = subprocess.Popen(args, cwd=REPO, env=env or self.env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     start_new_session=True)
            until = time.monotonic()+timeout
            next_monitor = time.monotonic()+2
            with selectors.DefaultSelector() as selector:
                for index, pipe in enumerate((child.stdout, child.stderr)):
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, index)
                while selector.get_map():
                    if time.monotonic() >= until:
                        raise TimeoutError("command timeout")
                    if monitor and time.monotonic() >= next_monitor:
                        monitor()
                        next_monitor = time.monotonic()+2
                    for key, _ in selector.select(min(.05, max(0, until-time.monotonic()))):
                        chunk = os.read(key.fd, PLAN["output"]["read_chunk_bytes"])
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        dest = streams[key.data]
                        limit = min(STREAM_LIMIT-len(dest), available-retained)
                        dest.extend(chunk[:limit])
                        retained += min(len(chunk), limit)
                        if len(chunk) > limit:
                            truncated = True
                            raise RuntimeError("command output/evidence bound")
                child.wait(timeout=max(.01, until-time.monotonic()))
            result = subprocess.CompletedProcess(args, child.returncode,
                                                streams[0].decode(errors="replace"),
                                                streams[1].decode(errors="replace"))
            if check and result.returncode:
                raise RuntimeError(f"command {self.number} exit {result.returncode}: {result.stderr[:1000]}")
            return result
        finally:
            # Stop the entire group even when its leader already exited and left
            # descendants holding pipes. Always reap the leader.
            if child is not None:
                try:
                    stop_group(child)
                finally:
                    child.stdout.close()
                    child.stderr.close()
            record.update(end=time.time(), exit=child.returncode if child else None,
                          retained_bytes=[len(s) for s in streams], truncated=truncated)
            for suffix, raw in zip(("out", "err"), streams):
                try:
                    target = self.root/f"command-{command_number:04d}.{suffix}"
                    if self.evidence_size()+len(raw) > EVIDENCE_LIMIT:
                        raise RuntimeError("final evidence bound")
                    target.write_bytes(raw)
                except (OSError, RuntimeError) as exc:
                    self.diagnostic_failure(exc)
            try:
                self.command_record(record)
            except (OSError, RuntimeError) as exc:
                self.diagnostic_failure(exc)

    def inventory(self):
        containers = self.run(["docker","ps","-a","--no-trunc","--format",'{{json .}}']).stdout
        # ps metadata only: never inspect foreign environment variables or mounts.
        cs = [{k:c[k] for k in ("ID","Names","State","Image")} for c in map(json.loads,filter(None,containers.splitlines()))]
        nets = []
        for identity in self.run(["docker","network","ls","-q","--no-trunc"]).stdout.split():
            template = '{'+','.join('"'+k+'":{{json .'+k+'}}' for k in ("Id","Name","Created","Driver","IPAM"))+'}'
            obj = json.loads(self.run(["docker","network","inspect","--format",template,identity]).stdout)
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
        fields = ("NCPU","MemTotal","KernelVersion","Architecture","ServerVersion","OSType")
        template = '{'+','.join('"'+k+'":{{json .'+k+'}}' for k in fields)+'}'
        selected = json.loads(self.run(["docker","info","--format",template]).stdout)
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
        args = ["docker","create","--name",name,"--label",LABEL+"="+self.owner,"--cpus",str(limits["cpu"]),"--cpuset-cpus",limits["cpuset"],"--memory",str(limits["memory_mib"])+"m","--memory-swap",str(limits["memory_mib"])+"m","--pids-limit",str(limits["pids"]),"--ulimit","nofile=4096:4096","--log-driver","local","--log-opt","max-size=4m","--log-opt","max-file=1","--log-opt","compress=false","--restart","no","--security-opt","no-new-privileges","--cap-drop","ALL"]
        args += extra+[image]+command
        cid = self.run(args).stdout.strip()
        self.containers[name] = cid
        self.save("owned-containers.json",self.containers)
        obj = self.owned(name)
        self.save(role+"-identity.json",{"id":cid,"image":obj["Image"],"host_config":obj["HostConfig"]})
        self.run(["docker","start",cid])
        return cid

    def owned(self, name):
        template = '{"Config":{"Labels":{{json .Config.Labels}}},'+','.join('"'+k+'":{{json .'+k+'}}' for k in ("Id","Name","Image","HostConfig","State","RestartCount"))+'}'
        obj = json.loads(self.run(["docker","inspect","--format",template,name]).stdout)
        if obj["Name"] != "/"+name or obj["Config"]["Labels"].get(LABEL)!=self.owner or (name in self.containers and obj["Id"]!=self.containers[name]):
            raise RuntimeError("container owner/ID mismatch")
        return obj

    def cleanup(self):
        self.cleaning = True
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
                inspect = self.run(["docker","inspect","--format","{{.Id}}",n],check=False)
                if inspect.returncode:
                    # A failed query is not proof of absence; verify exact name listing.
                    if self.run(["docker","ps","-aq","--filter","name=^/"+n+"$"]).stdout.strip():
                        raise RuntimeError("container absence unconfirmed")
                    return
                obj = self.owned(n)
                cid = obj["Id"]
                self.run(["docker","stop","--time","8",cid],timeout=15)
                final = self.owned(n)
                if final["State"].get("Running"):
                    raise RuntimeError("owned container still running after stop: "+cid)
                def diagnostics():
                    self.save(n+"-final-state.json", final["State"])
                    log = self.run(["docker", "logs", "--tail", "10000", cid])
                    self.save(n+"-final.log", log.stdout+log.stderr)
                attempt(n+": diagnostics", diagnostics)
                # Diagnostic failure cannot skip independently verified removal.
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
        def reclaim_intermediates():
            reclaimed=[]
            for name in ("client","client-host","client.tar","product-docker.tar"):
                path=self.root/name
                if path.exists():
                    if path.is_symlink() or not path.is_file():
                        raise RuntimeError("intermediate type changed")
                    reclaimed.append({"name":name,"bytes":path.stat().st_size,"sha256":sha(path)})
                    path.unlink()
            self.save("intermediate-reclamation.json",reclaimed)
        attempt("generated build/import intermediates",reclaim_intermediates)
        self.cleanup_result = result
        try:
            self.save("cleanup.json", {"resources": result, "diagnostic_errors": self.diagnostic_errors})
        except (OSError, RuntimeError) as exc:
            self.diagnostic_errors.append(str(exc)[:1000])
        return all(r["clean"] for r in result) and not self.diagnostic_errors


def stop_group(child):
    """Own session only. Darwin can report EPERM for a group of zombies."""
    def send(sig):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            return
        except PermissionError:
            # Do not treat EPERM as absence: query only this owned process group.
            query = subprocess.run(["ps", "-o", "pid=,pgid=,stat=", "-g", str(child.pid)],
                                   capture_output=True, text=True, timeout=2)
            if query.returncode not in (0, 1) or len(query.stdout) > 65536:
                raise RuntimeError("owned process group state unconfirmed")
            for line in query.stdout.splitlines():
                pid, group, state = line.split()
                if int(group) == child.pid and not state.startswith("Z"):
                    raise RuntimeError("owned process group remains: "+pid)
    try:
        send(signal.SIGTERM)
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
        send(signal.SIGKILL)
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=2)


def inventory_diff(before, after):
    b = {n["Name"]:n for n in before["networks"]}
    a = {n["Name"]:n for n in after["networks"]}
    return {"default_bridge_changed":b.get("bridge")!=a.get("bridge"),
            "bridge_before":b.get("bridge"),"bridge_after":a.get("bridge"),
            "nondefault_changed":before["containers"]!=after["containers"] or before["volumes"]!=after["volumes"] or {k:v for k,v in b.items() if k!="bridge"}!={k:v for k,v in a.items() if k!="bridge"}}
