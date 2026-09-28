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

    def run(self, args, timeout=30, check=True, options=None):
        options = options or {}
        monitor = options.get("monitor")
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
            child = subprocess.Popen(args, cwd=REPO, env=options.get("env", self.env),
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
                raise RuntimeError(f"command {command_number} exit {result.returncode}: {result.stderr[:1000]}")
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
                  (["docker","ps","-aq","--filter","name=^/"+self.owner+"-(es|weir|client)$"],"name collision"),
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
        for role, observer in getattr(self, 'observers', {}).items():
            attempt('observer '+role, observer.stop)
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


def group_states(group):
    """Bound the selected ps query too, including its timeout and final read."""
    query = subprocess.Popen(["ps", "-o", "pid=,pgid=,stat=", "-g", str(group)],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             start_new_session=True)
    raw = bytearray()
    until = time.monotonic()+2
    try:
        os.set_blocking(query.stdout.fileno(), False)
        with selectors.DefaultSelector() as selector:
            selector.register(query.stdout, selectors.EVENT_READ)
            while True:
                if time.monotonic() >= until:
                    raise RuntimeError("owned group query timeout")
                if not selector.select(.05):
                    continue
                chunk = os.read(query.stdout.fileno(), 4096)
                if not chunk:
                    break
                if len(raw)+len(chunk) > 65536:
                    raise RuntimeError("owned group query output bound")
                raw.extend(chunk)
        if query.wait(timeout=max(.01,until-time.monotonic())) not in (0, 1):
            raise RuntimeError("owned process group state unconfirmed")
        return raw.decode().splitlines()
    finally:
        if query.poll() is None:
            query.kill()
        query.wait(timeout=2)
        query.stdout.close()


def stop_group(child):
    """Own session only. Darwin can report EPERM for a group of zombies."""
    def send(sig):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            return
        except PermissionError:
            # Do not treat EPERM as absence: query only this owned process group.
            for line in group_states(child.pid):
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


class Observer:
    """One same-container exec, bounded pipes; stdin EOF cancels and joins helper."""
    def __init__(self, options):
        self.root = Path(options['root'])
        self.role = options['role']
        self.command = options['command']
        self.stream_limits = options.get('stream_limits', [64 << 20, 64 << 20])
        if (not isinstance(self.stream_limits, (list, tuple)) or len(self.stream_limits) != 2 or
                any(type(n) is not int or not 0 < n <= 64 << 20 for n in self.stream_limits)):
            raise ValueError('observer stream limits')
        self.streams = [bytearray(), bytearray()]
        self.entries = []
        self.parsed = 0
        self.eof = [False, False]
        self.failure = None
        self.first_exit_observed = None
        self.stop_requested = None
        self.stopped = None
        self.stop_error = None
        self.joined = False
        self.pipes_ready = False
        self.started = time.monotonic()
        self.child = subprocess.Popen(self.command, env=options.get('env'), stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.pipes = [self.child.stdout, self.child.stderr]
        try:
            for pipe in self.pipes:
                os.set_blocking(pipe.fileno(), False)
            self.pipes_ready = True
            self.record()
        except BaseException as exc:
            try:
                self.stop()
            except BaseException as closing:
                exc.add_note('observer initialization stop: '+str(closing))
            raise

    def record(self):
        code = self.child.poll()
        if code is not None and self.first_exit_observed is None:
            self.first_exit_observed = time.monotonic()
        value = dict(command=self.command, pid=self.child.pid, role=self.role, elapsed=time.monotonic()-self.started,
                     exit=code, bytes=[len(raw) for raw in self.streams], eof=self.eof, failure=self.failure,
                     started_monotonic=self.started, first_exit_observed_monotonic=self.first_exit_observed,
                     stop_requested_monotonic=self.stop_requested, stopped_monotonic=self.stopped, joined=self.joined,
                     stop_error=str(self.stop_error) if self.stop_error is not None else None)
        (self.root/(self.role+'-exec.json')).write_text(json.dumps(value, indent=2)+'\n')

    def poll(self):
        if self.stop_error is not None:
            raise self.stop_error
        if self.stopped is not None:
            return self.entries
        changed = False
        for index, pipe in enumerate(self.pipes):
            if self.eof[index] or pipe.closed:
                continue
            target = self.root/(self.role+('.jsonl' if index == 0 else '.err'))
            target.touch(exist_ok=True)
            # Return to the owner and the other pipe even under continuous output.
            for _ in range(16):
                try:
                    chunk = os.read(pipe.fileno(), 65536)
                except BlockingIOError:
                    break
                if not chunk:
                    self.eof[index] = True
                    break
                available = min((64 << 20)-sum(map(len, self.streams)),
                                self.stream_limits[index]-len(self.streams[index]))
                self.streams[index].extend(chunk[:available])
                with target.open('ab') as output:
                    output.write(chunk[:available])
                if len(chunk) > available:
                    self.failure = self.failure or 'observer output bound'
                changed = True
        raw = self.streams[0]
        end = raw.rfind(b'\n')+1
        try:
            for line in raw[self.parsed:end].splitlines():
                self.entries.append(json.loads(line))
        except ValueError as exc:
            self.failure = self.failure or 'invalid observer JSON: '+str(exc)
        self.parsed = end
        if any(entry.get('errors') for entry in self.entries):
            self.failure = self.failure or self.role+' observer sample error'
        if self.child.poll() not in (None, 0):
            self.failure = self.failure or self.role+' observer exited: '+bytes(self.streams[1]).decode(errors='replace')
        if all(self.eof) and raw and not raw.endswith(b'\n'):
            self.failure = self.failure or 'truncated observer output'
        if self.failure:
            error = RuntimeError(self.failure)
            if self.stop_requested is None:
                try:
                    self.record()
                except BaseException as recording:
                    error.add_note('observer record: '+str(recording))
            raise error
        if self.stop_requested is None and (changed or self.child.poll() is not None):
            self.record()
        return self.entries

    def write_input(self, data, deadline):
        """A single bounded stdin transfer, drained concurrently with output."""
        if self.stop_requested is not None:
            raise RuntimeError('observer already stopped')
        if not isinstance(data, bytes) or not 0 < len(data) <= 64 << 20:
            raise ValueError('observer stdin bound')
        os.set_blocking(self.child.stdin.fileno(), False)
        offset = 0
        with selectors.DefaultSelector() as selector:
            selector.register(self.child.stdin, selectors.EVENT_WRITE)
            while offset < len(data):
                if time.monotonic() >= deadline:
                    raise RuntimeError('observer stdin deadline')
                self.poll()
                if self.child.poll() is not None:
                    raise RuntimeError('observer exited during stdin transfer')
                for key, _ in selector.select(.05):
                    try:
                        offset += os.write(key.fd, data[offset:offset+65536])
                    except BlockingIOError:
                        pass
        self.child.stdin.close()
        return offset

    def stop(self, deadline=None):
        if self.stop_requested is not None:
            if self.stop_error is not None:
                raise self.stop_error
            return
        self.stop_requested = time.monotonic()
        errors = [RuntimeError(self.failure)] if self.failure else []
        until = min(self.stop_requested+4, deadline-4) if deadline is not None else self.stop_requested+4
        try:
            self.child.stdin.close()
        except BaseException as exc:
            errors.append(exc)
            # A failed buffered close must still deliver EOF without replaying
            # buffered input. These are the pipes owned by this Popen only.
            try:
                self.child.stdin.raw.close()
            except BaseException as closing:
                errors.append(closing)
        try:
            if self.pipes_ready:
                while time.monotonic() < until:
                    try:
                        self.poll()
                    except RuntimeError as exc:
                        if str(exc) != self.failure:
                            raise
                        if not errors:
                            errors.append(exc)
                    if self.child.poll() is not None and all(self.eof):
                        break
                    time.sleep(.02)
                if self.child.poll() is None or not all(self.eof):
                    self.failure = self.failure or 'observer exec did not stop/drain after stdin EOF'
                    errors.append(RuntimeError(self.failure))
        except BaseException as exc:
            errors.append(exc)
        finally:
            try:
                stop_group(self.child)
                self.joined = True
            except BaseException as exc:
                errors.append(exc)
                # A group-signal or Wait failure cannot skip the leader's Wait.
                try:
                    if self.child.returncode is None:
                        self.child.kill()
                except BaseException as signaling:
                    errors.append(signaling)
                try:
                    remaining = 2 if deadline is None else min(2, max(.001, deadline-time.monotonic()))
                    self.child.wait(timeout=remaining)
                    self.joined = True
                except BaseException as waiting:
                    errors.append(waiting)
            try:
                if self.pipes_ready:
                    self.poll()
            except BaseException as exc:
                errors.append(exc)
            finally:
                for pipe in (self.child.stdin, *self.pipes):
                    try:
                        pipe.close()
                    except BaseException as exc:
                        errors.append(exc)
                        try:
                            pipe.raw.close()
                        except BaseException as closing:
                            errors.append(closing)
                if self.joined and all(pipe.closed for pipe in (self.child.stdin, *self.pipes)):
                    self.stopped = time.monotonic()
        if errors:
            self.stop_error = errors[0]
            for exc in errors[1:]:
                self.stop_error.add_note('observer stop: '+str(exc))
        # Evidence storage is fallible and owns none of the process resources.
        try:
            self.record()
        except BaseException as exc:
            if self.stop_error is None:
                self.stop_error = exc
            else:
                self.stop_error.add_note('observer record: '+str(exc))
        if self.stop_error is not None:
            raise self.stop_error
