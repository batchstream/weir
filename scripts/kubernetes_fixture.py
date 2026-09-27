"""Owned kind lifecycle for test-kubernetes.py; no current context or user config."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

REPO = Path(__file__).resolve().parent.parent
NODE = "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"


class Fixture:
    def __init__(self, root, owner):
        self.root, self.owner = Path(root).resolve(), owner
        self.root.mkdir(parents=True, exist_ok=True)
        for folder in ("home", "docker"):
            (self.root / folder).mkdir(exist_ok=True)
        self.env = {k: os.environ[k] for k in ("PATH", "TMPDIR") if k in os.environ}
        self.env.update(HOME=str(self.root / "home"), DOCKER_CONFIG=str(self.root / "docker"),
                        DOCKER_HOST="unix:///var/run/docker.sock", KUBECONFIG=str(self.root / "kubeconfig"),
                        KIND_EXPERIMENTAL_PROVIDER="docker")
        self.nodes, self.network, self.children = {}, None, []
        self.cluster_started = False
        self.deadline = float("inf")
        self.command_number = 0

    def argv(self, args):
        if args[0] == "kubectl":
            return [str(REPO / ".tools/m20/kubectl"), "--kubeconfig", str(self.root / "kubeconfig"),
                    "--context", "kind-" + self.owner, "--request-timeout=10s", *args[1:]]
        if args[0] == "kind":
            return [str(REPO / ".tools/m20/kind-darwin-arm64"), *args[1:]]
        return args

    def run(self, args, timeout=30, check=True, **kwargs):
        timeout = min(timeout, self.deadline - time.monotonic())
        if timeout <= 0:
            raise TimeoutError("fixture phase deadline")
        self.command_number += 1
        with (self.root / "commands.jsonl").open("a") as log:
            log.write(json.dumps({"number": self.command_number, "time": time.time(), "argv": self.argv(args)}) + "\n")
        result = subprocess.run(self.argv(args), env=self.env, capture_output=True, text=True,
                                timeout=timeout, **kwargs)
        if check and result.returncode:
            raise RuntimeError(f"{args}: {result.stdout}\n{result.stderr}")
        return result

    def save(self, name, value):
        (self.root / name).write_text(value if isinstance(value, str) else json.dumps(value, indent=2))

    def kube(self, *args):
        return ["kubectl", "-n", "m21", *args]

    def apply(self, name, value):
        path = self.root / (name + ".json")
        self.save(path.name, value)
        self.run(self.kube("apply", "-f", str(path)))

    def start(self, name, args):
        output = (self.root / (name + ".log")).open("w")
        child = subprocess.Popen(self.argv(args), env=self.env, stdout=output, stderr=subprocess.STDOUT)
        self.children.append((child, output))
        return child

    def wait(self, child, timeout=65):
        child.wait(timeout=min(timeout, max(1, self.deadline-time.monotonic())))
        if child.returncode:
            raise RuntimeError(f"fixture child {child.pid} exit {child.returncode}")

    def owner_node(self, name):
        result = self.run(["docker", "inspect", "--format", "{{json .}}", name])
        obj = json.loads(result.stdout)
        assert obj["Config"]["Labels"].get("io.x-k8s.kind.cluster") == self.owner
        assert name in {self.owner + "-" + role for role in ("control-plane", "worker", "worker2")}
        old = self.nodes.get(name)
        assert old is None or old["id"] == obj["Id"]
        return obj

    def bootstrap(self, profile):
        assert not self.run(["docker", "ps", "-q"]).stdout.strip(), "another running fixture exists"
        existing = self.run(["docker", "ps", "-aq", "--filter", "label=io.x-k8s.kind.cluster="+self.owner])
        assert not existing.stdout.strip(), "owner already belongs to an existing cluster"
        names = "name=^/"+self.owner+"-(control-plane|worker|worker2)$"
        assert not self.run(["docker","ps","-aq","--filter",names]).stdout.strip(), "node name already exists"
        assert not self.run(["docker", "network", "ls", "-q", "--filter", "name=^kind$"]).stdout.strip()
        config = {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
                  "networking": {"apiServerAddress": "127.0.0.1", "apiServerPort": 0},
                  "nodes": [{"role": role, "image": NODE} for role in ("control-plane", "worker", "worker")]}
        self.save("kind.json", config)
        self.deadline = time.monotonic() + 1200
        started = time.monotonic()
        self.cluster_started = True
        child = self.start("bootstrap", ["kind", "create", "cluster", "--name", self.owner,
                           "--config", str(self.root/"kind.json"), "--kubeconfig", str(self.root/"kubeconfig"),
                           "--wait", "5m", "--retain"])
        try:
            while child.poll() is None:
                for role, limits in profile["node_limits"].items():
                    name = self.owner + "-" + role
                    if name in self.nodes:
                        continue
                    result = self.run(["docker", "inspect", "--format", "{{.Id}}", name], check=False)
                    if result.returncode:
                        continue
                    obj = self.owner_node(name)
                    self.nodes[name] = {"id": obj["Id"], "volumes": [m["Name"] for m in obj["Mounts"] if m["Type"] == "volume"]}
                    self.save("owned-nodes.json", self.nodes)
                    self.run(["docker", "update", "--memory", str(limits["memory_mib"])+"m", "--memory-swap", str(limits["memory_mib"])+"m",
                              "--cpus", str(limits["cpu"]), "--pids-limit", str(limits["pids"]), name])
                net = self.run(["docker", "network", "inspect", "kind"], check=False)
                if net.returncode == 0 and self.network is None:
                    self.network = json.loads(net.stdout)[0]["Id"]
                    self.save("owned-network.json", {"id": self.network})
                time.sleep(.2)
            self.wait(child, 1)
            assert len(self.nodes) == 3
            self.save("bootstrap-seconds.json", time.monotonic()-started)
            nodes=json.loads(self.run(["kubectl", "get", "nodes", "-o", "json"]).stdout)
            assert len(nodes["items"])==3
            assert all(n["status"]["nodeInfo"]["architecture"]=="arm64" and n["status"]["nodeInfo"]["kubeletVersion"]=="v1.36.4" for n in nodes["items"])
            self.save("nodes.json", nodes)
            for name in self.nodes:
                obj = self.owner_node(name)
                self.save(name+"-limits.json", {"id": obj["Id"], "image": obj["Image"], "limits": {k:obj["HostConfig"][k] for k in ("Memory","MemorySwap","NanoCpus","PidsLimit")}})
                self.save(name+"-system.log", self.run(["docker","exec",name,"sh","-c","uname -a; cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.events"]).stdout)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)

    def cleanup(self):
        # Every independent cleanup is attempted even after a previous failure.
        self.deadline = time.monotonic()+180
        results = []
        def attempt(name, action):
            try:
                action()
                results.append({"resource": name, "clean": True})
            except Exception as exc:
                results.append({"resource": name, "clean": False, "error": str(exc)[:1500]})
        def child_stop(child, output):
            try:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=3)
            finally:
                output.close()
        for child, output in self.children:
            attempt(f"child {child.pid}", lambda c=child, o=output: child_stop(c,o))
        # --retain can leave a node created just before bootstrap fails, before
        # the observation loop recorded it. Preflight proved this owner absent.
        def discover_node(name):
            if name in self.nodes:
                return
            found = self.run(["docker","inspect","--format","{{.Id}}",name], check=False)
            if found.returncode:
                return
            obj = self.owner_node(name)
            self.nodes[name] = {"id":obj["Id"], "volumes":[m["Name"] for m in obj["Mounts"] if m["Type"]=="volume"]}
            self.save("owned-nodes.json",self.nodes)
        def discover_network():
            if self.network is None:
                found = self.run(["docker","network","inspect","kind"],check=False)
                if found.returncode==0:
                    self.network=json.loads(found.stdout)[0]["Id"]
                    self.save("owned-network.json", {"id":self.network})
        if self.cluster_started:
            for role in ("control-plane", "worker", "worker2"):
                name = self.owner+"-"+role
                attempt("discover "+name,lambda n=name: discover_node(n))
            attempt("discover owned network",discover_network)
        # Recover a fault independently of deletion, using the exact recorded ID.
        for name, saved in self.nodes.items():
            def recover(n=name, s=saved):
                obj = self.owner_node(n)
                if obj["State"]["Paused"]:
                    self.run(["docker", "unpause", s["id"]], 10)
            attempt(name+" restore", recover)
        for name, saved in self.nodes.items():
            def remove(n=name, s=saved):
                self.owner_node(n)
                self.run(["docker", "rm", "-f", s["id"]], 30)
            attempt(name, remove)
            for volume in saved["volumes"]:
                # Volume ownership was recorded from this exact newly created node.
                def remove_volume(v=volume):
                    found=self.run(["docker","volume","inspect",v],check=False)
                    if found.returncode==0:
                        self.run(["docker","volume","rm",v])
                attempt(volume, remove_volume)
        if self.network:
            def remove_network():
                obj=json.loads(self.run(["docker","network","inspect",self.network]).stdout)[0]
                assert obj["Id"]==self.network and not obj["Containers"]
                self.run(["docker","network","rm",self.network])
            attempt("owned empty network", remove_network)
        attempt("generated kubeconfig", lambda: (self.root/"kubeconfig").unlink(missing_ok=True))
        self.save("cleanup.json", results)
        self.save("after-containers.log", self.run(["docker","ps","-a","--format","{{.ID}} {{.Names}} {{.Status}}"],check=False).stdout)
        self.save("after-networks.log", self.run(["docker","network","ls","--format","{{.ID}} {{.Name}}"],check=False).stdout)
        return all(item["clean"] for item in results)
