import hashlib,json,time,urllib.request
from pathlib import Path
from runner import launch_suite
root=Path(__file__).resolve().parent
out=Path("/Users/liran/Projects/liran/go/weir/docs/measurements/2026-10-02/mongo-goals")
base="http://127.0.0.1:18089/"
while True:
    if urllib.request.urlopen(base+"final/state",timeout=15).read().strip()==b"completed":
        try:
            expected=urllib.request.urlopen(base+"final.sha256",timeout=15).read().decode().split()[0]
            archive=urllib.request.urlopen(base+"final.tar.gz",timeout=60).read()
            if hashlib.sha256(archive).hexdigest()!=expected: raise RuntimeError("final archive checksum mismatch")
            (out/"final.tar.gz").write_bytes(archive)
            (out/"final.sha256").write_text(expected+"  final.tar.gz\n")
            print("downloaded final",len(archive),expected,flush=True)
            break
        except urllib.error.HTTPError as exc:
            if exc.code!=404:raise
    time.sleep(2)
launch_suite(json.loads((root/"collect10-cases.json").read_text()),"collect10")
(out/"collect10-cases.json").write_bytes((root/"collect10"/"cases.json").read_bytes())
