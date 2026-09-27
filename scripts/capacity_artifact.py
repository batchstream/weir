"""Translate verified OCI packaging to Docker's legacy archive without rebuilding."""
import gzip
import hashlib
import io
import json
import tarfile


def docker_archive(source, destination, identity):
    """Preserve config bytes and diff_ids, omit all tags, verify every selected blob."""
    with tarfile.open(source) as archive:
        def blob(digest):
            value=archive.extractfile("blobs/sha256/"+digest.removeprefix("sha256:")).read()
            if "sha256:"+hashlib.sha256(value).hexdigest()!=digest:
                raise ValueError("OCI blob hash")
            return value
        config=blob(identity["config"])
        rootfs=json.loads(config)["rootfs"]["diff_ids"]
        if len(rootfs)!=len(identity["layers"]):
            raise ValueError("OCI rootfs/layer count")
        config_name=identity["config"].removeprefix("sha256:")+".json"
        with tarfile.open(destination,"w") as output:
            def add(name,data):
                entry=tarfile.TarInfo(name);entry.size=len(data);entry.mode=0o444
                output.addfile(entry,io.BytesIO(data))
            add(config_name,config)
            layers=[]
            for n,digest in enumerate(identity["layers"]):
                raw=blob(digest)
                if raw.startswith(b"\x1f\x8b"):
                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                        raw=stream.read((64<<20)+1)
                if len(raw)>64<<20 or "sha256:"+hashlib.sha256(raw).hexdigest()!=rootfs[n]:
                    raise ValueError("uncompressed layer bound/diff_id")
                name=rootfs[n].removeprefix("sha256:")+"/layer.tar"
                add(name,raw);layers.append(name)
            manifest=[{"Config":config_name,"RepoTags":[],"Layers":layers}]
            add("manifest.json",json.dumps(manifest).encode())
