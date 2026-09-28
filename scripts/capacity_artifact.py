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


def application_binary(compressed, identity):
    """Read only the unique regular application member; never extract paths."""
    def require(value, message):
        if not value:
            raise ValueError(message)
    def digest(raw):
        return 'sha256:'+hashlib.sha256(raw).hexdigest()
    require(len(compressed) == identity['size'] and digest(compressed) == identity['digest'], 'application compressed size/hash')
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read((64 << 20)+1)
    require(len(raw) <= 64 << 20 and digest(raw) == identity['diff_id'], 'application diffID/bound')
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        entries = archive.getmembers()
        require(len(entries) == 1, 'unique application member required')
        member = entries[0]
        require(member.name == 'qualification' and member.isreg() and member.mode == 0o555 and
                not member.linkname and not member.pax_headers and 0 < member.size <= 64 << 20, 'application member identity/type/mode')
        value = archive.extractfile(member).read(member.size+1)
        require(len(value) == member.size and digest(value)[7:] == identity['binary'], 'application binary hash/size')
    return value
