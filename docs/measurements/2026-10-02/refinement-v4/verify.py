#!/usr/bin/env python3
"""Verify frozen sources and archived measurements without extracting files."""

import hashlib
import json
import pathlib
import subprocess
import tarfile


def digest(file_path):
    value = hashlib.sha256()
    with file_path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def verify_archive(folder):
    expected = json.loads((folder / "sha256.json").read_text())
    members = set()
    mismatches = []
    with tarfile.open(folder / "raw-evidence.tar.xz", "r:xz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            if member.name in members:
                mismatches.append("duplicate:" + member.name)
            members.add(member.name)
            with archive.extractfile(member) as stream:
                value = hashlib.sha256()
                for block in iter(lambda: stream.read(1 << 20), b""):
                    value.update(block)
            if expected.get(member.name) != value.hexdigest():
                mismatches.append(member.name)
    for name, value in expected.items():
        if name in members:
            continue
        file_path = folder / name
        if not file_path.is_file() or digest(file_path) != value:
            mismatches.append(name)
    result = {
        "raw_members": len(members),
        "listed_outputs": len(expected) - len(members),
        "mismatch": mismatches,
    }
    return result


def main():
    evidence = pathlib.Path(__file__).resolve().parent
    repository = evidence.parents[3]
    build = json.loads((evidence / "build-manifest.json").read_text())
    sources = build["production_sources"]
    changed = []
    for name, expected in sources.items():
        file_path = repository / name
        if not file_path.is_file() or digest(file_path) != expected:
            changed.append(name)
    listing = subprocess.run(
        ["rg", "--files", "-g", "*.go", "-g", "go.mod", "-g", "go.sum"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    current = set(listing.stdout.splitlines())
    source_set_difference = sorted(current.symmetric_difference(sources))
    archives = {
        name: verify_archive(evidence / name)
        for name in ("elasticsearch", "elasticsearch-pure-tail-abba", "mongodb")
    }
    previous = evidence.parent / "SHA256SUMS"
    rows = previous.read_text().splitlines()
    mismatches = []
    for row in rows:
        expected, name = row.split(None, 1)
        file_path = previous.parent / name.strip().lstrip("*")
        if not file_path.is_file() or digest(file_path) != expected:
            mismatches.append(name)
    result = {
        "frozen_source_count": len(sources),
        "frozen_sources_changed": changed,
        "source_set_difference": source_set_difference,
        "archives": archives,
        "previous_evidence": {"files": len(rows), "mismatch": mismatches},
    }
    result["pass"] = (
        not changed
        and not source_set_difference
        and not mismatches
        and all(not entry["mismatch"] for entry in archives.values())
    )
    print(json.dumps(result, indent=2))
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
