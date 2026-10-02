#!/usr/bin/env python3
"""Check frozen sources, preserved evidence, and formal pair classifications."""

import datetime
import hashlib
import json
import math
from pathlib import Path
import subprocess
import tarfile


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def verify_list(path):
    mismatch = []
    rows = path.read_text().splitlines()
    for row in rows:
        expected, name = row.split(None, 1)
        target = path.parent / name.strip().lstrip("*")
        if not target.is_file() or digest(target) != expected:
            mismatch.append(name)
    result = {"files": len(rows), "mismatch": mismatch}
    return result


def safe_member(name):
    parts = Path(name).parts
    return not Path(name).is_absolute() and all(
        part not in ("..", ".env", ".ssh", "credentials")
        and not part.startswith(".env.")
        and not part.endswith((".pem", ".key"))
        for part in parts
    )


def verify_archive(folder):
    expected = read_json(folder / "sha256.json")
    if "raw_evidence_regular_files" in expected:
        if expected.get("archive_pending"):
            raise RuntimeError("MongoDB archive is still pending")
        entries = expected["raw_evidence_regular_files"].copy()
        entries.update(expected["output_files"])
    else:
        entries = expected
    members = set()
    mismatch = []
    with tarfile.open(folder / "raw-evidence.tar.xz", "r:xz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            if not safe_member(member.name):
                raise RuntimeError("Unsafe evidence member name")
            if member.name in members:
                mismatch.append("duplicate:" + member.name)
            members.add(member.name)
            value = hashlib.sha256()
            with archive.extractfile(member) as stream:
                for block in iter(lambda: stream.read(1 << 20), b""):
                    value.update(block)
            if entries.get(member.name) != value.hexdigest():
                mismatch.append(member.name)
    for name, value in entries.items():
        if name in members:
            continue
        target = folder / name
        if not target.is_file() or digest(target) != value:
            mismatch.append(name)
    result = {"raw_members": len(members), "listed_outputs": len(entries) - len(members), "mismatch": mismatch}
    return result


def classify_pair(direct, weir):
    dc, wc = direct["cpu"], weir["cpu"]
    difference = abs(dc - wc) / ((dc + wc) / 2)
    matched = all(.095 <= row["cpu"] <= .105 and row["coverage"] for row in (direct, weir)) and difference <= .05
    result = {"cpu_difference": difference, "matched": matched, "jointly_qualified": matched and direct["healthy"] and weir["healthy"], "ratio": weir["rps"] / direct["rps"]}
    return result


def verify_mongo(folder):
    source = read_json(HERE / folder / "results.json")
    rows = {row["tag"]: row for row in source["formal_results"]}
    computed = []
    mismatch = []
    for pair in source["pairs"]:
        values = []
        for mode in ("direct", "weir"):
            row = rows[pair["tags"][mode]]
            if row["stage"] not in ("formal", "formal_supplement"):
                mismatch.append(row["tag"] + ":stage")
            if row["coverage"]["nominal_seconds"] != 60:
                mismatch.append(row["tag"] + ":duration")
            rate = row["overall_measure"]["success"] / 60
            if not math.isclose(rate, row["success_per_second"]):
                mismatch.append(row["tag"] + ":rps")
            healthy = all(value for key, value in row["qualifications"].items() if key not in ("CPU_in_target", "coverage"))
            value = {"cpu": row["database"]["process_cpu_cores"], "rps": rate, "coverage": row["coverage"]["pass"], "healthy": healthy}
            values.append(value)
        result = classify_pair(*values)
        if result["matched"] != pair["cpu_matched"] or result["jointly_qualified"] != pair["healthy_qualified"]:
            mismatch.append(str(pair["tags"]) + ":gate")
        if not math.isclose(result["ratio"], pair["RPS_ratio_weir_to_direct"]):
            mismatch.append(str(pair["tags"]) + ":ratio")
        result.update(workload=pair["workload"], number=pair["pair_index"], round=pair["cohort"])
        computed.append(result)
    groups = []
    for workload in ("mixed", "pure"):
        qualified = [row for row in computed if row["workload"] == workload and row["jointly_qualified"]]
        numbers = [row["number"] for row in qualified]
        if len(set(numbers)) != len(numbers):
            mismatch.append(workload + ":duplicate-qualified-pair")
        group = {"workload": workload, "qualified_pairs": len(qualified), "planned_pairs": 3, "complete": len(qualified) == 3}
        groups.append(group)
    result = {"attempted_pairs": len(computed), "pairs": computed, "groups": groups, "mismatch": mismatch}
    return result


def verify_es(folder):
    source = read_json(HERE / folder / "results.json")
    rows = {row["prefix"]: row for row in source["runs"]}
    mismatch = []
    computed = []
    for pair in source["pairs"]:
        values = []
        for mode in ("direct", "weir"):
            row = rows[pair[mode + "_prefix"]]
            if row["stage"] != "formal" or row["measure_seconds"] != 60:
                mismatch.append(row["prefix"] + ":stage-duration")
            rate = row["measure"]["all"]["success"] / 60
            if not math.isclose(rate, row["scalars"]["success_rps"]):
                mismatch.append(row["prefix"] + ":rps")
            h = row["health"]
            high_drop_windows = sum(window["all"]["client_drop"] / window["all"]["planned"] > .001 for window in row["windows"])
            healthy = h["api_errors"] == h["unknown"] == h["oom_kills"] == 0 and h["drop_fraction"] <= .001 and high_drop_windows < 2 and h["client_cpu"] < 1.8 and h["client_lag_p99_us"] <= 100000 and (h["weir_cpu"] is None or h["weir_cpu"] < 3.6)
            if healthy != h["qualified"]:
                mismatch.append(row["prefix"] + ":health")
            value = {"cpu": row["scalars"]["database_process_cpu"], "rps": rate, "coverage": row["cpu_coverage"]["qualified"], "healthy": healthy}
            values.append(value)
        result = classify_pair(*values)
        if result["matched"] != pair["matched"] or result["jointly_qualified"] != pair["jointly_qualified"]:
            mismatch.append(str(pair["prefixes"]) + ":gate")
        if not math.isclose(result["ratio"], pair["success_rps_ratio"]):
            mismatch.append(str(pair["prefixes"]) + ":ratio")
        result.update(workload=pair["workload"], number=pair["number"], round=pair["round"], selected=pair["selected"])
        computed.append(result)
    groups = []
    for workload in ("mixed", "pure"):
        selected = [row for row in computed if row["workload"] == workload and row["selected"]]
        if any(not row["jointly_qualified"] for row in selected):
            mismatch.append(workload + ":selected-failed-pair")
        stored = next((row for row in source["groups"] if row["workload"] == workload), None)
        if stored is None and any(row["workload"] == workload for row in computed):
            mismatch.append(workload + ":missing-group")
        if stored is not None and (len(selected) != stored["selected_jointly_qualified_pairs"] or (len(selected) == 3) != stored["complete"]):
            mismatch.append(workload + ":group-count")
        group = {"workload": workload, "qualified_pairs": len(selected), "planned_pairs": 3, "complete": len(selected) == 3}
        groups.append(group)
    result = {"attempted_pairs": len(computed), "pairs": computed, "groups": groups, "mismatch": mismatch}
    return result


def main():
    manifest = read_json(HERE / "frozen-v4-manifest.json")
    changed = [name for name, expected in manifest["production_sources"].items() if not (REPO / name).is_file() or digest(REPO / name) != expected]
    command = ["rg", "--files", "-g", "*.go", "-g", "go.mod", "-g", "go.sum"]
    listing = subprocess.run(command, cwd=REPO, check=True, capture_output=True, text=True)
    set_difference = sorted(set(listing.stdout.splitlines()).symmetric_difference(manifest["production_sources"]))
    archive = manifest["archive"]
    binary_directory = Path(archive["path"]).parent / "bin"
    binary_changes = [name for name, expected in manifest["binaries"].items() if not (binary_directory / name).is_file() or digest(binary_directory / name) != expected]
    frozen_bundle_unchanged = Path(archive["path"]).is_file() and digest(Path(archive["path"])) == archive["sha256"]
    archives = {name: verify_archive(HERE / name) for name in ("mongodb", "mongodb-pure", "elasticsearch", "elasticsearch-pure")}
    previous = {"v3": verify_list(HERE.parent / "SHA256SUMS"), "v4_refinement": verify_list(HERE.parent / "refinement-v4/SHA256SUMS")}
    pairs = {"mongodb": verify_mongo("mongodb"), "mongodb-pure": verify_mongo("mongodb-pure"), "elasticsearch": verify_es("elasticsearch"), "elasticsearch-pure": verify_es("elasticsearch-pure")}
    result = {"checked_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "frozen_source_count": len(manifest["production_sources"]), "frozen_source_changes": changed, "source_set_difference": set_difference, "frozen_manifest_unchanged": digest(HERE / "frozen-v4-manifest.json") == digest(HERE.parent / "refinement-v4/build-manifest.json"), "frozen_binary_changes": binary_changes, "frozen_bundle_unchanged": frozen_bundle_unchanged, "archives": archives, "previous_evidence": previous, "pair_recomputation": pairs}
    result["pass"] = not changed and not set_difference and not binary_changes and frozen_bundle_unchanged and result["frozen_manifest_unchanged"] and all(not row["mismatch"] for group in (archives, previous, pairs) for row in group.values())
    print(json.dumps(result, indent=2))
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
