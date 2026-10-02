#!/usr/bin/env python3
"""Keep qualified comparisons separate from every measured formal observation."""

import json
from pathlib import Path
import statistics


HERE = Path(__file__).resolve().parent


def distribution(values):
    if not values:
        return None
    result = {"median": statistics.median(values), "minimum": min(values), "maximum": max(values)}
    return result


def normalize_mongo(source):
    runs = {}
    for row in source["formal_results"]:
        all_ops = row["overall_measure"]
        value = {"rps": all_ops["success"] / 60, "cpu": row["database"]["process_cpu_cores"], "cgroup_cpu": row["database"]["cgroup_cpu_cores"], "rss_mib": row["database"]["rss_peak_mib"], "weir_cpu": row["weir"]["process_cpu_cores"] if row["weir"] else None, "weir_rss_mib": row["weir"]["rss_peak_mib"] if row["weir"] else None, "p95_ms": row["measure_p95_arrival_ms"], "drop": all_ops["client_drop"], "planned": all_ops["planned"], "api": row["API_failures"], "unknown": all_ops["unknown"], "applied_audited_including_warm": row["audit"]["applied"]}
        value["healthy"] = all(check for key, check in row["qualifications"].items() if key not in ("CPU_in_target", "coverage"))
        value["coverage_qualified"] = row["coverage"]["pass"]
        runs[row["tag"]] = value
    pairs = []
    for row in source["pairs"]:
        value = {"workload": row["workload"], "number": row["pair_index"], "round": row["cohort"], "direct": row["tags"]["direct"], "weir": row["tags"]["weir"], "cpu_matched": row["cpu_matched"], "jointly_qualified": row["healthy_qualified"], "selected": row["healthy_qualified"], "cpu_difference": row["cpu_relative_mean_difference"]}
        pairs.append(value)
    result = {"runs": runs, "pairs": pairs}
    return result


def normalize_es(source):
    runs = {}
    for row in source["runs"]:
        if row["stage"] != "formal":
            continue
        v = row["scalars"]
        value = {"rps": row["measure"]["all"]["success"] / 60, "cpu": v["database_process_cpu"], "cgroup_cpu": v["database_cgroup_cpu"], "rss_mib": v["database_rss_mib"], "weir_cpu": v["weir_cpu"], "weir_rss_mib": v["weir_rss_mib"], "p95_ms": v["success_p95_ms"], "drop": v["client_drop"], "planned": row["measure"]["all"]["planned"], "api": v["api_errors"], "unknown": v["unknown"], "applied_audited_including_warm": row["audit"]["applied"]}
        value["healthy"] = row["health"]["qualified"]
        value["coverage_qualified"] = row["cpu_coverage"]["qualified"]
        runs[row["prefix"]] = value
    pairs = []
    for row in source["pairs"]:
        value = {"workload": row["workload"], "number": row["number"], "round": row["round"], "direct": row["direct_prefix"], "weir": row["weir_prefix"], "cpu_matched": row["matched"], "jointly_qualified": row["jointly_qualified"], "selected": row["selected"], "cpu_difference": row["pair_relative_cpu_difference"]}
        pairs.append(value)
    result = {"runs": runs, "pairs": pairs}
    return result


def summarize(name, source):
    groups = []
    runs = source["runs"]
    for workload in ("mixed", "pure"):
        attempted = [pair for pair in source["pairs"] if pair["workload"] == workload]
        qualified = [pair for pair in attempted if pair["selected"] and pair["jointly_qualified"]]
        if len({pair["number"] for pair in qualified}) != len(qualified):
            raise RuntimeError("Duplicate qualified pair number")
        direct = [runs[pair["direct"]] for pair in qualified]
        weir = [runs[pair["weir"]] for pair in qualified]
        ratio = [w["rps"] / d["rps"] for d, w in zip(direct, weir)]
        result = {"database": name, "workload": workload, "attempted_pairs": len(attempted), "cpu_matched_pairs": sum(pair["cpu_matched"] for pair in attempted), "selected_jointly_qualified_pairs": len(qualified), "planned_pairs": 3, "complete": len(qualified) == 3, "selected_pairs": qualified, "success_rps_ratio": distribution(ratio), "throughput_gain_percent": distribution([(value - 1) * 100 for value in ratio]), "pair_cpu_difference": distribution([pair["cpu_difference"] for pair in qualified])}
        for mode, rows in (("direct", direct), ("weir", weir)):
            values = {key: distribution([row[key] for row in rows if row[key] is not None]) for key in ("rps", "cpu", "cgroup_cpu", "rss_mib", "weir_cpu", "weir_rss_mib", "p95_ms")}
            values.update(measurement_drop=sum(row["drop"] for row in rows), measurement_planned=sum(row["planned"] for row in rows), measurement_api=sum(row["api"] for row in rows), measurement_unknown=sum(row["unknown"] for row in rows))
            result[mode] = values
        groups.append(result)
    totals = {"formal_trials": len(runs), "formal_pairs": len(source["pairs"]), "measurement_drop": sum(row["drop"] for row in runs.values()), "measurement_api": sum(row["api"] for row in runs.values()), "measurement_unknown": sum(row["unknown"] for row in runs.values()), "applied_audited_including_warm": sum(row["applied_audited_including_warm"] for row in runs.values())}
    observations = []
    for pair in source["pairs"]:
        direct = runs[pair["direct"]]
        weir = runs[pair["weir"]]
        observation = {"pair": pair, "direct": direct, "weir": weir, "observed_success_rps_ratio": weir["rps"] / direct["rps"], "pair_cpu_difference_within_5_percent": pair["cpu_difference"] <= 0.05, "interpretation": "Actual fixed-window observations, including failed and unmatched pairs. These do not establish maximum capacity or completed repeated same-CPU comparisons."}
        healthy_covered = all(row["healthy"] and row["coverage_qualified"] for row in (direct, weir))
        observation["secondary_healthy_lower_cpu_higher_throughput"] = healthy_covered and weir["cpu"] < direct["cpu"] and weir["rps"] > direct["rps"]
        observation["secondary_healthy_pair_cpu_within_5_percent"] = healthy_covered and pair["cpu_difference"] <= 0.05
        observations.append(observation)
    result = {"groups": groups, "all_formal_attempts": totals, "all_formal_observations": observations}
    return result


def main():
    cohorts = []
    profiles = (("mongodb", "mongodb", "mixed"), ("mongodb-pure", "mongodb", "pure"), ("elasticsearch", "elasticsearch", "mixed"), ("elasticsearch-pure", "elasticsearch", "pure"))
    for folder, database, workload in profiles:
        raw = json.loads((HERE / folder / "results.json").read_text())
        source = normalize_mongo(raw) if database == "mongodb" else normalize_es(raw)
        result = summarize(database, source)
        group = next(row for row in result["groups"] if row["workload"] == workload)
        cohort = {"evidence_folder": folder, "group": group, "all_formal_attempts": result["all_formal_attempts"], "all_formal_observations": result["all_formal_observations"]}
        cohorts.append(cohort)
    result = {"scope": "Frozen v4 actual database total process CPU0.10 core +/-5%, pair relative difference<=5%, healthy formal trials; fixed60s successful throughput. No CPU-normalized estimates. Partial qualified groups are descriptive and are not reported as completed three-pair comparisons. Each workload is one separate database cohort; no cross-PID pooling.", "cohorts": cohorts}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
