#!/usr/bin/env python3
"""Compare medians of repeated standard Go benchmark output, without dependencies."""

import argparse
import csv
import json
import re
import statistics
import sys


def load(filename):
    rows = {}
    complete = False
    with open(filename, encoding="utf-8") as source:
        for line in source:
            if line.strip() == "PASS":
                complete = True
            fields = line.split()
            if not fields or not fields[0].startswith("Benchmark"):
                continue
            if len(fields) < 4:
                continue
            name = re.sub(r"-\d+$", "", fields[0])
            metrics = {}
            for position in range(2, len(fields) - 1, 2):
                metrics[fields[position + 1]] = float(fields[position])
            rows.setdefault(name, []).append(metrics)
    if not complete:
        raise SystemExit("Incomplete benchmark output: " + filename)
    return rows


def medians(rows):
    result = {}
    for name, samples in rows.items():
        result[name] = {}
        for metric in samples[0]:
            result[name][metric] = statistics.median(sample[metric] for sample in samples)
        result[name]["samples"] = len(samples)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline")
    parser.add_argument("candidate")
    parser.add_argument("--format", choices=("json", "csv"), default="json")
    args = parser.parse_args()
    before = medians(load(args.baseline))
    after = medians(load(args.candidate))
    if before.keys() != after.keys():
        missing = sorted(before.keys() - after.keys())
        extra = sorted(after.keys() - before.keys())
        raise SystemExit("Benchmark mismatch: missing=%r extra=%r" % (missing, extra))
    comparisons = {}
    for name in before.keys() & after.keys():
        old, new = before[name], after[name]
        speedup = old["ns/op"] / new["ns/op"]
        comparison = {
            "before": old,
            "after": new,
            "speedup": speedup,
            "baseline_raw": args.baseline,
            "candidate_raw": args.candidate,
        }
        comparisons[name] = comparison
    if args.format == "json":
        json.dump(comparisons, sys.stdout, indent=2, sort_keys=True)
        print()
        return
    metrics = ("ns/op", "operations/s", "calls/op", "ops/call", "mean-us", "p99-upper-us", "B/op", "allocs/op", "samples")
    fields = ["benchmark", "speedup", "baseline_raw", "candidate_raw"]
    for metric in metrics:
        fields.extend(("before_" + metric, "after_" + metric))
    options = {"fieldnames": fields}
    writer = csv.DictWriter(sys.stdout, **options)
    writer.writeheader()
    for name, comparison in sorted(comparisons.items()):
        row = {
            "benchmark": name,
            "speedup": comparison["speedup"],
            "baseline_raw": args.baseline,
            "candidate_raw": args.candidate,
        }
        for metric in metrics:
            row["before_" + metric] = comparison["before"].get(metric, "")
            row["after_" + metric] = comparison["after"].get(metric, "")
        writer.writerow(row)


if __name__ == "__main__":
    main()
