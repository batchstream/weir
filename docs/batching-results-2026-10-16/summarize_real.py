#!/usr/bin/env python3
"""Summarize retained weir-lab JSON reports without inferring missing counters."""

import argparse
import gzip
import json
from pathlib import Path


def summarize(filename):
    path = Path(filename)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as source:
        report = json.load(source)
    rows = []
    for concurrency in report['parameters']['concurrency_levels']:
        for kind in ('direct', 'weir'):
            stages = [pair[kind] for pair in report['pairs']
                      if pair['concurrency'] == concurrency]
            completed = sum(stage['succeeded'] for stage in stages)
            elapsed = sum(stage['elapsed_ns'] for stage in stages) / 1e9
            if not completed or not elapsed:
                raise ValueError(f'{path}: no completed work')
            validated = all(stage['verified'] and not any(stage.get(key, 0)
                            for key in ('errors', 'indeterminate', 'not_attempted',
                                        'applied_with_error', 'business_operation_timeouts'))
                            for stage in stages)
            command_counts = [stage.get('timed_physical_database_command_deltas')
                              for stage in stages]
            calls = sum(sum(counts.values()) for counts in command_counts) \
                if all(counts is not None for counts in command_counts) else None
            cpu_seconds = 0.0
            cpu_available = True
            for stage in stages:
                resource = stage['resources']
                percent = resource.get('mean_database_cpu_budget_percent')
                cpus = resource.get('database_allocated_cpus')
                if percent is None or cpus is None:
                    cpu_available = False
                    break
                cpu_seconds += percent / 100 * cpus * stage['elapsed_ns'] / 1e9
            row = dict(report=str(path), path=kind, concurrency=concurrency,
                       rounds=len(stages), completed=completed, validated=validated,
                       operations_per_second=completed / elapsed,
                       round_operations_per_second=[stage['successful_operations_per_second']
                                                    for stage in stages],
                       p99_upper_ms_by_round=[stage['latency']['p99_upper_bound_ns'] / 1e6
                                              for stage in stages],
                       physical_database_commands=calls,
                       physical_database_commands_per_operation=calls / completed
                       if calls is not None else None,
                       estimated_database_cpu_seconds_per_1000_operations=
                       cpu_seconds / completed * 1000 if cpu_available else None,
                       database_cpu_sampling_coverage_by_round=[
                           stage['resources'].get('cpu_sampling_time_coverage') for stage in stages])
            rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', nargs='+')
    parser.add_argument('--format', choices=('json', 'markdown'), default='json')
    args = parser.parse_args()
    rows = [row for report in args.reports for row in summarize(report)]
    if args.format == 'json':
        print(json.dumps(rows, indent=2))
        return
    grouped = {}
    for row in rows:
        key = (row['report'], row['concurrency'])
        if key not in grouped:
            grouped[key] = {}
        grouped[key][row['path']] = row
    print('| Experiment | Workers | Native / Weir ops/s | Weir/native | Business commands/op N / W | Estimated DB CPU s/1,000 ops N / W |')
    print('| --- | ---: | ---: | ---: | ---: | ---: |')
    for (report, concurrency), paths in grouped.items():
        native, weir = paths['direct'], paths['weir']
        name = Path(report).parent.name.removeprefix('real-')
        qps = f"{native['operations_per_second']:,.0f} / {weir['operations_per_second']:,.0f}"
        ratio = f"{weir['operations_per_second'] / native['operations_per_second']:.2f}"
        if not native['validated'] or not weir['validated']:
            ratio = 'unverified'
        commands = pair_value(paths, 'physical_database_commands_per_operation')
        cpu = pair_value(paths, 'estimated_database_cpu_seconds_per_1000_operations')
        print(f'| {name} | {concurrency} | {qps} | {ratio} | {commands} | {cpu} |')


def pair_value(paths, key):
    values = [paths[kind][key] for kind in ('direct', 'weir')]
    return ' / '.join('-' if value is None else f'{value:.4f}' for value in values)


if __name__ == '__main__':
    main()
