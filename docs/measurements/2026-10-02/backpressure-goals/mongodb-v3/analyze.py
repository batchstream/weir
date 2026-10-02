import datetime
import json
import math
import re
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parent

def stamp(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()

def metric_map(sample):
    result = {}
    for line in sample.get('metrics', '').splitlines():
        if line and not line.startswith('#'):
            name, value = line.rsplit(' ', 1)
            result[name] = float(value)
    return result

def metric(sample, prefix, default=0):
    values = [value for name, value in metric_map(sample).items() if name.startswith(prefix)]
    return sum(values) if values else default

def load(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    data = {}
    with tarfile.open(directory / 'raw.tar.gz') as archive:
        for member in archive.getmembers():
            if member.isfile() and member.name.endswith('.jsonl'):
                data[Path(member.name).name.split('-')[-1]] = []
                raw = archive.extractfile(member).read().splitlines()
                rows = [json.loads(line) for line in raw]
                for role in ['client', 'weir', 'database']:
                    if member.name.endswith('-' + role + '.jsonl'):
                        data[role] = rows
    return manifest, data

def resources(samples, interval):
    rows = [s for s in samples if interval[0] <= s['wall_time'] <= interval[1]]
    if len(rows) < 2:
        value = {'samples': len(rows)}
        return value
    first, last = rows[0], rows[-1]
    seconds = last['wall_time'] - first['wall_time']
    rss = sorted(s['rss_pages'] * 4096 / 1048576 for s in rows)
    value = {'samples': len(rows), 'seconds': seconds,
            'process_cpu_cores': (last['process_cpu_ticks'] - first['process_cpu_ticks']) / 100 / seconds,
            'cgroup_cpu_cores': (last['cpu']['usage_usec'] - first['cpu']['usage_usec']) / 1e6 / seconds,
            'rss_median_mib': rss[len(rss)//2], 'rss_peak_mib': max(rss),
            'cgroup_memory_peak_mib': max(s['memory_current'] for s in rows) / 1048576,
            'oom_kill_delta': last['memory_events']['oom_kill'] - first['memory_events']['oom_kill'],
            'process_start_ticks': sorted({s['process_start_ticks'] for s in rows}),
            'cfs_nr_periods_delta': last['cpu']['nr_periods']-first['cpu']['nr_periods'],
            'cfs_nr_throttled_delta': last['cpu']['nr_throttled']-first['cpu']['nr_throttled'],
            'cfs_throttled_usec_delta': last['cpu']['throttled_usec']-first['cpu']['throttled_usec']}
    return value

def phase_stats(samples, interval):
    rows = [s for s in samples if interval[0] <= s['wall_time'] <= interval[1]]
    if not rows:
        value = {'samples': 0}
        return value
    fields = {'window': 'weir_store_window{', 'pending': 'weir_store_pending_entries{',
              'active': 'weir_store_active_executions{', 'ratio': 'weir_store_latency_ratio{',
              'ready_profiles': 'weir_store_latency_ready_profiles{', 'cooldown': 'weir_store_cooldown{', 'recovery_hold': 'weir_store_latency_recovery_hold{'}
    result = {'samples': len(rows)}
    for name, prefix in fields.items():
        values = [metric(s, prefix) for s in rows]
        result[name] = {'min': min(values), 'max': max(values), 'last': values[-1],
                        'positive_samples':sum(v>0 for v in values),
                        'positive_sample_fraction':sum(v>0 for v in values)/len(values)}
    for name, prefix in [('latency_events', 'weir_store_backpressure_events_total{reason="latency"'),
                         ('backend_events', 'weir_store_backpressure_events_total{reason="backend"')]:
        result[name] = metric(rows[-1], prefix) - metric(rows[0], prefix)
    for name, prefix in [('execution_ms', 'weir_store_execution_seconds'), ('batch_operations', 'weir_store_batch_operations')]:
        count = metric(rows[-1], prefix + '_count') - metric(rows[0], prefix + '_count')
        total = metric(rows[-1], prefix + '_sum') - metric(rows[0], prefix + '_sum')
        result[name] = total / count * (1000 if name.endswith('_ms') else 1) if count else 0
    first_metrics=metric_map(rows[0]);last_metrics=metric_map(rows[-1])
    buckets={}
    for key,value in last_metrics.items():
        if key.startswith('weir_store_execution_seconds_bucket{'):
            match=re.search(r'le="([^"]+)"',key)
            bound=float(match.group(1))
            buckets[bound]=buckets.get(bound,0)+value-first_metrics.get(key,0)
    count=metric(rows[-1],'weir_store_execution_seconds_count')-metric(rows[0],'weir_store_execution_seconds_count')
    result['execution_histogram']={'samples':count,'cumulative_buckets':[{'upper_seconds':str(bound) if math.isinf(bound) else bound,'count':value} for bound,value in sorted(buckets.items())]}
    for name,quantile in [('p95_upper_ms',.95),('p99_upper_ms',.99)]:
        match=next((bound for bound,value in sorted(buckets.items()) if value>=count*quantile),None) if count else None
        result['execution_histogram'][name]=match*1000 if match is not None and not math.isinf(match) else None
    return result

def analyze(directory):
    manifest, data = load(directory)
    trial = next(row['trial'] for row in data['client'] if row['type'] == 'trial')
    run_error = next(row['run_error'] for row in data['client'] if row['type'] == 'trial')
    audit = next(row for row in data['client'] if row['type'] == 'audit')
    start = stamp(trial['start'])
    end = start + trial['options']['WarmSeconds'] + trial['options']['Seconds']
    transitions = manifest['transitions']
    intervals = {'healthy': [start + 5, transitions[1]['resize']['requested_wall_time']],
                 'slow': [transitions[1]['resize']['confirmed']['wall_time'] + 2, transitions[2]['resize']['requested_wall_time']],
                 'recovery': [transitions[2]['resize']['confirmed']['wall_time'] + 2, end]}
    result = {'tag': manifest['tag'], 'mode': manifest['mode'], 'batch_operations': manifest['batch_operations'],
              'trial_start': start, 'planned_end': end, 'run_error': run_error,
              'qualification': manifest.get('qualification', 'formal'), 'audit': {'error': audit['error'], **audit['audit']},
              'overall_measure': {k: v for k, v in trial['measure']['all'].items() if not isinstance(v, dict)},
              'measure_failures': trial['measure']['all']['failures'],
              'warm': {k: trial['warm']['all'][k] for k in ['planned','success','started','client_drop','failures','unknown']},
              'warm_success_per_second': trial['warm']['all']['success'] / trial['options']['WarmSeconds'],
              'warm_p99_arrival_ms':trial['warm']['all']['success_arrival']['p99_us']/1000,
              'warm_p99_dispatch_ms':trial['warm']['all']['success_dispatch']['p99_us']/1000,
              'phases': {}, 'trace': [], 'crossing_or_settling_windows': []}
    for name, interval in intervals.items():
        result['phases'][name] = {'interval': interval, 'database': resources(data['database'], interval),
                                 'weir': resources(data['weir'], interval),
                                 'controller': phase_stats(data['weir'], interval), 'windows': []}
    for index, window in enumerate(trial['ten_second_windows']):
        left = start + trial['options']['WarmSeconds'] + index * 10
        right = min(left + 10, end)
        value = {key: window['all'][key] for key in ['planned','started','success','client_drop','unknown','failures','worker_expired']}
        value.update(index=index, start=left, end=right, success_per_second=window['all']['success']/(right-left),
                     p99_dispatch_ms=window['all']['success_dispatch']['p99_us']/1000,
                     p99_arrival_ms=window['all']['success_arrival']['p99_us']/1000)
        phases = [name for name, interval in intervals.items() if left >= interval[0] and right <= interval[1]]
        if phases:
            result['phases'][phases[0]]['windows'].append(value)
        else:
            value['classification']='crosses quota transition or2s settling boundary'
            result['crossing_or_settling_windows'].append(value)
    for sample in data['weir']:
        if start <= sample['wall_time'] <= end:
            result['trace'].append({'time': sample['wall_time'], 'since_start': sample['wall_time']-start,
                                    **{name: metric(sample,prefix) for name,prefix in {
                                    'window':'weir_store_window{','latency_events':'weir_store_backpressure_events_total{reason="latency"',
                                    'backend_events':'weir_store_backpressure_events_total{reason="backend"','cooldown':'weir_store_cooldown{','recovery_hold':'weir_store_latency_recovery_hold{',
                                    'ratio':'weir_store_latency_ratio{','baseline_seconds':'weir_store_latency_baseline_seconds{',
                                    'sample_seconds':'weir_store_latency_sample_seconds{','ready_profiles':'weir_store_latency_ready_profiles{',
                                    'active':'weir_store_active_executions{','pending':'weir_store_pending_entries{'}.items()}})
    result['window_changes'] = []
    previous = None
    for point in result['trace']:
        if previous is None or point['window'] != previous['window']:
            result['window_changes'].append(point)
        previous = point
    result['final_control_events'] = {key: metric(data['weir'][-1], prefix) for key, prefix in {
        'latency': 'weir_store_backpressure_events_total{reason="latency"',
        'backend': 'weir_store_backpressure_events_total{reason="backend"'}.items()}
    (directory / 'analysis.json').write_text(json.dumps(result, indent=2))
    return result

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('tags', nargs='+')
    opts = parser.parse_args()
    for tag in opts.tags:
        result = analyze(ROOT / tag)
        compact = dict(result)
        compact.pop('trace')
        print(json.dumps(compact))
