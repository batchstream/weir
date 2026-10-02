#!/usr/bin/env python3
"""Compact v3 frozen ES paired matrix and authoritative quota evidence."""
import argparse
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import sys
import tarfile

REPO = Path('/Users/liran/Projects/liran/go/weir')
sys.path.insert(0, str(REPO / 'scripts'))
spec = importlib.util.spec_from_file_location('comparison', REPO / 'scripts/compare-load.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


def compact(value):
    if isinstance(value, dict):
        result = {k: compact(v) for k, v in value.items() if k not in ('buckets', 'raw', 'metrics', 'database_resources')}
        return result
    if isinstance(value, list):
        result = [compact(v) for v in value]
        return result
    return value


def stats(values):
    result = {'median': statistics.median(values), 'minimum': min(values), 'maximum': max(values)}
    return result


def rows_for_phase(options):
    raw, run, system = options['raw'], options['run'], options['system']
    samples = [json.loads(line) for line in (raw / (run['prefix'] + '-samples.jsonl')).read_text().splitlines()]
    client = [json.loads(line) for line in (raw / (run['prefix'] + '-client.jsonl')).read_text().splitlines()]
    trial = next(r['trial'] for r in client if r.get('type') == 'trial')
    start = datetime.datetime.fromisoformat(trial['start'].replace('Z', '+00:00')).timestamp() + trial['options']['WarmSeconds']
    result = {'prefix': run['prefix'], 'rate': run['rate'], 'seconds': run['seconds'], 'profile': run['profile'], 'audit': run['audit'], 'measure': run['measure'], 'phases': [], 'ten_second_windows': []}
    for control in run['controls']:
        begin, end = control['wall_time'], control['wall_time'] + control['duration_seconds']
        selected = [s for s in samples if begin <= s['wall_time'] <= end and 'metrics' in s]
        if len(selected) < 2:
            raise RuntimeError('phase samples missing')
        metrics = [s['metrics'] for s in selected]
        phase = {k: control[k] for k in ('phase', 'cpu_quota', 'actual_cpu_max', 'wall_time', 'duration_seconds', 'database_container', 'database_pid', 'database_start_ticks', 'docker_state', 'limits')}
        resource_options = {'cpu': control['cpu_quota'], 'ticks': system['clock_ticks'], 'page_size': system['page_size']}
        phase['database'] = comparison.resource_summary([s['database_resources'] for s in selected], resource_options)
        phase['weir_window'] = {'start': comparison.metric(metrics[0], 'weir_store_window'), 'end': comparison.metric(metrics[-1], 'weir_store_window'), 'minimum': min(comparison.metric(m, 'weir_store_window') for m in metrics), 'maximum': max(comparison.metric(m, 'weir_store_window') for m in metrics)}
        for name in ('latency_ratio', 'latency_ready_profiles', 'pending_entries', 'active_executions'):
            phase[name + '_max'] = max(comparison.metric(m, 'weir_store_' + name, optional=True) for m in metrics)
        phase['recovery_hold_samples'] = sum(comparison.metric(m, 'weir_store_latency_recovery_hold') > 0 for m in metrics)
        phase['samples'] = len(selected)
        phase['control_ready_profiles'] = comparison.metric(control['metrics'], 'weir_store_latency_ready_profiles')
        phase['control_window'] = comparison.metric(control['metrics'], 'weir_store_window')
        phase['runtime_active_at_window_samples'] = sum(comparison.metric(m, 'weir_store_active_executions') >= comparison.metric(m, 'weir_store_window') for m in metrics)
        phase['timing_note'] = 'Sample wall_time is recorded before sequential DB resources/ES diagnostics/Weir metrics; approximate window transition times, especially under slow DB quota. Pure10s throughput windows use client timestamps.'
        phase['first_observed_window_below4_seconds'] = next((s['wall_time']-begin for s in selected if comparison.metric(s['metrics'],'weir_store_window') < 4), None)
        phase['first_observed_window1_seconds'] = next((s['wall_time']-begin for s in selected if comparison.metric(s['metrics'],'weir_store_window') == 1), None)
        phase['cooldown_samples'] = sum(comparison.metric(m, 'weir_store_cooldown') > 0 for m in metrics)
        phase['events'] = {reason: comparison.labelled_metric(metrics[-1], 'weir_store_backpressure_events_total', 'reason="' + reason + '"') - comparison.labelled_metric(control['metrics'], 'weir_store_backpressure_events_total', 'reason="' + reason + '"') for reason in ('backend', 'latency')}
        phase['admission_rejections'] = {reason: comparison.labelled_metric(metrics[-1], 'weir_admission_rejections_total', 'reason="' + reason + '"') - comparison.labelled_metric(control['metrics'], 'weir_admission_rejections_total', 'reason="' + reason + '"') for reason in ('sessions', 'connections', 'overload')}
        phase['window_changes'] = {direction: comparison.labelled_metric(metrics[-1], 'weir_store_window_changes_total', 'direction="' + direction + '"') - comparison.labelled_metric(control['metrics'], 'weir_store_window_changes_total', 'direction="' + direction + '"') for direction in ('increase', 'decrease')}
        result['phases'].append(phase)
    for index, window in enumerate(run['windows']):
        begin, end = start + index * 10, start + (index + 1) * 10
        phase = next((c for c in run['controls'] if c['wall_time'] <= begin and end <= c['wall_time'] + c['duration_seconds']), None)
        selected = [s for s in samples if begin <= s['wall_time'] <= end and 'metrics' in s]
        row = {'relative_seconds': [index * 10, (index + 1) * 10], 'wall_time': [begin, end], 'phase_wholly_contained': phase['phase'] if phase else None, 'measure': window}
        if len(selected) >= 2:
            resource_options = {'cpu': phase['cpu_quota'] if phase else .5, 'ticks': system['clock_ticks'], 'page_size': system['page_size']}
            row['database'] = comparison.resource_summary([s['database_resources'] for s in selected], resource_options)
            row['window_min'] = min(comparison.metric(s['metrics'], 'weir_store_window') for s in selected)
            row['window_max'] = max(comparison.metric(s['metrics'], 'weir_store_window') for s in selected)
            row['recovery_hold_samples'] = sum(comparison.metric(s['metrics'], 'weir_store_latency_recovery_hold') > 0 for s in selected)
        result['ten_second_windows'].append(row)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--initial-pilot', type=Path, required=True)
    parser.add_argument('--secondary-pilot', type=Path, required=True)
    parser.add_argument('--extra-phase', type=Path, required=True)
    options = parser.parse_args()
    summary = json.loads((options.raw / 'summary.json').read_text())
    system = json.loads((options.raw / 'resource-system.json').read_text())
    cells = []
    for write_every in (10, 1):
        for rate in (800, 3200):
            for variant in ('matched-direct', 'new-tuned'):
                runs = [r for r in summary['runs'] if r.get('variant') == variant and r['rate'] == rate and r['write_every'] == write_every]
                if len(runs) != 3:
                    raise RuntimeError('missing paired repetition')
                cell = {'variant': variant, 'rate': rate, 'write_every': write_every, 'repetitions': 3, 'prefixes': [r['prefix'] for r in runs], 'profile': runs[0]['profile']}
                accessors = {'success_rps': lambda r: r['measure']['all']['success']/r['seconds'], 'p95_ms': lambda r: r['measure']['all']['success_arrival']['p95_us']/1000, 'p99_ms': lambda r: r['measure']['all']['success_arrival']['p99_us']/1000, 'database_cpu_cores': lambda r: r['database']['cpu_cores_mean'], 'database_cgroup_cpu_cores': lambda r: r['database']['cgroup_cpu_cores_mean'], 'database_rss_mib': lambda r: r['database']['rss_max']/2**20, 'weir_cpu_cores': lambda r: r.get('weir', {}).get('cpu_cores_mean',0), 'weir_rss_mib': lambda r: r.get('weir', {}).get('rss_max',0)/2**20, 'batch_mean': lambda r: r.get('weir',{}).get('batch_mean',1), 'client_cpu_cores': lambda r: r['client']['cpu_cores_measure_bracket'], 'client_rss_mib': lambda r: r['client']['rss_max']/2**20}
                cell.update({name: stats([fn(r) for r in runs]) for name,fn in accessors.items()})
                cell['totals'] = {key: sum(r['measure']['all'][key] for r in runs) for key in ('planned','started','success','client_drop','unknown')}
                cell['totals']['api_errors'] = sum(sum((r['measure']['all']['failures'] or {}).values()) for r in runs)
                cell['audit'] = {key: sum(r['audit'][key] for r in runs) for key in ('applied','found_version1','unknown_found','absent')}
                cell['oom_kills'] = sum(r['database']['oom_kills'] for r in runs)
                cell['runs'] = [{k:r.get(k) for k in ('prefix','database','weir','client','audit','measure','actual_resource_receipt')} for r in runs]
                cells.append(cell)
    phases = []
    for run in summary['runs']:
        if run.get('variant') == 'backpressure':
            phase_options = {'raw':options.raw,'run':run,'system':system}
            phases.append(rows_for_phase(phase_options))
    if len(phases) != 2:
        raise RuntimeError('two phase repetitions required')
    initial = json.loads((options.initial_pilot / 'summary.json').read_text())
    secondary = json.loads((options.secondary_pilot / 'summary.json').read_text())
    extra_summary = json.loads((options.extra_phase / 'summary.json').read_text())
    extra_system = json.loads((options.extra_phase / 'resource-system.json').read_text())
    extra_options = {'raw':options.extra_phase,'run':extra_summary['runs'][0],'system':extra_system}
    extra_analysis = rows_for_phase(extra_options)
    result = {'version':'frozen v3 slow-fraction confirmation', 'provenance':summary['provenance'], 'experiment':summary['experiment'], 'selected_tuning':summary['selected_tuning'], 'cells':cells, 'backpressure':phases, 'health_pilots':[r for r in summary['runs'] if r.get('variant')=='health-pilot'], 'initial_pilot':{'provenance':initial['provenance'],'runs':initial['runs'],'cleanup':initial['cleanup']}, 'secondary_pilot':{'provenance':secondary['provenance'],'runs':secondary['runs'],'cleanup':secondary['cleanup'],'instrument_error':'Post-audit cat unavailable in scratch Weir image; no phases or matrix executed. Fixed by /client snapshot shared cgroup receipt.'}, 'extra_phase_instrument_failure':{'analysis':extra_analysis,'cleanup':extra_summary['cleanup'],'provenance':extra_summary['provenance'],'instrument_error':'Post-audit /client snapshot missing explicit integration opt-in; bootstrap fixed and pre-load verified for final group. All quota/PID/sampler/audit evidence exists.'}, 'cleanup':summary['cleanup'], 'raw_run_count':len(summary['runs'])}
    options.output.mkdir(parents=True,exist_ok=False)
    (options.output/'results.json').write_text(json.dumps(compact(result),indent=2)+'\n')
    lines = ['# ES v3 最终参数与配额恢复验证（2026-10-02）','','冻结weir-v3与client-v2身份见manifest及原始归档；ES8.19.22/Linux arm64、DB固定0.25CPU/1536MiB，Weir2CPU/4608MiB。性能矩阵使用64workers/sessions、batch32、pool2、collect5ms、普通Read16KiB，direct HTTP pool4；相同生成器，800/3200输入，90%Read/10%Put与纯Put，每格3次新配对重复、交替路径顺序、10s warm+20s measurement，无重试。1024B文档、1000均匀散列读key及独立写ID；每次重建corpus。共同JVM预热，完整CPU/RSS/cgroup/APPLIED审计保留。','','参数调优包括客户端并发、会话/内存预算、收集窗与普通Read预算，不能只归因于代码。16KiB只适合有相应记录大小上界的读取；热点同key去重未实现。表格是重复中位数，min/max、所有drop/APIerror/UNKNOWN完整保留。成功p95从计划到达开始计时。','','|负载|输入/s|路径|成功/s|p95 ms|DB CPU|Weir CPU|DB RSS MiB|Weir RSS MiB|drop/API/UNKNOWN（三次合计）|','|---|---:|---|---:|---:|---:|---:|---:|---:|---|']
    for c in cells:
        workload='90%Read/10%Put' if c['write_every']==10 else '100%Put'
        lines.append(f"|{workload}|{c['rate']}|{c['variant']}|{c['success_rps']['median']:.1f}|{c['p95_ms']['median']:.1f}|{c['database_cpu_cores']['median']:.3f}|{c['weir_cpu_cores']['median']:.3f}|{c['database_rss_mib']['median']:.1f}|{c['weir_rss_mib']['median']:.1f}|{c['totals']['client_drop']}/{c['totals']['api_errors']}/{c['totals']['unknown']}|")
    lines += ['','## 实际配额下降与恢复','','背压使用单独profile：32workers/sessions、batch8/pool4/collect3ms/Read16KiB/client_queue32、Weir2CPU/2304MiB。固定mixed2500/s、连续90s。首次实际Execute后：DB0.5CPU20s → 0.08CPU30s → 0.5CPU30s，两次独立重启Weir；ES容器、权威serverPID、startticks、StartedAt/restart0保持一致，逐次核验真实cpu.max。首20s为本Weir时延baseline建立阶段，预热和ready_profile记录均保留。','','两个独立健康pilot均完整保留：第一次measurement窗口恒4、无decrease、API/UNKNOWN0，成功2441.75/2500，drop1165/50000；过严零drop守卫停止了该fixture。第二次成功2282.9/s、drop4342、API/UNKNOWN0，窗口3→2[min1,max3]、decrease5/increase4，hold非持续全1；因此不能宣称2500下健康窗口恒4。第二次audit后cat仪表命令在scratch镜像不可用，单独记录且不冒充负载错误。新fixture固定2500直接做两phase，没有再次健康pilot或恒4运行守卫，所有短时收缩/恢复及失败都保留。跨阶段的10s窗仅列为跨界，不作纯阶段对比。','','|重复|阶段|quota|DB CPU|窗口始→末[min,max]|latency/backend事件|hold样本/总样本|','|---:|---|---:|---:|---|---|---|']
    for r in phases:
        for p in r['phases']:
            w=p['weir_window']; e=p['events']
            lines.append(f"|{r['prefix']}|{p['phase']}|{p['cpu_quota']}|{p['database']['cpu_cores_mean']:.3f}|{w['start']:.0f}→{w['end']:.0f}[{w['minimum']:.0f},{w['maximum']:.0f}]|{e['latency']:.0f}/{e['backend']:.0f}|{p['recovery_hold_samples']}/{p['samples']}|")
    lines += ['','阶段内每10s成功吞吐、p95、API错误、drop、UNKNOWN和实际DBCPU见results.json。UNKNOWN不视作成功；各run APPLIED全量落库审计、UNKNOWN实际落库与缺失单独记录。DB/JVM RSS没有明显降低的承诺；新增Weir会消耗自己的CPU/内存。','','清理：'+json.dumps(summary['cleanup'],ensure_ascii=False)+'。初次pilot清理：'+json.dumps(initial['cleanup'],ensure_ascii=False)+'。仅默认bridge ID漂移不会被误称全inventory一致；所有owned资源必须为空且non-default inventory一致。','','证据：[results.json](results.json)、[完整原始归档](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
    (options.output/'findings.md').write_text('\n'.join(lines)+'\n')
    hashes={}
    dependencies=[Path(__file__),Path(__file__).with_name('es-v3-runner.py'),Path(__file__).with_name('es-v2-stable-phase-runner-fixedpid.py'),Path(__file__).with_name('es-v2-runner.py'),REPO/'scripts/compare-load.py',REPO/'scripts/config_yaml.py']
    for source in dependencies:
        target = options.raw/source.name
        if not target.exists():
            shutil.copyfile(source,target)
    with tarfile.open(options.output/'raw-evidence.tar.xz','w:xz') as archive:
        for tag,raw in [('main',options.raw),('initial-pilot',options.initial_pilot),('secondary-pilot',options.secondary_pilot),('extra-phase',options.extra_phase)]:
            for path in sorted(raw.iterdir()):
                if path.is_file() and path.name!='binaries.tar':
                    name=tag+'/'+path.name;hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest();archive.add(path,arcname=name)
    shutil.copyfile(Path(__file__),options.output/'report.py')
    for p in options.output.iterdir():
        if p.name!='sha256.json':hashes[p.name]=hashlib.sha256(p.read_bytes()).hexdigest()
    (options.output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    report_result = {'cells':len(cells),'phases':len(phases),'output':str(options.output)}
    print(json.dumps(report_result))


if __name__=='__main__':
    main()
