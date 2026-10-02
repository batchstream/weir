#!/usr/bin/env python3
"""Compact frozen ES v3/v4 ABBA quota phases and high-load regression evidence."""
import argparse
import base64
import collections
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import sys
import tarfile


def compact(value):
    if isinstance(value, dict):
        result = {k: compact(v) for k, v in value.items() if k not in ('buckets', 'raw', 'metrics', 'database_resources')}
        return result
    if isinstance(value, list):
        result = [compact(v) for v in value]
        return result
    return value


def counters(values):
    keys = ('planned', 'started', 'completed', 'success', 'client_drop', 'unknown', 'client_late', 'worker_expired')
    result = {k: sum(v[k] for v in values) for k in keys}
    for key in ('failures', 'error_messages', 'drop_reasons'):
        total = collections.Counter()
        for value in values:
            total.update(value.get(key) or {})
        result[key] = dict(total)
    for key in ('success_arrival', 'arrival', 'dispatch'):
        buckets = collections.Counter()
        for value in values:
            buckets.update({b['upper_us']: b['count'] for b in value[key]['buckets']})
        total = sum(buckets.values())
        percentiles = {}
        for label, percent in [('p50_us', .5), ('p95_us', .95), ('p99_us', .99)]:
            cumulative = 0
            answer = 0
            for upper, count in sorted(buckets.items()):
                cumulative += count
                if cumulative >= total * percent:
                    answer = upper
                    break
            percentiles[label] = answer
        percentiles['max_ns'] = max((v[key]['max_ns'] for v in values), default=0)
        result[key] = percentiles
    return result


def outcomes(ledger):
    names = ('client_drop', 'applied', 'unknown', 'not_applied', 'not_started')
    if any(x >= len(names) for x in ledger):
        raise RuntimeError('unrecognized mutation ledger outcome')
    result = {name: ledger.count(i) for i, name in enumerate(names)}
    return result


def read_client(raw, run):
    records = [json.loads(line) for line in (raw / (run['prefix'] + '-client.jsonl')).read_text().splitlines()]
    trial = next(r['trial'] for r in records if r.get('type') == 'trial')
    audit = next(r for r in records if r.get('type') == 'audit')
    ledger = base64.b64decode(audit['ledger'], validate=True)
    if len(ledger) != run['audit']['planned_writes'] or outcomes(ledger)['applied'] != run['audit']['applied']:
        raise RuntimeError('ledger/audit disagreement')
    extra_ack = run['audit']['applied'] - trial['warm']['put']['success'] - trial['measure']['put']['success']
    if extra_ack < 0:
        raise RuntimeError('negative APPLIED with RPC failure count')
    result = {'trial': trial, 'ledger': ledger, 'outcomes': outcomes(ledger), 'applied_with_rpc_error_all_trial': extra_ack}
    return result


def sample_summary(options):
    samples, comparison, system, quota = (options[k] for k in ('samples', 'comparison', 'system', 'quota'))
    if len(samples) < 2:
        raise RuntimeError('phase resource samples missing')
    metrics = [s['metrics'] for s in samples]
    resource_options = {'cpu': quota, 'ticks': system['clock_ticks'], 'page_size': system['page_size']}
    result = {'samples': len(samples), 'database': comparison.resource_summary([s['database_resources'] for s in samples], resource_options)}
    result['weir'] = {'window_start': comparison.metric(metrics[0], 'weir_store_window'), 'window_end': comparison.metric(metrics[-1], 'weir_store_window'), 'window_min': min(comparison.metric(m, 'weir_store_window') for m in metrics), 'window_max': max(comparison.metric(m, 'weir_store_window') for m in metrics), 'rss_max': max(comparison.metric(m, 'process_resident_memory_bytes') for m in metrics), 'cpu_cores_mean': comparison.delta(metrics[0], metrics[-1], 'process_cpu_seconds_total') / (samples[-1]['wall_time'] - samples[0]['wall_time']), 'recovery_hold_samples': sum(comparison.metric(m, 'weir_store_latency_recovery_hold') > 0 for m in metrics), 'cooldown_samples': sum(comparison.metric(m, 'weir_store_cooldown') > 0 for m in metrics), 'saturated_execution_samples': sum(comparison.metric(m, 'weir_store_active_executions') >= comparison.metric(m, 'weir_store_window') for m in metrics), 'ready_profiles_max': max(comparison.metric(m, 'weir_store_latency_ready_profiles') for m in metrics), 'latency_ratio_max': max(comparison.metric(m, 'weir_store_latency_ratio') for m in metrics)}
    for label, key, metric_name in [('events', 'reason', 'weir_store_backpressure_events_total'), ('changes', 'direction', 'weir_store_window_changes_total'), ('admission', 'reason', 'weir_admission_rejections_total')]:
        values = {'events': ('latency', 'backend'), 'changes': ('increase', 'decrease'), 'admission': ('sessions', 'connections', 'overload')}[label]
        result[label] = {v: comparison.labelled_metric(metrics[-1], metric_name, key+'="'+v+'"') - comparison.labelled_metric(metrics[0], metric_name, key+'="'+v+'"') for v in values}
    return result


def phase_analysis(options):
    raw, run, comparison, system = (options[k] for k in ('raw', 'run', 'comparison', 'system'))
    client = read_client(raw, run)
    trial, ledger = client['trial'], client['ledger']
    warm = trial['options']['WarmSeconds']
    started = datetime.datetime.fromisoformat(trial['start'].replace('Z', '+00:00')).timestamp()
    measure_start, measure_end = started + warm, started + warm + run['seconds']
    samples = [json.loads(line) for line in (raw / (run['prefix']+'-samples.jsonl')).read_text().splitlines()]
    controls = run['controls']
    result = {k: run[k] for k in ('prefix', 'version', 'variant', 'profile', 'binary_sha256', 'helper_sha256', 'audit', 'measure', 'database', 'weir', 'client', 'actual_resource_receipt')}
    result.update(outcomes_all_trial=client['outcomes'], applied_with_rpc_error_all_trial=client['applied_with_rpc_error_all_trial'], warm=trial['warm'], controls=controls, phases=[], ten_second_windows=[])
    for index, window in enumerate(run['windows']):
        begin, end = measure_start + index * 10, measure_start + (index + 1) * 10
        phase = None
        for ci, control in enumerate(controls):
            # Requests were not timestamped separately in this already-running
            # fixture. Discard a complete 10s adjacent margin before each next
            # confirmed transition (9s margin +1s completion), plus2s after it;
            # no exact boundary claim.
            next_time = controls[ci+1]['wall_time']-9 if ci+1<len(controls) else measure_end
            if control['wall_time']+2 <= begin and end+1 <= next_time:
                phase = control
                break
        per_write_seconds = run['rate']/run['write_every']
        lo, hi = int((warm+index*10)*per_write_seconds), int((warm+(index+1)*10)*per_write_seconds)
        write_outcomes = outcomes(ledger[lo:hi])
        extra_ack = write_outcomes['applied'] - window['put']['success']
        if extra_ack < 0:
            raise RuntimeError('window APPLIED/success inconsistency')
        row = {'relative_measure_seconds': [index*10,(index+1)*10], 'wall_time': [begin,end], 'conservative_phase': phase['phase'] if phase else None, 'measure': window, 'write_outcomes': write_outcomes, 'applied_with_rpc_error': extra_ack, 'success_rps': window['all']['success']/10}
        selected = [s for s in samples if begin<=s['wall_time']<=end and 'metrics' in s]
        if len(selected)>=2:
            summary_options = {'samples':selected,'comparison':comparison,'system':system,'quota':phase['cpu_quota'] if phase else .5}
            row['resources'] = sample_summary(summary_options)
        result['ten_second_windows'].append(row)
    for ci, control in enumerate(controls):
        end = min(measure_end, controls[ci+1]['wall_time']-10 if ci+1<len(controls) else measure_end)
        begin = max(measure_start,control['wall_time']+2)
        selected = [s for s in samples if begin<=s['wall_time']<=end and 'metrics' in s]
        summary_options = {'samples':selected,'comparison':comparison,'system':system,'quota':control['cpu_quota']}
        phase = sample_summary(summary_options)
        phase.update(phase=control['phase'], confirmed_cpu_max=control['actual_cpu_max'], resource_interval_wall_time=[begin,end], warm_excluded=True)
        contained = [w for w in result['ten_second_windows'] if w['conservative_phase']==control['phase']]
        phase['pure_arrival_window_count'] = len(contained)
        phase['window_seconds'] = [w['relative_measure_seconds'] for w in contained]
        phase['pure_arrival_metrics'] = {kind:counters([w['measure'][kind] for w in contained]) for kind in ('all','read','put')}
        phase['pure_arrival_success_rps'] = phase['pure_arrival_metrics']['all']['success']/(len(contained)*10) if contained else None
        phase['pure_arrival_write_outcomes'] = {k:sum(w['write_outcomes'][k] for w in contained) for k in ('client_drop','applied','unknown','not_applied','not_started')}
        phase['pure_arrival_applied_with_rpc_error'] = sum(w['applied_with_rpc_error'] for w in contained)
        phase['first_sample_window1_seconds_after_confirm'] = next((s['wall_time']-control['wall_time'] for s in samples if control['wall_time']<=s['wall_time']<=end and 'metrics' in s and comparison.metric(s['metrics'],'weir_store_window')==1),None)
        result['phases'].append(phase)
    result['timing_limit'] = 'Quota request start has no separate timestamp. Control wall_time follows update/inspect/cpu.max/PID verification. Windows are planned-arrival buckets; allow 1s completion spill, 2s after confirmed transition and combined10s arrival/completion margin before next confirmed transition. Sequential sampler wall_time precedes DB+diagnostic+Weir reads. Stage labeling and observed shrink time are conservative/approximate, not exact execution timestamps.'
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw',type=Path)
    parser.add_argument('output',type=Path)
    parser.add_argument('--preview',action='store_true')
    options=parser.parse_args()
    sys.path.insert(0,str(options.raw))
    spec=importlib.util.spec_from_file_location('comparison',options.raw/'compare-load.py')
    comparison=importlib.util.module_from_spec(spec);spec.loader.exec_module(comparison)
    summary=json.loads((options.raw/'summary.json').read_text())
    system=json.loads((options.raw/'resource-system.json').read_text())
    phases=[];performance=[]
    for run in summary['runs']:
        if 'controls' in run:
            analysis_options={'raw':options.raw,'run':run,'comparison':comparison,'system':system}
            phases.append(phase_analysis(analysis_options))
        else:
            run_result=run.copy();cl=read_client(options.raw,run)
            run_result.update(outcomes_all_trial=cl['outcomes'],applied_with_rpc_error_all_trial=cl['applied_with_rpc_error_all_trial'],warm=cl['trial']['warm'])
            performance.append(run_result)
    all_runs = phases + performance
    safety = {'applied_audited': sum(r['audit']['applied'] for r in all_runs), 'unknown_found': sum(r['audit']['unknown_found'] for r in all_runs), 'outcomes_all_trial': {key: sum(r['outcomes_all_trial'][key] for r in all_runs) for key in ('client_drop', 'applied', 'unknown', 'not_applied', 'not_started')}, 'applied_with_rpc_error_all_trial': sum(r['applied_with_rpc_error_all_trial'] for r in all_runs), 'instrument_errors': [{'prefix': r['prefix'], 'error': r['instrument_error']} for r in summary['runs'] if r.get('instrument_error')]}
    result={'version':'frozen v4 Search certain unsent-write outcome + RPC cancellation isolation','provenance':summary['provenance'],'experiment':summary['experiment'],'prewarm':summary.get('prewarm'),'backpressure':phases,'performance_single_repeat':performance,'cleanup':summary['cleanup'],'raw_run_count':len(summary['runs']),'safety':safety}
    if options.preview:
        print(json.dumps(compact(result)))
        return
    if len(phases)!=6 or len(performance)!=4 or summary['cleanup'] is None:
        raise RuntimeError('incomplete final experiment')
    options.output.mkdir(parents=True,exist_ok=False)
    (options.output/'results.json').write_text(json.dumps(compact(result),indent=2)+'\n')
    lines=['# ES v4 同资源公平恢复对照（2026-10-02）','','冻结 v3/v4 使用同一新 helper，ES 8.19.22/Linux arm64、同一权威 Java PID/startticks/容器、逐段核验真实 cpu.max 50000→8000→50000 /100000、DB 1536MiB。输入固定2500/s、90%Read/10%Put、1s caller deadline、20s warm +120s measurement。降额在首次实际执行40s后，限额30s后恢复。API失败仍失败；已验证写确认与 UNKNOWN 分开统计，没有自动重试。','','ABBA 顺序 v3/v4/v4/v3，两次/版本；压力 profile w32/s32/batch8/pool4/3ms/Read16KiB/queue32、Weir2CPU/2304MiB。v4另两轮仅 batch32/pool2/5ms 参数调优，w32/s32/内存不变。温暖期从健康统计排除；临界10s窗不作为纯阶段对比。观察CPU/窗口的采样时间先于顺序读取，配额变更请求前戳缺失；阶段时间仅保守/近似。','','|run|版本/参数|全measurement成功/s|API DL/Internal/ResourceExhausted|UNKNOWN|drop|APPLIED审计/UNKNOWN实存|APPLIED但RPC失败|','|---|---|---:|---|---:|---:|---|---:|']
    for r in phases:
        a=r['measure']['all'];f=a['failures'] or {};audit=r['audit']
        lines.append(f"|{r['prefix']}|{r['variant']}|{a['success']/120:.1f}|{f.get('transport_DeadlineExceeded',0)}/{f.get('transport_Internal',0)}/{f.get('transport_ResourceExhausted',0)}|{a['unknown']}|{a['client_drop']}|{audit['applied']}/{audit['unknown_found']}|{r['applied_with_rpc_error_all_trial']}|")
    lines += ['','|run|纯阶段|成功/s|成功p95 ms|API/UNKNOWN/drop|DB CPU process/cgroup|Weir CPU/RSS MiB|窗口起→末[min,max]|hold/样本|','|---|---|---:|---:|---|---|---|---|---|']
    for r in phases:
        for p in r['phases']:
            a=p['pure_arrival_metrics']['all'];w=p['weir'];d=p['database'];errors=sum(a['failures'].values())
            lines.append(f"|{r['prefix']}|{p['phase']}|{p['pure_arrival_success_rps']:.1f}|{a['success_arrival']['p95_us']/1000:.1f}|{errors}/{a['unknown']}/{a['client_drop']}|{d['cpu_cores_mean']:.3f}/{d['cgroup_cpu_cores_mean']:.3f}|{w['cpu_cores_mean']:.3f}/{w['rss_max']/2**20:.1f}|{w['window_start']:.0f}→{w['window_end']:.0f}[{w['window_min']:.0f},{w['window_max']:.0f}]|{w['recovery_hold_samples']}/{p['samples']}|")
    lines += ['','ABBA同profile的错误区间高度重叠：v3两轮API1533/142、UNKNOWN141/15；v4两轮API1093/108、UNKNOWN121/13。只有每版2轮且时间漂移明显，不支持live错误/UNKNOWN已被代码显著降低的结论。每轮恢复纯窗API/UNKNOWN均0；低配额仍远低于2500/s并出现大量真实client_queue_full。参数组两轮限额纯窗602/1270成功/s，成功p95392/157ms；改善属于batch32/pool2/5ms参数效果，仍不是同2500满承载。','','Search代码只在标准HTTP Transport确定尚未拿到连接或Do前取消时返回NOT_APPLIED；拿到连接后仍UNKNOWN。server取消修复隔离当前RPC，保留独立stalled-peer watchdog。新helper在RPC错误时保留已验证mutation结果，但API仍失败。本轮10trials的额外APPLIED+RPCerror和NOT_APPLIED均0，未实际触发这两语义分支；它们由[完整离线与race验证](../validation/checks.json)及真实transport/gRPC针对性测试覆盖，不能冒称live证明减少不确定写。','','## 单次高负载回归','','同资源 v3/v4 3200/s × mixed/pure，各1次，仅检查回归，不替代前轮3重复性能矩阵。DB0.25CPU/1536MiB、Weir2CPU/4608MiB、w64/s64/batch32/pool2/5ms/Read16KiB/queue256、warm10s+measurement20s。','','|run|版本|写比例|成功/s|p95 ms|DB CPU|Weir CPU/RSS MiB|drop/API/UNKNOWN|','|---|---|---|---:|---:|---:|---|---|']
    for r in performance:
        a=r['measure']['all'];w=r['weir'];d=r['database']
        lines.append(f"|{r['prefix']}|{r['version']}|{'10%' if r['write_every']==10 else '100%'}|{a['success']/20:.1f}|{a['success_arrival']['p95_us']/1000:.1f}|{d['cpu_cores_mean']:.3f}|{w['cpu_cores_mean']:.3f}/{w['rss_max']/2**20:.1f}|{a['client_drop']}/{sum((a['failures'] or {}).values())}/{a['unknown']}|")
    lines += ['','全试验APPLIED审计共'+str(safety['applied_audited'])+'，UNKNOWN实际落库'+str(safety['unknown_found'])+'；完整 error_messages、每10s窗、warm、三阶段资源/控制器事件/hold、写outcome ledger与全APPLIED审计见results和raw。所有 transport_Internal 与 http2_internal_reset 的匹配只支持当前RPC写deadline reset解释，不是业务成功；不能保证所有deadline/UNKNOWN消除。NOT_APPLIED仅在真正未拿到连接/未发送证明下可确定，拿到连接后失败继续UNKNOWN。参数组与代码组分别报告，极端CPU容量不足的客户端drop保留。','','清理：'+json.dumps(summary['cleanup'],ensure_ascii=False)+'。','','证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
    (options.output/'findings.md').write_text('\n'.join(lines)+'\n')
    shutil.copyfile(Path(__file__),options.raw/'es-v4-report.py')
    hashes={}
    with tarfile.open(options.output/'raw-evidence.tar.xz','w:xz') as archive:
        for path in sorted(options.raw.iterdir()):
            if path.is_file() and path.name!='binaries.tar':
                name='main/'+path.name;hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest();archive.add(path,arcname=name)
    shutil.copyfile(Path(__file__),options.output/'report.py')
    for path in options.output.iterdir():
        if path.name!='sha256.json':hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
    (options.output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'phases':len(phases),'performance':len(performance),'output':str(options.output),'hash_members':len(hashes)}))


if __name__=='__main__':
    main()
