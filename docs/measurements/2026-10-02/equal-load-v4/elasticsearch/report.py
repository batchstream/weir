#!/usr/bin/env python3
"""Preserve complete ES equal-CPU trials and jointly qualified pairs."""
import argparse
import collections
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import tarfile


def import_analysis(path):
    spec=importlib.util.spec_from_file_location('ledger_analysis',path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stats(values):
    result={'median':statistics.median(values),'minimum':min(values),'maximum':max(values)}
    return result


def analyze_run(options):
    raw,run,analysis=(options[key] for key in ('raw','run','analysis'))
    client=analysis.read_client(raw,run)
    trial=client['trial'];warm_writes=trial['warm']['put']['planned']
    system=json.loads((raw/'resource-system.json').read_text())
    begin=datetime.datetime.fromisoformat(trial['start'].replace('Z','+00:00')).timestamp()+trial['options']['WarmSeconds']
    end=begin+run['seconds']
    observations=[json.loads(line) for line in (raw/(run['prefix']+'-samples.jsonl')).read_text().splitlines()]
    selected=[row['database_resources'] for row in observations if 'database_resources' in row and begin<=row['database_resources']['request_wall_time'] and row['database_resources']['read_wall_time']<=end]
    first,last=selected[0],selected[-1]
    span=last['read_monotonic']-first['read_monotonic']
    cpu=(last['process_cpu_ticks']-first['process_cpu_ticks'])/system['clock_ticks']/span
    lower=last['request_monotonic']-first['read_monotonic']
    coverage=run['cpu_coverage']
    computed_qualified=lower>=run['seconds']*.95 and first['read_wall_time']-begin<=2 and end-last['request_wall_time']<=2
    if abs(cpu-run['database']['cpu_cores_mean'])>1e-12 or abs(span-run['database']['sample_seconds'])>1e-12 or computed_qualified!=coverage['qualified'] or len(selected)!=coverage['samples']:
        raise RuntimeError('independent complete-window CPU/coverage recomputation mismatch')
    measure_ledger=client['ledger'][warm_writes:]
    result=run.copy()
    if run.get('stage')=='formal':
        h=run['health'].copy()
        h['high_drop_windows']=sum(w['all']['client_drop']/w['all']['planned']>.001 for w in run['windows'])
        h['qualified_before_window_check']=h['qualified']
        h['qualified']=h['qualified'] and h['high_drop_windows']<2
        h['gate']+='; high_drop_10s_windows<2(each drop/planned>0.1%)'
        result['health']=h
    if run['mode']=='weir':
        rows=[row for row in observations if 'metrics' in row and begin<=row['database_resources']['request_wall_time'] and row['database_resources']['read_wall_time']<=end]
        def metric(text,name):
            values=[float(line.split()[-1]) for line in text.splitlines() if line.startswith(name+' ') or line.startswith(name+'{')]
            return sum(values)
        hold=[metric(row['metrics'],'weir_store_latency_recovery_hold') for row in rows]
        result['weir_diagnostics']={'samples':len(rows),'recovery_hold_samples':sum(value>0 for value in hold),'recovery_hold_fraction':sum(value>0 for value in hold)/len(hold),'latency_ready_profiles_max':max(metric(row['metrics'],'weir_store_latency_ready_profiles') for row in rows),'latency_ratio_max':max(metric(row['metrics'],'weir_store_latency_ratio') for row in rows),'note':'Auxiliary Weir metrics follow each resource and ES diagnostic read. Retain raw; timestamps are sequential observation entry times and CPU/RSS approximate, DB CPU match uses authoritative proc request/read brackets.'}
    result.update(warm=trial['warm'],trial_start=trial['start'],trial_options=trial['options'],outcomes_all_trial=client['outcomes'],outcomes_measure=analysis.outcomes(measure_ledger),applied_with_rpc_error_all_trial=client['applied_with_rpc_error_all_trial'])
    if len(measure_ledger)!=trial['measure']['put']['planned']:
        raise RuntimeError('measurement mutation ledger length disagreement')
    a=trial['measure']['all']
    result['scalars']={'success_rps':a['success']/run.get('measure_seconds',run['seconds']),'success_p95_ms':a['success_arrival']['p95_us']/1000,'success_p99_ms':a['success_arrival']['p99_us']/1000,'api_errors':sum((a['failures'] or {}).values()),'client_drop':a['client_drop'],'drop_fraction':a['client_drop']/a['planned'],'unknown':a['unknown'],'database_process_cpu':run['database']['cpu_cores_mean'],'database_cgroup_cpu':run['database']['cgroup_cpu_cores_mean'],'database_rss_mib':run['database']['rss_max']/2**20,'weir_cpu':run['weir']['cpu_cores_mean'] if run.get('weir') else None,'weir_rss_mib':run['weir']['rss_max']/2**20 if run.get('weir') else None,'client_cpu':run['client']['cpu_cores_measure_bracket'],'client_lag_p99_ms':run['client']['lag_p99_us']/1000}
    return result


def analyze_pairs(summary,runs):
    by_id={r['prefix']:r for r in runs};pairs=[]
    selected={(p['workload'],p['number'],p['round']) for values in summary.get('selected_pairs',{}).values() for p in values}
    for original in summary.get('pairs',[]):
        result=original.copy();records=[by_id[prefix] for prefix in result['prefixes']]
        d=next(r for r in records if r['mode']=='direct');w=next(r for r in records if r['mode']=='weir')
        dc,wc=d['scalars']['database_process_cpu'],w['scalars']['database_process_cpu']
        difference=abs(dc-wc)/((dc+wc)/2)
        computed=.095<=dc<=.105 and .095<=wc<=.105 and difference<=.05 and all(r['cpu_coverage']['qualified'] for r in records)
        healthy_before_window=all(r['health']['qualified_before_window_check'] for r in records)
        healthy=all(r['health']['qualified'] for r in records)
        if computed!=result['matched'] or healthy_before_window!=result['healthy_qualified']:
            raise RuntimeError('pair gate recomputation mismatch')
        result.update(final_candidate=(result['workload'],result['number'],result['round']) in selected,selected=(result['workload'],result['number'],result['round']) in selected and computed and healthy,healthy_qualified_before_window_check=healthy_before_window,healthy_qualified=healthy,direct_high_drop_windows=d['health']['high_drop_windows'],weir_high_drop_windows=w['health']['high_drop_windows'],jointly_qualified=computed and healthy,direct_success_rps=d['scalars']['success_rps'],weir_success_rps=w['scalars']['success_rps'],success_rps_ratio=w['scalars']['success_rps']/d['scalars']['success_rps'],direct_prefix=d['prefix'],weir_prefix=w['prefix'])
        pairs.append(result)
    return pairs


def seal(options):
    hashes={}
    roots=[(options.raw,'main')]
    if options.workload=='mixed':
        roots += [(options.scratch/name,name) for name in ('owned','owned-retry1','owned-retry2')]
    with tarfile.open(options.output/'raw-evidence.tar.xz','w:xz') as archive:
        for root,label in roots:
            for path in sorted(root.rglob('*')):
                if path.is_file() and path.name not in ('binaries.tar',) and '__pycache__' not in path.parts:
                    name=label+'/'+str(path.relative_to(root));hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest();archive.add(path,arcname=name)
        for path in sorted(options.scratch.glob('*.jsonl'))+sorted(options.scratch.glob('*.log')):
            pure='pure-final' in path.name
            if pure != (options.workload=='pure'):continue
            name='progress/'+path.name;hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest();archive.add(path,arcname=name)
    for path in options.output.iterdir():
        if path.name!='sha256.json':hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
    (options.output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'sealed':str(options.output),'hash_members':len(hashes)}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scratch',type=Path)
    parser.add_argument('raw',type=Path)
    parser.add_argument('output',type=Path)
    parser.add_argument('--preview',action='store_true')
    parser.add_argument('--workload',choices=('mixed','pure'),default='mixed')
    parser.add_argument('--no-archive',action='store_true')
    parser.add_argument('--archive-only',action='store_true')
    parser.add_argument('--refresh',action='store_true')
    options=parser.parse_args()
    if options.archive_only:
        seal(options);return
    analysis=import_analysis(Path(__file__).parent/'ledger-analysis.py')
    summary=json.loads((options.raw/'summary.json').read_text())
    runs=[]
    for run in summary['runs']:
        run_options={'raw':options.raw,'run':run,'analysis':analysis}
        runs.append(analyze_run(run_options))
    pairs=analyze_pairs(summary,runs)
    groups=[]
    for workload in (options.workload,):
        qualified=[p for p in pairs if p['workload']==workload and p['selected'] and p['jointly_qualified']]
        group={'workload':workload,'selected_jointly_qualified_pairs':len(qualified),'planned_pairs':3,'complete':len(qualified)==3,'pairs':[(p['number'],p['round']) for p in qualified]}
        if qualified:
            for name in ('direct_success_rps','weir_success_rps','success_rps_ratio','direct_cpu','weir_cpu','pair_relative_cpu_difference'):
                group[name]=stats([p[name] for p in qualified])
        groups.append(group)
    stage_counts=collections.Counter(r['stage'] for r in runs)
    safety=[]
    for stage in sorted(stage_counts):
        selected=[r for r in runs if r['stage']==stage]
        record={'stage':stage,'completed_trials':len(selected),'applied_audited_including_warm':sum(r['audit']['applied'] for r in selected),'measurement_applied':sum(r['outcomes_measure']['applied'] for r in selected),'unknown_found_all_trial':sum(r['audit']['unknown_found'] for r in selected),'applied_with_rpc_error_all_trial':sum(r['applied_with_rpc_error_all_trial'] for r in selected),'measurement_api_errors':sum(r['scalars']['api_errors'] for r in selected),'measurement_unknown':sum(r['scalars']['unknown'] for r in selected),'measurement_client_drop':sum(r['scalars']['client_drop'] for r in selected)}
        safety.append(record)
    preceding=[]
    for name in (('owned','owned-retry1','owned-retry2') if options.workload=='mixed' else ()):
        root=options.scratch/name
        if not root.exists():continue
        previous=json.loads((root/'summary.json').read_text())
        entry={'raw_directory':name,'instrument_error':json.loads((root/'instrument-error.json').read_text()),'cleanup':previous.get('cleanup'),'volume_cleanup':json.loads((root/'owned-volume-cleanup.json').read_text()),'completed_runs':len(previous['runs']),'included_in_formal_result':False,'completed_applied_audited':sum(r['audit']['applied'] for r in previous['runs'])}
        preceding.append(entry)
    result={'kind':'frozen v4 ES actual database process CPU matched throughput, target0.10 core','provenance':summary['provenance'],'experiment':summary.get('experiment'),'protocol_amendment':summary.get('protocol_amendment'),'instrumentation_revision':summary.get('instrumentation_revision'),'interrupted_trials':summary.get('interrupted_trials',[]),'observer_only_baseline':{k:v for k,v in json.loads((options.raw/'observer-only-2hz-baseline.json').read_text()).items() if k!='samples'} if (options.raw/'observer-only-2hz-baseline.json').exists() else None,'calibrations':summary.get('calibrations',[]),'pairs':pairs,'groups':groups,'runs':runs,'idle':summary.get('idle',[]),'safety_by_stage':safety,'preceding_attempts':preceding,'cleanup':summary.get('cleanup'),'volume_cleanup':json.loads((options.raw/'owned-volume-cleanup.json').read_text()) if (options.raw/'owned-volume-cleanup.json').exists() else None,'cpu_timing_limit':'/proc counter read lies in saved request/read bracket. CPU uses read-complete monotonic span; conservatively require full brackets inside complete fixed measurement, first-read/last-request boundary gaps<=2s and last-request minus first-read>=57s. Preserve bracket uncertainty; successful RPS denominator fixed60s. No10s CPU selection or interpolation.','health_limit':'CPU matched and health qualified are distinct; only joint qualification used as stable-capacity evidence. Formal health: APIUNKNOWN/OOM0, client_drop<=0.1%, clientCPU<1.8, WeirCPU<3.6, clientlagp99<=100ms, fewer than2 high-drop10s windows(each drop/planned>0.1%). Matching CPU does not match IO/RSS or isolate pool size.'}
    result['failed_trials']=[json.loads(path.read_text()) for path in sorted(options.raw.glob('*-failed-trial.json'))]
    result['idle_client_restarts']=summary.get('idle_client_restarts',[])
    if options.preview:
        print(json.dumps(analysis.compact(result)));return
    if summary.get('cleanup') is None or result['volume_cleanup'] is None:
        raise RuntimeError('final cleanup not yet recorded')
    options.output.mkdir(parents=True,exist_ok=options.refresh)
    (options.output/'results.json').write_text(json.dumps(analysis.compact(result),indent=2)+'\n')
    lines=['# ES v4 同实际数据库 CPU 吞吐对照（2026-10-02）','','DB固定0.25CPU/1536MiB，同一ES8.19.22 Java进程，整个新cohort使用新建owned普通磁盘volume，逐trial重建owned index与1000keys。冻结v4与共同helper。Weir4CPU/4608MiB，client2CPU，worker/session64、queue256；直连pool4，Weir pool2/batch32/collect5ms/Read16KiB。两者是已有部署选型，不是pool单因素实验；单Weir进程。','','每workload先共同预热至少60s，分别校准输入率（warm20/measure30，每path每round最多6次），随后锁率。正式warm20/measure60，每workload3pair D/W、W/D、D/W；至多2round完整pair补测，旧失配保留。唯一processCPU目标0.10核：两侧各0.095..0.105，pair相对均值差≤5%，同时满足采样覆盖。全部60s吞吐按success/60；CPU用完整正式窗口内全部合格request/read bracket的首末counter与实际read-complete采样span，不按名义60s代替counterspan、不挑10s窗、不折算吞吐。','','健康状态另判：API/UNKNOWN/OOM0、drop≤0.1%、client与Weir CPU各低于配额90%、clientlagp99≤100ms，且每10s窗drop/planned>0.1%的窗少于2个。只有CPU与健康都合格的pair用于稳定吞吐结论；失败与缺额如实保留。','','|workload/pair/round|D/W prefixes|D/W offered|D/W成功/s|D/W processCPU|CPU差|CPUmatched/healthy/selected|D/W高drop窗| W/D成功吞吐比|','|---|---|---|---|---|---:|---|---|---:|']
    for p in pairs:
        lines.append(f"|{p['workload']}/{p['number']}/{p['round']}|{p['direct_prefix']}/{p['weir_prefix']}|{p['rates']['direct']}/{p['rates']['weir']}|{p['direct_success_rps']:.2f}/{p['weir_success_rps']:.2f}|{p['direct_cpu']:.5f}/{p['weir_cpu']:.5f}|{p['pair_relative_cpu_difference']*100:.2f}%|{p['matched']}/{p['healthy_qualified']}/{p['selected']}|{p['direct_high_drop_windows']}/{p['weir_high_drop_windows']}|{p['success_rps_ratio']:.3f}|")
    lines+=['','|workload|最终联合合格pair数|D/W成功/s中位|W/D比中位[min,max]|','|---|---:|---|---|']
    for g in groups:
        if g['selected_jointly_qualified_pairs']:
            ratio=g['success_rps_ratio'];text=f"{g['direct_success_rps']['median']:.2f}/{g['weir_success_rps']['median']:.2f}|{ratio['median']:.3f}[{ratio['minimum']:.3f},{ratio['maximum']:.3f}]"
        else:text='无联合合格对|未建立同CPU稳定吞吐对照'
        lines.append(f"|{g['workload']}|{g['selected_jointly_qualified_pairs']}/3|{text}|")
    lines+=['','|run|stage/workload/path|rate|成功/s/p95ms|DB process/cgroupCPU/RSS MiB|Weir CPU/RSS MiB|drop/API/UNKNOWN|CPU span下界/首末gap秒|','|---|---|---:|---|---|---|---|---|']
    for r in runs:
        v=r['scalars'];c=r['cpu_coverage'];w='—' if v['weir_cpu'] is None else f"{v['weir_cpu']:.3f}/{v['weir_rss_mib']:.1f}"
        lines.append(f"|{r['prefix']}|{r['stage']}/{r['workload']}/{r['mode']}|{r['rate']}|{v['success_rps']:.2f}/{v['success_p95_ms']:.1f}|{v['database_process_cpu']:.5f}/{v['database_cgroup_cpu']:.5f}/{v['database_rss_mib']:.1f}|{w}|{v['client_drop']}/{v['api_errors']}/{v['unknown']}|{c['counter_span_lower_seconds']:.3f}/{c['first_sample_margin_seconds']:.3f}/{c['last_sample_margin_seconds']:.3f}|")
    lines+=['','同进程无业务original ESdiagnostics observer baseline实际processCPU0.01718核/cgroup0.04585核、span29.690s；proc-onlyidle约0.01655核。该结果否定了诊断采样单独造成0.13核Javafloor的假说，保持原诊断频率，不扣baseline。原每循环0.5s wait加exec/HTTP约1.69Hz，并非严格2Hz，也只读取thread_pool/http/jvm/process/indexing_pressure/fs指定类别。直连低rate校准CPU已从约0.139下降到0.1167，因此没有证据声称物理恒定CPUfloor。经root批准mixed额外commonwarm D/W220各30s，共60s，之后使用既定round1/2总重校准预算；r008中断无完整确认证据，不声明完成audit。', '', '预热和校准不计正式结果。冷启动失败、未达目标校准、失配完整pair、替换轮与前置instrument/pacing失败全部保留。rate6400仅是冻结helper硬上限，pureformalcap3730；超过合法计划预算时没有增rate或重编helper。部分pair合格不等于完成3pair；目标不可达应报告缺额，不能插值或按CPU线性折算。','','同CPU并不意味着IO、GC、内存负载相同。本组采用普通磁盘volume，不能与先前tmpfs矩阵当同环境重复合并；同进程的JIT/merge/GC漂移、CPU计数10ms量化和采样读区间仍有不确定性。idle/IO/df/indexsize/CFS/GC与完整每10s窗保留供复核。','','按stage分开的APPLIED全内容审计、measurement ledger、API/UNKNOWN/drop见results。warm+measure写都审计；中断的未完成校准不声称完成审计。收到APPLIED且RPC失败仍API失败；拿到连接后失回复写保留UNKNOWN，不自动重试。','','清理：'+json.dumps(summary['cleanup'],ensure_ascii=False)+'；ownedvolume：'+json.dumps(result['volume_cleanup'],ensure_ascii=False)+'。','','证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
    if options.workload=='mixed':
        lines += ['', '本cohort最终mixed联合合格仅1/3对：17.226倍仅描述一对观测点，不支持完成3次重复的稳定改善结论。该对r017正式37drop，预热另有3601drop，均保留。其他4对失败全部保留。', '', '原pure校准r034出现3002正式drop、123API失败（120Deadline/3http2_internal_reset）与123UNKNOWN，trial后未产生完整ledger/audit。136752个warm+measure API成功put未计入APPLIED审计；UNKNOWN实际是否落库未知。45min idle helper寿命与失败时序匹配，但finally前没有保存精确exitState，原因仅假说，不将API失败全归因夹具。此r034不作同CPU或安全审计合格数据。后续fresh pure独立cohort另存elasticsearch-pure，不跨DB PID合并。']
    else:
        lines += ['', '本cohort是最后一次获授权的fresh pure-only独立DB与ordinaryvolume，未复用旧mixed/pure CPU或直连基线。round0旧fixture失败独立保留；本次只使用round1和round2，每path每轮最多6次校准，剩余补测只替换完整pair。', '', '首次load前已验证exact-labelled闲置client重启sameCID/image/limits，top仅idle；每trial前寿命>=25min才重启client，并保留before/after及DB不重启证明。生产与helper冻结，无自动重试写。']
    (options.output/'findings.md').write_text('\n'.join(lines)+'\n')
    if options.workload=='pure':
        lines=[line for line in lines if not line.startswith('同进程无业务original ESdiagnostics')]
        (options.output/'findings.md').write_text('\n'.join(lines)+'\n')
    method={'plan':summary['experiment']['plan'],'protocol_amendment':summary.get('protocol_amendment'),'instrumentation_revision':summary.get('instrumentation_revision'),'cpu_timing':result['cpu_timing_limit'],'health_gate':result['health_limit'],'observer_baseline':result['observer_only_baseline'],'preceding_attempts':preceding}
    (options.output/'method.json').write_text(json.dumps(analysis.compact(method),indent=2)+'\n')
    cleanup={'main':summary['cleanup'],'main_volume':result['volume_cleanup'],'preceding_attempts':[{k:r[k] for k in ('raw_directory','cleanup','volume_cleanup')} for r in preceding]}
    (options.output/'cleanup.json').write_text(json.dumps(cleanup,indent=2)+'\n')
    index={'main_raw':options.raw.name,'all_completed_trials':len(runs),'formal_trials':stage_counts.get('formal',0),'formal_pairs':len(pairs),'groups':groups,'stage_counts':dict(stage_counts),'safety_by_stage':safety,'interrupted_trials':summary.get('interrupted_trials',[]),'archive_pending':options.no_archive,'failed_trials':result['failed_trials'],'idle_client_restarts':len(result['idle_client_restarts']),'archive_directories':['main','owned','owned-retry1','owned-retry2','progress'] if options.workload=='mixed' else ['main','progress'],'note':'Every previous failure and unmatched pair retained. APPLIED safety counts only completed audited trials, no claim for interrupted r008.'}
    (options.output/'index.json').write_text(json.dumps(index,indent=2)+'\n')
    shutil.copyfile(Path(__file__),options.output/'report.py')
    shutil.copyfile(Path(__file__).parent/'ledger-analysis.py',options.output/'ledger-analysis.py')
    if not options.no_archive:seal(options)
    else:
        hashes={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in options.output.iterdir() if path.name!='sha256.json'}
        (options.output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'runs':len(runs),'groups':groups,'safety':safety,'output':str(options.output),'archive_pending':options.no_archive}))


if __name__=='__main__':main()
