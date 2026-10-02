#!/usr/bin/env python3
"""Independent frozen ES v3/v4 pure-write ABBA tail follow-up evidence."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import tarfile


def stats(values):
    result={'median':statistics.median(values),'minimum':min(values),'maximum':max(values)}
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw',type=Path)
    parser.add_argument('output',type=Path)
    options=parser.parse_args()
    spec=importlib.util.spec_from_file_location('analysis',options.raw/'es-v4-report.py')
    analysis=importlib.util.module_from_spec(spec);spec.loader.exec_module(analysis)
    summary=json.loads((options.raw/'summary.json').read_text())
    if len(summary['runs'])!=4 or [r['version'] for r in summary['runs']]!=['v3','v4','v4','v3'] or summary['cleanup'] is None:
        raise RuntimeError('incomplete independent ABBA follow-up')
    runs=[]
    for run in summary['runs']:
        result=run.copy();client=analysis.read_client(options.raw,run)
        result.update(outcomes_all_trial=client['outcomes'],applied_with_rpc_error_all_trial=client['applied_with_rpc_error_all_trial'],warm=client['trial']['warm'])
        runs.append(result)
    cells=[]
    for version in ('v3','v4'):
        selected=[r for r in runs if r['version']==version]
        cell={'version':version,'repetitions':2,'prefixes':[r['prefix'] for r in selected],'profile':selected[0]['profile']}
        accessors={'success_rps':lambda r:r['measure']['all']['success']/20,'p95_ms':lambda r:r['measure']['all']['success_arrival']['p95_us']/1000,'p99_ms':lambda r:r['measure']['all']['success_arrival']['p99_us']/1000,'db_cpu_cores':lambda r:r['database']['cpu_cores_mean'],'db_cgroup_cpu_cores':lambda r:r['database']['cgroup_cpu_cores_mean'],'db_rss_mib':lambda r:r['database']['rss_max']/2**20,'cfs_seconds':lambda r:r['database']['throttled_seconds'],'gc_ms':lambda r:r['database']['gc_ms'],'weir_cpu_cores':lambda r:r['weir']['cpu_cores_mean'],'weir_rss_mib':lambda r:r['weir']['rss_max']/2**20,'client_lag_p99_ms':lambda r:r['client']['lag_p99_us']/1000}
        cell.update({name:stats([fn(r) for r in selected]) for name,fn in accessors.items()})
        cell['totals']=analysis.counters([r['measure']['all'] for r in selected])
        cell['audit']={key:sum(r['audit'][key] for r in selected) for key in ('planned_writes','applied','found_version1','unknown_found','absent')}
        cells.append(cell)
    result={'kind':'independent ES pure-write3200/s v3/v4 ABBA,2repetitions/version','provenance':summary['provenance'],'experiment':summary['experiment'],'prewarm':summary.get('prewarm'),'original_single_tail':json.loads((options.raw/'original-single-tail-analysis.json').read_text()),'cells':cells,'runs':runs,'cleanup':summary['cleanup'],'safety':{'applied_audited':sum(r['audit']['applied'] for r in runs),'unknown_found':sum(r['audit']['unknown_found'] for r in runs),'applied_with_rpc_error_all_trial':sum(r['applied_with_rpc_error_all_trial'] for r in runs),'instrument_errors':[{'prefix':r['prefix'],'error':r['instrument_error']} for r in runs if r.get('instrument_error')]}}
    options.output.mkdir(parents=True,exist_ok=False)
    (options.output/'results.json').write_text(json.dumps(analysis.compact(result),indent=2)+'\n')
    lines=['# ES v4 纯写尾延迟独立 ABBA 复核（2026-10-02）','','原十轮试验单次pure3200中v4 p9538ms、v3 8.9ms；v4 CFS1.159s对v3 .313s，客户端lag p9998ms对31ms；两版WeirCPU约.55、控制器window2恒定，GC11ms对10ms。限流/排队与尾延迟相伴，不能仅从原单次定代码因果。原异常、raw和SHA全部保留在[主对照](../elasticsearch/findings.md)。','','本次独立新owned ES进程，v3/v4/v4/v3顺序每版2次，不与旧DB当3repeat合并。同冻结helper/binaries、同资源profile：DB.25CPU/1536MiB、Weir2CPU/4608MiB、w64/s64/batch32/pool2/5ms/Read16KiB/queue256，pure3200/s、warm10+measure20、deadline1s、不重试。共同JVM预热，逐格actualquota/PID/startticks/CPU/RSS与全APPLIED审计。','','|run|版本|成功/s|p95/p99 ms|drop/API/UNKNOWN|DB CPU/CFS s|Weir CPU/RSS MiB|client lag p99 ms|','|---|---|---:|---|---|---|---|---:|']
    for r in runs:
        a=r['measure']['all'];d=r['database'];w=r['weir']
        lines.append(f"|{r['prefix']}|{r['version']}|{a['success']/20:.1f}|{a['success_arrival']['p95_us']/1000:.1f}/{a['success_arrival']['p99_us']/1000:.1f}|{a['client_drop']}/{sum((a['failures'] or {}).values())}/{a['unknown']}|{d['cpu_cores_mean']:.3f}/{d['throttled_seconds']:.3f}|{w['cpu_cores_mean']:.3f}/{w['rss_max']/2**20:.1f}|{r['client']['lag_p99_us']/1000:.1f}|")
    lines+=['','|版本|2次成功/s中位[min,max]|p95 ms中位[min,max]|DB CPU中位|Weir CPU中位|drop/API/UNKNOWN合计|','|---|---|---|---:|---:|---|']
    for c in cells:
        rate=c['success_rps'];tail=c['p95_ms'];a=c['totals']
        lines.append(f"|{c['version']}|{rate['median']:.1f}[{rate['minimum']:.1f},{rate['maximum']:.1f}]|{tail['median']:.1f}[{tail['minimum']:.1f},{tail['maximum']:.1f}]|{c['db_cpu_cores']['median']:.3f}|{c['weir_cpu_cores']['median']:.3f}|{a['client_drop']}/{sum(a['failures'].values())}/{a['unknown']}|")
    lines+=['','全APPLIED落库审计'+str(result['safety']['applied_audited'])+'；UNKNOWN实存'+str(result['safety']['unknown_found'])+'。每10s窗、client队列/lag、CFS/GC、真实resource receipt与原单次诊断完整保留。仅每版2次、同新DB进程的短测，避免宣称百分比延迟保证；原异常不能被这组成功覆盖。','','清理：'+json.dumps(summary['cleanup'],ensure_ascii=False)+'。','','证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
    (options.output/'findings.md').write_text('\n'.join(lines)+'\n')
    shutil.copyfile(Path(__file__),options.raw/Path(__file__).name)
    hashes={}
    with tarfile.open(options.output/'raw-evidence.tar.xz','w:xz') as archive:
        for path in sorted(options.raw.iterdir()):
            if path.is_file() and path.name!='binaries.tar':
                name='main/'+path.name;hashes[name]=hashlib.sha256(path.read_bytes()).hexdigest();archive.add(path,arcname=name)
    shutil.copyfile(Path(__file__),options.output/'report.py')
    for path in options.output.iterdir():
        if path.name!='sha256.json':hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
    (options.output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'runs':len(runs),'cells':len(cells),'output':str(options.output),'hash_members':len(hashes)}))


if __name__=='__main__':
    main()
