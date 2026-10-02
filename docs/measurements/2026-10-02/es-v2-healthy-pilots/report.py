#!/usr/bin/env python3
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import tarfile

root=Path('/var/folders/91/pzs4g26n4_s925wqcxc1xmd40000gn/T/weir-goals-n3vnh2vv')
fixtures=[('invalid-process-sampler','es-v2-stable-quota-phases-retry1',False),('healthy-2500-rejected','es-v2-stable-quota-phases-fixedpid',True),('healthy-1600-rejected','es-v2-stable-quota-phases-1600',True)]
output=Path('docs/measurements/2026-10-02/es-v2-healthy-pilots');output.mkdir(exist_ok=True)
result={'version':'v2 before slow-fraction confirmation','host_tool_tests_interval':[1790899541.496,1790899601.802],'fixtures':[]}
hashes={};lines=['# ES v2 健康基线筛选与仪表失败证据（2026-10-02）','','本目录保存全部过程，不能把未通过健康pilot或者错选Java launcher的样本当作正式验收。固定batch8/pool4/w32/session32/read16KiB/collect3ms/queue32；Weir2CPU/2304MiB；DB1536MiB，健康候选quota .5CPU。二进制SHA/source manifest和完整client/APPLIED审计/metrics/ES/proc/cgroup/config都在归档。','','|fixture|输入/s|性质|成功/s|drop|API error/UNKNOWN|measurement window|DB proc/cgroup CPU|APPLIED/found|','|---|---:|---|---:|---:|---|---|---|---|']
with tarfile.open(output/'raw-evidence.tar.xz','w:xz') as archive:
 for tag,folder,valid in fixtures:
  raw=root/folder;s=json.loads((raw/'summary.json').read_text());assert s['cleanup']['pass'] and s['cleanup']['inventory_restored'] and not s['cleanup']['errors']
  if valid:
   for source,name in [(Path('scripts/compare-load.py'),'compare-load-loaded-snapshot.py'),(Path('scripts/config_yaml.py'),'config_yaml.py'),(root/'es-v2-runner.py','es-v2-runner.py'),(root/'es-v2-stable-phase-runner-fixedpid.py','es-v2-stable-phase-runner-fixedpid.py')]:
    shutil.copyfile(source,raw/name)
   progress='es-v2-stable-quota-fixedpid' if folder.endswith('fixedpid') else 'es-v2-stable-quota-1600'
   shutil.copyfile(root/(progress+'-progress.jsonl'),raw/'progress.jsonl');shutil.copyfile(root/(progress+'-stderr.log'),raw/'runner-stderr.log')
  fixture={'tag':tag,'owner':s['owner'],'valid_database_process_sampler':valid,'experiment':s.get('experiment'),'binary_sha256':s['provenance']['binary_sha256'],'database_image':s['provenance']['database_image'],'resources':s['provenance']['resources'],'cleanup':s['cleanup'],'runs':[]}
  for r in s['runs']:
   m=r['measure']['all'];clients=[json.loads(l)for l in (raw/(r['prefix']+'-client.jsonl')).read_text().splitlines()];t=next(x['trial']for x in clients if x.get('type')=='trial');start=datetime.datetime.fromisoformat(t['start'].replace('Z','+00:00')).timestamp()+t['options']['WarmSeconds']
   row={'prefix':r['prefix'],'variant':r.get('variant'),'rate':r['rate'],'measurement_seconds':r['seconds'],'measure_start':start,'measure_end':start+r['seconds'],'overlaps_host_tool_tests':start<1790899601.802 and start+r['seconds']>1790899541.496,'qualified_healthy_capacity':r.get('qualified_healthy_capacity'),'success_rps':m['success']/r['seconds'],'p95_ms':m['success_arrival']['p95_us']/1000,'totals':{k:m[k]for k in ('planned','started','success','client_drop','unknown')},'api_failures':m['failures'],'database_process_cpu':r['database']['cpu_cores_mean'] if valid else None,'database_process_rss':r['database']['rss_max'] if valid else None,'untrusted_reported_process_cpu':r['database']['cpu_cores_mean'] if not valid else None,'database_cgroup_cpu':r['database']['cgroup_cpu_cores_mean'],'database_cgroup_memory':r['database']['cgroup_memory_max'],'database_throttled_seconds':r['database']['throttled_seconds'],'database_gc_ms':r['database']['gc_ms'],'database_api_cpu_time_ms':r['database']['cpu_time_ms'],'weir':r['weir'],'audit':r['audit']}
   fixture['runs'].append(row);cpu=f"{row['database_process_cpu']:.3f}" if valid else 'invalid PID'
   lines.append(f"|{tag}|{r['rate']}|{r.get('variant')}|{row['success_rps']:.1f}|{m['client_drop']}|{sum((m['failures']or{}).values())}/{m['unknown']}|{r['weir']['window_start']:.0f}→{r['weir']['window_end']:.0f} [{r['weir']['window_min']:.0f},{r['weir']['window_max']:.0f}]|{cpu}/{row['database_cgroup_cpu']:.3f}|{r['audit']['applied']}/{r['audit']['found_version1']}|")
  result['fixtures'].append(fixture)
  for p in sorted(raw.iterdir()):
   if p.is_file() and p.name!='binaries.tar':
    name=tag+'/'+p.name;hashes[name]=hashlib.sha256(p.read_bytes()).hexdigest();archive.add(p,arcname=name)
lines+=['','第一个fixture枚举第一个comm=java，误选PID6 launcher；其/proc CPU/RSS/startticks不代表ES server，且部分pilot/phase与宿主离线工具测试UTC00:05:41.496–00:06:41.802重叠。cgroup/ES API/Weir数据及完整审计仍保留，不用于干净性能因果或正式权威PID验收。','','后两个fixture使用ES /_nodes/_local/process提供的serverPID并核验/proc，且measurement不重叠宿主工具测试。2500在measurement内4→1；1600在10s warm内已降至1，measurement全1。实际CPU平均有余量而有周期CFS/GC尖峰；均未形成健康不收缩的基线，脚本没有进入quota phase。它们促成后续v3慢比例确认修正，不能改写为成功。','','全部已完成run的APPLIED均查到version1；UNKNOWN中实际落库数量在audit明列。所有owned containers/network/imported image最终精确清理，inventory一致。pacing失败另存相邻es-v2-stable-startup-failure。','','证据：[results.json](results.json)、[完整原始归档](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
(output/'results.json').write_text(json.dumps(result,indent=2));(output/'findings.md').write_text('\n'.join(lines)+'\n');shutil.copyfile(Path(__file__),output/'report.py')
for p in output.iterdir():
 if p.name!='sha256.json':hashes[p.name]=hashlib.sha256(p.read_bytes()).hexdigest()
(output/'sha256.json').write_text(json.dumps(hashes,indent=2));print('pilot evidence saved',str(output),len(result['fixtures']))
