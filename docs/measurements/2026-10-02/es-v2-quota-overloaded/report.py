#!/usr/bin/env python3
"""Summarize the frozen ES experiment without hiding loss or audit evidence."""
import argparse
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


def stats(values):
    result = {'median': statistics.median(values), 'minimum': min(values), 'maximum': max(values)}
    return result


def compact(value):
    if isinstance(value, dict):
        result = {key: compact(item) for key, item in value.items() if key != 'buckets'}
        return result
    if isinstance(value, list):
        result = [compact(item) for item in value]
        return result
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--version', choices=('v1', 'v2'), default='v1')
    parser.add_argument('--phase-only', action='store_true')
    options = parser.parse_args()
    summary = json.loads((options.raw / 'summary.json').read_text())
    resource_system = json.loads((options.raw / 'resource-system.json').read_text())
    stable_phase = summary.get('experiment', {}).get('kind') == 'healthy-quota-phase-only'
    cells = []
    for write_every in (() if options.phase_only else (10, 1)):
        for rate in (800, 3200):
            variants = ('direct', 'old-baseline', 'new-default', 'new-tuned') if options.version == 'v1' else ('matched-direct', 'new-tuned')
            for variant in variants:
                runs = [r for r in summary['runs'] if r.get('variant') == variant and r['write_every'] == write_every and r['rate'] == rate]
                if len(runs) != 3:
                    raise RuntimeError('incomplete three-repeat cell: ' + str((write_every, rate, variant)))
                cell = {'variant': variant, 'write_every': write_every, 'rate': rate, 'repetitions': 3, 'prefixes': [r['prefix'] for r in runs], 'profile': runs[0]['profile']}
                cell['success_rps'] = stats([r['measure']['all']['success'] / r['seconds'] for r in runs])
                cell['p95_ms'] = stats([r['measure']['all']['success_arrival']['p95_us'] / 1000 for r in runs])
                cell['p99_ms'] = stats([r['measure']['all']['success_arrival']['p99_us'] / 1000 for r in runs])
                cell['database_cpu_cores'] = stats([r['database']['cpu_cores_mean'] for r in runs])
                cell['database_cgroup_cpu_cores'] = stats([r['database']['cgroup_cpu_cores_mean'] for r in runs])
                cell['database_rss_mib'] = stats([r['database']['rss_max'] / 2**20 for r in runs])
                cell['database_cgroup_memory_mib'] = stats([r['database']['cgroup_memory_max'] / 2**20 for r in runs])
                cell['weir_cpu_cores'] = stats([r.get('weir', {}).get('cpu_cores_mean', 0) for r in runs])
                cell['weir_rss_mib'] = stats([r.get('weir', {}).get('rss_max', 0) / 2**20 for r in runs])
                cell['weir_cgroup_memory_mib'] = stats([r.get('weir', {}).get('cgroup_memory_max', 0) / 2**20 for r in runs])
                cell['batch_mean'] = stats([r.get('weir', {}).get('batch_mean', 1) for r in runs])
                cell['client_cpu_cores'] = stats([r['client']['cpu_cores_measure_bracket'] for r in runs])
                cell['client_rss_mib'] = stats([r['client']['rss_max'] / 2**20 for r in runs])
                cell['totals'] = {name: sum(r['measure']['all'][name] for r in runs) for name in ('planned', 'started', 'success', 'client_drop', 'unknown')}
                cell['totals']['api_errors'] = sum(sum((r['measure']['all']['failures'] or {}).values()) for r in runs)
                cell['audit'] = {name: sum(r['audit'][name] for r in runs) for name in ('applied', 'found_version1', 'unknown_found', 'absent')}
                cell['oom_kills'] = sum(r['database']['oom_kills'] for r in runs)
                cells.append(cell)
    phases = []
    for run in [r for r in summary['runs'] if r.get('variant') == 'backpressure']:
        samples = [json.loads(line) for line in (options.raw / (run['prefix'] + '-samples.jsonl')).read_text().splitlines()]
        controls = run['controls']
        result = {'prefix': run['prefix'], 'repetition': run['repetition'], 'audit': run['audit'], 'measure': run['measure'], 'windows': run['windows'], 'phases': []}
        for control in controls:
            begin, end = control['wall_time'], control['wall_time'] + control['duration_seconds']
            chosen = [s for s in samples if begin <= s['wall_time'] <= end and 'metrics' in s]
            if len(chosen) < 2:
                raise RuntimeError('phase evidence missing')
            metrics = [s['metrics'] for s in chosen]
            resource_options = {'cpu': control['cpu_quota'], 'ticks': resource_system['clock_ticks'], 'page_size': resource_system['page_size']}
            phase = {'name': control['phase'], 'cpu_quota': control['cpu_quota'], 'wall_start': begin, 'wall_end': end, 'database_pid': control['database_pid'], 'database_container': control['database_container'], 'limits': control['limits']}
            phase['database'] = comparison.resource_summary([s['database_resources'] for s in chosen], resource_options)
            phase['window'] = {'start': comparison.metric(metrics[0], 'weir_store_window'), 'end': comparison.metric(metrics[-1], 'weir_store_window'), 'minimum': min(comparison.metric(m, 'weir_store_window') for m in metrics), 'maximum': max(comparison.metric(m, 'weir_store_window') for m in metrics)}
            phase['latency_ratio_max'] = max(comparison.metric(m, 'weir_store_latency_ratio') for m in metrics)
            phase['latency_ready_profiles_max'] = max(comparison.metric(m, 'weir_store_latency_ready_profiles') for m in metrics)
            phase['cooldown_samples'] = sum(comparison.metric(m, 'weir_store_cooldown') > 0 for m in metrics)
            phase['pending_max'] = max(comparison.metric(m, 'weir_store_pending_entries') for m in metrics)
            phase['recovery_hold_samples'] = sum(comparison.metric(m, 'weir_store_latency_recovery_hold', optional=True) > 0 for m in metrics)
            phase['actual_cpu_max'] = control.get('actual_cpu_max')
            phase['database_start_ticks'] = control.get('database_start_ticks')
            phase['backpressure_events'] = {reason: comparison.labelled_metric(metrics[-1], 'weir_store_backpressure_events_total', 'reason="' + reason + '"') - comparison.labelled_metric(control['metrics'], 'weir_store_backpressure_events_total', 'reason="' + reason + '"') for reason in ('backend', 'latency')}
            phase['window_changes'] = {direction: comparison.labelled_metric(metrics[-1], 'weir_store_window_changes_total', 'direction="' + direction + '"') - comparison.labelled_metric(control['metrics'], 'weir_store_window_changes_total', 'direction="' + direction + '"') for direction in ('increase', 'decrease')}
            result['phases'].append(phase)
        phases.append(result)
    version = 'v1 intermediate controller before CFS hold fix' if options.version == 'v1' else 'v2 final CFS hold fix'
    result = {'version': version, 'provenance': summary['provenance'], 'selected_tuning': summary.get('selected_tuning'), 'experiment': summary.get('experiment'), 'cells': cells, 'backpressure': phases, 'cleanup': summary['cleanup'], 'raw_run_count': len(summary['runs']), 'selected_offered_rate': summary.get('selected_offered_rate'), 'health_pilots': [r for r in summary['runs'] if r.get('variant') == 'health-pilot']}
    options.output.mkdir(parents=True, exist_ok=False)
    (options.output / 'results.json').write_text(json.dumps(compact(result), indent=2))
    runner = ('es-v2-stable-phase-runner.py' if stable_phase else 'es-v2-phase-runner.py') if options.phase_only else ('es-final-runner.py' if options.version == 'v1' else 'es-v2-runner.py')
    shutil.copyfile(Path(__file__).with_name(runner), options.output / 'runner.py')
    shutil.copyfile(Path(__file__), options.output / 'report.py')
    if options.phase_only:
        shutil.copyfile(Path(__file__).with_name('es-v2-runner.py'), options.output / 'es-v2-runner.py')
    hashes = {}
    with tarfile.open(options.output / 'raw-evidence.tar.xz', 'w:xz') as archive:
        for path in sorted(options.raw.iterdir()):
            if path.is_file() and path.name != 'binaries.tar':
                hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
                archive.add(path, arcname=path.name)
    hashes['raw-evidence.tar.xz'] = hashlib.sha256((options.output / 'raw-evidence.tar.xz').read_bytes()).hexdigest()
    for name in (('results.json', 'runner.py', 'report.py', 'es-v2-runner.py') if options.phase_only else ('results.json', 'runner.py', 'report.py')):
        hashes[name] = hashlib.sha256((options.output / name).read_bytes()).hexdigest()
    (options.output / 'sha256.json').write_text(json.dumps(hashes, indent=2))
    lines = ['# ES v1 中间版本资源对照（2026-10-02）', '', '该宽矩阵使用 CFS 恢复抑制修正之前的 v1（SHA 见 provenance），最终 v2 确认另行报告，不能混用身份。ES 8.19.22 Linux arm64，DB 固定 0.25 CPU / 1536 MiB，Weir 2 CPU / 2304 MiB；单负载进程、32 workers、32 batch operations。所有路径同一最终生成器；4 HTTP/store 并发。每格 3 配对重复、10s warm + 20s measurement，先共同以 1 CPU 训练 JVM，每次重建相同 corpus；1024B 文档、1000 个均匀散列读 key、独立写 ID、无重试。', '', 'old-baseline/new-default 都保留 1ms collect 与 2MiB 普通 Read 预算；这里 default 指保留这些默认值，session/batch 的测试容量与旧版本固定相同。new-tuned 的声明与筛选结果见 results.json，不能将 16KiB 声明泛化到任意大文档。表格为重复中位数，原始值及 min/max 完整保留。p95 是成功请求从计划到达开始的延迟；client drop 与 API 错误独立列出。', '', '|负载|offered/s|路径|成功/s|p95 ms|DB CPU|Weir CPU|DB RSS MiB|Weir RSS MiB|drop/API error（3次合计）|', '|---|---:|---|---:|---:|---:|---:|---:|---:|---:|']
    if options.version == 'v2' and not options.phase_only:
        tuning = summary['selected_tuning']
        memory = 2304 if tuning['workers'] == 32 else 4608
        lines[0] = '# ES v2 最终参数与直连资源对照（2026-10-02）'
        lines[2] = f"最终 v2（实际 SHA/source manifest 完整保留），同一 client-v2、同一 DB/JVM、同 {tuning['workers']} workers；DB 0.25 CPU / 1536 MiB，Weir 2 CPU / {memory} MiB。direct HTTP pool4，Weir pool{tuning['pool']} / collect{tuning['collect_ms']}ms / max_read_size16KiB / batch32 / sessions{tuning['workers']}。每格3配对重复、10s warm+20s measurement、交替路径顺序；共同以1CPU训练JVM，每次重建相同1024B文档corpus，读1000均匀散列key、写独立ID、无重试。"
        lines[4] = '最终调参完整地包含客户端并发、Weir会话/内存预算、批量窗口及普通读取profile，不能只归因于代码。direct与Weir使用相同最终生成器和并发，DB预算保持固定。16KiB声明不适合任意大文档；普通Read超限安全拒绝，写入/Lua/Scan/Native保留既有预算。中间v1四路径矩阵另行保留，不冒充最终代码。表格为重复中位数，p95从成功请求的计划到达开始计时；原始min/max、client drop、API error、UNKNOWN与所有APPLIED审计均保留。'
    for cell in cells:
        workload = '90% Read / 10% Put' if cell['write_every'] == 10 else '100% Put'
        lines.append(f"|{workload}|{cell['rate']}|{cell['variant']}|{cell['success_rps']['median']:.1f}|{cell['p95_ms']['median']:.1f}|{cell['database_cpu_cores']['median']:.3f}|{cell['weir_cpu_cores']['median']:.3f}|{cell['database_rss_mib']['median']:.1f}|{cell['weir_rss_mib']['median']:.1f}|{cell['totals']['client_drop']}/{cell['totals']['api_errors']}|")
    lines += ['', 'CPU 为进程真实 CPU seconds 的采样差值；DB /proc 与 cgroup 原始样本、Weir Prometheus CPU/RSS、memory.current、OOM、ES rejection/queue、生成器 CPU/RSS、采样错误与所有 APPLIED 落库审计保留在原始归档。预算不是实际 RSS；DB JVM/RSS 可能随测试顺序漂移，应结合重复范围判断。', '', '## 实际 CPU 配额下降与恢复', '', '同一最终 Weir 进程、pool4/read16KiB/collect1ms，mixed 3200/s；观测首个实际 Execute 后，DB .25CPU 20s → .08CPU 30s → .25CPU 30s；客户端连续 90s，并完整审计。两个重复独立重启 Weir。', '', '|重复|阶段|quota|真实 DB CPU|窗口始→末[min,max]|latency/backpressure events|ratio max|pending max|', '|---:|---|---:|---:|---|---|---:|---:|']
    for result in phases:
        for phase in result['phases']:
            window, events = phase['window'], phase['backpressure_events']
            lines.append(f"|{result['repetition']}|{phase['name']}|{phase['cpu_quota']}|{phase['database']['cpu_cores_mean']:.3f}|{window['start']:.0f}→{window['end']:.0f}[{window['minimum']:.0f},{window['maximum']:.0f}]|{events['latency']:.0f}/{events['backend']:.0f}|{phase['latency_ratio_max']:.2f}|{phase['pending_max']:.0f}|")
    if options.version == 'v2':
        description = '同一最终 Weir 进程、pool4/read16KiB/collect1ms，mixed 3200/s；观测首个实际 Execute 后，DB .25CPU 20s → .08CPU 30s → .25CPU 30s；客户端连续 90s，并完整审计。两个重复独立重启 Weir。'
        replacement = 'v2背压单独使用batch8/worker32/session32/Weir2304MiB/pool4/read16KiB/collect3ms/client_queue32以形成真实store积压，不能把其性能与上方batch32吞吐配置混用。连续mixed3200/s：观测首个Execute后DB .25CPU20s → .08CPU30s → .25CPU30s；完整90s并审计。两个重复独立重启Weir、DB保持同PID/startticks/DockerStartedAt/restart0。实际cpu.max与recovery_hold样本在JSON和原始controls中。'
        lines = [replacement if line == description else line for line in lines]
    if options.phase_only:
        first_phase = lines.index('## 实际 CPU 配额下降与恢复')
        quota = 0.5 if stable_phase else 0.25
        rate = summary.get('selected_offered_rate', 3200)
        lines = ['# ES v2 实际 CPU 下降与恢复（2026-10-02）', '',
            f'此实验与吞吐矩阵使用独立 owned fixture。冻结 weir-v2/client-v2 SHA 及 source manifest 保留；ES8.19.22，DB1536MiB、同 Java PID/startticks/StartedAt/restart0，Weir2CPU/2304MiB、单进程。每个重复重启 Weir，固定 batch8/pool4/worker32/session32/read16KiB/collect3ms/client_queue32；JVM先按同profile预热，连续{rate}/s mixed90/10负载的前20s {quota}CPU建立本进程时延baseline。', '',
            f'实际DB quota {quota}→.08→{quota}：控制前读取并核验 cpu.max 和进程身份；每0.5s采样完整Prometheus、/proc、cgroup、ES队列与RSS；所有drop/APIerror/UNKNOWN和APPLIED审计完整保留。阶段跨界的10s窗口不当作单阶段结果。', ''] + lines[first_phase:]
        if stable_phase:
            lines = [line.replace('.25CPU20s', '.5CPU20s').replace('.25CPU30s', '.5CPU30s').replace('mixed3200/s', f'mixed{rate}/s') for line in lines]
            lines += ['', '先按同profile做健康容量pilot，仅当 offered=success、无drop/APIerror且measurement window固定4/无decrease才选择该输入；所有未通过pilot也完整保留在results.json/raw archive。']
        else:
            lines += ['', '该参数档在初始.25CPU健康候选阶段已经由4降到1，两个重复恢复到.25CPU仍停在1；不能把恢复到仍过载的配额预设为应升窗，也不能把此实验报告为完整下降/恢复成功。0.5CPU健康基线的独立实验另行保留。']
    elif not phases:
        first_phase = lines.index('## 实际 CPU 配额下降与恢复')
        lines = lines[:first_phase] + ['实际 CPU 下降与恢复在独立 v2 quota fixture 报告；本矩阵只评估固定 .25CPU。主 runner 在全部24格完成审计后受控停止，尚未执行旧参数可选phase；随后 finally inventory 清理成功，controlled-stop.json保留。']
    lines += ['', '恢复结果与阶段内每 10 秒成功吞吐、p95、API error、client drop 见 results.json；原始 controls 包含实际 quota、容器 ID、DB PID 和完整指标。取消会隔离时延学习，不能把未知/取消结果视为 DB 健康。' if phases else '高负载client drop仍存在，不能声称任意负载均达到3200/s；batch等参数只对该1024B/均匀key实验有效。', '', '清理结果：' + json.dumps(summary['cleanup'], ensure_ascii=False) + '。', '', '证据：[results.json](results.json)、[原始归档（含完整summary.json与histograms）](raw-evidence.tar.xz)、[SHA256](sha256.json)。']
    (options.output / 'findings.md').write_text('\n'.join(lines) + '\n')
    hashes['findings.md'] = hashlib.sha256((options.output / 'findings.md').read_bytes()).hexdigest()
    (options.output / 'sha256.json').write_text(json.dumps(hashes, indent=2))
    result = {'cells': len(cells), 'backpressure_repetitions': len(phases), 'output': str(options.output)}
    print(json.dumps(result))


if __name__ == '__main__':
    main()
