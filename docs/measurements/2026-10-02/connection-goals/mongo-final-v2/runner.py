import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace
import urllib.request

WORKSPACE = Path('/Users/liran/Projects/liran/go/weir')
OWNER = 'weir-bp-9282b4b43a1e'
LABEL = 'weir.load-comparison.owner'
HTTP = 'http://127.0.0.1:19183'
BACKEND = 'mongodb://127.0.0.1:27017/?directConnection=true'
CLIENT_BINARY = 'client-final'
WEIR_BINARY = 'weir-final'
ENV = dict(os.environ, KUBECTL_REMOTE_COMMAND_WEBSOCKETS='false')
sys.path.insert(0, str(WORKSPACE / 'scripts'))
def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value
LOAD = module('connection_load', WORKSPACE / 'scripts/connection-load.py')
FIXTURE = module('connection_fixture', WORKSPACE / 'scripts/connection-fixture.py')


def kube(args, role=None, timeout=60):
    command = ['kubectl', '-n', OWNER]
    if role:
        command += ['exec', 'fixture', '-c', role, '--']
    result = subprocess.run(command + args, env=ENV, capture_output=True,
                            text=True, timeout=timeout, check=True)
    return result.stdout.strip()


def shell(script, role='client'):
    return kube(['sh', '-c', script], role)


def http(name):
    with urllib.request.urlopen(HTTP + '/' + name, timeout=10) as response:
        value = response.read((8 << 20) + 1)
        if len(value) > 8 << 20:
            raise RuntimeError('owned HTTP file exceeds bound')
        return value


def put(path):
    kube(['cp', str(path), 'fixture:/bench/' + path.name, '-c', 'client'], timeout=90)


def identity():
    raw = shell('cat /proc/1/comm /proc/1/stat /sys/fs/cgroup/cpu.max; getconf CLK_TCK; getconf PAGESIZE; date +%s.%N', 'database')
    lines = raw.splitlines()
    stat = lines[1][lines[1].rfind(')') + 2:].split()
    pod = json.loads(kube(['get', 'pod', 'fixture', '-o', 'json']))
    db = next(c for c in pod['status']['containerStatuses'] if c['name'] == 'database')
    result = {'comm': lines[0], 'start_ticks': int(stat[19]), 'cpu_max': lines[2],
              'ticks_per_second': int(lines[3]), 'page_size': int(lines[4]),
              'wall_time': float(lines[5]), 'container_id': db['containerID'],
              'restart_count': db['restartCount'], 'node': pod['spec']['nodeName']}
    if result['comm'] != 'mongod' or result['cpu_max'] != '50000 100000':
        raise RuntimeError('database identity/quota mismatch')
    return result


def guard():
    ns = json.loads(kube(['get', 'namespace', OWNER, '-o', 'json']))
    pod = json.loads(kube(['get', 'pod', 'fixture', '-o', 'json']))
    if any(v['metadata']['labels'].get(LABEL) != OWNER for v in (ns, pod)):
        raise RuntimeError('owned fixture label mismatch')
    return ns, pod


def stop(stem, filename, comm, role):
    expression = 'stop_pid=$(cat ' + stem + '-' + filename + '.pid); test "$(cat /proc/$stop_pid/comm)" = ' + comm + '; kill -TERM "$stop_pid"; for n in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$stop_pid" 2>/dev/null || exit 0; test "$(cut -d \' \' -f 3 /proc/$stop_pid/stat)" = Z && exit 0; sleep 0.2; done; exit 1'
    shell(expression, role)


def resource_summary(records, report, ident):
    start = LOAD.timestamp(report['clients'][0]['start']).timestamp()
    end = start + report['measurement_seconds']
    active = [r for r in records if start <= r['wall_time'] <= end]
    if len(active) < 15 or active[-1]['monotonic'] - active[0]['monotonic'] < 15:
        raise RuntimeError('insufficient active resource coverage')
    if len({r['process_start_ticks'] for r in active}) != 1:
        raise RuntimeError('resource process restarted')
    first, last = active[0], active[-1]
    elapsed = last['monotonic'] - first['monotonic']
    value = {'samples': len(active), 'sample_seconds': elapsed,
             'cpu_cores': (last['process_cpu_ticks'] - first['process_cpu_ticks']) / ident['ticks_per_second'] / elapsed,
             'rss_max': max(r['rss_pages'] for r in active) * ident['page_size'],
             'cgroup_memory_max': max(r['memory_current'] for r in active),
             'oom_kills': last['memory_events']['oom_kill'] - first['memory_events']['oom_kill']}
    return value


def observe(output, name, seconds):
    command = ['kubectl', '-n', OWNER, 'exec', 'fixture', '-c', 'database', '--',
               'env', 'WEIR_CAPACITY_INTEGRATION=1', '/bench/' + CLIENT_BINARY,
               '-mode', 'connection-observe', '-pid', '1', '-connection-port', '27017',
               '-seconds', str(seconds), '-connection-interval-ms', '200']
    result = subprocess.run(command, env=ENV, capture_output=True, text=True, timeout=seconds + 25)
    (output / (name + '.jsonl')).write_text(result.stdout)
    (output / (name + '.stderr.log')).write_text(result.stderr)
    result.check_returncode()
    return [json.loads(line) for line in result.stdout.splitlines()]


def trial(options):
    root, name = options.root, options.name
    processes, use_weir, initial = options.processes, options.use_weir, options.initial
    guard()
    ident = identity()
    if any(ident[k] != initial[k] for k in ('container_id', 'restart_count', 'start_ticks')):
        raise RuntimeError('database restarted across trials')
    remote = 'connection-' + name
    shell('mkdir /bench/' + remote)
    stem = '/bench/' + remote + '/' + remote
    weir_started, db_sampler, weir_sampler = False, False, False
    try:
        if use_weir:
            basic = {'listeners': {'application': '127.0.0.1:7447'}, 'diagnostics': {'address': '127.0.0.1:7449'},
                     'memory': '2304MiB', 'transport': {'max_connections': 64, 'max_sessions': 32}}
            local = {'max_concurrency': 4, 'max_batch_operations': 16, 'max_read_size': '16KiB', 'batch_collect': '3ms',
                     'mongodb': {'uri': BACKEND}}
            routes = {'services': [{'name': 'database', 'local': local}], 'routes': [{'store': 'records', 'service': 'database'}]}
            for kind, value in [('node', basic), ('routes', routes)]:
                path = root / ('connection-' + kind + '.json')
                path.write_text(json.dumps(value, indent=2))
                put(path)
            shell('/bench/' + WEIR_BINARY + ' serve --config /bench/connection-node.json --routes /bench/connection-routes.json > ' + stem + '-weir.log 2>&1 < /dev/null & echo $! > ' + stem + '-weir.pid', 'weir')
            weir_started = True
            shell('for n in 1 2 3 4 5 6 7 8 9 10; do wget -q -O /dev/null http://127.0.0.1:7449/readyz && exit 0; sleep 0.5; done; exit 1', 'weir')
            before = shell('wget -q -O - http://127.0.0.1:7449/metrics', 'weir')
            (root / (name + '-before.metrics')).write_text(before)
            shell('/bench/sampler-metrics -pid "$(cat ' + stem + '-weir.pid)" -output ' + stem + '-weir.jsonl -metrics http://127.0.0.1:7449/metrics > ' + stem + '-weir-sampler.log 2>&1 < /dev/null & echo $! > ' + stem + '-weir-sampler.pid', 'weir')
            weir_sampler = True
        shell('/bench/sampler -pid 1 -output ' + stem + '-database.jsonl > ' + stem + '-database-sampler.log 2>&1 < /dev/null & echo $! > ' + stem + '-database-sampler.pid', 'database')
        db_sampler = True
        opts = SimpleNamespace(backend=BACKEND, targets='127.0.0.1:7447' if use_weir else '',
                client_command_json=json.dumps(['kubectl', '-n', OWNER, 'exec', 'fixture', '-c', 'client', '--', 'env', 'WEIR_CAPACITY_INTEGRATION=1', '/bench/' + CLIENT_BINARY]),
                observer_command_json=json.dumps(['kubectl', '-n', OWNER, 'exec', 'fixture', '-c', 'database', '--', 'env', 'WEIR_CAPACITY_INTEGRATION=1', '/bench/' + CLIENT_BINARY]),
                rate=200, processes=processes, seconds=20, idle_seconds=12, closed_seconds=5, start_delay=20,
                database_pid='1', database_port=27017, interval_ms=200, workers=8, pool=4, output=root/name)
        os.environ['KUBECTL_REMOTE_COMMAND_WEBSOCKETS'] = 'false'
        code = LOAD.run_probes(opts)
        report = json.loads((root/name/'summary.json').read_text())
        stop(stem, 'database-sampler', 'sampler', 'database')
        db_sampler = False
        database = [json.loads(line) for line in http(remote + '/' + remote + '-database.jsonl').splitlines()]
        (root/(name + '-database-resources.jsonl')).write_bytes(http(remote + '/' + remote + '-database.jsonl'))
        report['process_resources'] = {'database': resource_summary(database, report, ident)}
        report.update(name=name, weir_instances=int(use_weir), database_identity=ident, workers_each=8, pool_each=4, corpus_size=1000, distinct_keys_planned=min(1000,max(client['planned'] for client in report['clients'])), payload_bytes=1024, mutation_operations=0)
        if use_weir:
            after = shell('wget -q -O - http://127.0.0.1:7449/metrics', 'weir')
            (root/(name + '-after.metrics')).write_text(after)
            report['owner_after_clients_closed'] = FIXTURE.owner_snapshot(after)
            rejections = lambda text: {m.group(1):int(m.group(2)) for m in re.finditer(r'weir_admission_rejections_total\{reason="([^"]+)"\} (\d+)', text)}
            a, b = rejections(after), rejections(before)
            report['admission_rejections_full_trial'] = {key:a[key]-b[key] for key in a}
            stop(stem, 'weir-sampler', 'sampler-metrics', 'weir')
            weir_sampler = False
            raw = http(remote + '/' + remote + '-weir.jsonl')
            (root/(name + '-weir-resources.jsonl')).write_bytes(raw)
            records = [json.loads(line) for line in raw.splitlines()]
            report['process_resources']['weir'] = resource_summary(records, report, ident)
            stdout = (root/(name + '-weir-shutdown-connections.jsonl')).open('w')
            stderr = (root/(name + '-weir-shutdown-observer.stderr.log')).open('w')
            command = json.loads(opts.observer_command_json) + ['-mode','connection-observe','-pid','1','-connection-port','27017','-seconds','10','-connection-interval-ms','200']
            watcher = subprocess.Popen(command, env=ENV, stdout=stdout, stderr=stderr)
            try:
                time.sleep(1)
                stop(stem, 'weir', WEIR_BINARY[:15], 'weir')
                weir_started = False
                if watcher.wait(timeout=35):
                    raise RuntimeError('shutdown observer failed')
            finally:
                if watcher.poll() is None:
                    watcher.terminate(); watcher.wait(timeout=5)
                stdout.close(); stderr.close()
            log = http(remote + '/' + remote + '-weir.log').decode()
            (root/(name + '-shutdown.log')).write_text(log)
            report['closed_owner'] = FIXTURE.closed_owner(log)
            samples = LOAD.read_jsonl(root/(name + '-weir-shutdown-connections.jsonl'))
            report['weir_shutdown_last_five'] = [s['established'] for s in samples[-5:]]
        if code:
            report['qualification_note'] = 'failed offered-work success qualification; no retry or deadline changes'
        (root/(name + '-summary.json')).write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'name':name,'success':report['success'],'planned':report['planned'],'qualified':report['qualified'],'active':report['phases']['active'],'resources':report['process_resources']}),flush=True)
        return report
    finally:
        errors = []
        for live, file, comm, role in [(db_sampler,'database-sampler','sampler','database'),(weir_sampler,'weir-sampler','sampler-metrics','weir'),(weir_started,'weir',WEIR_BINARY[:15],'weir')]:
            if live:
                try:
                    stop(stem,file,comm,role)
                except Exception as error:
                    errors.append(str(error))
        try:
            shell('tar -czf /bench/' + remote + '.tar.gz -C /bench ' + remote)
            archive = http(remote + '.tar.gz')
            digest = hashlib.sha256(archive).hexdigest()
            expected = shell('sha256sum /bench/' + remote + '.tar.gz').split()[0]
            if digest != expected:
                raise RuntimeError('remote raw archive hash mismatch')
            (root/(name + '-raw.tar.gz')).write_bytes(archive)
            (root/(name + '-raw.sha256')).write_text(digest+'\n')
        except Exception as error:
            errors.append(str(error))
        if errors:
            (root/(name + '-cleanup-errors.json')).write_text(json.dumps(errors,indent=2))
            raise RuntimeError('owned trial finalization failed: ' + str(errors))


def main():
    global CLIENT_BINARY, WEIR_BINARY
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--handoff-confirmed',action='store_true',required=True)
    parser.add_argument('--weir-binary',default='weir-final')
    parser.add_argument('--client-binary',default='client-final')
    parser.add_argument('--weir-sha256',required=True)
    parser.add_argument('--client-sha256',required=True)
    args = parser.parse_args()
    if not all(re.fullmatch(r'[a-z0-9-]{1,32}',v) for v in (args.weir_binary,args.client_binary)):
        raise RuntimeError('owned binary filename invalid')
    CLIENT_BINARY, WEIR_BINARY = args.client_binary,args.weir_binary
    root = args.output.resolve(); root.mkdir(exist_ok=False)
    (root/'runner.py').write_bytes(Path(__file__).read_bytes())
    ns,pod = guard()
    initial = identity()
    (root/'namespace-before.json').write_text(json.dumps(ns,indent=2))
    (root/'pod-before.json').write_text(json.dumps(pod,indent=2))
    for role in ('client','weir','database'):
        processes = shell('for task_proc in /proc/[0-9]*; do test -r "$task_proc/stat" && cat "$task_proc/stat" 2>/dev/null; done',role)
        (root/(role+'-processes-before.txt')).write_text(processes+'\n')
        for line in processes.splitlines():
            begin,end = line.find('('),line.rfind(')')
            name = line[begin+1:end]
            state = line[end+2:].split()[0]
            if state != 'Z' and name in ('client-final','weir-final','baseline-weir','sampler','sampler-metrics'):
                raise RuntimeError('previous fixture load/sampler still running')
    hashes = shell('sha256sum /bench/' + CLIENT_BINARY + ' /bench/' + WEIR_BINARY + ' /bench/sampler /bench/sampler-metrics')
    lines = hashes.splitlines()
    if lines[0].split()[0] != args.client_sha256 or lines[1].split()[0] != args.weir_sha256:
        raise RuntimeError('frozen binary hash mismatch')
    (root/'binary-sha256.txt').write_text(hashes+'\n')
    output = {'owner':OWNER,'database_identity':initial,'binary_sha256':hashes,'runs':[]}
    try:
        baseline = observe(root,'database-baseline-before',4)
        output['baseline_last_five'] = [s['established'] for s in baseline[-5:]]
        for name,n,weir in [('direct-1',1,False),('direct-16',16,False),('weir-1-clients-1',1,True),('weir-1-clients-16',16,True)]:
            trial_options = SimpleNamespace(root=root,name=name,processes=n,use_weir=weir,initial=initial)
            output['runs'].append(trial(trial_options))
            (root/'fixture-summary.json').write_text(json.dumps(output,indent=2)+'\n')
        final = observe(root,'database-baseline-after',4)
        output['final_last_five'] = [s['established'] for s in final[-5:]]
        if output['final_last_five'] != output['baseline_last_five']:
            raise RuntimeError('database connections did not return to measured baseline')
    finally:
        guard()
        deletion = kube(['delete','namespace',OWNER,'--wait=false'])
        evidence = None
        for _ in range(40):
            result = subprocess.run(['kubectl','get','namespace',OWNER,'-o','json'],env=ENV,capture_output=True,text=True,timeout=30)
            if result.returncode and 'NotFound' in result.stderr:
                evidence = result.stderr.strip()
                break
            time.sleep(1)
        output['cleanup'] = {'deleted':deletion,'namespace_not_found':evidence}
        (root/'fixture-summary.json').write_text(json.dumps(output,indent=2)+'\n')
        if evidence is None:
            raise RuntimeError('owned namespace cleanup did not confirm NotFound')
        print(json.dumps(output['cleanup']),flush=True)



if __name__ == '__main__':
    main()
