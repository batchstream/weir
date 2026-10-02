import argparse
import datetime
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
OWNER = json.loads((ROOT / 'state.json').read_text())['owner']
ENV = dict(os.environ)
ENV['KUBECTL_REMOTE_COMMAND_WEBSOCKETS'] = 'false'
HTTP = 'http://127.0.0.1:19183'


def kube(args, role=None, timeout=60):
    command = ['kubectl', '-n', OWNER]
    if role:
        command += ['exec', 'fixture', '-c', role, '--']
    result = subprocess.run(command + args, env=ENV, capture_output=True,
                            text=True, timeout=timeout, check=True)
    return result.stdout.strip()


def put(path):
    kube(['cp', str(path), 'fixture:/bench/' + path.name, '-c', 'client'], timeout=90)


def shell(script, role='client'):
    return kube(['sh', '-c', script], role)


def http(name):
    with urllib.request.urlopen(HTTP + '/' + name, timeout=5) as response:
        return response.read()


def identity():
    raw = shell('cat /proc/1/comm /proc/1/stat /sys/fs/cgroup/cpu.max; date +%s.%N', 'database')
    lines = raw.splitlines()
    stat = lines[1][lines[1].rfind(')') + 2:].split()
    pod = json.loads(kube(['get', 'pod', 'fixture', '-o', 'json']))
    database = next(c for c in pod['status']['containerStatuses'] if c['name'] == 'database')
    value = {'comm': lines[0], 'start_ticks': int(stat[19]), 'cpu_max': lines[2],
             'wall_time': float(lines[3]), 'container_id': database['containerID'],
             'restart_count': database['restartCount']}
    if value['comm'] != 'mongod':
        raise RuntimeError('database PID1 is not mongod')
    with (ROOT / 'database-identity.jsonl').open('a') as log:
        log.write(json.dumps({'raw': raw, 'parsed': value}) + '\n')
    return value


def resize(cpu):
    before = identity()
    resources = {'requests': {'cpu': '50m', 'memory': '1536Mi'},
                 'limits': {'cpu': str(cpu) + 'm', 'memory': '1536Mi'}}
    patch = {'spec': {'containers': [{'name': 'database', 'resources': resources}]}}
    requested = time.time()
    kube(['patch', 'pod', 'fixture', '--subresource=resize', '--type=strategic',
          '-p', json.dumps(patch)])
    for _ in range(30):
        after = identity()
        quota, period = map(int, after['cpu_max'].split())
        if abs(quota / period - cpu / 1000) < 1e-8:
            if any(before[k] != after[k] for k in ['start_ticks', 'container_id', 'restart_count']):
                raise RuntimeError('database restarted during resize')
            value = {'requested_wall_time': requested, 'confirmed': after, 'before': before}
            return value
        time.sleep(1)
    raise RuntimeError('CPU resize did not reach the requested live cgroup quota')


def wait_file(name, timeout=180):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            return http(name)
        except Exception:
            time.sleep(0.5)
    raise RuntimeError('missing owned evidence file: ' + name)


def run(opts):
    mode, tag, rate = opts.mode, opts.tag, opts.rate
    write_every, healthy_cpu, slow_cpu = opts.write_every, opts.healthy_cpu, opts.slow_cpu
    namespace = json.loads(kube(['get', 'namespace', OWNER, '-o', 'json']))
    if namespace['metadata']['labels'].get('weir.load-comparison.owner') != OWNER:
        raise RuntimeError('namespace owner mismatch')
    output = ROOT / tag
    output.mkdir()
    hashes = shell('sha256sum /bench/weir-final /bench/baseline-weir /bench/client-final /bench/sampler /bench/sampler-metrics')
    (output / 'remote-binary-hashes.txt').write_text(hashes + '\n')
    transitions = [{'phase': 'healthy', 'resize': resize(healthy_cpu)}]
    binary = 'baseline-weir' if mode == 'baseline' else 'weir-final'
    basic = {'listeners': {'application': '127.0.0.1:7447'},
             'diagnostics': {'address': '127.0.0.1:7449'}, 'memory': '2304MiB',
             'transport': {'max_connections': 16, 'max_sessions': 32}}
    local = {'max_concurrency': 4, 'max_batch_operations': opts.batch,
             'mongodb': {'uri': 'mongodb://127.0.0.1:27017/?directConnection=true'}}
    if mode != 'baseline':
        local.update(batch_collect='3ms', max_read_size='16KiB')
    routes = {'services': [{'name': 'database', 'local': local}],
              'routes': [{'store': 'records', 'service': 'database'}]}
    for kind, value in [('node', basic), ('routes', routes)]:
        path = ROOT / (tag + '-' + kind + '.json')
        path.write_text(json.dumps(value, indent=2))
        put(path)
    shell('mkdir /bench/' + tag)
    stem = '/bench/' + tag + '/' + tag
    start = ' '.join(['/bench/' + binary, 'serve', '--config', '/bench/' + tag + '-node.json',
                      '--routes', '/bench/' + tag + '-routes.json'])
    shell(start + ' > ' + stem + '-weir.log 2>&1 < /dev/null & echo $! > ' + stem + '-weir.pid', 'weir')
    shell('for n in 1 2 3 4 5 6 7 8 9 10; do wget -q -O /dev/null http://127.0.0.1:7449/readyz && exit 0; sleep 0.5; done; exit 1', 'weir')
    shell('/bench/sampler-metrics -pid "$(cat ' + stem + '-weir.pid)" -output ' + stem + '-weir.jsonl -metrics http://127.0.0.1:7449/metrics > ' + stem + '-weir-sampler.log 2>&1 < /dev/null & echo $! > ' + stem + '-weir-sampler.pid', 'weir')
    shell('/bench/sampler -pid 1 -output ' + stem + '-database.jsonl > ' + stem + '-database-sampler.log 2>&1 < /dev/null & echo $! > ' + stem + '-database-sampler.pid', 'database')
    command = ['env', 'WEIR_CAPACITY_INTEGRATION=1', '/bench/client-final', '-mode', 'trial',
               '-backend', 'mongodb://127.0.0.1:27017/?directConnection=true', '-target', '127.0.0.1:7447',
               '-rate', str(rate), '-warm', '20', '-seconds', str(opts.seconds), '-prefix', tag,
               '-pool', '4', '-workers', '32', '-write-every', str(write_every),
               '-arrival-expiry-ms', '100', '-max-catchup', '512', '-client-queue', str(opts.client_queue),
               '-load-delay-ms', '3000', '-mutation-reservation', str(1000 + rate * (20 + opts.seconds) // write_every)]
    script = 'set -eu\nmkfifo ' + stem + '.pipe\ncat ' + stem + '.pipe > ' + stem + '-client.jsonl &\nreader=$!\n' + shlex.join(command) + ' > ' + stem + '.pipe 2> ' + stem + '-client-stderr.log\nwait "$reader"\ntouch ' + stem + '.done\n'
    path = ROOT / (tag + '-client.sh')
    path.write_text(script)
    put(path)
    shell('sh /bench/' + path.name + ' > ' + stem + '-client-launch.log 2>&1 < /dev/null &')
    begin = None
    for _ in range(90):
        try:
            raw = http(tag + '/' + tag + '-client.jsonl')
        except Exception:
            time.sleep(0.5)
            continue
        for line in raw.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get('type') == 'client_start':
                begin = datetime.datetime.fromisoformat(value['sample']['end'].replace('Z', '+00:00')).timestamp() + 3.02
                break
        if begin is not None:
            break
        time.sleep(0.5)
    if begin is None:
        raise RuntimeError('client start marker absent')
    message = {'tag': tag, 'mode': mode, 'estimated_load_start': begin}
    print(json.dumps(message), flush=True)
    for phase, cpu in [('slow', slow_cpu), ('recovery', healthy_cpu)]:
        until = begin + 20 if phase == 'slow' else transitions[-1]['resize']['confirmed']['wall_time'] + 30
        while time.time() < until:
            time.sleep(min(0.5, until - time.time()))
        transition = {'phase': phase, 'resize': resize(cpu)}
        transitions.append(transition)
        (output / 'transitions.json').write_text(json.dumps(transitions, indent=2))
        message = {'tag': tag, **transition}
        print(json.dumps(message), flush=True)
    wait_file(tag + '/' + tag + '.done')
    shell('stop_pid=$(cat ' + stem + '-database-sampler.pid); test "$(cat /proc/$stop_pid/comm)" = sampler; kill -TERM "$stop_pid"', 'database')
    shell('stop_pid=$(cat ' + stem + '-weir-sampler.pid); test "$(cat /proc/$stop_pid/comm)" = sampler-metrics; kill -TERM "$stop_pid"; stop_pid=$(cat ' + stem + '-weir.pid); test "$(cat /proc/$stop_pid/comm)" = ' + binary + '; kill -TERM "$stop_pid"; for n in 1 2 3 4 5 6 7 8 9 10; do test -f /proc/$stop_pid/stat || exit 0; grep -q ") Z " /proc/$stop_pid/stat && exit 0; sleep 0.2; done; exit 1', 'weir')
    shell('tar -czf /bench/' + tag + '.tar.gz -C /bench ' + tag)
    archive = http(tag + '.tar.gz')
    (output / 'raw.tar.gz').write_bytes(archive)
    remote_hash = shell('sha256sum /bench/' + tag + '.tar.gz').split()[0]
    if hashlib.sha256(archive).hexdigest() != remote_hash:
        raise RuntimeError('raw archive checksum mismatch')
    manifest = {'tag': tag, 'mode': mode, 'rate': rate, 'write_every': write_every,
                'healthy_cpu_m': healthy_cpu, 'slow_cpu_m': slow_cpu, 'batch_operations': opts.batch, 'client_queue': opts.client_queue, 'measure_seconds': opts.seconds, 'transitions': transitions,
                'raw_sha256': remote_hash, 'database_identity': identity(), 'binary_hashes': hashes,
                'database_fixed_request_cpu_m': 50, 'weir_request_cpu_m': 250, 'client_request_cpu_m': 50}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    message = {'tag': tag, 'finished': True, 'raw_sha256': remote_hash}
    print(json.dumps(message), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['baseline', 'current'], required=True)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--rate', type=int, default=2500)
    parser.add_argument('--batch', type=int, default=8)
    parser.add_argument('--client-queue', type=int, default=32)
    parser.add_argument('--seconds', type=int, default=90)
    parser.add_argument('--write-every', type=int, choices=[1, 10], default=10)
    parser.add_argument('--healthy-cpu', type=int, default=500)
    parser.add_argument('--slow-cpu', type=int, default=50)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]{1,30}', args.tag):
        raise RuntimeError('trial tag must be a bounded safe filename')
    if not (1 <= args.rate <= 10000 and 1 <= args.batch <= 128 and 0 <= args.client_queue <= 512 and 60 <= args.seconds <= 120):
        raise RuntimeError('finite load parameter out of range')
    if not (50 <= args.slow_cpu <= args.healthy_cpu <= 2000):
        raise RuntimeError('CPU limits must fit the fixed 50m request')
    run(args)
