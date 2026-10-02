import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import hashlib
import tarfile

ROOT = Path(__file__).resolve().parent
OWNER = json.loads((ROOT / 'state.json').read_text())['owner']
ENV = dict(os.environ)
ENV['KUBECTL_REMOTE_COMMAND_WEBSOCKETS'] = 'false'


def kube(args, role=None, timeout=60):
    command = ['kubectl', '-n', OWNER]
    if role:
        command += ['exec', 'fixture', '-c', role, '--']
    result = subprocess.run(command + args, capture_output=True, text=True,
                            env=ENV, timeout=timeout, check=True)
    return result.stdout.strip()


def put(local, remote):
    kube(['cp', str(local), 'fixture:' + remote, '-c', 'client'], timeout=300)


def shell(script, role='client'):
    return kube(['sh', '-c', script], role)


def launch_suite(cases, tag):
    out = ROOT / tag
    out.mkdir()
    for index, case in enumerate(cases):
        name = tag + '-' + str(index)
        case['prefix'] = name
        basic = {
            'listeners': {'application': '127.0.0.1:7447'},
            'diagnostics': {'address': '127.0.0.1:7449'},
            'memory': '2304MiB',
            'transport': {'max_connections': 16, 'max_sessions': case.get('workers', 32)},
        }
        local = {
            'max_concurrency': case.get('pool', 4),
            'max_batch_operations': case.get('batch', 32),
            'mongodb': {'uri': 'mongodb://127.0.0.1:27017/?directConnection=true'},
        }
        if case['mode'] != 'baseline':
            if 'collect' in case:
                local['batch_collect'] = str(case['collect']) + 'ms'
            if 'read_limit' in case:
                local['max_read_size'] = case['read_limit']
        routes = {'services': [{'name': 'database', 'local': local}],
                  'routes': [{'store': 'records', 'service': 'database'}]}
        for kind, data in [('node', basic), ('routes', routes)]:
            path = out / (name + '-' + kind + '.yaml')
            path.write_text(json.dumps(data, indent=2))
    (out / 'cases.json').write_text(json.dumps(cases, indent=2))
    suite = ['set -eu']
    if tag == 'resource':
        suite += ['while [ "$(cat /bench/formal/state)" != completed ]; do sleep 1; done']
    suite += ['mkdir /bench/' + tag,
             'printf started > /bench/' + tag + '/state',
             'exec 3> /bench/' + tag + '-control.pipe', 'exec 4> /bench/' + tag + '-database.pipe']
    for case in cases:
        name = case['prefix']
        stem = '/bench/' + tag + '/' + name
        if case['mode'] != 'direct':
            binary = 'baseline-weir' if case['mode'] == 'baseline' else 'weir'
            suite += ['printf "%s %s\\n" ' + shlex.quote(name) + ' ' + binary + ' >&3',
                      'while [ ! -f ' + stem + '.ready ]; do sleep 0.1; done']
        command = [
            'env', 'WEIR_CAPACITY_INTEGRATION=1', '/bench/client',
            '-mode', 'trial', '-backend', 'mongodb://127.0.0.1:27017/?directConnection=true',
            '-rate', str(case['rate']), '-warm', str(case.get('warm', 5)),
            '-seconds', str(case.get('seconds', 15)), '-prefix', name,
            '-pool', str(case.get('direct_pool', case.get('pool', 4))),
            '-workers', str(case.get('workers', 32)),
            '-write-every', str(case.get('write_every', 10)),
            '-arrival-expiry-ms', '100', '-max-catchup', '512', '-client-queue', '256',
            '-mutation-reservation', str(1000 + case['rate'] *
                (case.get('warm', 5) + case.get('seconds', 15)) // case.get('write_every', 10)),
        ]
        if case['mode'] != 'direct':
            command += ['-target', '127.0.0.1:7447']
        suite += [
            'printf "%s start\\n" ' + shlex.quote(name) + ' >&4',
            'while [ ! -f ' + stem + '.database_ready ]; do sleep 0.1; done',
            'mkfifo ' + stem + '.pipe',
            'cat ' + stem + '.pipe > ' + stem + '-client.jsonl &',
            'reader=$!', shlex.join(command) + ' > ' + stem + '.pipe 2> ' + stem + '-stderr.log',
            'wait "$reader"', 'printf "%s stop\\n" ' + shlex.quote(name) + ' >&4',
            'while [ ! -f ' + stem + '.database_stopped ]; do sleep 0.1; done',
        ]
        if case['mode'] != 'direct':
            suite += ['printf "%s stop\\n" ' + shlex.quote(name) + ' >&3',
                      'while [ ! -f ' + stem + '.stopped ]; do sleep 0.1; done']
        suite += ['printf completed > ' + stem + '.done']
    suite += ['exec 3>&-', 'exec 4>&-', 'printf completed > /bench/' + tag + '/state',
              'tar -czf /bench/' + tag + '.tar.gz -C /bench ' + tag + ' ' + tag + '-cases.json',
              'sha256sum /bench/' + tag + '.tar.gz > /bench/' + tag + '.sha256']
    path = out / 'suite.sh'
    path.write_text('\n'.join(suite) + '\n')
    control = [
        'set -eu', 'while IFS=" " read -r name binary; do',
        'tag=$(printf "%s" "$name" | sed "s/-[0-9]*$//")',
        'stem=/bench/$tag/$name', 'if [ "$binary" = stop ]; then',
        'kill -TERM "$(cat "$stem.sampler_pid")"',
        'weir_pid=$(cat "$stem.weir_pid")',
        'test "$(cat /proc/$weir_pid/comm)" = weir || test "$(cat /proc/$weir_pid/comm)" = baseline-weir',
        'kill -TERM "$weir_pid"',
        'for i in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$weir_pid" 2>/dev/null || break; sleep 0.5; done',
        'touch "$stem.stopped"', 'continue', 'fi',
        '/bench/$binary serve --config /bench/$name-node.yaml --routes /bench/$name-routes.yaml > "$stem-weir.log" 2>&1 < /dev/null &',
        'weir_pid=$!', 'printf "%s" "$weir_pid" > "$stem.weir_pid"',
        'for i in 1 2 3 4 5 6 7 8 9 10; do wget -q -O /dev/null http://127.0.0.1:7449/readyz && break; sleep 0.5; done',
        'wget -q -O /dev/null http://127.0.0.1:7449/readyz',
        '/bench/sampler-metrics -pid "$weir_pid" -output "$stem-weir.jsonl" -metrics http://127.0.0.1:7449/metrics > "$stem-weir-sampler.log" 2>&1 < /dev/null &',
        'printf "%s" "$!" > "$stem.sampler_pid"', 'touch "$stem.ready"',
        'done < /bench/' + tag + '-control.pipe',
    ]
    path = out / 'control.sh'
    path.write_text('\n'.join(control) + '\n')
    database = [
        'set -eu', 'test "$(cat /proc/1/comm)" = mongod',
        'cat /sys/fs/cgroup/cpu.max > /bench/' + tag + '-database-quota.txt',
        'cat /proc/1/stat > /bench/' + tag + '-database-identity.txt',
        'while IFS=" " read -r name action; do',
        'stem=/bench/' + tag + '/$name',
        'if [ "$action" = stop ]; then',
        'kill -TERM "$(cat "$stem.database_sampler_pid")"',
        'touch "$stem.database_stopped"', 'continue', 'fi',
        '/bench/sampler -pid 1 -output "$stem-database.jsonl" > "$stem-database-sampler.log" 2>&1 < /dev/null &',
        'printf "%s" "$!" > "$stem.database_sampler_pid"',
        'touch "$stem.database_ready"',
        'done < /bench/' + tag + '-database.pipe',
    ]
    (out / 'database.sh').write_text('\n'.join(database) + '\n')
    archive = out / 'inputs.tar.gz'
    with tarfile.open(archive, 'w:gz') as bundle:
        for local_path in out.iterdir():
            if local_path == archive: continue
            arcname = local_path.name if local_path.name.endswith('.yaml') else tag + '-' + local_path.name
            bundle.add(local_path, arcname=arcname)
    expected = hashlib.sha256(archive.read_bytes()).hexdigest()
    put(archive, '/bench/' + tag + '-inputs.tar.gz')
    actual = shell('sha256sum /bench/' + tag + '-inputs.tar.gz').split()[0]
    if actual != expected: raise RuntimeError('input archive checksum mismatch')
    shell('tar -xzf /bench/' + tag + '-inputs.tar.gz -C /bench')
    shell('mkfifo /bench/' + tag + '-control.pipe /bench/' + tag + '-database.pipe')
    shell('sh /bench/' + tag + '-control.sh > /bench/' + tag + '-control.log 2>&1 < /dev/null &', 'weir')
    shell('sh /bench/' + tag + '-database.sh > /bench/' + tag + '-database-control.log 2>&1 < /dev/null &', 'database')
    shell('sh /bench/' + tag + '-suite.sh > /bench/' + tag + '-suite.log 2>&1 < /dev/null &')
    print('launched ' + tag + ': ' + str(len(cases)) + ' cases', flush=True)


if __name__ == '__main__':
    launch_suite(json.loads(Path(sys.argv[1]).read_text()), sys.argv[2])
