#!/usr/bin/env python3
"""One frozen, owned network-none ES startup prerequisite; no database writes."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import time

import eks_loopback as loop
import eks_pacing as common

RECEIPT = "local-readonly-sample-complete; PodIP negative not run\n"
RESOLVER = '''import java.net.InetAddress;
class LocalHostname {
  public static void main(String[] args) throws Exception {
    var local = InetAddress.getLocalHost();
    System.out.println("jdk-local=" + local.getHostName() + " " + local.getHostAddress());
    if (!local.getHostName().equals(args[0]) || !local.getHostAddress().equals("127.0.0.1"))
      throw new IllegalStateException("local hostname mismatch");
    for (var address : InetAddress.getAllByName(args[0])) {
      System.out.println("jdk-named=" + address.getHostAddress());
      if (!address.getHostAddress().equals("127.0.0.1"))
        throw new IllegalStateException("nonloopback hostname");
    }
  }
}
'''
# Only fields from this newly created container, excluding unrelated image env.
INSPECT = '{' + ','.join('"'+key+'":{{json .'+key+'}}' for key in
    ('Id', 'Name', 'Image', 'Path', 'Args', 'State', 'RestartCount', 'Mounts')) + \
    ',"Config":{' + ','.join('"'+key+'":{{json .Config.'+key+'}}' for key in
    ('Hostname', 'User', 'Entrypoint', 'Cmd', 'Labels')) + \
    '},"HostConfig":{' + ','.join('"'+key+'":{{json .HostConfig.'+key+'}}' for key in
    ('NetworkMode', 'NanoCpus', 'Memory', 'MemorySwap', 'PidsLimit', 'CapDrop', 'CapAdd',
     'SecurityOpt', 'Privileged', 'PortBindings', 'PublishAllPorts', 'Binds', 'Tmpfs',
     'ReadonlyRootfs', 'RestartPolicy', 'LogConfig', 'PidMode', 'IpcMode', 'CgroupnsMode')) + '}}'


def local_check():
    original = (common.REPO/'scripts/eks_loopback_check.sh').read_text()
    check = original[original.index('curl_args='):original.index('for port in 9200 9300 7447 7449; do')]
    check = check.replace('printf \'own-pod-ip=%s\\n\' "$POD_IP"\n', '')
    prefix = 'set -euo pipefail\nulimit -f 1024\n[[ ${AWS_EC2_METADATA_DISABLED:-} == true ]]\n'
    result = prefix + check + 'printf "' + RECEIPT.replace('\n', '\\n') + '"\n'
    return result


def identity(actual, plan, container):
    common.require(actual['Id'] == container and actual['Name'] == '/'+plan['owner'] and
                   actual['Config']['Labels'].get(common.LABEL) == plan['owner'], 'container ID/owner drift')


def configuration_check(actual, plan):
    config, host = actual['Config'], actual['HostConfig']
    common.require(config['Hostname'] == plan['owner'] and
                   plan['extra_hosts'] == [plan['owner']+':127.0.0.1'], 'hostname/hosts plan drift')
    common.require(actual['ExtraHosts'] == plan['extra_hosts'], 'named loopback mapping missing/drift')
    common.require(actual['Image'] == loop.ES['config'] and config['User'] == '1000:0' and
                   config['Entrypoint'] == ['/bin/tini', '--', '/usr/local/bin/docker-entrypoint.sh'] and
                   config['Cmd'] == plan['es']['args'], 'image/entrypoint/user/args drift')
    expected = dict(NetworkMode='none', NanoCpus=3000000000, Memory=3072*1024**2,
                    MemorySwap=3072*1024**2, PidsLimit=512, CapDrop=['ALL'], CapAdd=None,
                    SecurityOpt=['no-new-privileges=true'], Privileged=False, PublishAllPorts=False,
                    PortBindings={}, Binds=None, ReadonlyRootfs=False, PidMode='', IpcMode='private',
                    RestartPolicy=dict(Name='no', MaximumRetryCount=0),
                    LogConfig=dict(Type='json-file', Config={'max-size':'4m', 'max-file':'1'}),
                    Tmpfs={'/usr/share/elasticsearch/data':'rw,nosuid,nodev,size=1073741824,uid=1000,gid=0,mode=0770'})
    common.require(host == expected, 'container resource/security drift')
    mounts = actual['Mounts']
    common.require(len(mounts) == 2 and all(m['Type'] == 'bind' and not m['RW'] for m in mounts), 'mount drift')
    common.require({m['Source']:m['Destination'] for m in mounts} == plan['mounts'], 'mount source/destination drift')


def inspect(run, container):
    actual = json.loads(run.run(['docker', 'inspect', container, '--format', INSPECT]))
    actual['ExtraHosts'] = json.loads(run.run(['docker', 'inspect', container, '--format', '{{json .HostConfig.ExtraHosts}}']))
    return actual


def cleanup(run, plan, container):
    errors = []
    run.deadline = time.monotonic()+120
    run.cleaning = True
    if container:
        try:
            identity(inspect(run, container), plan, container)
        except BaseException as exc:
            errors.append('ownership: '+str(exc))
        else:
            try:
                run.save('es.log', run.run(['docker', 'logs', container], 20))
            except BaseException as exc:
                errors.append('logs: '+str(exc))
            try:
                run.run(['docker', 'stop', '--time', '20', container], 30)
                run.save('wait-exit.txt', run.run(['docker', 'wait', container], 10))
                actual = inspect(run, container)
                run.save('final-container.json', actual)
                identity(actual, plan, container)
                common.require(not actual['State']['Running'], 'container still running')
                if actual['State']['OOMKilled'] or actual['RestartCount'] != 0:
                    errors.append('final OOM/restart state')
                run.run(['docker', 'rm', container], 10)
            except BaseException as exc:
                errors.append('stop/wait/remove: '+str(exc))
    try:
        remaining = run.run(['docker', 'ps', '-aq', '--no-trunc', '--filter', 'label='+common.LABEL+'='+plan['owner']])
        common.require(not remaining.strip(), 'owned container remains')
        for name, command in inventories().items():
            after = run.run(command)
            run.save(name+'-after.txt', after)
            common.require(set(after.splitlines()) == set((run.root/(name+'-before.txt')).read_text().splitlines()), name+' inventory changed')
    except BaseException as exc:
        errors.append('inventory: '+str(exc))
    errors.extend(run.diagnostic_errors)
    return errors


def inventories():
    result = dict(containers=['docker', 'ps', '-aq', '--no-trunc'],
                  networks=['docker', 'network', 'ls', '--no-trunc', '--format', '{{.ID}}'])
    return result


def execute(run, plan):
    container = None
    result = dict(passed=False, samples=0, scope='local-only; no PodIP/CNI qualification')
    started = time.monotonic()
    run.deadline = started+300
    try:
        common.require(all(common.digest(p) == value for p, value in plan['inputs'].items()), 'frozen input drift')
        for name, command in inventories().items():
            run.save(name+'-before.txt', run.run(command))
        container = run.run(plan['command']).strip()
        common.require(re.fullmatch('[0-9a-f]{64}', container), 'create ID missing/ambiguous; no adoption')
        run.save('created-id.txt', container)
        actual = inspect(run, container)
        run.save('container.json', actual)
        identity(actual, plan, container)
        configuration_check(actual, plan)
        run.run(['docker', 'start', container])
        raw = run.run(['docker', 'exec', container, '/usr/bin/timeout', '30', '/bin/bash', '--noprofile', '--norc', '-c',
                      plan['resolution_command'], '--', plan['owner']], 35)
        run.save('resolution.log', raw)
        until = min(run.deadline, started+240)
        while True:
            actual = inspect(run, container)
            run.save('latest-container.json', actual)
            common.require(actual['State']['Running'] and not actual['State']['OOMKilled'] and actual['RestartCount'] == 0, 'ES stopped/OOM/restarted')
            try:
                raw = run.run(['docker', 'exec', container]+loop.CURL+['http://127.0.0.1:9200/'], 8)
            except ValueError:
                record = json.loads((run.root/f'command-{run.number:04d}.json').read_text())
                common.require(record['exit'] in (7, 28) and time.monotonic() < until, 'HTTP readiness failed; see command')
                time.sleep(2)
                continue
            version = json.loads(raw)
            common.require(version['version']['number'] == '8.19.22', 'ES version')
            run.save('version.json', version)
            break
        for index in range(3):
            raw = run.run(['docker', 'exec', container, '/usr/bin/timeout', '20', '/bin/bash', '--noprofile', '--norc', '/qualification/local-check.sh'], 25)
            run.save('sample-'+str(index)+'.log', raw)
            common.require(raw.endswith(RECEIPT), 'HTTP/settings/guard receipt missing')
            result['samples'] += 1
            if index < 2:
                time.sleep(2)
        actual = inspect(run, container)
        run.save('running-container.json', actual)
        common.require(actual['State']['Running'] and not actual['State']['OOMKilled'] and actual['RestartCount'] == 0, 'runtime failure')
        result['passed'] = True
    except BaseException as exc:
        result['error'] = str(exc)
    finally:
        result['startup_and_samples_seconds'] = time.monotonic()-started
        result['cleanup_errors'] = cleanup(run, plan, container)
        result['cleanup'] = not result['cleanup_errors']
        result['container'] = container
        run.save('result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--owner', required=True)
    args = parser.parse_args()
    root = args.evidence.absolute()
    common.require(root.parent == common.REPO/'.testdata/m26r5' and not root.exists(), 'new controlled evidence path required')
    common.require(re.fullmatch('weir-m26r5-es-[a-z0-9-]{1,32}', args.owner), 'owner syntax')
    previous = list(root.parent.glob('es-attempt-*'))
    common.require(len(previous) < 2, 'two local attempts exhausted')
    for attempt in previous:
        prior_result = json.loads((attempt/'result.json').read_text())
        common.require(prior_result['cleanup'] and not prior_result['passed'], 'previous attempt must be failed and fully cleaned')
    if previous:
        common.require((root.parent/'attempt-2-rationale.json').is_file(), 'documented fixture correction required')
    root.mkdir(mode=0o700)
    docker_config = root/'docker-config'
    docker_config.mkdir(mode=0o700)
    (docker_config/'config.json').write_text('{}\n')
    os.environ['DOCKER_CONFIG'] = str(docker_config)
    run = common.Run(root, loop.TARGET)
    run.save('local-check.sh', local_check())
    run.save('LocalHostname.java', RESOLVER)
    shell = '''set -euo pipefail
uname -smr
id
readlink /proc/self/ns/net
while read -r key value; do
  case "$key" in NoNewPrivs:|Seccomp:|CapEff:) printf '%s %s\\n' "$key" "$value";; esac
done < /proc/self/status
for file in cpu.max memory.max memory.swap.max pids.max; do printf '%s=' "$file"; cat "/sys/fs/cgroup/$file"; done
[[ $HOSTNAME == "$1" && $AWS_EC2_METADATA_DISABLED == true && $ES_JAVA_OPTS == '-Xms1024m -Xmx1024m' ]]
printf 'hostname=%s\\nAWS_EC2_METADATA_DISABLED=%s\\nES_JAVA_OPTS=%s\\n' "$HOSTNAME" "$AWS_EC2_METADATA_DISABLED" "$ES_JAVA_OPTS"
cat /etc/hosts
getent --version
resolved=$(getent --no-addrconfig ahostsv4 "$1")
[[ -n $resolved ]]
printf '%s\\n' "$resolved"
while read -r address rest; do [[ $address == 127.0.0.1 ]]; done <<< "$resolved"
exec /usr/share/elasticsearch/jdk/bin/java -Xms16m -Xmx64m /qualification/LocalHostname.java "$1"
'''
    seed = dict(owner=args.owner, namespace=args.owner, node=dict(name='local-only'))
    es = loop.objects(seed)['job']['spec']['template']['spec']['initContainers'][0]
    command = ['docker', 'create', '--pull=never', '--name', args.owner, '--label', common.LABEL+'='+args.owner,
               '--network', 'none', '--hostname', args.owner, '--add-host', args.owner+':127.0.0.1',
               '--platform', 'linux/arm64', '--user', '1000:0', '--cap-drop=ALL', '--security-opt', 'no-new-privileges=true',
               '--cpus', '3', '--memory', '3072m', '--memory-swap', '3072m', '--pids-limit', '512',
               '--log-driver', 'json-file', '--log-opt', 'max-size=4m', '--log-opt', 'max-file=1',
               '--tmpfs', '/usr/share/elasticsearch/data:rw,nosuid,nodev,size=1073741824,uid=1000,gid=0,mode=0770']
    mounts = {str(root/name):'/qualification/'+name for name in ('local-check.sh', 'LocalHostname.java')}
    for source, destination in mounts.items():
        command += ['--mount', 'type=bind,src='+source+',dst='+destination+',readonly']
    for setting in es['env']:
        if 'value' in setting:
            command += ['--env', setting['name']+'='+setting['value']]
    command += [loop.ES['reference']]+es['args']
    prior = common.REPO/'.testdata/m26r4'
    common.require(common.digest(prior/'manifest.json') == 'dc2402ef7adb8d2fc37044d71e0e375599d822331065710180a7f473c40fb8ae', 'accepted SDK evidence manifest drift')
    for name, item in json.loads((prior/'manifest.json').read_text()).items():
        common.require(common.digest(prior/name) == item['sha256'] and (prior/name).stat().st_size == item['bytes'], 'accepted evidence drift: '+name)
    files = [common.REPO/name for name in loop.FILES]+[Path(__file__), root/'local-check.sh', root/'LocalHostname.java',
             prior/'manifest.json', prior/'sdk/plan.json', prior/'sdk/command-0004.out', prior/'sdk/result.json',
             common.REPO/'scripts/local_es_prerequisite_test.py', common.REPO/'.testdata/m26r5/reused-evidence.json']
    if previous:
        files.append(root.parent/'attempt-2-rationale.json')
    plan = dict(owner=args.owner, command=command, mounts=mounts, es=es, image=loop.ES,
                extra_hosts=[args.owner+':127.0.0.1'], resolution_command=shell,
                inputs={str(p):common.digest(p) for p in files}, seconds=300, cleanup_seconds=120,
                scope='readonly startup; local-only; no PodIP or CNI qualification')
    run.save('driver.py', Path(__file__).read_text())
    run.save('plan.json', plan)
    (root/'plan.json').chmod(0o400)
    run.save('plan.sha256', common.digest(root/'plan.json'))
    def interrupted(signum, frame):
        raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    result = execute(run, plan)
    print(json.dumps(result), flush=True)
    return 0 if result['passed'] and result['cleanup'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
