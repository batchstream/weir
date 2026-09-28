#!/usr/bin/env python3
"""One frozen EKS resource invocation, exact M29 helper, no document workload."""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import time

import eks_loopback as loop
import eks_pacing as common
from capacity_artifact import application_binary
from capacity_fixture import Observer
from capacity_report import counter, integer, prom, timestamp, weir_metrics
from resource_report import process, observer_identity, read_stream, require
from observer_completion import observation_samples, finish_observation, abort_observation

SOURCE = '4abc8761f9f0e08af978d5ae5c14176f8188cfa3'
IMAGES = dict(
    version=dict(reference='ghcr.io/batchstream/weir@sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a',
                 manifest='sha256:100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e',
                 binary='f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e'),
    tool=dict(reference='ghcr.io/batchstream/weir-qualification@sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7',
              manifest='sha256:9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351',
              config='sha256:063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee',
              binary='d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482'), es=loop.ES)
FILES = tuple(dict.fromkeys(loop.FILES + ('scripts/eks_resource_preflight.py', 'scripts/eks_resource_preflight_test.py',
    'scripts/capacity_artifact.py', 'scripts/resource_report.py', 'scripts/resource_report_test.py', 'scripts/observer_completion.py',
    'scripts/fixtures/eks-resource-admitted-job-m30.json', 'scripts/fixtures/eks-resource-release-m30r2.json')))
BUDGET = dict(remote_seconds=900, cleanup_seconds=300, artifact_ready_seconds=420, upload_seconds=300,
              role_seconds=60, observer_eof_seconds=4, command_stop_seconds=4, namespace_reserve_seconds=45,
              observer_seconds=10, interval_seconds=2, samples=6, planned=0, document_mutations=0, empty_index_put=1,
              helper_volume_mib=64, helper_charged_to='bootstrap 256Mi ephemeral; aggregate remains 2560Mi')


def verified_helper(root):
    artifact = root/'artifact'
    raw = (artifact/'index.json').read_bytes()
    require('sha256:'+hashlib.sha256(raw).hexdigest() == IMAGES['tool']['reference'].split('@')[1], 'helper index')
    index = json.loads(raw)
    require([m['digest'] for m in index['manifests'] if m.get('platform') == dict(os='linux', architecture='arm64')] == [IMAGES['tool']['manifest']], 'helper platform')
    raw = (artifact/'manifest.json').read_bytes()
    require('sha256:'+hashlib.sha256(raw).hexdigest() == IMAGES['tool']['manifest'], 'helper manifest')
    manifest = json.loads(raw)
    raw = (artifact/'config.json').read_bytes()
    require('sha256:'+hashlib.sha256(raw).hexdigest() == IMAGES['tool']['config'] == manifest['config']['digest'] and len(raw) == manifest['config']['size'], 'helper config')
    config = json.loads(raw)
    require(config['os'] == 'linux' and config['architecture'] == 'arm64' and config['config']['Labels']['org.opencontainers.image.revision'] == SOURCE, 'helper source/platform')
    layer = manifest['layers'][-1]
    require(layer['digest'] == 'sha256:e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7' and layer['size'] == 10810877, 'helper layer')
    require(len(config['rootfs']['diff_ids']) == len(manifest['layers']), 'rootfs count')
    identity = dict(layer, diff_id=config['rootfs']['diff_ids'][-1], binary=IMAGES['tool']['binary'])
    binary = application_binary((artifact/'application.tar.gz').read_bytes(), identity)
    require((artifact/'qualification').read_bytes() == binary and (artifact/'qualification').stat().st_mode & 0o777 == 0o555, 'local helper file')
    result = dict(path=str(artifact/'qualification'), size=len(binary), sha256=identity['binary'], diff_id=identity['diff_id'], layer=layer)
    return result


def transfer_scripts(helper):
    size, sha = helper['size'], helper['sha256']
    require(type(size) is int and 0 < size < 64 << 20 and re.fullmatch('[0-9a-f]{64}', sha), 'transfer identity')
    check = f'''[[ $(stat -c %s /helper/qualification) == {size} ]]
[[ $(sha256sum /helper/qualification) == '{sha}  /helper/qualification' ]]
[[ $(stat -c %a /helper/qualification) == 555 ]]
'''
    bootstrap = '''set -euo pipefail
umask 022
mkfifo -m 600 /helper/release
printf '{"bootstrap":"waiting-for-verified-helper"}\\n'
IFS= read -r release < /helper/release
'''+f"[[ $release == '{sha}' ]]\n"+check+'''printf '{"artifact":"verified-before-management"}\\n'
exec /bin/bash --noprofile --norc /qualification/bootstrap.sh
'''
    upload = f'''set -euo pipefail
umask 022
[[ -p /helper/release && ! -e /helper/qualification && ! -e /helper/qualification.part ]]
ulimit -f 65536
head -c {size+1} > /helper/qualification.part
[[ $(stat -c %s /helper/qualification.part) == {size} ]]
[[ $(sha256sum /helper/qualification.part) == '{sha}  /helper/qualification.part' ]]
chmod 0555 /helper/qualification.part
mv -T /helper/qualification.part /helper/qualification
'''+check+f'''printf '{{"artifact":"uploaded","size":{size},"sha256":"{sha}"}}\\n'
'''
    release = 'set -euo pipefail\n'+check+f'''printf '%s\\n' '{sha}' > /helper/release
printf '{{"artifact":"released"}}\\n'
'''
    # Explicit failure exits also hold on older bash compound-command semantics.
    scripts = (bootstrap, upload, release)
    bootstrap, upload, release = ('\n'.join(line+' || exit 41' if line.startswith('[[') else line for line in script.split('\n')) for script in scripts)
    result = dict(bootstrap=bootstrap, upload=upload, release=release)
    return result


def objects(plan):
    result = loop.objects(plan)
    spec = result['job']['spec']['template']['spec']
    scripts = transfer_scripts(plan['helper'])
    volume=dict(name='helper',emptyDir=dict(sizeLimit='64Mi'))
    spec['volumes'].append(volume)
    for name in ('weir', 'elasticsearch', 'bootstrap'):
        container = next(c for c in spec['containers']+spec['initContainers'] if c['name'] == name)
        mount=dict(name='helper',mountPath='/helper')
        if name != 'bootstrap':mount['readOnly']=True
        container['volumeMounts'].append(mount)
        if name != 'bootstrap':
            container.setdefault('env', []).extend([dict(name='WEIR_CAPACITY_INTEGRATION', value='1'), dict(name='GOMAXPROCS', value='2' if name == 'weir' else '1')])
        else:
            container['command'] = ['/usr/bin/timeout', '420', '/bin/bash', '--noprofile', '--norc', '/qualification/helper-bootstrap.sh']
    result['config']['data'].update({'helper-bootstrap.sh':scripts['bootstrap'], 'helper-upload.sh':scripts['upload'], 'helper-release.sh':scripts['release']})
    result['config']['immutable'] = True
    return result


def bootstrap_receipt(raw):
    require(0 < len(raw.encode()) < 262144 and raw.endswith('\n'), 'bootstrap log incomplete/bounded')
    lines = raw.splitlines()
    waiting = '{"bootstrap":"waiting-for-verified-helper"}'
    verified = '{"artifact":"verified-before-management"}'
    started = '{"management":"create-empty-records","reserved":1,"started":1}'
    completed = '{"management":"create-empty-records","completed":1,"document_mutations":0}'
    require(lines[:2] == [waiting, verified] and lines[-1] == completed, 'bootstrap ordered receipt')
    records = [line for line in lines if any('"'+key+'"' in line for key in ('bootstrap', 'artifact', 'management', 'acknowledged'))]
    acknowledgement = '{"acknowledged":true,"shards_acknowledged":true,"index":"records"}'
    require(records == [waiting, verified, started, acknowledgement, completed], 'bootstrap unique management receipts')
    require(lines[-4:] == ['loopback-check-complete', started, acknowledgement, completed] and
            lines.count('loopback-check-complete') == 1, 'bootstrap complete boundary/order')
    # Check the complete frozen guard's output shape, including raw table byte
    # counts. A success suffix alone must not hide missing middle log evidence.
    require(all(re.fullmatch(r'/\S*/'+name, line) for name, line in zip(('curl', 'nc', 'timeout'), lines[2:5])) and
            len(lines) > 5, 'bootstrap command paths')
    rest = '\n'.join(lines[5:-4])+'\n'
    for field in ('version', 'nodes'):
        value, end = json.JSONDecoder().raw_decode(rest)
        require(isinstance(value, dict) and field in value, 'bootstrap guard HTTP logs')
        rest = rest[end:].lstrip('\n')
    address, rest = rest.split('\n', 1)
    require(address.startswith('own-pod-ip=') and ipaddress.ip_address(address[11:]).version == 4, 'bootstrap PodIP log')
    rows = []
    for table in ('tcp', 'tcp6', 'udp', 'udp6'):
        name = '/proc/net/'+table
        begin = 'socket-table-begin table='+name+' atomic=false max_bytes=65536 max_lines=256 read_seconds=2 process=unknown\n'
        require(rest.startswith(begin), 'bootstrap socket log order')
        raw_table, rest = rest[len(begin):].split('\nsocket-table-end table='+name+' bytes=', 1)
        count, rest = rest.split('\n', 1)
        require(count == str(len(raw_table.encode()))+' read_status=1' and raw_table.endswith('\n'), 'bootstrap socket log truncated')
        table_lines = raw_table.splitlines()
        require(table_lines and 'local_address' in table_lines[0] and 'inode' in table_lines[0], 'bootstrap socket header')
        for row in table_lines[1:]:
            if not row:
                continue
            parts = row.split()
            require(len(parts) >= 10, 'bootstrap socket row incomplete')
            rows.append(f'socket-row table={name} local={parts[1]} remote={parts[2]} state={parts[3]} uid={parts[7]} inode={parts[9]} process=unknown raw={row}')
    require(rest.splitlines() == rows, 'bootstrap socket decisions incomplete/contradictory')


class Run(loop.Run):
    def job_template(self, step):
        require(step == 'loopback', 'single Job only')
        return objects(self.plan)['job']

    def current_pod(self):
        common.owner_check(self.selected_object('Namespace', self.plan['namespace']), self.namespace)
        return super().current_pod()

    def configuration(self):
        expected = next(e for e in self.owned if e['kind'] == 'ConfigMap' and e['name'] == 'configuration')
        config = self.selected_object('ConfigMap', expected['name'])
        common.owner_check(config, expected)
        require(config.get('immutable') is True and config.get('data') == self.plan['objects']['config']['data'] and
                not config['metadata'].get('deletionTimestamp'), 'frozen bootstrap configuration drift')
        self.save('bootstrap-configuration-'+str(self.number)+'.json', config)

    def bootstrap(self, *, completed=False):
        pod = self.current_pod()
        require(pod is not None, 'bootstrap Pod missing')
        states = [s for s in pod['status'].get('initContainerStatuses', []) if s['name'] == 'bootstrap']
        require(len(states) == 1, 'bootstrap status missing')
        state = states[0]
        require(state.get('restartCount') == 0 and not state.get('lastState'), 'bootstrap restart/history')
        terminal = state.get('state', {}).get('terminated')
        if completed:
            require(terminal and terminal.get('exitCode') == 0 and terminal.get('reason') == 'Completed', 'bootstrap not Completed0')
        else:
            require(state.get('state', {}).get('running'), 'bootstrap not stable Running')
        require(state['imageID'] == loop.ES['reference'] and state['containerID'].startswith('containerd://'), 'bootstrap runtime image')
        result = dict(namespace_uid=self.namespace['uid'], job_uid=self.job['uid'], pod_uid=self.pod_entry['uid'], imageID=state['imageID'], containerID=state['containerID'])
        return result

    def exec_command(self, container, command):
        argv = ['kubectl', '--context', self.target['context'], '--request-timeout=10s', '--namespace', self.plan['namespace'],
                'exec', '-i', self.pod_entry['name'], '--container', container, '--']+command
        return argv

    def observe(self, role):
        require(role in ('weir', 'es'), 'observer role')
        total_deadline = self.deadline
        started = time.monotonic()
        until = min(total_deadline, started+BUDGET['role_seconds'])
        closing = BUDGET['observer_eof_seconds']+BUDGET['command_stop_seconds']
        require(started+(BUDGET['observer_seconds']+closing) <= until, 'observer/Stop/Wait budget')
        operation = dict(started_monotonic=started, deadline_monotonic=until)
        # Claim before the identity reads; a failed role is never replayed.
        with (self.root/(role+'-operation.json')).open('x') as output:
            json.dump(operation, output, indent=2)
        # Identity CLI owns its own Stop/Wait; no observer exists yet.
        self.deadline = until-BUDGET['command_stop_seconds']
        observer = None
        failure = None
        try:
            self.monitor()
            require(time.monotonic()+(BUDGET['observer_seconds']+closing) <= until, 'observer/Stop/Wait budget after identity')
            self.deadline = until-closing
            container = 'weir' if role == 'weir' else 'elasticsearch'
            command = self.exec_command(container, ['/helper/qualification', '-mode', 'observe', '-role', role,
                                       '-pid', '1' if role == 'weir' else 'java', '-seconds', str(BUDGET['observer_seconds'])])
            options = dict(root=self.root, role=role, command=command)
            observer = Observer(options)
            try:
                native = (self.root/'native-identity.txt').read_text().splitlines()
                profile = dict(role=role, samples=BUDGET['samples'], seconds=BUDGET['observer_seconds'],
                               minimum_seconds=9.5, max_gap_seconds=4, native=native,
                               hashes=dict(weir=IMAGES['version']['binary'], client=IMAGES['tool']['binary']))
                while True:
                    require(time.monotonic() < self.deadline, 'observer deadline')
                    _, complete = observation_samples(observer, profile)
                    require(time.monotonic() < self.deadline, 'observer deadline')
                    if complete:
                        observation_series(observer.entries, role, native)
                        finish_observation(observer, profile, until)
                        break
                    time.sleep(.02)
            except BaseException as exc:
                try:
                    abort_observation(observer, until)
                except BaseException as closing:
                    exc.add_note('observer stop: '+str(closing))
                raise
            # No child is alive while a control-plane call can block.
            self.deadline = until-BUDGET['command_stop_seconds']
            require(time.monotonic() < self.deadline, 'post-observation identity budget')
            self.monitor()
            require(time.monotonic() <= until, 'observer role deadline')
        except BaseException as exc:
            failure = exc
            operation['error'] = str(exc)
            raise
        finally:
            self.deadline = total_deadline
            operation.update(finished_monotonic=time.monotonic(), pid=observer.child.pid if observer else None)
            try:
                self.save(role+'-operation.json', operation)
            except BaseException as recording:
                if failure is None:
                    raise
                failure.add_note('observer operation record: '+str(recording))

    def transfer(self, until):
        require(not (self.root/'release.json').exists(), 'release already attempted')
        self.configuration()
        before = self.bootstrap()
        self.save('upload-before.json', before)
        helper = self.plan['helper']
        data = Path(helper['path']).read_bytes()
        require(len(data) == helper['size'] and hashlib.sha256(data).hexdigest() == helper['sha256'], 'helper before transfer')
        command = self.exec_command('bootstrap', ['/usr/bin/timeout', '300', '/bin/bash', '--noprofile', '--norc', '/qualification/helper-upload.sh'])
        options = dict(root=self.root, role='upload', command=command)
        observer = Observer(options)
        try:
            deadline = min(until, self.deadline, time.monotonic()+300)-5
            started=time.monotonic()
            sent=observer.write_input(data, deadline)
            transfer=dict(bytes_sent=sent,sha256=helper['sha256'],elapsed_seconds=time.monotonic()-started,deadline_monotonic=deadline)
            self.save('upload-input.json',transfer)
            while observer.child.poll() is None:
                require(time.monotonic() < deadline, 'upload completion deadline')
                observer.poll(); time.sleep(.02)
            entries = observer.poll()
            expected = dict(artifact='uploaded', size=helper['size'], sha256=helper['sha256'])
            require(entries == [expected] and observer.child.returncode == 0, 'complete upload receipt')
        except BaseException as exc:
            try:
                observer.stop()
            except BaseException as closing:
                exc.add_note('upload stop: '+str(closing))
            raise
        else:
            observer.stop()
        after = self.bootstrap()
        self.save('upload-after.json', after)
        require(before == after, 'bootstrap/Pod UID changed during upload')
        # Release is separate from upload so no index operation can precede the
        # post-transfer API identity check and exact stdout receipt.
        require(time.monotonic() < min(until, self.deadline)-10, 'release deadline')
        attempt = dict(identity=before, attempted_at=time.time(), attempts=1, transport='pending', remote_outcome='UNKNOWN')
        with (self.root/'release.json').open('x') as output:
            json.dump(attempt, output, indent=2)
        raw = ''
        try:
            command = self.exec_command('bootstrap', ['/usr/bin/timeout', '5', '/bin/bash', '--noprofile', '--norc', '/qualification/helper-release.sh'])
            raw = self.run(command, 10)
            attempt['transport'] = 'exit0'
        except common.CommandFailure as exc:
            # Only a completed nonzero CLI is an uncertain reply. Cancellation,
            # local deadlines, output bounds and evidence failures still stop.
            attempt.update(transport='failed', error=str(exc), command=exc.number, exit=exc.code)
            raw = (self.root/f'command-{exc.number:04d}.out').read_text()
        finally:
            self.save('release.json', attempt)
        if raw:
            require(raw == '{"artifact":"released"}\n', 'contradictory/incomplete release receipt')
            attempt['receipt'] = json.loads(raw)
        self.save('release.json', attempt)
        # Both reply paths converge here; no release/upload/PUT is replayed.
        while time.monotonic() < min(until, self.deadline):
            self.current_pod()
            if self.pod_ready:
                break
            time.sleep(.5)
        require(self.pod_ready and time.monotonic() < min(until, self.deadline), 'artifact/runtime ready deadline')
        require(self.bootstrap(completed=True) == before, 'bootstrap completion identity drift')
        self.configuration()
        raw = self.kube(['logs', self.pod_entry['name'], '--container=bootstrap', '--limit-bytes=262144', '--tail=-1'], self.plan['namespace'])
        self.save('bootstrap.log', raw)
        bootstrap_receipt(raw)
        require(self.bootstrap(completed=True) == before, 'bootstrap log identity drift')
        require(time.monotonic() < min(until, self.deadline), 'bootstrap confirmation deadline')
        attempt['remote_outcome'] = 'confirmed Completed0 with full ordered logs'
        self.save('release.json', attempt)


def sample_check(sample, role):
    require(sample['role'] == role and not sample.get('errors'), 'sample role/errors')
    target, observer = process(sample['process']), process(sample['observer'])
    uid = 1000 if role == 'es' else 65532
    require(target['uid'] == observer['uid'] == uid and target['namespaces'] == observer['namespaces'] and target['cgroup'] == observer['cgroup'], 'same UID/container')
    require(observer['exe_sha256'] == IMAGES['tool']['binary'], 'observer binary')
    if role in ('weir', 'client'):
        require(target['exe_sha256'] == IMAGES['version' if role == 'weir' else 'tool']['binary'], 'target binary')
    if role == 'weir': require(target['pid'] == '1', 'Weir PID1')
    if role == 'client': require(target == observer and sample['gomaxprocs'] == 1, 'snapshot self identity')
    else: require(target['pid'] != observer['pid'], 'observer independent process')
    files = sample['files']
    required = {'stat','status','limits','cgroup','memory.current','memory.max','memory.swap.max','cpu.max','cpu.stat','memory.events','pids.max','pids.current','cpuset.cpus.effective','io.stat','net/tcp','net/tcp6'}
    require(required <= files.keys() and all(isinstance(v,str) and len(v.encode()) <= 262144 for v in files.values()), 'raw fields/bounds')
    require(files['stat'] == sample['process']['stat'] and files['status'] == sample['process']['status'] and files['cgroup'] == target['cgroup'], 'raw identity')
    require(sample['rss_bytes'] == sample['process']['rss_bytes'] and sample['fd'] == sample['process']['fd'], 'raw summary')
    duration = integer(sample['duration_ns'], True)
    require(duration <= 2_000_000_000 and sample['end_monotonic_ns']-sample['monotonic_ns'] == duration and
            abs(timestamp(sample['end'])-timestamp(sample['time'])-duration/1e9) < .05, 'sample time/bound')
    rows = [line.split() for line in files['limits'].splitlines()]
    require(rows[0] == ['Limit','Soft','Limit','Hard','Limit','Units'], 'limits header')
    fd = [r[3:] for r in rows[1:] if r[:3] == ['Max','open','files']]
    require(len(fd) == 1 and len(fd[0]) == 3 and fd[0][2] == 'files', 'limits unique fields')
    soft, hard = fd[0][:2]
    require(all(v.isascii() and v.isdecimal() for v in (soft, hard)) and 0 < int(soft) <= int(hard), 'finite FD limits')
    quota, period = map(int, files['cpu.max'].split())
    cpu, memory = dict(weir=(2,1024), es=(3,3072), client=(1,512))[role]
    require(quota > 0 and period > 0 and quota/period == cpu and int(files['memory.max']) == memory*1024**2, 'declared CPU/memory')
    require(0 < int(files['memory.current']) <= int(files['memory.max']) and int(files['memory.swap.max']) >= 0, 'memory values')
    pid_max = integer(int(files['pids.max']), True)
    require(0 < int(files['pids.current']) <= pid_max, 'finite PID values')
    require(re.fullmatch(r'\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*\n?', files['cpuset.cpus.effective']), 'cpuset')
    events = counter(files['memory.events']); cpu_stats = counter(files['cpu.stat'])
    require(all(k in events for k in ('oom','oom_kill','oom_group_kill')) and not any(events[k] for k in ('oom','oom_kill','oom_group_kill')), 'OOM events')
    require(all(k in cpu_stats for k in ('usage_usec','user_usec','system_usec','nr_periods','nr_throttled','throttled_usec')), 'CPU counters')
    for values in (events,cpu_stats):
        for v in values.values(): integer(v)
    for line in files['io.stat'].splitlines():
        fields=line.split(); require(re.fullmatch(r'\d+:\d+',fields[0]) and len(fields)>1,'IO device')
        pairs=[p.split('=') for p in fields[1:]]
        require(len({p[0] for p in pairs}) == len(pairs), 'duplicate IO counter')
        for key,value in pairs: integer(int(value))
    loop.network_check(files, require_listeners=True)
    if role == 'weir':
        reasons, _, _ = weir_metrics(sample['metrics'], True)
        require(not reasons, 'Weir metrics guard/ledger')
        values=prom(sample['metrics'])
        for key in ('go_goroutines','go_memstats_heap_alloc_bytes','process_resident_memory_bytes','process_cpu_seconds_total','process_open_fds'):
            require(key in values and values[key] >= 0, 'runtime metric missing')
    elif role == 'es':
        nodes=json.loads(sample['db'])['nodes'];require(isinstance(nodes,dict) and len(nodes)==1,'ES stats node')
        node=next(iter(nodes.values()))
        require(node['jvm']['mem']['heap_max_in_bytes']==1024**3 and 0 < node['jvm']['mem']['heap_used_in_bytes'] <= 1024**3, 'ES heap')
        for key in ('write','get'): require(node['thread_pool'][key]['rejected']==0,'ES rejected')
        integer(node['process']['open_file_descriptors'],True);integer(node['process']['cpu']['total_in_millis'])
    result=dict(target=target,observer=observer,fd_soft=int(soft),fd_hard=int(hard),pids_max=pid_max,
                cpu_max=files['cpu.max'],memory_max=files['memory.max'],swap_max=files['memory.swap.max'],cpuset=files['cpuset.cpus.effective'],hidden_ancestors='unknown')
    return result


def observation_report(root, native):
    result = {}
    for role in ('weir','es'):
        entries=read_stream(root/(role+'.jsonl'))
        result[role]=observation_series(entries, role, native)
    result['client']=sample_check(json.loads((root/'client-snapshot.json').read_text()),'client')
    identities=[result[role]['target']['namespaces'] for role in ('weir','es','client')]
    for key in ('pid','mnt','cgroup'): require(len({i[key] for i in identities})==3,'role namespace not independent: '+key)
    require(len({i['net'] for i in identities})==1,'shared loopback namespace')
    return result


def observation_series(entries, role, native):
    require(len(entries)==8 and entries[-1].get('type')=='observer_end' and entries[-1]['samples']==6 and entries[-1]['role']==role, 'six samples/normal observer_end')
    samples=entries[1:-1]
    require(0 <= timestamp(entries[-1]['ended_at'])-timestamp(samples[-1]['end']) <= 2, 'observer completion clock')
    options=dict(role=role,hashes=dict(weir=IMAGES['version']['binary'],client=IMAGES['tool']['binary']),native=native)
    observer_identity(entries[0],samples[0],options)
    values=[sample_check(s,role) for s in samples]
    require(all(v==values[0] for v in values),'sample identity/limits drift')
    require([s['sequence'] for s in samples]==list(range(6)), 'sample sequence')
    seconds=(samples[-1]['monotonic_ns']-samples[0]['monotonic_ns'])/1e9
    require(9.5 <= seconds <= 12, 'ten second observation')
    for previous,current in zip(samples,samples[1:]):
        require(0 < current['monotonic_ns']-previous['monotonic_ns'] <= 4e9,'observation gap')
        for field in ('cpu.stat','memory.events'):
            before,after=counter(previous['files'][field]),counter(current['files'][field])
            require(all(k in after and after[k]>=v for k,v in before.items()),'counter decrease')
    result=dict(values[0], samples=6,seconds=seconds,sampled_maximum_rss=max(s['rss_bytes'] for s in samples))
    return result


def scope_check(root, owner=None):
    name = root.parent.name
    require(re.fullmatch(r'[a-z][a-z0-9]{0,15}', name), 'single evidence run name')
    base = common.REPO/'.testdata'
    require(root == base/name/'native' and root.resolve() == root and base.resolve() == base and
            not root.is_symlink() and not root.parent.is_symlink(), 'controlled absolute evidence root')
    if owner is not None:
        require(isinstance(owner, str) and re.fullmatch('weir-qual-'+name+r'-[a-z0-9-]{1,25}', owner), 'evidence owner scope')


def prepare(run, owner):
    milestone=run.root.parent.name
    scope_check(run.root, owner)
    helper=verified_helper(run.root.parent)
    tool_inputs={p:common.digest(common.REPO/p) for p in FILES}
    require(run.run(['kubectl','config','current-context']).strip()==loop.TARGET['context'],'current context drift')
    context=loop.prepare_context(run,owner,image_source=SOURCE,owner_pattern='weir-qual-'+milestone+r'-[a-z0-9-]{1,25}')
    plan=dict(schema_version=1,profile=milestone+'-eks-no-load-resource-preflight',evidence_root=str(run.root.resolve()),target=loop.TARGET,owner=owner,namespace=owner,
              node=context['node'],initial_spare=context['initial_spare'],resource_preflight=context['resource_preflight'],cluster=context['cluster'],source=context['source'],image_source=SOURCE,
              images=IMAGES,helper=helper,budgets=BUDGET,minimum=loop.MINIMUM,sampled_at=time.time(),atomic_snapshot=False,
              tool_inputs=tool_inputs,prepared_at_monotonic=time.monotonic(),
              sequence=['ES native sidecar','bootstrap upload verified','UID recheck','release','empty records index','Weir/client','two ten-second observers','one client snapshot','cleanup'])
    plan['objects']=objects(plan)
    run.save('plan.json',plan);(run.root/'plan.json').chmod(0o400)
    print(json.dumps(dict(plan_sha256=common.digest(run.root/'plan.json'),node_uid=plan['node']['uid'])),flush=True)


def execute(run, plan_sha256):
    require(common.digest(run.root/'plan.json')==plan_sha256,'plan hash')
    plan=json.loads((run.root/'plan.json').read_text());run.plan=plan
    scope_check(run.root, plan['owner'])
    require(plan['namespace']==plan['owner'] and plan['profile']==run.root.parent.name+'-eks-no-load-resource-preflight','frozen owner/profile')
    require(plan['evidence_root']==str(run.root.resolve()),'frozen evidence path')
    run.resource_preflight=plan['resource_preflight']
    require(isinstance(run.resource_preflight, dict), "frozen resource preflight required; cannot start a new window")
    require(plan['target']==loop.TARGET and plan['images']==IMAGES and plan['image_source']==SOURCE and plan['budgets']==BUDGET and plan['minimum']==loop.MINIMUM,'frozen boundary')
    require(plan['helper']==verified_helper(run.root.parent) and plan['objects']==objects(plan),'artifact/template drift')
    require(plan['tool_inputs']=={p:common.digest(common.REPO/p) for p in FILES},'script drift')
    require(run.run(['git','rev-parse','HEAD']).strip()==plan['source'] and not run.run(['git','status','--porcelain']).strip(),'source/clean tree')
    run.run(['git','diff','--exit-code',SOURCE,'--','*.go','go.mod','go.sum','packaging','scripts/qualification.Dockerfile'])
    with (run.root.parent/'invocation.json').open('x') as output:json.dump(dict(start=time.time(),plan_sha256=plan_sha256),output)
    run.pod_entry=None;run.pod_ready=False;run.runtime_evidence=False;run.job_create_attempted=False
    result=dict(profile=plan['profile'] if 'profile' in plan else 'm30-eks-no-load-resource-preflight',passed=False,network_isolation='unqualified',resource_evidence='partial/not-qualified',timing='not-run',candidate=None,
                             trials=0,seeds=0,planned=0,document_mutations=0,empty_index_put_completed=None,errors=[])
    run.remote_started=time.monotonic();run.deadline=run.remote_started+BUDGET['remote_seconds']
    try:
        run.check_node()
        require(not run.kube(['get','namespace',plan['namespace'],'--ignore-not-found','-o','name']).strip(),'namespace collision')
        loop.namespace_start(run)
        run.template=plan['objects']['job']
        management=dict(reserved=1,started_lower_bound=0,started_upper_bound=1,completed=None,document_mutations=0)
        run.save('bootstrap-management.json',management)
        total_deadline=run.deadline
        until=min(total_deadline,time.monotonic()+BUDGET['artifact_ready_seconds'])
        run.deadline=until-5  # Reserve local Stop/Wait inside startup allowance.
        run.job=run.create(run.template)
        while time.monotonic()<until:
            pod=run.current_pod()
            if pod and any(s['name']=='bootstrap' and s.get('state',{}).get('running') for s in pod['status'].get('initContainerStatuses',[])):
                raw=run.kube(['logs',run.pod_entry['name'],'--container=bootstrap','--limit-bytes=65536','--tail=-1'],plan['namespace'])
                if raw=='{"bootstrap":"waiting-for-verified-helper"}\n':break
            time.sleep(.5)
        require(time.monotonic()<until,'bootstrap startup deadline')
        run.transfer(until)
        run.deadline=total_deadline
        result['empty_index_put_completed']=1
        management=dict(reserved=1,started_lower_bound=1,started_upper_bound=1,completed=1,document_mutations=0)
        run.save('bootstrap-management.json',management)
        version=json.loads(run.exec_owned('weir',['/weir','-version']))
        expected=dict(product='weir',revision=SOURCE,go='go1.27.1',target='linux/arm64',state='clean-commit',dirty='false',version='local-'+SOURCE)
        require(version==expected,'Weir version');run.save('version.json',version)
        pod=run.current_pod();address=ipaddress.ip_address(pod['status']['podIP'])
        require(address.version==4 and not address.is_loopback and not address.is_unspecified,'own PodIP')
        boundary=run.exec_owned('elasticsearch',['/usr/bin/timeout','25','/bin/bash','--noprofile','--norc','/qualification/loopback-check.sh','main'])
        run.save('loopback-boundary.log',boundary)
        require(boundary.endswith('loopback-check-complete\n') and 'own-pod-ip='+str(address)+'\n' in boundary,'loopback boundary')
        native=run.exec_owned('elasticsearch',['/bin/bash','--noprofile','--norc','-c','set -eu; uname -smr; id; getconf CLK_TCK; sha256sum /usr/share/elasticsearch/jdk/bin/java'])
        run.save('native-identity.txt',native)
        for role in ('weir', 'es'):
            run.observe(role)
        raw=run.exec_owned('qualification',['/qualification','-mode','snapshot'])
        run.save('client-snapshot.json',raw)
        result['observations']=observation_report(run.root,native.splitlines())
        run.monitor();run.save('final-pod.json',run.current_pod())
        result['passed']=True
    except BaseException as exc:
        result['errors'].append(str(exc))
        # Retain owned startup failures within the original overall window.
        run.deadline=run.remote_started+BUDGET['remote_seconds']
        if run.pod_entry and not isinstance(exc, (KeyboardInterrupt, SystemExit)):
            for container in ('bootstrap','elasticsearch','weir','qualification'):
                try:
                    pod=run.selected_object('Pod',run.pod_entry['name'])
                    identity=dict(job=run.job,template=run.template,pod_uid=run.pod_entry['uid'])
                    common.pod_identity(pod,identity)
                    command=['kubectl','--context',run.target['context'],'--request-timeout=5s','--namespace',plan['namespace'],
                             'logs',run.pod_entry['name'],'--container='+container,'--limit-bytes=65536','--tail=-1']
                    run.save('failure-'+container+'.log',run.run(command,6))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as diagnostic:
                    error=dict(error=str(diagnostic))
                    run.save('failure-'+container+'-error.json',error)
    finally:
        signal.signal(signal.SIGINT,signal.SIG_IGN);signal.signal(signal.SIGTERM,signal.SIG_IGN)
        result['fixture_seconds']=time.monotonic()-run.remote_started
        cleanup_started=time.monotonic()
        cleanup_deadline=cleanup_started+BUDGET['cleanup_seconds']
        run.deadline=cleanup_deadline-BUDGET['namespace_reserve_seconds'];run.cleaning=True
        if run.pod_entry:
            try:
                pod=run.selected_object('Pod',run.pod_entry['name'])
                identity=dict(job=run.job,template=run.template,pod_uid=run.pod_entry['uid'])
                common.pod_identity(pod,identity);run.save('cleanup-before-pod.json',pod)
            except BaseException as exc:result['errors'].append('final Pod: '+str(exc))
        result['cleanup']=run.cleanup(deadline=cleanup_deadline)
        result['cleanup_seconds']=time.monotonic()-cleanup_started
        result['remote_helpers_closed_by']='namespace absence' if result['cleanup']['confirmed'] else 'unknown; inspect exact owned UIDs'
        result['passed'] &= result['cleanup']['confirmed']
        responses={}
        for role in ('weir','es'):
            stream=run.root/(role+'.jsonl')
            try:
                entries=read_stream(stream) if stream.exists() else []
                responses[role]=sum(bool(e.get('metrics' if role=='weir' else 'db')) for e in entries)
            except (ValueError,OSError):responses[role]='unknown; retained incomplete stream'
        audit=dict(document_mutations=0,trials=0,seeds=0,planned=0,empty_index_put_completed=result['empty_index_put_completed'],
                   empty_index_put_reserved=1 if run.job_create_attempted else 0,observer_successful_http_responses=responses,
                   boundary_http='two GETs per completed bootstrap/main loopback check; raw logs retained',
                   kubelet_startup_and_readiness_probe_count='unknown',client_snapshot_http=0,mutation_replay=0)
        run.save('operation-audit.json',audit)
        run.save('result.json',result)
    print(json.dumps(dict(passed=result['passed'],errors=result['errors'],cleanup=result['cleanup']['confirmed'])),flush=True)
    return 0 if result['passed'] else 1


def main():
    require(os.environ.get('WEIR_EKS_M30')=='1','explicit WEIR_EKS_M30=1 required')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=('prepare','run'));parser.add_argument('--owner');parser.add_argument('--plan-sha256')
    parser.add_argument('--evidence',required=True)
    parser.add_argument('--node-name');parser.add_argument('--node-uid')
    args=parser.parse_args();root=common.REPO/'.testdata'/args.evidence/'native'
    scope_check(root, args.owner if args.mode=='prepare' else None)
    if args.mode=='prepare':root.mkdir(mode=0o700)
    else:require(root.is_dir() and root.stat().st_mode & 0o777==0o700,'private evidence')
    run=Run(root,loop.TARGET);run.number=max([int(p.stem.split('-')[1]) for p in root.glob('command-*.json')]+[0])
    def interrupted(signum,frame):raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,interrupted);signal.signal(signal.SIGTERM,interrupted)
    if args.mode=='prepare':
        run.node_scope=dict(name=args.node_name,uid=args.node_uid)
        prepare(run,args.owner);return 0
    return execute(run,args.plan_sha256)


if __name__=='__main__':raise SystemExit(main())
