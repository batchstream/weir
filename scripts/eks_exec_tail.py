#!/usr/bin/env python3
"""M30R7 only: three fixed byte streams, no services or resource qualification."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import time

import eks_pacing as common
import eks_loopback as loop
from capacity_fixture import Observer
from eks_pacing_report import require
from eks_resources import pod_requests

SOURCE = '4abc8761f9f0e08af978d5ae5c14176f8188cfa3'
PROFILE = 'm30r7-exec-tail-diagnostic-only'
NODE = dict(name='ip-172-31-12-243.us-west-1.compute.internal', uid='18f03c58-83c1-424e-be34-44ef88c07831')
BUDGET = dict(remote_seconds=900, cleanup_seconds=300, arm_seconds=60, producer_seconds=20,
              ack_seconds=4, stdout_bytes=1048576, stderr_bytes=65536)
ARMS = [dict(name='A', mode='immediate', ack=False), dict(name='B', mode='eof', ack=True),
        dict(name='C', mode='eof', ack=False)]
KUBECTL_SHA = 'd6487d72d341c1db4d7fe6840e96cd3c99d75e8fe6dcef205ceb1c987ebf84f5'
SCRIPT = common.REPO/'scripts/exec_tail_producer.sh'
FILES = tuple(dict.fromkeys(loop.FILES + ('scripts/eks_exec_tail.py', 'scripts/eks_exec_tail_test.py', 'scripts/exec_tail_producer.sh')))
COMMAND = ['/usr/bin/timeout', '--signal=TERM', '--kill-after=1s', '19s',
           '/bin/bash', '--noprofile', '--norc', '/diagnostic/producer.sh']


def payload():
    records = [dict(kind='identity', schema=1, diagnostic='m30r7-fixed-bytes')]
    records.extend(dict(kind='diagnostic', sequence=i, padding='X'*49104) for i in range(6))
    records.append(dict(kind='terminal', records=6, diagnostic='m30r7-fixed-bytes'))
    return b''.join((json.dumps(r, separators=(',', ':'))+'\n').encode() for r in records)


def payload_contract():
    raw = payload()
    result = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                  line_bytes=[len(line) for line in raw.splitlines(keepends=True)],
                  terminal=raw.splitlines(keepends=True)[-1].decode(), sequences=list(range(6)))
    return result


def integrity(raw):
    expected = payload()
    lines = raw.splitlines(keepends=True)
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except ValueError:
            break
    sequence = [r.get('sequence') for r in records if isinstance(r, dict) and r.get('kind') == 'diagnostic']
    result = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                  length_ok=len(raw) == len(expected), hash_ok=hashlib.sha256(raw).digest() == hashlib.sha256(expected).digest(),
                  ordered=sequence == list(range(6)), sequences=sequence,
                  terminal_ok=bool(lines) and lines[-1] == expected.splitlines(keepends=True)[-1],
                  line_bytes=[len(line) for line in lines], exact=raw == expected)
    result['complete'] = all(result[k] for k in ('length_ok', 'hash_ok', 'ordered', 'terminal_ok', 'exact'))
    return result


def collect(options):
    """Use the existing process owner; only B closes stdin before process exit."""
    started = time.monotonic()
    options = dict(options, stream_limits=[BUDGET['stdout_bytes'], BUDGET['stderr_bytes']])
    observer = Observer(options)
    first = None
    ack_at = None
    complete_at = None
    expected_size = len(payload())
    until = options['deadline']
    try:
        while True:
            require(time.monotonic() < until-8, 'exec drain deadline')
            try:
                observer.poll()
            except RuntimeError:
                if observer.failure is None:
                    raise
                # Nonzero/truncation is a measured outcome. Keep draining both
                # pipes until exit and EOF; never acknowledge an invalid stream.
            require(observer.failure != 'observer output bound', 'diagnostic output bound')
            require(len(observer.streams[0]) <= BUDGET['stdout_bytes'] and
                    len(observer.streams[1]) <= BUDGET['stderr_bytes'], 'diagnostic output bound')
            if len(observer.streams[0]) >= expected_size and complete_at is None:
                checked = integrity(bytes(observer.streams[0]))
                if checked['complete']:
                    complete_at = time.monotonic()
                    if options['ack']:
                        observer.child.stdin.close()
                        ack_at = time.monotonic()
            if observer.child.poll() is not None and all(observer.eof):
                break
            time.sleep(.005)
    except BaseException as exc:
        first = exc
    finally:
        try:
            observer.stop(deadline=until)
        except BaseException as exc:
            # Observer preserves the measured transport failure as stop_error.
            if first is not None:
                first.add_note('closing: '+str(exc))
            elif not (isinstance(exc, RuntimeError) and str(exc) == observer.failure and
                      observer.joined and all(observer.eof) and
                      all(n == 'observer stop: '+observer.failure for n in getattr(exc, '__notes__', []))):
                first = exc
    if first is not None:
        raise first
    require(observer.joined and observer.stopped is not None and all(observer.eof) and
            all(p.closed for p in (observer.child.stdin, *observer.pipes)), 'exec ownership incomplete')
    result = integrity(bytes(observer.streams[0]))
    result.update(exit=observer.child.returncode, stderr=bytes(observer.streams[1]).decode(errors='replace'),
                  transport_failure=observer.failure, eof=list(observer.eof), joined=observer.joined,
                  pipes_closed=True, ack_monotonic=ack_at, complete_monotonic=complete_at,
                  started_monotonic=started, stopped_monotonic=observer.stopped, pid=observer.child.pid,
                  elapsed=time.monotonic()-started)
    result['successful'] = result['complete'] and result['exit'] == 0 and not result['transport_failure']
    return result


def objects(plan):
    labels = {common.LABEL: plan['owner']}
    meta = dict(name='diagnostic', namespace=plan['namespace'], labels=labels)
    security = dict(runAsNonRoot=True, runAsUser=1000, runAsGroup=1000, allowPrivilegeEscalation=False,
                    readOnlyRootFilesystem=True, capabilities=dict(drop=['ALL']), seccompProfile=dict(type='RuntimeDefault'))
    limits = dict(cpu='1', memory='256Mi', **{'ephemeral-storage': '128Mi'})
    mount = dict(name='diagnostic', mountPath='/diagnostic', readOnly=True)
    container = dict(name='shell', image=loop.ES['reference'], imagePullPolicy='IfNotPresent',
                     command=['/bin/bash', '--noprofile', '--norc', '-c', 'exec /bin/sleep 880'],
                     securityContext=security, resources=dict(requests=limits, limits=limits), volumeMounts=[mount])
    volume = dict(name='diagnostic', configMap=dict(name='configuration', defaultMode=292))
    spec = dict(nodeName=plan['node']['name'], restartPolicy='Never', activeDeadlineSeconds=900,
                terminationGracePeriodSeconds=10, automountServiceAccountToken=False, enableServiceLinks=False,
                dnsPolicy='None', dnsConfig=dict(nameservers=['127.0.0.1']),
                securityContext=dict(runAsNonRoot=True, runAsUser=1000, runAsGroup=1000, seccompProfile=dict(type='RuntimeDefault')),
                containers=[container], volumes=[volume])
    job = dict(apiVersion='batch/v1', kind='Job', metadata=meta,
               spec=dict(completions=1, parallelism=1, backoffLimit=0, activeDeadlineSeconds=900,
                         template=dict(metadata=dict(labels=labels), spec=spec)))
    config = dict(apiVersion='v1', kind='ConfigMap', metadata=dict(meta, name='configuration'), immutable=True,
                  data={'producer.sh': SCRIPT.read_text()})
    hard = {'pods': '1', 'count/pods': '1', 'count/jobs.batch': '1', 'requests.cpu': '1', 'limits.cpu': '1',
            'requests.memory': '256Mi', 'limits.memory': '256Mi', 'requests.ephemeral-storage': '128Mi',
            'limits.ephemeral-storage': '128Mi', 'services': '0', 'persistentvolumeclaims': '0',
            'count/secrets': '0', 'count/configmaps': '2'}
    quota = dict(apiVersion='v1', kind='ResourceQuota', metadata=dict(meta, name='budget'), spec=dict(hard=hard))
    policy = dict(apiVersion='networking.k8s.io/v1', kind='NetworkPolicy', metadata=dict(meta, name='default-deny'),
                  spec=dict(podSelector={}, policyTypes=['Ingress', 'Egress'], ingress=[], egress=[]))
    ns_labels = dict(labels, **{'pod-security.kubernetes.io/enforce': 'restricted',
                     'pod-security.kubernetes.io/enforce-version': 'v1.36', 'pod-security.kubernetes.io/audit': 'restricted',
                     'pod-security.kubernetes.io/warn': 'restricted'})
    namespace = dict(apiVersion='v1', kind='Namespace', metadata=dict(name=plan['namespace'], labels=ns_labels))
    result = dict(namespace=namespace, quota=quota, policy=policy, config=config, job=job)
    return result


def pod_check(pod, options):
    common.pod_identity(pod, options)
    common.admitted_spec(pod['spec'], options['template']['spec']['template']['spec'], pod=True)
    status = pod.get('status') or {}
    require(not status.get('initContainerStatuses') and not status.get('ephemeralContainerStatuses'), 'extra runtime container')
    require(status.get('phase') not in ('Failed', 'Succeeded') and not pod['metadata'].get('deletionTimestamp'), 'Pod ended/deleting')
    states = status.get('containerStatuses', [])
    require(len(states) <= 1, 'extra shell runtime')
    if not states:
        return None
    state = states[0]
    require(state['name'] == 'shell' and state['restartCount'] == 0 and not state.get('lastState') and
            not state.get('state', {}).get('terminated'), 'shell restart/termination')
    if state.get('imageID'):
        require(state['imageID'] == loop.ES['reference'], 'runtime image drift')
    if not state.get('state', {}).get('running'):
        return None
    require(status.get('phase') == 'Running' and not status.get('resize') and
            not any(c.get('type', '').startswith('PodResize') and c.get('status') != 'False'
                    for c in status.get('conditions', [])), 'runtime phase/resize drift')
    require(state.get('ready') is True and state.get('imageID') == loop.ES['reference'] and
            re.fullmatch(r'containerd://[0-9a-f]{64}', state.get('containerID', '')), 'runtime identity missing')
    user = state.get('user', {}).get('linux', {})
    require(type(user.get('uid')) is int and user['uid'] == 1000 and
            type(user.get('gid')) is int and user['gid'] == 1000, 'actual runtime UID/GID drift')
    projected = dict(resources=pod['spec'].get('resources'), statusResources=status.get('resources'),
                     allocatedResources=status.get('allocatedResources'), overhead=pod['spec'].get('overhead'),
                     resize=status.get('resize'), resizeConditions=[], unsupported=False,
                     containers=pod['spec']['containers'], initContainers=[], containerStatuses=states, initContainerStatuses=[])
    effective, _ = pod_requests(projected)
    wanted = dict(cpu=1, memory=256*1024**2, **{'ephemeral-storage': 128*1024**2})
    require(effective == wanted, 'actual resource budget drift')
    result = dict(pod_uid=pod['metadata']['uid'], containerID=state['containerID'], imageID=state['imageID'],
                  node=pod['spec']['nodeName'], started=state['state']['running']['startedAt'], user=user)
    return result


class Run(common.Run):
    node_minimum = loop.MINIMUM

    def job_template(self, step):
        require(step == 'diagnostic', 'only diagnostic Job')
        return objects(self.plan)['job']

    def identity(self):
        common.owner_check(self.selected_object('Namespace', self.plan['namespace']), self.namespace)
        job = self.selected_object('Job', self.job['name'])
        common.owner_check(job, self.job)
        common.job_check(job, self.template)
        require(not job['metadata'].get('deletionTimestamp'), 'Job deleting')
        config_entry = next(e for e in self.owned if e['kind'] == 'ConfigMap')
        config = self.selected_object('ConfigMap', config_entry['name'])
        common.owner_check(config, config_entry)
        require(config.get('immutable') is True and config.get('data') == self.plan['objects']['config']['data'], 'script drift')
        names = [self.pod_entry['name']] if self.pod_entry else self.kube(
            ['get', 'pods', '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'], self.plan['namespace']).split()
        require(len(names) <= 1, 'extra Pod')
        if not names:
            return None
        pod = self.selected_object('Pod', names[0])
        options = dict(job=self.job, template=self.template, pod_uid=self.pod_entry['uid'] if self.pod_entry else None)
        common.pod_identity(pod, options)
        if not self.pod_entry:
            self.pod_entry = dict(kind='Pod', name=names[0], uid=pod['metadata']['uid'], owner=self.plan['owner'])
            self.owned.append(self.pod_entry)
            self.save('owned.json', self.owned)
        self.save('pod-identity-'+str(self.number)+'.json', pod)
        current = pod_check(pod, options)
        if current:
            require(not self.runtime or current == self.runtime, 'runtime identity changed')
            self.runtime = current
            self.save('runtime-identity.json', current)
        return current

    def arm(self, arm):
        started, overall = time.monotonic(), self.deadline
        until = min(overall, started+BUDGET['arm_seconds'])
        require(until-started >= BUDGET['arm_seconds'], 'insufficient full arm budget')
        result = dict(arm=arm, start=started, deadline=until)
        with (self.root/(arm['name']+'-operation.json')).open('x') as output:
            json.dump(result, output)
        failure = None
        try:
            self.deadline = until-4
            require(self.identity() is not None, 'pre-arm Pod not ready')
            require(until-time.monotonic() >= 28, 'producer plus closing budget')
            command = ['kubectl', '--context', self.target['context'], '--request-timeout=10s',
                       '--namespace', self.plan['namespace'], 'exec', '-i', self.pod_entry['name'], '--container=shell',
                       '--']+COMMAND+[arm['mode']]
            options = dict(root=self.root, role=arm['name'], command=command, deadline=until, ack=arm['ack'])
            result['stream'] = collect(options)
            require(not any(word in result['stream']['stderr'].lower() for word in
                            ('forbidden', 'unauthorized', 'permission denied', 'not found', 'cannot exec', 'early-input', 'read-error')),
                    'exec permission/producer failure')
            # collect has joined and closed every pipe before any control-plane CLI.
            require(self.identity() is not None, 'post-arm Pod not ready')
            result['identity'] = self.runtime
            require(time.monotonic() < until, 'arm deadline')
        except BaseException as exc:
            failure = exc
            result['error'] = str(exc)
        finally:
            self.deadline = overall
            result['end'] = time.monotonic()
            try:
                self.save(arm['name']+'-result.json', result)
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    failure.add_note('arm result: '+str(exc))
        if failure is not None:
            raise failure
        return result


def plan_check(plan, root):
    require(root == common.REPO/'.testdata/m30r7/native' and root.resolve() == root, 'evidence scope')
    require(re.fullmatch(r'weir-qual-m30r7-[a-z0-9-]{1,25}', plan['owner']) and plan['namespace'] == plan['owner'], 'owner scope')
    frozen = dict(profile=PROFILE, target=loop.TARGET, budgets=BUDGET, arms=ARMS, command=COMMAND,
                  minimum=loop.MINIMUM, image=loop.ES, payload=payload_contract())
    require(all(common.exact_value(plan.get(k), v) for k, v in frozen.items()), 'frozen diagnostic contract')
    require({k: plan['node'][k] for k in NODE} == NODE and plan['objects'] == objects(plan), 'node/template drift')
    require(plan['tool_inputs'] == {p: common.digest(common.REPO/p) for p in FILES}, 'input drift')
    require(plan['kubectl_sha256'] == KUBECTL_SHA, 'kubectl drift')
    require(plan['resource_preflight']['node'] == NODE and
            plan['resource_preflight']['deadline']-plan['resource_preflight']['started'] == 120, 'resource window drift')
    require(plan['stage_deadline']-plan['stage_started'] == BUDGET['remote_seconds'], 'stage budget drift')


def kubectl_check():
    executable = shutil.which('kubectl')
    require(executable is not None, 'kubectl missing')
    require(common.digest(executable) == KUBECTL_SHA, 'kubectl drift')


def prepare(run, owner):
    require(run.root == common.REPO/'.testdata/m30r7/native' and run.root.resolve() == run.root, 'evidence scope')
    kubectl_check()
    with (run.root.parent/'prepare-once.json').open('x') as output:
        json.dump(dict(start=time.time(), owner=owner), output)
    require(run.run(['kubectl', 'version', '--client', '-o', 'json']), 'kubectl identity')
    require(run.run(['kubectl', 'config', 'current-context']).strip() == loop.TARGET['context'], 'current context drift')
    run.node_scope = NODE
    context = loop.prepare_context(run, owner, image_source=SOURCE, owner_pattern=r'weir-qual-m30r7-[a-z0-9-]{1,25}')
    plan = dict(context, profile=PROFILE, target=loop.TARGET, owner=owner, namespace=owner, budgets=BUDGET,
                stage_started=run.deadline-BUDGET['remote_seconds'], stage_deadline=run.deadline,
                arms=ARMS, command=COMMAND, minimum=loop.MINIMUM, image=loop.ES, payload=payload_contract(),
                kubectl_sha256=KUBECTL_SHA, tool_inputs={p: common.digest(common.REPO/p) for p in FILES})
    plan['objects'] = objects(plan)
    plan_check(plan, run.root)
    run.save('plan.json', plan)
    (run.root/'plan.json').chmod(0o400)


def namespace_start(run):
    obj = run.plan['objects']
    run.create(obj['namespace'])
    entry = run.create(obj['quota'])
    until = min(run.deadline, time.monotonic()+20)
    while True:
        quota = run.selected_object('ResourceQuota', 'budget')
        common.owner_check(quota, entry)
        common.quota_check(quota['spec'], obj['quota']['spec'])
        if quota.get('status', {}).get('used', {}).get('count/secrets') == '0':
            break
        require(time.monotonic() < until, 'quota deadline')
        time.sleep(.5)
    run.create(obj['policy'])
    rows = run.inventory()
    for _, kind, name, uid, _, _, _ in rows:
        if (kind, name) in (('ServiceAccount', 'default'), ('ConfigMap', 'kube-root-ca.crt')):
            run.defaults[kind, name] = uid
    run.foreign_check(rows)
    run.save('namespace-defaults.json', [dict(kind=k[0], name=k[1], uid=v) for k, v in run.defaults.items()])
    run.create(obj['config'])


def execute(run, plan_sha):
    kubectl_check()
    require(common.digest(run.root/'plan.json') == plan_sha, 'plan hash')
    plan = json.loads((run.root/'plan.json').read_text())
    plan_check(plan, run.root)
    require(run.run(['git', 'rev-parse', 'HEAD']).strip() == plan['source'] and
            not run.run(['git', 'status', '--porcelain']).strip(), 'clean committed source')
    with (run.root.parent/'invocation.json').open('x') as output:
        json.dump(dict(start=time.time(), plan_sha256=plan_sha), output)
    run.plan, run.resource_preflight = plan, plan['resource_preflight']
    run.pod_entry, run.runtime = None, None
    run.remote_started = time.monotonic()
    run.deadline = plan['stage_deadline']
    result = dict(profile=PROFILE, errors=[], arms=[], conclusion='inconclusive', resource_evidence='not-qualified',
                  network_isolation='unqualified', candidate=None, database_operations=0, artifact_uploads=0)
    try:
        run.check_node()
        require(not run.kube(['get', 'namespace', plan['namespace'], '--ignore-not-found', '-o', 'name']).strip(), 'namespace collision')
        namespace_start(run)
        run.template = plan['objects']['job']
        run.job = run.create(run.template)
        while not run.identity():
            require(time.monotonic() < run.deadline-180, 'startup budget')
            time.sleep(.5)
        for arm in ARMS:
            result['arms'].append(run.arm(arm))
        a, b, c = [r['stream'] for r in result['arms']]
        require(c['exit'] == 74 and 'ack-timeout\n' in c['stderr'] and c['ack_monotonic'] is None,
                'C missing explicit nonzero no-ACK timeout')
        if not a['complete'] and b['successful'] and b['ack_monotonic'] is not None and b['stderr'] == 'ack-eof\n':
            result['conclusion'] = 'supports bounded EOF mitigation under this fixed diagnostic only'
    except BaseException as exc:
        result['errors'].append(str(exc))
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        result['fixture_seconds'] = time.monotonic()-run.remote_started
        result['stage_seconds'] = time.monotonic()-plan['stage_started']
        cleanup_started = time.monotonic()
        # Closing/report failures must never bypass the one original cleanup.
        result['cleanup'] = run.cleanup(deadline=cleanup_started+BUDGET['cleanup_seconds'])
        result['cleanup_seconds'] = time.monotonic()-cleanup_started
        try:
            run.save('result.json', result)
        except BaseException as exc:
            result['errors'].append('final evidence: '+str(exc))
    print(json.dumps(result), flush=True)
    return int(bool(result['errors']) or not result['cleanup']['confirmed'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'run'])
    parser.add_argument('--owner')
    parser.add_argument('--plan-sha256')
    args = parser.parse_args()
    require(__debug__ and os.environ.get('WEIR_CAPACITY_INTEGRATION') == '1', 'explicit native opt-in required')
    root = common.REPO/'.testdata/m30r7/native'
    if args.action == 'prepare':
        root.mkdir(mode=0o700)
    run = Run(root, loop.TARGET)
    if args.action == 'run':
        run.number = max(int(p.stem.split('-')[1]) for p in root.glob('command-*.json'))
    def interrupted(signum, frame):
        raise KeyboardInterrupt('signal '+str(signum))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)
    if args.action == 'prepare':
        prepare(run, args.owner)
        return 0
    return execute(run, args.plan_sha256)


if __name__ == '__main__':
    raise SystemExit(main())
