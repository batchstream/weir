"""M30R7 deterministic streams, real host pipes and strict shared admission."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import eks_exec_tail as tail
import eks_pacing as common
from capacity_fixture import Observer, stop_group

HOST_PRODUCER = '''import select,sys,time
def early():
 if select.select([sys.stdin],[],[],0)[0]:
  sys.stderr.write('early-input-or-eof\\n');sys.exit(72)
early()
lines=open(sys.argv[1],'rb').readlines()
sys.stdout.buffer.write(lines[0]);sys.stdout.flush()
for i,line in enumerate(lines[1:-1]):
 if i: time.sleep(2)
 early();sys.stdout.buffer.write(line);sys.stdout.flush()
early();sys.stdout.buffer.write(lines[-1]);sys.stdout.flush()
if sys.argv[2]=='immediate':sys.exit(0)
if not select.select([sys.stdin],[],[],4)[0]:
 sys.stderr.write('ack-timeout\\n');sys.exit(74)
if sys.stdin.buffer.read(1):sys.exit(73)
sys.stderr.write('ack-eof\\n')
'''


class TailTests(unittest.TestCase):
    def test_integrity_rejects_every_incomplete_or_changed_contract(self):
        raw = tail.payload()
        self.assertTrue(tail.integrity(raw)['complete'])
        self.assertEqual(tail.payload_contract()['line_bytes'], [64]+[49152]*6+[65])
        lines = raw.splitlines(keepends=True)
        cases = [raw[:-1], raw[:-65], raw[:-5000], raw.replace(b'XXX', b'XXY', 1),
                 b''.join([lines[0], lines[2], lines[1]]+lines[3:]), raw+lines[-1], b'{}\n']
        for case in cases:
            self.assertFalse(tail.integrity(case)['complete'])

    def test_three_real_host_pipe_controller_arms(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)/'payload'
            source.write_bytes(tail.payload())
            results = []
            for arm in tail.ARMS:
                command = [sys.executable, '-c', HOST_PRODUCER, str(source), arm['mode']]
                options = dict(root=directory, role=arm['name'], command=command, ack=arm['ack'], deadline=time.monotonic()+35)
                result = tail.collect(options)
                results.append(result)
                self.assertTrue(result['complete'])
                self.assertTrue(result['joined'])
                self.assertTrue(result['pipes_closed'])
                self.assertEqual(result['eof'], [True, True])
                self.assertGreaterEqual(result['elapsed'], 10)
            self.assertEqual([r['exit'] for r in results], [0, 0, 74])
            self.assertEqual(results[1]['stderr'], 'ack-eof\n')
            self.assertIsNotNone(results[1]['ack_monotonic'])
            self.assertGreaterEqual(results[1]['ack_monotonic'], results[1]['complete_monotonic'])
            self.assertEqual(results[2]['stderr'], 'ack-timeout\n')
            self.assertIsNone(results[2]['ack_monotonic'])
            self.assertFalse(results[2]['successful'])
            self.assertLess(results[2]['elapsed'], 16)
            print('HOST_ARMS '+json.dumps(results), flush=True)

    def test_controller_fixture_early_eof_and_data(self):
        for data in (b'', b'x', b'\n'):
            command = [sys.executable, '-c', HOST_PRODUCER, 'unused-on-early-input', 'eof']
            child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     start_new_session=True)
            try:
                stdout, stderr = child.communicate(data, timeout=5)
                self.assertEqual(child.returncode, 72)
                self.assertEqual(stderr, b'early-input-or-eof\n')
                self.assertEqual(stdout, b'')
            finally:
                stop_group(child)
                for pipe in (child.stdin, child.stdout, child.stderr):
                    pipe.close()

    def test_collect_never_acknowledges_truncation_hash_error_or_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory)/'source'
            for index, data in enumerate((tail.payload()[:-1], tail.payload().replace(b'XXX', b'XXY', 1), b'{}\n')):
                raw.write_bytes(data)
                code = 'import sys; sys.stdout.buffer.write(open(sys.argv[1],"rb").read());sys.exit(7)'
                options = dict(root=directory, role=str(index), command=[sys.executable, '-c', code, str(raw)],
                               ack=True, deadline=time.monotonic()+12)
                result = tail.collect(options)
                self.assertIsNone(result['ack_monotonic'])
                self.assertFalse(result['successful'])
                self.assertTrue(result['joined'])

    def test_failure_closes_owner_on_cancel_output_or_record_error(self):
        for failure in ('cancel', 'output', 'record'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                options = dict(root=directory, role='failure', command=[sys.executable, '-c',
                               'import sys,time;print("{}",flush=True);sys.stdin.read();time.sleep(.02)'],
                               ack=False, deadline=time.monotonic()+12)
                original = Observer.poll
                owners = []
                def fail(observer):
                    if not owners:
                        owners.append(observer)
                        if failure == 'cancel':
                            raise KeyboardInterrupt('injected cancel')
                        target = Path(directory)/('failure.jsonl' if failure == 'output' else 'failure-exec.json')
                        if target.exists():
                            target.unlink()
                        target.mkdir()
                    return original(observer)
                with patch.object(Observer, 'poll', fail):
                    with self.assertRaises(BaseException):
                        tail.collect(options)
                owner = owners[0]
                self.assertTrue(owner.joined)
                self.assertIsNotNone(owner.stopped)
                self.assertTrue(all(p.closed for p in (owner.child.stdin, *owner.pipes)))
                with self.assertRaises(ChildProcessError):
                    os.waitpid(owner.child.pid, os.WNOHANG)

    def test_stream_limits_retain_only_bounded_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            options = dict(root=directory, role='bounds', command=[sys.executable, '-c',
                           'import os;os.write(2,b"x"*70000)'], stream_limits=[1048576, 65536])
            observer = Observer(options)
            try:
                until = time.monotonic()+5
                while time.monotonic() < until:
                    observer.poll()
                    time.sleep(.005)
            except RuntimeError as exc:
                self.assertEqual(str(exc), 'observer output bound')
            finally:
                with self.assertRaises(RuntimeError):
                    observer.stop()
            self.assertEqual(len(observer.streams[1]), 65536)
            self.assertTrue(observer.joined)


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.plan = dict(owner='weir-qual-m30r7-unit', namespace='weir-qual-m30r7-unit', node=tail.NODE)
        self.objects = tail.objects(self.plan)
        self.template = self.objects['job']
        self.job = dict(kind='Job', name='diagnostic', uid='job', owner=self.plan['owner'])
        self.pod = dict(apiVersion='v1', kind='Pod',
                        metadata=dict(name='diagnostic-abc', namespace=self.plan['namespace'], uid='pod',
                                      labels={common.LABEL: self.plan['owner']},
                                      ownerReferences=[dict(uid='job', name='diagnostic', kind='Job', apiVersion='batch/v1', controller=True)]),
                        spec=copy.deepcopy(self.template['spec']['template']['spec']),
                        status=dict(phase='Running', containerStatuses=[dict(name='shell', restartCount=0,
                                    imageID=tail.loop.ES['reference'], containerID='containerd://'+'a'*64, ready=True,
                                    user=dict(linux=dict(uid=1000, gid=1000, supplementalGroups=[1000])),
                                    state=dict(running=dict(startedAt='2026-09-29T00:00:00Z')))]))
        self.pod['spec'].update(priority=0, preemptionPolicy='PreemptLowerPriority')
        self.options = dict(job=self.job, template=self.template, pod_uid='pod')

    def test_manifest_and_admitted_inert_defaults(self):
        self.assertTrue(tail.pod_check(self.pod, self.options))
        resources = self.pod['spec']['containers'][0]['resources']
        for values in resources.values():
            values.update(cpu='1000m', memory=str(256*1024**2))
        self.assertTrue(tail.pod_check(self.pod, self.options))
        spec = self.pod['spec']
        for key, value in dict(hostNetwork=False, hostPID=False, hostIPC=False, schedulerName='default-scheduler').items():
            spec[key] = value
        self.assertTrue(tail.pod_check(self.pod, self.options))
        self.assertNotIn('initContainers', spec)
        self.assertNotIn('emptyDir', str(spec))
        self.assertEqual(self.objects['quota']['spec']['hard']['count/secrets'], '0')

    def test_uid_image_resource_injected_field_and_runtime_drift_rejected(self):
        variants = []
        changed = copy.deepcopy(self.pod); changed['metadata']['uid'] = 'wrong'; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['status']['containerStatuses'][0]['imageID'] = 'wrong'; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['spec']['hostPID'] = True; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['spec']['containers'][0]['resources']['limits']['cpu'] = '2'; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['status']['containerStatuses'][0]['restartCount'] = 1; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['status']['containerStatuses'][0]['allocatedResources'] = {'cpu':'2'}; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['status']['resize'] = 'InProgress'; variants.append(changed)
        changed = copy.deepcopy(self.pod); changed['status']['containerStatuses'][0]['user']['linux']['uid'] = 0; variants.append(changed)
        for changed in variants:
            with self.assertRaises(ValueError):
                tail.pod_check(changed, self.options)

    def test_frozen_plan_refuses_profile_arms_budget_and_payload_drift(self):
        plan = dict(self.plan, profile=tail.PROFILE, target=tail.loop.TARGET, budgets=tail.BUDGET,
                    arms=tail.ARMS, command=tail.COMMAND, minimum=tail.loop.MINIMUM, image=tail.loop.ES,
                    payload=tail.payload_contract(), objects=self.objects,
                    stage_started=100, stage_deadline=1000,
                    tool_inputs={p:common.digest(common.REPO/p) for p in tail.FILES}, kubectl_sha256=tail.KUBECTL_SHA,
                    resource_preflight=dict(node=tail.NODE, started=100, deadline=220))
        root = common.REPO/'.testdata/m30r7/native'
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory)/'kubectl'
            executable.write_text('owned fixture, never executed\n')
            executable.chmod(0o700)
            for path in ('', directory):
                with self.subTest(path=path), patch.dict(os.environ, PATH=path):
                    tail.plan_check(plan, root)
                    for key, value in (('profile','m30r6-eks-no-load-resource-preflight'), ('arms',tail.ARMS[:2]),
                                       ('arms',tail.ARMS+tail.ARMS[:1]), ('budgets',dict(tail.BUDGET, arm_seconds=61)),
                                       ('payload',dict(tail.payload_contract(),bytes=1)), ('tool_inputs',{}),
                                       ('kubectl_sha256','0'*64), ('stage_deadline',1001)):
                        changed = copy.deepcopy(plan); changed[key] = value
                        with self.assertRaises(ValueError):
                            tail.plan_check(changed, root)

    def test_runtime_missing_or_changed_kubectl_refuses_before_commands_and_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory).resolve()
            executable = repository/'kubectl'
            executable.write_text('owned fixture, never executed\n')
            executable.chmod(0o700)
            root = repository/'.testdata/m30r7/native'
            run = tail.Run(root, tail.loop.TARGET)
            for path, reason in (('', 'kubectl missing'), (directory, 'kubectl drift')):
                with self.subTest(path=path), patch.dict(os.environ, PATH=path), patch.object(common, 'REPO', repository), \
                        patch.object(run, 'run', side_effect=AssertionError('external command')) as command:
                    with self.assertRaisesRegex(ValueError, reason):
                        tail.prepare(run, self.plan['owner'])
                    with self.assertRaisesRegex(ValueError, reason):
                        tail.execute(run, 'unused')
                    command.assert_not_called()
                    self.assertEqual(list(repository.iterdir()), [executable])

    def test_execute_refuses_changed_source_before_invocation_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root/'kubectl'
            executable.write_text('owned fixture, never executed\n')
            executable.chmod(0o700)
            plan_path = root/'plan.json'
            plan_path.write_text(json.dumps(dict(source='a'*40)))
            run = tail.Run(root, tail.loop.TARGET)
            with patch.dict(os.environ, PATH=directory), patch.object(tail, 'KUBECTL_SHA', common.digest(executable)), \
                    patch.object(tail, 'plan_check'), patch.object(run, 'run', return_value='b'*40) as command:
                with self.assertRaisesRegex(ValueError, 'clean committed source'):
                    tail.execute(run, common.digest(plan_path))
                command.assert_called_once_with(['git', 'rev-parse', 'HEAD'])
            self.assertEqual(set(root.iterdir()), {executable, plan_path})

    def test_insufficient_stage_budget_dispatches_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            run = tail.Run(Path(directory), tail.loop.TARGET)
            run.deadline = time.monotonic()+59
            with self.assertRaisesRegex(ValueError, 'arm budget'):
                run.arm(tail.ARMS[0])
            self.assertEqual(run.number, 0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_arm_record_failure_preserves_first_error(self):
        with tempfile.TemporaryDirectory() as directory:
            run = tail.Run(Path(directory), tail.loop.TARGET)
            with patch.object(run, 'identity', side_effect=ValueError('first identity failure')), \
                    patch.object(run, 'save', side_effect=OSError('closing evidence failure')):
                with self.assertRaisesRegex(ValueError, 'first identity failure') as caught:
                    run.arm(tail.ARMS[0])
            self.assertIn('arm result: closing evidence failure', caught.exception.__notes__)

    def test_foreign_cleanup_resource_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            run = tail.Run(Path(directory), tail.loop.TARGET)
            run.plan = self.plan
            rows = [['v1','ConfigMap','foreign','foreign-uid','','','']]
            with self.assertRaisesRegex(ValueError, 'foreign resource'):
                run.foreign_check(rows)


if __name__ == '__main__':
    unittest.main()
