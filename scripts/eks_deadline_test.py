"""Controlled clocks only: absolute budget boundaries, no EKS commands."""
import copy
import json
import math
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import eks_cleanup as cleanup
import eks_exec_tail as tail
import eks_pacing as common
import eks_resource_preflight as pre

STARTS = (4.1, 4.4, 8.2, 124.1, 212.2, 212.3, 1024.1, 1000000.1, 2**32+.1)


def neighbors(deadline):
    return (math.nextafter(deadline, -math.inf), deadline, math.nextafter(deadline, math.inf))


class AbsoluteDeadlines(unittest.TestCase):
    def test_full_arm_boundary_before_identity_or_operation(self):
        for started in STARTS:
            until = started+tail.BUDGET['arm_seconds']
            for overall in neighbors(until):
                with self.subTest(started=started, overall=overall), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    with patch.object(tail.time, 'monotonic', return_value=started):
                        run = tail.Run(root, tail.loop.TARGET)
                        run.deadline = overall
                        with patch.object(run, 'identity', side_effect=ValueError('first identity failure')) as identity, \
                                patch.object(run, 'save', side_effect=OSError('closing evidence failure')), \
                                patch.object(run, 'run', side_effect=AssertionError('external command')) as command:
                            reason = 'arm budget' if overall < until else 'first identity failure'
                            with self.assertRaisesRegex(ValueError, reason) as caught:
                                run.arm(tail.ARMS[0])
                            command.assert_not_called()
                        self.assertEqual(run.deadline, overall)
                        self.assertEqual(run.number, 0)
                        if overall < until:
                            identity.assert_not_called()
                            self.assertEqual(list(root.iterdir()), [])
                        else:
                            identity.assert_called_once_with()
                            operation = json.loads((root/'A-operation.json').read_text())
                            self.assertEqual(operation['start'], started)
                            self.assertEqual(operation['deadline'], until)
                            self.assertEqual(caught.exception.__notes__, ['arm result: closing evidence failure'])
                    receipt = dict(kind='arm', started=started, deadline=until, overall=overall,
                                   admitted=identity.called, commands=command.call_count, files=len(list(root.iterdir())))
                    print('DEADLINE_BOUNDARY '+json.dumps(receipt), flush=True)

    def test_observer_minimum_before_and_after_identity(self):
        closing = pre.BUDGET['observer_eof_seconds']+pre.BUDGET['command_stop_seconds']
        required = pre.BUDGET['observer_seconds']+closing
        for started in STARTS:
            for after_identity in (False, True):
                now = started+1 if after_identity else started
                for overall in neighbors(now+required):
                    with self.subTest(started=started, after_identity=after_identity, overall=overall), \
                            tempfile.TemporaryDirectory() as directory:
                        clock = [started]
                        with patch.object(pre.time, 'monotonic', side_effect=lambda:clock[0]):
                            run = pre.Run(Path(directory), pre.loop.TARGET)
                            run.deadline = overall
                            def monitor():
                                if after_identity:
                                    clock[0] = now
                                else:
                                    raise ValueError('first identity failure')
                            with patch.object(run, 'monitor', side_effect=monitor) as identity, \
                                    patch.object(run, 'exec_command', side_effect=ValueError('admitted observer')) as dispatch, \
                                    patch.object(run, 'run', side_effect=AssertionError('external command')) as command, \
                                    patch.object(pre, 'Observer') as observer:
                                admitted = overall >= now+required
                                reason = ('admitted observer' if after_identity else 'first identity failure') if admitted else 'observer/Stop/Wait budget'
                                with self.assertRaisesRegex(ValueError, reason):
                                    run.observe('weir')
                                command.assert_not_called()
                                observer.assert_not_called()
                            self.assertEqual(run.deadline, overall)
                            operation_path = Path(directory)/'weir-operation.json'
                            if not after_identity and not admitted:
                                identity.assert_not_called()
                                self.assertFalse(operation_path.exists())
                            else:
                                identity.assert_called_once_with()
                                operation = json.loads(operation_path.read_text())
                                self.assertEqual(operation['started_monotonic'], started)
                                self.assertEqual(operation['deadline_monotonic'], overall)
                            self.assertEqual(dispatch.called, after_identity and admitted)
                        receipt = dict(kind='observer', started=started, checked_at=now, deadline=overall,
                                       minimum_deadline=now+required, after_identity=after_identity,
                                       admitted=admitted, commands=command.call_count, operation=operation_path.exists())
                        print('DEADLINE_BOUNDARY '+json.dumps(receipt), flush=True)

    def test_resource_cli_and_recovery_minimum(self):
        scope = dict(name='node', uid='node-uid')
        for started in STARTS:
            for recovery in (False, True):
                for deadline in neighbors(started+(25+4)):
                    with self.subTest(started=started, recovery=recovery, deadline=deadline), \
                            tempfile.TemporaryDirectory() as directory:
                        clock = [started-1 if recovery else started]
                        with patch.object(common.time, 'monotonic', side_effect=lambda:clock[0]):
                            run = common.Run(Path(directory), tail.loop.TARGET)
                            run.deadline = deadline
                            run.resource_preflight = dict(node=scope, started=started, deadline=started+120, recovery=None)
                            def command(argv, timeout=25):
                                if recovery and clock[0] != started:
                                    clock[0] = started
                                    failure = common.CommandFailure(1, 1, 'net/http: timeout awaiting response headers')
                                    raise failure
                                raise ValueError('admitted CLI')
                            with patch.object(run, 'run', side_effect=command) as dispatch:
                                admitted = deadline >= started+(25+4)
                                reason = 'admitted CLI' if admitted else '资源窗口余额不足'
                                with self.assertRaisesRegex(ValueError, reason):
                                    run.nodes(scope)
                            self.assertEqual(dispatch.call_count, int(recovery)+int(admitted))
                            self.assertEqual(run.resource_preflight['deadline'], started+120)
                            self.assertEqual(run.resource_preflight['recovery'] is not None, recovery and admitted)
                            self.assertFalse(run.mutation_attempted)
                            self.assertFalse(list(Path(directory).glob('command-*')))
                        receipt = dict(kind='resource', started=started, deadline=deadline, recovery=recovery,
                                       admitted=admitted, simulated_calls=dispatch.call_count, external_commands=0)
                        print('DEADLINE_BOUNDARY '+json.dumps(receipt), flush=True)

    def test_prepare_retains_original_start_and_frozen_plan_rejects_adjacent_drift(self):
        # Copy only the explicit public tool inputs so real hashing remains in
        # plan_check while all command execution stays local and simulated.
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory).resolve()
            for name in tail.FILES:
                destination = repository/name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(common.REPO/name, destination)
            executable = repository/'kubectl'
            executable.write_text('owned fixture, never executed\n')
            executable.chmod(0o700)
            root = repository/'.testdata/m30r7/native'
            root.mkdir(parents=True)
            tool_sha = common.digest(executable)
            for started in STARTS:
                with self.subTest(started=started):
                    clock = [started]
                    context = dict(node=tail.NODE, resource_preflight=dict(node=tail.NODE, started=started, deadline=started+120), source='synthetic')
                    with patch.object(common, 'REPO', repository), patch.object(tail, 'KUBECTL_SHA', tool_sha), \
                            patch.object(tail.shutil, 'which', return_value=str(executable)), \
                            patch.object(tail.time, 'monotonic', side_effect=lambda:clock[0]):
                        run = tail.Run(root, tail.loop.TARGET)
                        clock[0] = started+30
                        with patch.object(run, 'run', return_value=tail.loop.TARGET['context']), \
                                patch.object(tail.loop, 'prepare_context', return_value=context):
                            tail.prepare(run, 'weir-qual-m30r7-unit')
                        frozen = json.loads((root/'plan.json').read_text())
                        self.assertEqual(frozen['stage_started'], started)
                        self.assertEqual(frozen['stage_deadline'], started+900)
                        tail.plan_check(frozen, root)
                        for key in ('stage_deadline', 'resource_preflight'):
                            expected = frozen[key] if key == 'stage_deadline' else frozen[key]['deadline']
                            for deadline in neighbors(expected):
                                changed = copy.deepcopy(frozen)
                                if key == 'stage_deadline':
                                    changed[key] = deadline
                                else:
                                    changed[key]['deadline'] = deadline
                                (root/'plan.json').chmod(0o600)
                                (root/'plan.json').write_text(json.dumps(changed))
                                with patch.object(run, 'run', side_effect=ValueError('admitted source check')) as command:
                                    reason = 'admitted source check' if deadline == expected else ('stage budget drift' if key == 'stage_deadline' else 'resource window drift')
                                    with self.assertRaisesRegex(ValueError, reason):
                                        tail.execute(run, common.digest(root/'plan.json'))
                                    self.assertEqual(command.call_count, int(deadline == expected))
                                self.assertFalse((root.parent/'invocation.json').exists())
                                receipt = dict(kind='frozen-plan', started=started, field=key, expected=expected,
                                               deadline=deadline, admitted=deadline == expected, external_commands=0, invocations=0)
                                print('DEADLINE_BOUNDARY '+json.dumps(receipt), flush=True)
                    (root.parent/'prepare-once.json').unlink()
                    (root/'plan.json').unlink()

    def test_cleanup_creation_json_and_default_budget(self):
        fixture = json.loads((common.REPO/'scripts/fixtures/eks-cleanup-m26r5.json').read_text())
        for started in STARTS:
            for seconds in (180, 300):
                with self.subTest(started=started, seconds=seconds), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    original = root/'original-owned.json'
                    original.write_text(json.dumps(fixture['owned']))
                    owned = [e for e in fixture['owned'] if e['kind'] in ('Namespace', 'ResourceQuota')]
                    stopped = [e for e in fixture['owned'] if e not in owned]
                    frozen = dict(target=fixture['target'], owner=fixture['namespace'], namespace=fixture['namespace'],
                                  seconds=seconds, inputs={str(original):common.digest(original)}, owned=owned, stopped=stopped,
                                  original_owned=str(original), implementation='synthetic', defaults=fixture['defaults'])
                    (root/'plan.json').write_text(json.dumps(frozen))
                    (root/'plan.json').chmod(0o400)
                    def command(run, argv):
                        return 'synthetic' if argv[:2] == ['git', 'rev-parse'] else ''
                    with patch.object(cleanup.time, 'monotonic', return_value=started), \
                            patch.object(common.Run, 'run', command), \
                            patch.object(common.Run, 'selected_object', return_value=None) as lookup:
                        result = cleanup.execute(root, common.digest(root/'plan.json'))
                        invocation = json.loads((root/'invocation.json').read_text())
                        self.assertTrue(result['confirmed'])
                        self.assertEqual(invocation['monotonic_start'], started)
                        self.assertEqual(invocation['deadline'], started+seconds)
                        self.assertEqual(result['deadline'], invocation['deadline'])
                        with self.assertRaisesRegex(ValueError, 'already invoked'):
                            cleanup.execute(root, common.digest(root/'plan.json'))
                        lookup.assert_called_once()
                        run = common.Run(root, fixture['target'])
                        run.cleanup()
                        self.assertEqual(run.deadline, started+180)
                    receipt = dict(kind='cleanup', started=started, seconds=seconds, deadline=invocation['deadline'],
                                   default_deadline=run.deadline, invocations=1, external_commands=0, synthetic_absent=True)
                    print('DEADLINE_BOUNDARY '+json.dumps(receipt), flush=True)


if __name__ == '__main__':
    unittest.main()
