"""Actual three consumers and real process pipes; sample data is synthetic."""
import copy
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import eks_resource_preflight as pre
import resource_local as local
from capacity_fixture import Observer, stop_group
from capacity_contract import PLAN
from eks_loopback_test import tcp_table
from observer_completion import observation_samples, finish_observation, abort_observation
from resource_report_test import complete_fixture


def stream_fixture(root, role, count=6):
    source = root/'synthetic'
    source.mkdir(exist_ok=True)
    streams = complete_fixture(source)
    records = streams[role]
    samples = records[1:-1]
    if count > len(samples):
        first = copy.deepcopy(samples[0])
        start = datetime.datetime.fromisoformat(first['time'])
        samples = []
        for i in range(count):
            sample = copy.deepcopy(first)
            sample.update(sequence=i, monotonic_ns=first['monotonic_ns']+i*2_000_000_000,
                          end_monotonic_ns=first['end_monotonic_ns']+i*2_000_000_000,
                          time=(start+datetime.timedelta(seconds=i*2)).isoformat(),
                          end=(start+datetime.timedelta(seconds=i*2,microseconds=1)).isoformat())
            if role == 'es':
                db = json.loads(sample['db'])
                next(iter(db['nodes'].values()))['process']['timestamp'] = int((start.timestamp()+i*2)*1000)
                sample['db'] = json.dumps(db)
            samples.append(sample)
    else:
        samples = samples[:count]
    # Clock covers the profile, while serialization runs quickly in this test.
    # The external Go helper stamps terminal time now, so keep final end current.
    end = datetime.datetime.now(datetime.timezone.utc)
    first_time = datetime.datetime.fromisoformat(samples[0]['time'])
    shift = end-datetime.timedelta(seconds=(count-1)*2)-first_time
    for sample in samples:
        for key in ('time','end'):
            sample[key] = (datetime.datetime.fromisoformat(sample[key])+shift).isoformat()
        if role == 'es':
            db = json.loads(sample['db'])
            next(iter(db['nodes'].values()))['process']['timestamp'] = int(datetime.datetime.fromisoformat(sample['time']).timestamp()*1000)
            sample['db'] = json.dumps(db)
        sample['files']['net/tcp'] = tcp_table()
        for who in ('process','observer'):
            sample[who]['status'] += 'Cpus_allowed_list: '+PLAN['resources'][role]['cpuset']+'\n'
        sample['files']['status'] = sample['process']['status']
        sample['observer']['identity']['exe_sha256'] = pre.IMAGES['tool']['binary']
        if role == 'weir':
            sample['process']['identity']['exe_sha256'] = pre.IMAGES['version']['binary']
    first = records[0]
    first.update(target=samples[0]['process']['identity'], observer=samples[0]['observer']['identity'],
                 exe_sha256=samples[0]['process']['identity']['exe_sha256'])
    terminal = dict(type='observer_end', role=role, samples=count, ended_at=samples[-1]['end'])
    native = (source/'native-identity.txt').read_text()
    (root/'native-identity.txt').write_text(native)
    profile = dict(role=role, samples=count, seconds=(count-1)*2, native=native.splitlines(),
                   hashes=dict(weir=pre.IMAGES['version']['binary'], client=pre.IMAGES['tool']['binary']))
    return [first]+samples+[terminal], profile


def stream_command(root, records, *, mode='complete', seconds=0):
    path = root/'input.jsonl'
    raw = ''.join(json.dumps(e)+'\n' for e in records)
    if mode == 'truncated':
        raw = raw[:-3]
    if mode == 'fragment':
        raw += '{'
    path.write_text(raw)
    code = ('import os,sys,time,json; '
            'lines=open(sys.argv[1],"rb").readlines(); start=time.monotonic()\n'
            'for n,line in enumerate(lines):\n'
            ' if '+repr(seconds)+' and b"sequence" in line:\n'
            '  sequence=json.loads(line)["sequence"];time.sleep(max(0,start+sequence*'+repr(seconds)+'/5-time.monotonic()))\n'
            ' sys.stdout.buffer.write(line);sys.stdout.buffer.flush()\n'
            ' if n<8:sys.stderr.buffer.write(b"e"*48000);sys.stderr.buffer.flush()\n'
            'control=sys.stdin.buffer.read()\n')
    if mode == 'late-extra':
        code += 'sys.stdout.write("{}\\n");sys.stdout.flush()\n'
    if mode == 'late-fragment':
        code += 'sys.stdout.write("{");sys.stdout.flush()\n'
    if mode == 'nonzero':
        code += 'sys.exit(7)\n'
    if mode == 'exit-before-ack':
        code = code.replace('control=sys.stdin.buffer.read()', 'sys.exit(0)')
    return [sys.executable, '-c', code, str(path)]


class CompletionConsumers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            binary = Path(os.environ.get('WEIR_COMPLETION_TEST_BINARY', ''))
            if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
                raise RuntimeError('CI requires an absolute executable WEIR_COMPLETION_TEST_BINARY')

    def setUp(self):
        self.owners = []

    def tearDown(self):
        for owner in self.owners:
            child = owner.child
            joined = owner.joined
            closed = all(p.closed for p in (child.stdin, *owner.pipes))
            reaped = False
            try:
                os.waitpid(child.pid, os.WNOHANG)
            except ChildProcessError:
                reaped = True
            stop_group(child)
            for pipe in (child.stdin, *owner.pipes):
                pipe.close()
            print('OWNER '+json.dumps(dict(test=self.id(), pid=child.pid, exit=child.returncode,
                                          joined=joined, pipes_closed=closed, reaped=reaped)), flush=True)
            self.assertTrue(joined and closed and reaped)

    def consumer(self, root, kind, case):
        records, profile = case['records'], case['profile']
        mode, command = case.get('mode', 'complete'), case.get('command')
        role = profile['role']
        options = dict(root=root, role=role, command=command or stream_command(root, records, mode=mode))
        owner = Observer(options)
        self.owners.append(owner)
        if kind == 'eks':
            run = pre.Run(root, pre.loop.TARGET)
            def monitor():
                if owner.stop_requested is not None:
                    self.assertTrue(owner.joined and owner.stopped is not None and all(owner.eof))
            with patch.object(pre, 'Observer', return_value=owner), patch.object(run, 'exec_command', return_value=options['command']), patch.object(run, 'monitor', side_effect=monitor):
                run.observe(role)
        else:
            deadline = time.monotonic()+5
            f = SimpleNamespace(root=root, observations={}, observers={role:owner},
                                observation_profiles={role:profile}, deadline=deadline)
            def save(name, value):
                (root/name).write_text(value if isinstance(value, str) else json.dumps(value))
            f.save = save
            error = None
            if kind == 'calibration':
                spec = importlib.util.spec_from_file_location('completion_capacity', Path(__file__).with_name('test-capacity.py'))
                entry = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(entry)
            try:
                if kind == 'calibration':
                    while not owner.poll():
                        if time.monotonic() >= deadline:raise RuntimeError('test identity deadline')
                        time.sleep(.005)
                while owner.stopped is None:
                    if kind == 'local':
                        profiles = {role:profile}
                        local.monitor_observations(f.observers, profiles, deadline)
                    else:
                        entry.read_observer(f, role)
                    if time.monotonic() >= deadline:
                        raise RuntimeError('test consumer deadline')
                    time.sleep(.005)
            except BaseException as exc:
                error = exc
                raise
            finally:
                try:
                    if kind == 'calibration':
                        entry.stop_observers(f)
                    else:
                        abort_observation(owner, time.monotonic()+8)
                except BaseException as closing:
                    if error is None:
                        raise
                    error.add_note('consumer cleanup: '+str(closing))
        return owner

    def test_three_consumers_complete_and_reject_malformed_streams(self):
        kinds = ['eks','local']+(['calibration'] if __debug__ else [])
        for kind in kinds:
            for fault in ('complete','role','count','sequence','identity','errors','end','extra','fragment','truncated','late-extra','late-fragment','nonzero','exit-before-ack'):
                with self.subTest(kind=kind,fault=fault), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    count = {'eks':6,'local':71,'calibration':1350}[kind]
                    records, profile = stream_fixture(root, 'weir', count)
                    if fault == 'role':records[2]['role'] = 'es'
                    if fault == 'count':records.pop(-2)
                    if fault == 'sequence':records[2]['sequence'] = 0
                    if fault == 'identity':records[2]['process']['identity']['start_ticks'] += 1
                    if fault == 'errors':records[2]['errors'] = ['sample failed']
                    if fault == 'end':records[-1]['samples'] += 1
                    if fault == 'extra':records.append(copy.deepcopy(records[-1]))
                    case = dict(records=records, profile=profile, mode=fault)
                    if fault == 'complete':
                        owner = self.consumer(root, kind, case)
                        self.assertEqual(owner.child.returncode, 0)
                        receipt = json.loads((root/'weir-completion.json').read_text())
                        self.assertEqual(receipt['outcome'], 'complete')
                        self.assertLessEqual(receipt['validated_monotonic'], receipt['eof_sent_monotonic'])
                        self.assertLessEqual(receipt['eof_sent_monotonic'], receipt['waited_monotonic'])
                    else:
                        with self.assertRaises((ValueError, RuntimeError)):
                            self.consumer(root, kind, case)
                        receipt_path = root/'weir-completion.json'
                        if receipt_path.exists():
                            receipt = json.loads(receipt_path.read_text())
                            self.assertNotEqual(receipt['outcome'], 'complete')

    def test_all_consumers_record_io_failure_and_cancel_close_owners(self):
        kinds = ['eks','local']+(['calibration'] if __debug__ else [])
        for kind in kinds:
            for fault in ('weir-exec.json','weir-completion.json','cancel'):
                with self.subTest(kind=kind,fault=fault), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    count = {'eks':6,'local':71,'calibration':1350}[kind]
                    records, profile = stream_fixture(root, 'weir', count)
                    original = Observer.poll
                    injected = False
                    def poll(owner):
                        nonlocal injected
                        entries = original(owner)
                        if entries and not injected and owner.stop_requested is None:
                            injected = True
                            if fault == 'cancel':raise KeyboardInterrupt('injected cancel')
                            target = root/fault
                            if target.exists():target.unlink()
                            target.mkdir()
                        return entries
                    with patch.object(Observer, 'poll', poll):
                        with self.assertRaises((OSError, KeyboardInterrupt)):
                            case = dict(records=records, profile=profile)
                            self.consumer(root, kind, case)

    def test_external_final_go_completion_to_actual_consumers(self):
        binary = os.environ.get('WEIR_COMPLETION_TEST_BINARY')
        if not binary:
            self.skipTest('explicit locally compiled completion test binary')
        for kind in ['eks','local']+(['calibration'] if __debug__ else []):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                count = {'eks':6,'local':71,'calibration':1350}[kind]
                records, profile = stream_fixture(root, 'weir', count)
                path = root/'go-synthetic.jsonl'
                path.write_text(''.join(json.dumps(e)+'\n' for e in records[:-1]))
                command = [binary, '-test.run=^TestObservationCompletionChild$']
                env = dict(WEIR_COMPLETION_CHILD='external', WEIR_COMPLETION_STREAM=str(path))
                with patch.dict(os.environ, env):
                    case = dict(records=records, profile=profile, command=command)
                    owner = self.consumer(root, kind, case)
                self.assertEqual(owner.child.returncode, 0)
                receipt = json.loads((root/'weir-completion.json').read_text())
                record = dict(consumer=kind, samples=count, optimized=not __debug__,
                              source=os.environ.get('GITHUB_SHA'), platform=sys.platform,
                              machine=os.uname().machine, binary_sha256=hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
                              stdout_bytes=len(owner.streams[0]), stdout_sha256=hashlib.sha256(owner.streams[0]).hexdigest(),
                              receipt=receipt)
                print('COMPLETION_PIPE=' + json.dumps(record, sort_keys=True), flush=True)
                evidence = os.environ.get('WEIR_COMPLETION_EVIDENCE')
                if evidence:
                    target = Path(evidence)/('external-'+kind)
                    target.mkdir()
                    for name in ('weir.jsonl','weir.err','weir-exec.json','weir-completion.json','go-synthetic.jsonl'):
                        (target/name).write_bytes((root/name).read_bytes())

    def test_rejected_terminal_cannot_receive_normal_eof(self):
        binary = os.environ.get('WEIR_COMPLETION_TEST_BINARY')
        if not binary:
            self.skipTest('explicit locally compiled completion test binary')
        original = Observer.poll
        def collect(owner):
            until = time.monotonic()+3
            while True:
                entries = original(owner)
                if owner.stop_requested is not None or (entries and entries[-1].get('type') == 'observer_end'):
                    return entries
                if time.monotonic() >= until:raise TimeoutError('test terminal collection bound')
                time.sleep(.002)
        for kind in ['eks','local']+(['calibration'] if __debug__ else []):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                count = {'eks':6,'local':71,'calibration':1350}[kind]
                records, profile = stream_fixture(root, 'weir', count)
                records[0]['exe_sha256'] = 'd'*64
                records[0]['target']['exe_sha256'] = 'd'*64
                for record in records[1:-1]:record['process']['identity']['exe_sha256'] = 'd'*64
                path = root/'rejected.jsonl'
                path.write_text(''.join(json.dumps(e)+'\n' for e in records[:-1]))
                command = [binary, '-test.run=^TestObservationCompletionChild$']
                env = dict(WEIR_COMPLETION_CHILD='external', WEIR_COMPLETION_STREAM=str(path))
                with patch.dict(os.environ, env), patch.object(Observer, 'poll', collect):
                    with self.assertRaisesRegex(ValueError, '^target artifact drift$'):
                        case = dict(records=records, profile=profile, command=command)
                        self.consumer(root, kind, case)
                owner = self.owners[-1]
                self.assertEqual(owner.child.returncode, 1)
                self.assertFalse((root/'weir-completion.json').exists())
                self.assertIn(b'observer control byte received', owner.streams[1])

    @unittest.skipUnless(__debug__, 'legacy calibration explicitly rejects -O')
    def test_calibration_explicit_cancel_uses_nonzero_without_four_second_wait(self):
        binary = os.environ.get('WEIR_COMPLETION_TEST_BINARY')
        if not binary:
            self.skipTest('explicit locally compiled completion test binary')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records, profile = stream_fixture(root, 'weir', 1350)
            path = root/'partial.jsonl'
            path.write_text(''.join(json.dumps(e)+'\n' for e in records[:3]))
            env = dict(os.environ, WEIR_COMPLETION_CHILD='external-early', WEIR_COMPLETION_STREAM=str(path))
            options = dict(root=directory,role='weir',command=[binary,'-test.run=^TestObservationCompletionChild$'],env=env)
            owner = Observer(options)
            self.owners.append(owner)
            started = time.monotonic()
            until = started+2
            while not owner.poll() and time.monotonic() < until:time.sleep(.005)
            spec = importlib.util.spec_from_file_location('cancel_capacity', Path(__file__).with_name('test-capacity.py'))
            entry = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(entry)
            f = SimpleNamespace(root=root, observers=dict(weir=owner), observation_profiles=dict(weir=profile), deadline=started+8)
            def save(name, value):
                (root/name).write_text(value if isinstance(value,str) else json.dumps(value))
            f.save = save
            entry.stop_observers(f)
            self.assertEqual(f.observers, {})
            self.assertLess(time.monotonic()-started, 2)
            receipt = json.loads(Path(directory,'weir-cancellation.json').read_text())
            self.assertEqual(receipt['outcome'], 'cancelled-not-complete')
            self.assertEqual(receipt['exit'], 1)

    @unittest.skipUnless(__debug__, 'legacy calibration explicitly rejects -O')
    def test_calibration_bad_stream_still_aborts_every_owner(self):
        binary = os.environ.get('WEIR_COMPLETION_TEST_BINARY')
        if not binary:
            self.skipTest('explicit locally compiled completion test binary')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            f = SimpleNamespace(root=root, observers={}, observation_profiles={}, deadline=time.monotonic()+8)
            def save(name, value):
                (root/name).write_text(value if isinstance(value, str) else json.dumps(value))
            f.save = save
            for role in ('weir','es'):
                records, profile = stream_fixture(root, role, 1350)
                if role == 'weir':records[1]['process']['identity']['start_ticks'] += 1
                path = root/(role+'-partial.jsonl')
                path.write_text(''.join(json.dumps(e)+'\n' for e in records[:3]))
                env = dict(os.environ, WEIR_COMPLETION_CHILD='external-early', WEIR_COMPLETION_STREAM=str(path))
                options = dict(root=root, role=role, command=[binary,'-test.run=^TestObservationCompletionChild$'], env=env)
                owner = Observer(options)
                self.owners.append(owner)
                f.observers[role], f.observation_profiles[role] = owner, profile
                until = time.monotonic()+2
                while len(owner.poll()) < 3 and time.monotonic() < until:time.sleep(.005)
            spec = importlib.util.spec_from_file_location('cancel_capacity', Path(__file__).with_name('test-capacity.py'))
            entry = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(entry)
            with self.assertRaises(ValueError):entry.stop_observers(f)
            self.assertEqual(f.observers, {})
            self.assertTrue(all(owner.child.returncode == 1 for owner in self.owners))


if __name__ == '__main__':unittest.main()
