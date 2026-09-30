"""Shared completion and capacity consumer tests with real pipes and synthetic data."""
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

from capacity_fixture import Observer, stop_group
from capacity_contract import PLAN
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
        for who in ('process','observer'):
            sample[who]['status'] += 'Cpus_allowed_list: '+PLAN['resources'][role]['cpuset']+'\n'
        sample['files']['status'] = sample['process']['status']
    first = records[0]
    first.update(target=samples[0]['process']['identity'], observer=samples[0]['observer']['identity'],
                 exe_sha256=samples[0]['process']['identity']['exe_sha256'])
    terminal = dict(type='observer_end', role=role, samples=count, ended_at=samples[-1]['end'])
    native = (source/'native-identity.txt').read_text()
    (root/'native-identity.txt').write_text(native)
    profile = dict(role=role, samples=count, seconds=(count-1)*2, native=native.splitlines(),
                   hashes=dict(weir=streams['weir'][0]['exe_sha256'],
                               client=streams['weir'][0]['observer']['exe_sha256']))
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
        # Synthetic caller only: receive/validate 4s + EOF/drain 4s + Stop/Wait 4s.
        # The 5s override is retained solely for the original-window counterexample.
        deadline = time.monotonic()+case.get('budget_seconds', 12)
        options = dict(root=root, role=role, command=command or stream_command(root, records, mode=mode))
        owner = Observer(options)
        self.owners.append(owner)
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
                if kind == 'observation':
                    _, complete = observation_samples(owner, profile)
                    if complete:
                        finish_observation(owner, profile, deadline)
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
                    abort_observation(owner, deadline)
            except BaseException as closing:
                if error is None:
                    raise
                error.add_note('consumer cleanup: '+str(closing))
        return owner

    @unittest.skipUnless(__debug__, 'legacy calibration explicitly rejects -O')
    def test_calibration_frozen_budget_preserves_old_window_counterexample(self):
        for budget in (5, 12):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                records, profile = stream_fixture(root, 'weir', 1350)
                command = stream_command(root, records)
                # Only producer startup is delayed; synthetic sample/terminal clocks
                # remain a coherent fixed 2698s profile, not 2698s of real sampling.
                command[2] = 'import time;time.sleep(1.2);'+command[2]
                started = time.monotonic()
                timeline = dict(budget_seconds=budget, producer_start_delay=1.2, started=started,
                                sample_clock='synthetic 2s intervals, fixed terminal', stops=[])
                original_poll, original_stop = Observer.poll, Observer.stop
                def poll(owner):
                    entries = original_poll(owner)
                    now = time.monotonic()
                    if entries:
                        timeline.setdefault('first_record', now)
                    if entries and entries[-1].get('type') == 'observer_end':
                        timeline.setdefault('terminal_received', now)
                    if all(owner.eof):
                        timeline.setdefault('both_eof', now)
                    return entries
                def stop(owner, deadline=None):
                    now = time.monotonic()
                    detail = dict(entered=now, deadline=deadline, stdin_closed=owner.child.stdin.closed,
                                  drain_deadline=min(now+4, deadline-4), global_remaining=deadline-now)
                    timeline['stops'].append(detail)
                    try:
                        return original_stop(owner, deadline)
                    finally:
                        detail.update(returned=time.monotonic(), exit=owner.child.returncode,
                                      joined=owner.joined, stopped=owner.stopped)
                case = dict(records=records, profile=profile, command=command, budget_seconds=budget)
                with patch.object(Observer, 'poll', poll), patch.object(Observer, 'stop', stop):
                    if budget == 5:
                        with self.assertRaisesRegex(RuntimeError, 'did not stop/drain') as caught:
                            self.consumer(root, 'calibration', case)
                        timeline['error'] = str(caught.exception)
                    else:
                        self.consumer(root, 'calibration', case)
                owner = self.owners[-1]
                receipt = json.loads((root/'weir-completion.json').read_text())
                timeline.update(finished=time.monotonic(), receipt=receipt, samples=len(owner.entries)-2,
                                exit=owner.child.returncode, eof=owner.eof, joined=owner.joined,
                                pipes_closed=all(p.closed for p in (owner.child.stdin, *owner.pipes)))
                print('COMPLETION_BUDGET=' + json.dumps(timeline, sort_keys=True), flush=True)
                evidence = os.environ.get('WEIR_COMPLETION_EVIDENCE')
                if evidence:
                    target = Path(evidence)/('budget-'+str(budget))
                    target.mkdir()
                    (target/'timeline.json').write_text(json.dumps(timeline, indent=2)+'\n')
                    for name in ('weir.jsonl','weir.err','weir-exec.json','weir-completion.json','input.jsonl'):
                        (target/name).write_bytes((root/name).read_bytes())
                self.assertEqual(timeline['samples'], 1350)
                self.assertTrue(owner.joined and all(owner.eof) and timeline['pipes_closed'])
                self.assertLess(timeline['finished'], started+budget)
                first = timeline['stops'][0]
                self.assertTrue(first['stdin_closed'])
                self.assertGreater(first['global_remaining'], 0)
                self.assertLessEqual(timeline['terminal_received'], receipt['validated_monotonic'])
                self.assertLessEqual(receipt['validated_monotonic'], receipt['eof_sent_monotonic'])
                if budget == 5:
                    self.assertLess(first['drain_deadline'], first['entered'])
                    self.assertEqual(receipt['outcome'], 'failed')
                else:
                    self.assertGreater(first['drain_deadline'], first['entered'])
                    self.assertLessEqual(receipt['validated_monotonic'], started+4)
                    self.assertLessEqual(timeline['both_eof'], receipt['waited_monotonic'])
                    self.assertEqual(receipt['outcome'], 'complete')
                    self.assertEqual(owner.child.returncode, 0)

    def test_frozen_total_deadline_still_aborts_and_waits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records, profile = stream_fixture(root, 'weir', 71)
            command = [sys.executable, '-c', 'import time;time.sleep(15)']
            case = dict(records=records, profile=profile, command=command)
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, 'test consumer deadline'):
                self.consumer(root, 'observation', case)
            elapsed = time.monotonic()-started
            owner = self.owners[-1]
            self.assertGreaterEqual(elapsed, 12)
            self.assertLess(elapsed, 13)
            self.assertNotEqual(owner.child.returncode, 0)
            self.assertFalse((root/'weir-completion.json').exists())
            print('COMPLETION_DEADLINE=' + json.dumps(dict(budget=12, elapsed=elapsed, exit=owner.child.returncode,
                                                          joined=owner.joined, eof=owner.eof)), flush=True)

    def test_completion_and_calibration_reject_malformed_streams(self):
        kinds = ['observation']+(['calibration'] if __debug__ else [])
        for kind in kinds:
            for fault in ('complete','role','count','sequence','identity','errors','end','extra','fragment','truncated','late-extra','late-fragment','nonzero','exit-before-ack'):
                with self.subTest(kind=kind,fault=fault), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    count = {'observation':71,'calibration':1350}[kind]
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
        kinds = ['observation']+(['calibration'] if __debug__ else [])
        for kind in kinds:
            for fault in ('weir-exec.json','weir-completion.json','cancel'):
                with self.subTest(kind=kind,fault=fault), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    count = {'observation':71,'calibration':1350}[kind]
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
        for kind in ['observation']+(['calibration'] if __debug__ else []):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                count = {'observation':71,'calibration':1350}[kind]
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
        for kind in ['observation']+(['calibration'] if __debug__ else []):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                count = {'observation':71,'calibration':1350}[kind]
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
