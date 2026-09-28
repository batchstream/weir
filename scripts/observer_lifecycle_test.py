"""Real local pipes only; no Docker, Kubernetes or external network."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import capacity_fixture as fixture

from capacity_fixture import Observer


class ObserverLifecycle(unittest.TestCase):
    def launch(self, root, code):
        options = dict(root=root, role='weir', command=[sys.executable, '-c', code])
        return Observer(options)

    def finish(self, observer):
        deadline = time.monotonic()+5
        while observer.child.poll() is None:
            self.assertLess(time.monotonic(), deadline)
            observer.poll()
            time.sleep(.005)
        observer.poll()

    def test_exit_buffer_eof_and_distinct_stop_time(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root, 'import sys; print("{}",flush=True); sys.stderr.write("tail"); sys.exit(0)')
            self.finish(observer)
            before = json.loads(Path(root, 'weir-exec.json').read_text())
            time.sleep(.03)
            observer.stop()
            after = json.loads(Path(root, 'weir-exec.json').read_text())
            self.assertEqual(observer.entries, [{}])
            self.assertEqual(bytes(observer.streams[1]), b'tail')
            self.assertEqual(after['eof'], [True, True])
            self.assertEqual(before['first_exit_observed_monotonic'], after['first_exit_observed_monotonic'])
            self.assertGreater(after['stopped_monotonic'], after['first_exit_observed_monotonic'])
            self.assertEqual(after['exit'], 0)

    def test_sample_error_exit_zero_cannot_be_erased_by_stop(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root, 'print(\'{"errors":["failed"]}\',flush=True)')
            try:
                with self.assertRaisesRegex(RuntimeError, 'sample error'):
                    self.finish(observer)
            finally:
                with self.assertRaisesRegex(RuntimeError, 'sample error'):
                    observer.stop()
            saved = json.loads(Path(root, 'weir-exec.json').read_text())
            self.assertIn('sample error', saved['failure'])
            self.assertIsNotNone(observer.child.returncode)

    def test_nonzero_partial_and_invalid_json_remain_failed(self):
        cases = [('import sys; print("{}"); sys.exit(7)', 'exited'),
                 ('import sys; sys.stdout.write("{")', 'truncated'),
                 ('print("invalid")', 'JSON')]
        for code, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as root:
                observer = self.launch(root, code)
                try:
                    self.finish(observer)
                except (ValueError, RuntimeError):
                    pass
                with self.assertRaises((ValueError, RuntimeError)):
                    observer.stop()
                saved = json.loads(Path(root, 'weir-exec.json').read_text())
                self.assertTrue(saved['failure'])
                self.assertIsNotNone(observer.child.returncode)

    def test_slow_consumer_simultaneous_high_output_and_cancel_tail(self):
        code = ('import sys,json; '
                '[(sys.stdout.write(json.dumps({"padding":"x"*48000})+"\\n"),sys.stdout.flush(),'
                'sys.stderr.write("e"*48000),sys.stderr.flush()) for _ in range(20)]; '
                'sys.stdin.read();print("{\\"cancelled\\":true}",flush=True)')
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root, code)
            time.sleep(.05)
            observer.stop()
            self.assertEqual(len(observer.entries), 21)
            self.assertTrue(observer.entries[-1]['cancelled'])
            self.assertEqual(len(observer.streams[1]), 20*48000)
            self.assertEqual(observer.eof, [True, True])
            self.assertEqual(observer.child.returncode, 0)

    def test_output_bound_keeps_failure_and_waits(self):
        code = 'import os; [(os.write(2,b"x"*65536)) for _ in range(1100)]'
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root, code)
            try:
                with self.assertRaisesRegex(RuntimeError, 'output bound'):
                    self.finish(observer)
            finally:
                with self.assertRaisesRegex(RuntimeError, 'output bound'):
                    observer.stop()
            self.assertLessEqual(sum(map(len, observer.streams)), 64 << 20)
            self.assertIsNotNone(observer.child.returncode)


class ObserverFinalization(unittest.TestCase):
    def setUp(self):
        self.observers = []

    def launch(self, root, code='import sys; print("{}",flush=True); sys.stdin.read()'):
        observer = Observer.__new__(Observer)
        self.observers.append(observer)
        options = dict(root=root, role='weir', command=[sys.executable, '-c', code])
        observer.__init__(options)
        return observer

    def tearDown(self):
        # Inspect the real owner independently of the intentionally broken
        # evidence file. Fallback cleanup also makes a failing test safe.
        for observer in self.observers:
            child = observer.child
            reaped = False
            try:
                os.waitpid(child.pid, os.WNOHANG)
            except ChildProcessError:
                reaped = True
            record = dict(test=self.id(), pid=child.pid, exit=child.returncode,
                          joined=observer.joined, stopped=observer.stopped,
                          pipes_closed=[p.closed for p in (child.stdin, child.stdout, child.stderr)],
                          reaped_before_test_cleanup=reaped)
            fixture.stop_group(child)
            for pipe in (child.stdin, child.stdout, child.stderr):
                pipe.close()
            record['fallback_waited'] = child.returncode is not None
            print('OWNER '+json.dumps(record), flush=True)

    def assert_closed(self, observer):
        self.assertTrue(observer.joined)
        self.assertIsNotNone(observer.stopped)
        self.assertTrue(all(p.closed for p in (observer.child.stdin, *observer.pipes)))
        with self.assertRaises(ChildProcessError):
            os.waitpid(observer.child.pid, os.WNOHANG)

    def test_record_and_data_files_fail_independently_without_leaks(self):
        for name in ('weir-exec.json', 'weir.jsonl'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                observer = self.launch(root)
                target = Path(root, name)
                if target.exists():
                    target.unlink()
                target.mkdir()
                time.sleep(.03)
                with self.assertRaises(IsADirectoryError) as caught:
                    observer.stop()
                self.assert_closed(observer)
                with patch.object(observer, 'record') as record, patch.object(observer, 'poll') as poll:
                    with self.assertRaises(IsADirectoryError) as repeated:
                        observer.stop()
                self.assertIs(repeated.exception, caught.exception)
                record.assert_not_called()
                poll.assert_not_called()

    def test_stdin_and_output_close_errors_still_release_every_pipe(self):
        for name in ('stdin', 'stdout', 'stderr'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                observer = self.launch(root)
                pipe = getattr(observer.child, name)
                with patch.object(pipe, 'close', side_effect=OSError(name+' close failed')):
                    with self.assertRaisesRegex(OSError, name+' close failed'):
                        observer.stop()
                self.assert_closed(observer)
                self.assertEqual(observer.child.returncode, 0)

    def test_final_drain_failure_cannot_skip_close_or_final_record(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root)
            original = observer.poll
            def poll():
                if observer.joined:
                    raise OSError('final drain failed')
                return original()
            with patch.object(observer, 'poll', side_effect=poll):
                with self.assertRaisesRegex(OSError, 'final drain failed'):
                    observer.stop()
            self.assert_closed(observer)
            self.assertTrue(json.loads(Path(root, 'weir-exec.json').read_text())['joined'])

    def test_initialization_record_and_pipe_setup_failure_cleanup(self):
        for mode in ('record', 'nonblocking'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                if mode == 'record':
                    Path(root, 'weir-exec.json').mkdir()
                    with self.assertRaises(IsADirectoryError):
                        self.launch(root)
                else:
                    with patch.object(fixture.os, 'set_blocking', side_effect=OSError('pipe setup failed')):
                        with self.assertRaisesRegex(OSError, 'pipe setup failed'):
                            self.launch(root)
                self.assert_closed(self.observers[-1])

    def test_primary_nonzero_and_sample_errors_survive_record_and_close_errors(self):
        for code, expected in [('import sys;print("{}");sys.exit(7)', 'exited'),
                               ('print(\'{"errors":["sample failed"]}\')', 'sample error')]:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as root:
                observer = self.launch(root, code)
                observer.child.wait(timeout=3)
                target = Path(root, 'weir-exec.json');target.unlink();target.mkdir()
                with self.assertRaisesRegex(RuntimeError, expected) as polled:
                    observer.poll()
                self.assertTrue(polled.exception.__notes__)
                with patch.object(observer.child.stdout, 'close', side_effect=OSError('close failed')):
                    with self.assertRaisesRegex(RuntimeError, expected) as stopped:
                        observer.stop()
                self.assertIn('close failed', str(stopped.exception.__notes__))
                self.assert_closed(observer)

    def test_cancel_and_system_exit_during_stop_keep_first_error(self):
        for error in (KeyboardInterrupt('cancelled'), SystemExit('exit requested')):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as root:
                observer = self.launch(root)
                with patch.object(observer.child.stdin, 'close', side_effect=error), patch.object(observer.child.stderr, 'close', side_effect=OSError('close failed')):
                    with self.assertRaises(type(error)) as caught:
                        observer.stop()
                self.assertIs(caught.exception, error)
                self.assertIn('close failed', str(error.__notes__))
                self.assert_closed(observer)

    def test_group_stop_failure_still_waits_and_releases_pipes(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root)
            with patch.object(fixture, 'stop_group', side_effect=OSError('group signal failed')):
                with self.assertRaisesRegex(OSError, 'group signal failed'):
                    observer.stop()
            self.assert_closed(observer)

    def test_failed_kill_cannot_skip_leader_wait(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root, 'import sys,time;sys.stdin.read();time.sleep(.05)')
            with patch.object(fixture, 'stop_group', side_effect=OSError('group failed')), patch.object(observer.child, 'kill', side_effect=OSError('kill failed')):
                with self.assertRaises(RuntimeError) as caught:
                    observer.stop(deadline=time.monotonic()+4)
            self.assertIn('kill failed', str(caught.exception.__notes__))
            self.assert_closed(observer)

    def test_failed_wait_is_never_marked_joined_or_replayed(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root)
            with patch.object(observer.child, 'wait', side_effect=OSError('wait failed')):
                with self.assertRaisesRegex(OSError, 'wait failed'):
                    observer.stop()
            self.assertFalse(observer.joined)
            self.assertIsNone(observer.stopped)
            self.assertTrue(all(p.closed for p in (observer.child.stdin, *observer.pipes)))
            with patch.object(observer, 'poll') as poll, patch.object(observer, 'record') as record:
                with self.assertRaisesRegex(OSError, 'wait failed'):
                    observer.stop()
            poll.assert_not_called();record.assert_not_called()
            # Explicit external recovery, not a false successful Observer Wait.
            observer.child.wait(timeout=3)

    def test_successful_stop_and_poll_are_read_only_when_repeated(self):
        with tempfile.TemporaryDirectory() as root:
            observer = self.launch(root)
            observer.stop()
            before = Path(root, 'weir-exec.json').read_bytes()
            with patch.object(observer, 'record') as record:
                observer.stop()
                self.assertEqual(observer.poll(), [{}])
                with self.assertRaisesRegex(RuntimeError, 'already stopped'):
                    observer.write_input(b'no replay', time.monotonic()+1)
            record.assert_not_called()
            self.assertEqual(Path(root, 'weir-exec.json').read_bytes(), before)
            self.assert_closed(observer)


if __name__ == '__main__':
    unittest.main()
