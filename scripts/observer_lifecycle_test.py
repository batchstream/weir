"""Real local pipes only; no Docker, Kubernetes or external network."""
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

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


if __name__ == '__main__':
    unittest.main()
