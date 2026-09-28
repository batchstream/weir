import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from capacity_fixture import Observer
from resource_local import commands, inventory_check


class LocalResourceFixture(unittest.TestCase):
    def test_frozen_isolation_commands(self):
        options=dict(root=Path('/fixture'),artifacts=Path('/artifacts'),owner='weir-m28-test')
        result=commands(options)
        self.assertEqual(set(result),{'weir','es','client'})
        for role,command in result.items():
            self.assertIn('--read-only',command);self.assertIn('--cap-drop=ALL',command)
            self.assertNotIn('--privileged',command);self.assertNotIn('--pid',command)
            self.assertEqual(command[command.index('--network')+1],'none' if role=='es' else 'container:{es}')
        self.assertIn('AWS_EC2_METADATA_DISABLED=true',result['es'])
        self.assertIn('weir-m28-test-es:127.0.0.1',result['es'])

    def test_inventory_keeps_nondefault_identity(self):
        before='{"Name":"bridge","ID":"old"}\n{"Name":"owned-by-other","ID":"stable"}\n'
        after=before.replace('"old"','"new"')
        self.assertEqual(len(inventory_check(before,after,'networks')),1)
        with self.assertRaises(ValueError):inventory_check(before,after.replace('"stable"','"changed"'),'networks')

    def test_real_pipe_cancel_and_wait(self):
        with tempfile.TemporaryDirectory() as root:
            source='import sys; print("{\\\"type\\\":\\\"identity\\\"}",flush=True); sys.stdin.read(); print("{\\\"type\\\":\\\"cancelled\\\"}",flush=True)'
            options=dict(root=root,role='weir',command=[sys.executable,'-c',source])
            observer=Observer(options)
            until=time.monotonic()+2
            while not observer.poll() and time.monotonic()<until:time.sleep(.01)
            observer.stop()
            self.assertEqual(observer.child.returncode,0)
            self.assertEqual(len(Path(root,'weir.jsonl').read_text().splitlines()),2)
            self.assertIsNotNone(json.loads(Path(root,'weir-exec.json').read_text())['exit'])


if __name__=='__main__':unittest.main()
