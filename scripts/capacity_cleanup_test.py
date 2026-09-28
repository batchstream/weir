"""Controlled local children and fake owned Docker metadata, never a daemon."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from capacity_fixture import Fixture, STREAM_LIMIT, OUTPUT_LIMIT, EVIDENCE_LIMIT, group_states


@unittest.skipUnless(__debug__, "fixture requires ordinary Python")
class BoundedChildren(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.env=patch.dict(os.environ,WEIR_CAPACITY_INTEGRATION="1");self.env.start();self.addCleanup(self.env.stop)
        self.f=Fixture(Path(self.temp.name)/"new","weir-m22-local-test")

    def run_child(self, program, **kwargs):
        return self.f.run([sys.executable,"-c",program],**kwargs)

    def test_fast_continuous_nonzero_and_stderr_overflow(self):
        programs=["import os; os.write(1,b'x'*(17<<20))",
                  "import os\nwhile True: os.write(1,b'x'*65536)",
                  "import os; os.write(2,b'x'*(17<<20)); raise SystemExit(3)",
                  "import os; os.write(1,b'x'*(7<<20)); os.write(2,b'x'*(9<<20))"]
        for code in programs:
            with self.subTest(code=code):
                with self.assertRaises(RuntimeError):self.run_child(code,timeout=4)
                record=json.loads((self.f.root/'commands.jsonl').read_text().splitlines()[-1])
                self.assertTrue(record["truncated"])
                self.assertLessEqual(sum(record["retained_bytes"]),OUTPUT_LIMIT)
                self.assertTrue(all(n<=STREAM_LIMIT for n in record["retained_bytes"]))
                self.assertIsNotNone(record["exit"])

    def test_normal_nonzero_timeout_and_descendant_pipe(self):
        self.assertEqual(self.run_child("print('done')").stdout,"done\n")
        self.assertEqual(self.run_child("raise SystemExit(7)",check=False).returncode,7)
        with self.assertRaises(TimeoutError):self.run_child("import time; time.sleep(10)",timeout=.05)
        code="import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); print(p.pid,flush=True)"
        with self.assertRaises(TimeoutError):self.run_child(code,timeout=.2)
        pid=int((self.f.root/f'command-{self.f.number:04d}.out').read_text().strip())
        result=subprocess.run(['ps','-o','state=','-p',str(pid)],capture_output=True,text=True,timeout=2)
        self.assertTrue(not result.stdout.strip() or result.stdout.strip().startswith('Z'))

    def test_signals_stop_wait_local_process_group(self):
        for signum in (signal.SIGINT,signal.SIGTERM):
            root=Path(self.temp.name)/str(signum)
            script="""import os,signal,sys
from pathlib import Path
from capacity_fixture import Fixture,FixtureInterrupted
f=Fixture(Path(sys.argv[1]),'weir-m22-signal')
def stop(s,f): raise FixtureInterrupted(str(s))
signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
try: f.run([sys.executable,'-c','import os,time; print(os.getpid(),flush=True); time.sleep(30)'],timeout=5)
except FixtureInterrupted: pass
"""
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).parent))
            process=subprocess.Popen([sys.executable,'-c',script,str(root)],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                until=time.monotonic()+3
                while not (root/'commands.jsonl').exists() and time.monotonic()<until:time.sleep(.01)
                time.sleep(.1);process.send_signal(signum)
                out,err=process.communicate(timeout=4)
                self.assertEqual(process.returncode,0,(out,err))
                record=json.loads((root/'commands.jsonl').read_text().splitlines()[-1])
                self.assertIsNotNone(record["exit"])
            finally:
                if process.poll() is None:process.kill()
                process.wait()

    def test_observation_failure_stops_active_command(self):
        called=[]
        def monitor():
            called.append(True)
            raise RuntimeError("synthetic invalid observer")
        options=dict(monitor=monitor)
        with self.assertRaisesRegex(RuntimeError,"invalid observer"):
            self.run_child("import time; time.sleep(10)",timeout=4,options=options)
        self.assertEqual(called,[True])
        record=json.loads((self.f.root/'commands.jsonl').read_text().splitlines()[-1])
        self.assertIsNotNone(record["exit"])

    def test_auxiliary_process_query_has_capture_bound(self):
        real = subprocess.Popen
        def noisy(*args,**kwargs):
            return real([sys.executable,"-c","import os; os.write(1,b'x'*1000000)"],**kwargs)
        with patch('capacity_fixture.subprocess.Popen',side_effect=noisy):
            with self.assertRaisesRegex(RuntimeError,"output bound"):
                group_states(99999999)

    def test_before_write_size_limit_and_cleanup_without_disk(self):
        with patch.object(self.f,'evidence_size',return_value=EVIDENCE_LIMIT):
            with self.assertRaises(RuntimeError):self.f.save('too-big','x')
            self.assertFalse((self.f.root/'too-big').exists())
            self.f.cleaning=True
            self.assertEqual(self.run_child("print('independent cleanup')").stdout,"independent cleanup\n")
            self.assertTrue(self.f.diagnostic_errors)

    def test_diagnostics_failure_does_not_skip_owned_removal(self):
        f=self.f
        f.attempted=[f.owner+'-es',f.owner+'-weir'];f.containers={n:'id-'+n for n in f.attempted}
        for failure in ('state','logs','all-save'):
            calls=[]
            def run(args,*a,**kw):
                calls.append(args)
                if failure=='logs' and args[1]=='logs':raise RuntimeError('injected logs failure')
                return subprocess.CompletedProcess(args,0,'{}' if args[1]=='inspect' else '','')
            def owned(name):
                return dict(Id=f.containers[name],State=dict(Running=False))
            real=f.save
            def save(name,value):
                if failure=='all-save' or name.endswith('-final-state.json'):raise OSError('synthetic ENOSPC')
                return real(name,value)
            with patch.object(f,'run',side_effect=run),patch.object(f,'owned',side_effect=owned),patch.object(f,'save',side_effect=save):
                self.assertFalse(f.cleanup())
            for identity in f.containers.values():self.assertIn(['docker','rm',identity],calls)

    def test_partial_create_uses_exact_owner_and_rejects_replacement(self):
        f=self.f;name=f.owner+'-client';f.attempted=[name]
        obj=dict(Id='exact',State=dict(Running=False))
        calls=[]
        def run(args,*a,**kw):
            calls.append(args);return subprocess.CompletedProcess(args,0,'{}' if args[1]=='inspect' else '','')
        with patch.object(f,'run',side_effect=run),patch.object(f,'owned',return_value=obj):
            self.assertTrue(f.cleanup())
        self.assertIn(['docker','rm','exact'],calls)
        calls.clear()
        with patch.object(f,'run',side_effect=run),patch.object(f,'owned',side_effect=RuntimeError('owner mismatch')):
            self.assertFalse(f.cleanup())
        self.assertFalse(any(c[1] in ('stop','rm') for c in calls))


class Optimized(unittest.TestCase):
    def test_fixture_rejects_before_creating_root(self):
        with tempfile.TemporaryDirectory() as base:
            root=Path(base)/'new'
            code="from capacity_fixture import Fixture; Fixture(__import__('sys').argv[1],'weir-m22-test')"
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).parent),WEIR_CAPACITY_INTEGRATION='1')
            r=subprocess.run([sys.executable,'-O','-c',code,str(root)],env=env,capture_output=True,timeout=5)
            self.assertNotEqual(r.returncode,0);self.assertFalse(root.exists())


if __name__=='__main__':unittest.main()
