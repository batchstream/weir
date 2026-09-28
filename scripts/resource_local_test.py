import copy
import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import resource_local as local

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

    def test_native_entry_cannot_reuse_window_after_preserved_failure(self):
        original=local.common.REPO
        with tempfile.TemporaryDirectory() as directory:
            repo=Path(directory).resolve();stage=repo/'.testdata/m28r2';stage.mkdir(parents=True)
            (repo/'scripts').mkdir()
            for name in ('eks_loopback_check.sh','eks_loopback_bootstrap.sh'):
                (repo/'scripts'/name).write_bytes((original/'scripts'/name).read_bytes())
            (repo/'deploy/kubernetes').mkdir(parents=True)
            (repo/'deploy/kubernetes/node.example.json').write_bytes((original/'deploy/kubernetes/node.example.json').read_bytes())
            (stage/'artifacts').mkdir();(stage/'artifacts/client').write_text('synthetic')
            budget=dict(evidence_root=str(stage),owner='weir-test',attempts=['fixture-1','fixture-2'],seconds=900,cleanup_seconds=120,planned=6000,document_mutations=2400)
            (stage/'stage-budget.json').write_text(json.dumps(budget))
            (stage/('source-inputs-'+'f'*40+'.json')).write_text('{}');(stage/'artifacts.json').write_text('{"source":"old-binary-source"}')
            root=stage/'fixture-1'
            argv=['resource_local.py','--evidence',str(root),'--owner','weir-test-fixture-1','--source','f'*40]
            environment=dict(WEIR_CAPACITY_INTEGRATION='1')
            failed=dict(passed=False,cleanup=True)
            with patch.object(local.common,'REPO',repo), patch.object(sys,'argv',argv), patch.dict(os.environ,environment), patch.object(local,'execute',return_value=failed) as execute:
                self.assertEqual(local.main(),1)
                plan=json.loads((root/'plan.json').read_text())
                self.assertEqual(plan['source'],'f'*40)
                self.assertEqual(plan['observer_samples'],71)
                self.assertEqual(plan['seconds'],900)
                self.assertEqual(plan['artifact_source'],'old-binary-source')
                root.rename(stage/'preserved-failure')
                with self.assertRaises(FileExistsError):local.main()
                self.assertEqual(execute.call_count,1)

    def test_shared_budget_paths_and_second_attempt_gate(self):
        for rejected in ('load','unclean','success','no-review','same-source','expired','changed-budget','alias','owner','third'):
            with self.subTest(rejected=rejected),tempfile.TemporaryDirectory() as directory:
                repo=Path(directory).resolve();stage=repo/'.testdata/m28r2';stage.mkdir(parents=True)
                budget=dict(evidence_root=str(stage),owner='weir-test',attempts=['fixture-1','fixture-2'],seconds=900,cleanup_seconds=120,planned=6000,document_mutations=2400)
                (stage/'stage-budget.json').write_text(json.dumps(budget))
                (stage/'artifacts').mkdir();(stage/'artifacts/client').write_text('synthetic')
                (stage/'artifacts.json').write_text('{}')
                for source in ('a'*40,'b'*40):(stage/('source-inputs-'+source+'.json')).write_text('{}')
                args=SimpleNamespace(evidence=stage/'fixture-1',owner='weir-test-fixture-1',source='a'*40)
                with patch.object(local.common,'REPO',repo):
                    root,_,deadline=local.stage_window(args);root.mkdir()
                    plan=dict(source=args.source)
                    (root/'plan.json').write_text(json.dumps(plan))
                    result=dict(passed=False,cleanup=True,load_started=False)
                    (stage/'retry-raw-replay.json').write_text('{"synthetic_test_only":true}')
                    review=dict(classification='fixture-text-wiring',previous_plan_sha256=local.common.digest(root/'plan.json'),new_source='b'*40,replay_sha256=local.common.digest(stage/'retry-raw-replay.json'))
                    (stage/'retry-review.json').write_text(json.dumps(review))
                    args=SimpleNamespace(evidence=stage/'fixture-2',owner='weir-test-fixture-2',source='b'*40)
                    if rejected=='load':(root/'load-started.json').write_text('{}')
                    if rejected=='unclean':result['cleanup']=False
                    if rejected=='success':result['passed']=True
                    if rejected=='no-review':(stage/'retry-review.json').write_text('{}')
                    if rejected=='same-source':args.source='a'*40
                    if rejected=='expired':
                        window=json.loads((stage/'native-window.json').read_text());window['deadline_monotonic']=time.monotonic()-1
                        (stage/'native-window.json').write_text(json.dumps(window))
                    if rejected=='changed-budget':(stage/'stage-budget.json').write_text(json.dumps(budget)+'\n')
                    if rejected=='alias':
                        (repo/'alias').symlink_to(stage,target_is_directory=True);args.evidence=repo/'alias/fixture-2'
                    if rejected=='owner':args.owner='foreign'
                    if rejected=='third':args.evidence=stage/'fixture-3'
                    (root/'result.json').write_text(json.dumps(result))
                    with self.assertRaises((ValueError,KeyError)):local.stage_window(args)
                    # With the same original window and reviewed zero-load failure,
                    # the second claim succeeds once, never extends the deadline.
                    (stage/'stage-budget.json').write_text(json.dumps(budget))
                    window=json.loads((stage/'native-window.json').read_text());window['deadline_monotonic']=deadline
                    (stage/'native-window.json').write_text(json.dumps(window))
                    (stage/'retry-review.json').write_text(json.dumps(review))
                    result=dict(passed=False,cleanup=True,load_started=False)
                    (root/'result.json').write_text(json.dumps(result))
                    if (root/'load-started.json').exists():continue
                    args=SimpleNamespace(evidence=stage/'fixture-2',owner='weir-test-fixture-2',source='b'*40)
                    self.assertEqual(local.stage_window(args)[2],deadline)
                    with self.assertRaises(FileExistsError):local.stage_window(args)

    def test_execute_failure_stops_execs_before_owned_cleanup(self):
        for foreign in (False,True):
            with self.subTest(foreign=foreign), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)/'fixture-1';root.mkdir()
                source=dict(source='f'*40,inputs={})
                (root.parent/'source-inputs.json').write_text(json.dumps(source))
                run=local.common.Run(root,None);owner='weir-m28r-wiring'
                options=dict(root=root,artifacts=root,owner=owner)
                plan=dict(source='f'*40,source_manifest=str(root.parent/'source-inputs.json'),inputs={},owner=owner,fixture_deadline_monotonic=time.monotonic()+20,commands=commands(options))
                calls=[];objects={};observers=[]
                def fake_command(argv,timeout=25,**kwargs):
                    calls.append(argv)
                    action=argv[1]
                    if argv[0]=='uname':return 'Darwin synthetic'
                    if action in ('ps','network'):return ''
                    if action=='image':return json.dumps(dict(Os='linux',Architecture='arm64'))
                    if action=='version':return '{}'
                    if action=='create':
                        role=argv[argv.index('--name')+1].removeprefix(owner+'-')
                        cid={'es':'a','client':'b','weir':'c'}[role]*64
                        cpus,mib,pids=local.LIMITS[role]
                        host=dict(NanoCpus=cpus*10**9,Memory=mib*1024**2,MemorySwap=mib*1024**2,PidsLimit=pids,
                                  CapDrop=['ALL'],CapAdd=[],SecurityOpt=['no-new-privileges=true'],Privileged=False,
                                  ReadonlyRootfs=True,PidMode='',IpcMode='private',CgroupnsMode='private',
                                  PublishAllPorts=False,PortBindings={},NetworkMode='none' if role=='es' else 'container:'+'a'*64)
                        mounts={str(root.parent/'artifacts'):'/qualification'}
                        if role=='es':mounts.update({str(root/name):'/'+name for name in ('local-check.sh','es-start.sh')})
                        if role=='weir':mounts[str(root/'node.json')]='/node.json'
                        objects[cid]=dict(Id=cid,Name='/'+owner+'-'+role,Config=dict(User='1000:0' if role=='es' else '65532:65532',Labels={local.LABEL:owner}),
                                          Image=local.loop.ES['config'] if role=='es' else local.ENVIRONMENT,HostConfig=host,Mounts=[dict(Source=s,Destination=d,Type='bind',RW=False) for s,d in mounts.items()],State=dict(Running=True,OOMKilled=False),RestartCount=0)
                        return cid
                    if action=='inspect':
                        actual=copy.deepcopy(objects[argv[2]])
                        if foreign and run.cleaning:actual['Config']['Labels'][local.LABEL]='foreign'
                        return json.dumps(actual)
                    if action=='exec':
                        if 'getconf CLK_TCK' in argv[-1]:return 'Linux synthetic aarch64\nuid=1000\n100\n'+'d'*64+' /own/java\n'
                        if argv[-1]=='http://127.0.0.1:9200/':return '{"version":{"number":"8.19.22"}}'
                        if '-X' in argv:return '{"acknowledged":true}'
                        return ''
                    if action=='logs':raise ValueError('injected log read failure')
                    if action=='stop':
                        self.assertTrue(all(o.child.poll() is not None for o in observers))
                        objects[argv[-1]]['State']['Running']=False
                    if action=='wait':return '0'
                    return ''
                def fake_observer(opts):
                    source='import sys; print("{\\\"type\\\":\\\"identity\\\",\\\"errors\\\":[\\\"injected sample failure\\\"]}",flush=True); sys.stdin.read()'
                    actual=dict(root=opts['root'],role=opts['role'],command=[sys.executable,'-c',source])
                    observer=Observer(actual);observers.append(observer)
                    return observer
                with patch.object(run,'run',side_effect=fake_command), patch.object(local,'Observer',side_effect=fake_observer):
                    result=local.execute(run,plan)
                self.assertFalse(result['passed']);self.assertTrue(result['cleanup_errors'])
                self.assertTrue(all(o.child.poll() is not None for o in observers))
                self.assertEqual(len(observers),2,result)
                self.assertTrue(any('observer' in error for error in result['errors']),result)
                actions=[a[1] for a in calls]
                if foreign:
                    self.assertNotIn('stop',actions);self.assertNotIn('rm',actions)
                else:
                    self.assertEqual(actions.count('rm'),3)
                    for cid in objects:
                        selected=[a[1] for a in calls if a[-1]==cid and a[1] in ('stop','wait','rm')]
                        self.assertEqual(selected,['stop','wait','rm'])
                self.assertLess(result['cleanup_seconds'],10)


if __name__=='__main__':unittest.main()
