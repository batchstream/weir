"""Offline M30 transfer, actual API shapes and raw semantics; no EKS qualification."""
import copy
import gzip
import hashlib
import io
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

import eks_resource_preflight as pre
from capacity_artifact import application_binary
from capacity_fixture import Observer
from eks_loopback_admission_test import responses
from eks_loopback_test import live_pod, tcp_table
from resource_report_test import complete_fixture


def plan():
    helper=dict(size=5,sha256=hashlib.sha256(b'hello').hexdigest(),path='/unused')
    result=dict(owner='weir-qual-m30-offline',namespace='weir-qual-m30-offline',node=dict(name='node',uid='node-uid'),
                images=pre.IMAGES,helper=helper,target=pre.loop.TARGET,image_source=pre.SOURCE,budgets=pre.BUDGET,minimum=pre.loop.MINIMUM,source='synthetic')
    result['objects']=pre.objects(result)
    result['tool_inputs']={p:pre.common.digest(pre.common.REPO/p) for p in pre.FILES}
    return result


def completion_log():
    # Synthetic success receipts only. The original M30R2 bootstrap logs were
    # not recovered; combining these with its status is offline replay only.
    raw = ('{"bootstrap":"waiting-for-verified-helper"}\n'
           '{"artifact":"verified-before-management"}\n'
           'BOUNDARY\n'
           '{"management":"create-empty-records","reserved":1,"started":1}\n'
           '{"acknowledged":true,"shards_acknowledged":true,"index":"records"}\n'
           '{"management":"create-empty-records","completed":1,"document_mutations":0}\n')
    header = tcp_table().splitlines()[0]+'\n'
    boundary = '/usr/bin/curl\n/usr/bin/nc\n/usr/bin/timeout\n'
    boundary += '{"version":{"number":"8.19.22"}}\n{"nodes":{}}\nown-pod-ip=10.0.0.2\n'
    for table in ('tcp', 'tcp6', 'udp', 'udp6'):
        boundary += f'socket-table-begin table=/proc/net/{table} atomic=false max_bytes=65536 max_lines=256 read_seconds=2 process=unknown\n'
        boundary += header+f'\nsocket-table-end table=/proc/net/{table} bytes={len(header.encode())} read_status=1\n'
    boundary += 'loopback-check-complete\n'
    return raw.replace('BOUNDARY\n', boundary)


class Artifact(unittest.TestCase):
    def archive(self, names=('qualification',), kind=tarfile.REGTYPE, mode=0o555):
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w') as archive:
            for name in names:
                member=tarfile.TarInfo(name);member.size=5;member.mode=mode;member.type=kind
                if kind==tarfile.SYMTYPE:member.linkname='/outside'
                archive.addfile(member,io.BytesIO(b'hello'))
        raw=stream.getvalue();compressed=gzip.compress(raw)
        identity=dict(size=len(compressed),digest='sha256:'+hashlib.sha256(compressed).hexdigest(),
                      diff_id='sha256:'+hashlib.sha256(raw).hexdigest(),binary=hashlib.sha256(b'hello').hexdigest())
        return compressed,identity

    def test_exact_layer_and_unsafe_members(self):
        raw,identity=self.archive();self.assertEqual(application_binary(raw,identity),b'hello')
        for names,kind,mode in [(('../qualification',),tarfile.REGTYPE,0o555),(('/qualification',),tarfile.REGTYPE,0o555),
                                (('qualification','extra'),tarfile.REGTYPE,0o555),(('qualification',),tarfile.SYMTYPE,0o555),
                                (('qualification',),tarfile.REGTYPE,0o755)]:
            with self.subTest(names=names,kind=kind,mode=mode):
                raw,identity=self.archive(names,kind,mode)
                with self.assertRaises(ValueError):application_binary(raw,identity)

    def test_truncated_hash_and_diff_id(self):
        raw,identity=self.archive()
        with self.assertRaises(ValueError):application_binary(raw[:-1],identity)
        for key in ('digest','diff_id','binary'):
            wrong=dict(identity);wrong[key]='f'*64
            with self.assertRaises(ValueError):application_binary(raw,wrong)


class Admission(unittest.TestCase):
    def test_actual_response_shape_with_explicit_new_fields(self):
        p=plan();expected=p['objects']['job'];old=responses()['job_response']
        old['metadata'].update(expected['metadata'])
        # Preserve recorded controller/default fields while applying the exact
        # requested M30 delta. This is a derived offline response, not new raw.
        wanted=expected['spec']['template']['spec'];actual=old['spec']['template']['spec']
        actual.update(copy.deepcopy(wanted));old['spec']['template']['metadata']['labels'].update(expected['metadata']['labels'])
        pre.common.job_check(old,expected)
        pod=live_pod();pod['spec']=copy.deepcopy(actual);pod['spec'].update(priority=0,preemptionPolicy='PreemptLowerPriority')
        pod['metadata'].update(namespace=p['namespace'],labels=expected['metadata']['labels'])
        for key in ('containerStatuses','initContainerStatuses'):
            for state in pod['status'][key]:
                image=pre.loop.ES if state['name'] in ('elasticsearch','bootstrap') else pre.IMAGES['version' if state['name']=='weir' else 'tool']
                state['imageID']=image['reference']
        opts=dict(job=dict(name='loopback',uid='synthetic-job'),template=expected,pod_uid=pod['metadata']['uid'],images=pre.IMAGES)
        self.assertTrue(pre.loop.pod_check(pod,opts))
        self.assertEqual(wanted['volumes'][-1],dict(name='helper',emptyDir=dict(sizeLimit='64Mi')))
        mounts=[(c['name'],m.get('readOnly',False)) for c in wanted['containers']+wanted['initContainers'] for m in c['volumeMounts'] if m['name']=='helper']
        self.assertEqual(mounts,[('weir',True),('elasticsearch',True),('bootstrap',False)])
        self.assertEqual(pre.loop.IMAGES['version']['binary'],'e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41')
        for field in ('imageID','containerID'):
            # imageID drift is rejected; stable containerID is checked by Run.
            if field=='imageID':
                changed=copy.deepcopy(pod);changed['status']['containerStatuses'][0][field]='foreign'
                with self.assertRaises(ValueError):pre.loop.pod_check(changed,opts)
        changed=copy.deepcopy(pod);changed['metadata']['uid']='foreign'
        with self.assertRaises(ValueError):pre.loop.pod_check(changed,opts)
        changed=copy.deepcopy(pod);changed['spec']['initContainers'][0]['volumeMounts'][-1]['readOnly']=False
        with self.assertRaises(ValueError):pre.loop.pod_check(changed,opts)


    def test_original_m30_api_omits_default_false_mount(self):
        path=Path(__file__).with_name('fixtures')/'eks-resource-admitted-job-m30.json'
        actual=json.loads(path.read_text());p=plan()
        p.update(owner=actual['metadata']['labels'][pre.common.LABEL],namespace=actual['metadata']['namespace'])
        p['node']['name']=actual['spec']['template']['spec']['nodeName']
        expected=pre.objects(p)['job']
        pre.common.job_check(actual,expected)
        bootstrap=actual['spec']['template']['spec']['initContainers'][1]
        self.assertNotIn('readOnly',bootstrap['volumeMounts'][-1])
        original=copy.deepcopy(expected)
        original['spec']['template']['spec']['initContainers'][1]['volumeMounts'][-1]['readOnly']=False
        with self.assertRaisesRegex(ValueError,'volumeMounts'):pre.common.job_check(actual,original)
        for group,index in (('containers',0),('initContainers',0)):
            unsafe=copy.deepcopy(actual)
            unsafe['spec']['template']['spec'][group][index]['volumeMounts'][-1].pop('readOnly')
            with self.assertRaisesRegex(ValueError,'volumeMounts'):pre.common.job_check(unsafe,expected)

    def test_recorded_pod_defaults_and_bootstrap_running_then_completed(self):
        # Exact M30 Job spec plus recorded Pod admission defaults from M25.
        # Metadata and runtime states are synthetic; this is not native M30R evidence.
        recorded=json.loads((Path(__file__).with_name('fixtures')/'eks-resource-admitted-job-m30.json').read_text())
        p=plan();p.update(owner=recorded['metadata']['labels'][pre.common.LABEL],namespace=recorded['metadata']['namespace'])
        p['node']['name']=recorded['spec']['template']['spec']['nodeName']
        p['objects']=pre.objects(p)
        pod=live_pod();pod['spec']=copy.deepcopy(recorded['spec']['template']['spec'])
        admitted=responses()['pod_response']
        for key in ('priority','preemptionPolicy','serviceAccount','serviceAccountName','tolerations'):
            pod['spec'][key]=copy.deepcopy(admitted['spec'][key])
        pod['metadata'].update(namespace=p['namespace'],labels=p['objects']['job']['metadata']['labels'])
        for group in ('containerStatuses','initContainerStatuses'):
            for state in pod['status'][group]:
                image=pre.loop.ES if state['name'] in ('elasticsearch','bootstrap') else pre.IMAGES['version' if state['name']=='weir' else 'tool']
                state['imageID']=image['reference']
        admission=copy.deepcopy(pod)
        admission['metadata'].update(name='loopback-admission')
        admission['metadata'].pop('ownerReferences')
        with tempfile.TemporaryDirectory() as directory:
            run=pre.Run(Path(directory),pre.loop.TARGET);run.plan=p;run.template=p['objects']['job']
            with patch.object(run,'kube',return_value=json.dumps(admission)):
                run.pod_dry_run(run.template)
            run.namespace=dict(kind='Namespace',name=p['namespace'],uid='synthetic-ns',owner=p['owner'])
            run.job=dict(kind='Job',name='loopback',uid=recorded['metadata']['uid'],owner=p['owner'])
            run.pod_entry=None;run.pod_ready=False
            namespace=dict(metadata=dict(name=p['namespace'],uid='synthetic-ns',labels=run.template['metadata']['labels']))
            job=copy.deepcopy(recorded)
            pod['metadata']['ownerReferences'][0]['uid']=run.job['uid']
            lookup=dict(Namespace=namespace,Job=job,Pod=pod)
            bootstrap=pod['status']['initContainerStatuses'][1]
            completed=bootstrap['state'];bootstrap['state']=dict(running=dict(startedAt='synthetic-time'))
            with patch.object(run,'selected_object',side_effect=lambda kind,name:lookup[kind]),patch.object(run,'kube',return_value=pod['metadata']['name']),patch.object(run,'job_events'):
                identity=run.bootstrap();self.assertFalse(run.pod_ready)
                self.assertEqual(identity['pod_uid'],pod['metadata']['uid'])
                bootstrap['state']=completed
                self.assertEqual(run.current_pod(),pod);self.assertTrue(run.pod_ready)
                with self.assertRaisesRegex(ValueError,'not stable Running'):run.bootstrap()


class Transfer(unittest.TestCase):
    def test_real_bounded_pipe_and_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            data=b'x'*(2<<20)
            code='import sys,json; data=sys.stdin.buffer.read(); print(json.dumps({"bytes":len(data)}))'
            options=dict(root=directory,role='upload',command=[sys.executable,'-c',code])
            observer=Observer(options)
            try:
                self.assertEqual(observer.write_input(data,time.monotonic()+3),len(data))
                until=time.monotonic()+3
                while observer.child.poll() is None and time.monotonic()<until:observer.poll();time.sleep(.01)
                self.assertEqual(observer.poll(),[dict(bytes=len(data))])
            finally:observer.stop()
            self.assertIsNotNone(observer.child.returncode)

    def test_deadline_cancels_and_waits(self):
        with tempfile.TemporaryDirectory() as directory:
            code='import sys; sys.stdin.buffer.read()'
            options=dict(root=directory,role='upload',command=[sys.executable,'-c',code])
            observer=Observer(options)
            try:
                with self.assertRaisesRegex(RuntimeError,'deadline'):observer.write_input(b'hello',time.monotonic()-1)
            finally:observer.stop()
            self.assertIsNotNone(observer.child.returncode)

    def test_upload_uid_change_and_wrong_receipt_never_release(self):
        for failure in ('uid','hash','truncated','exit','deadline',None):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);helper=root/'qualification';helper.write_bytes(b'hello')
                run=pre.Run(root,pre.loop.TARGET);run.plan=plan();run.plan['helper']['path']=str(helper)
                run.pod_entry=dict(name='bound-pod');calls=[]
                receipt=dict(artifact='uploaded',size=5,sha256=run.plan['helper']['sha256'])
                if failure=='hash':receipt['sha256']='wrong'
                raw=json.dumps(receipt)+'\n'
                if failure=='truncated':raw=raw[:-3]
                code='import sys; sys.stdin.buffer.read(); sys.stdout.write('+repr(raw)+'); sys.exit('+('1' if failure=='exit' else '0')+')'
                def command(container,argv):
                    calls.append(argv)
                    return [sys.executable,'-c',code]
                def release(*args,**kwargs):
                    calls.append('released');return '{"artifact":"released"}\n'
                before=dict(pod_uid='owned',containerID='same')
                after=dict(before,pod_uid='foreign') if failure=='uid' else before
                with patch.object(run,'configuration'),patch.object(run,'bootstrap',side_effect=[before,after,before,before]),patch.object(run,'exec_command',side_effect=command),patch.object(run,'run',side_effect=release),patch.object(run,'current_pod'),patch.object(run,'kube',return_value=completion_log()):
                    run.pod_ready=True
                    until=time.monotonic()+(-1 if failure=='deadline' else 20)
                    if failure:
                        with self.assertRaises((ValueError,RuntimeError)):run.transfer(until)
                        self.assertNotIn('released',calls)
                    else:
                        run.transfer(until);self.assertIn('released',calls)
                child=json.loads((root/'upload-exec.json').read_text());self.assertIsNotNone(child['exit'])
                self.assertEqual(sum('/qualification/helper-upload.sh' in c for c in calls if isinstance(c,list)),1)

    def test_cancel_preserves_original_when_upload_exits_nonzero_after_eof(self):
        self.addCleanup(signal.signal, signal.SIGINT, signal.getsignal(signal.SIGINT))
        signal.signal(signal.SIGINT, signal.default_int_handler)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);helper=root/'qualification';helper.write_bytes(b'hello')
            run=pre.Run(root,pre.loop.TARGET);run.plan=plan();run.plan['helper']['path']=str(helper)
            run.pod_entry=dict(name='bound-pod')
            code=('import sys,os,signal; os.kill(os.getppid(),signal.SIGINT); '
                  'sys.stdin.buffer.read(); sys.exit(9)')
            command=[sys.executable,'-c',code]
            identity=dict(pod_uid='owned',containerID='same')
            with patch.object(run,'configuration'),patch.object(run,'bootstrap',return_value=identity),patch.object(run,'exec_command',return_value=command),patch.object(run,'run') as release:
                with self.assertRaises(KeyboardInterrupt):run.transfer(time.monotonic()+30)
            release.assert_not_called()
            saved=json.loads((root/'upload-exec.json').read_text())
            self.assertEqual(saved['exit'],9)
            self.assertIn('exited',saved['failure'])
            self.assertIsNotNone(saved['stopped_monotonic'])

    def test_shell_gate_rejects_truncated_and_corrupt_input(self):
        # Use actual bash/head/hash/FIFO/rename. Only GNU stat formatting and
        # mv -T are adapted to macOS syscall semantics in this local fixture.
        for data in (b'hello',b'hell',b'jello',b'hello!'):
            with self.subTest(data=data),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);bins=root/'bin';bins.mkdir();helper=root/'helper';helper.mkdir()
                os.mkfifo(helper/'release')
                stat=bins/'stat';stat.write_text('#!'+sys.executable+'\nimport os,sys\ns=os.stat(sys.argv[-1]);print(s.st_size if sys.argv[2]=="%s" else oct(s.st_mode & 0o777)[2:])\n');stat.chmod(0o700)
                mv=bins/'mv';mv.write_text('#!'+sys.executable+'\nimport os,sys\nos.rename(sys.argv[-2],sys.argv[-1])\n');mv.chmod(0o700)
                script=pre.transfer_scripts(plan()['helper'])['upload'].replace('/helper/',str(helper)+'/')
                env=dict(os.environ,PATH=str(bins)+os.pathsep+os.environ['PATH'])
                result=subprocess.run(['/bin/bash','--noprofile','--norc','-c',script],input=data,capture_output=True,env=env,timeout=3)
                self.assertEqual(result.returncode==0,data==b'hello')
                self.assertEqual((helper/'qualification').exists(),data==b'hello')
                self.assertTrue((helper/'release').exists())
                if data==b'hello':self.assertEqual((helper/'qualification').stat().st_mode & 0o777,0o555)


class ReleaseReplay(unittest.TestCase):
    """Real bounded CLI children, original reset/status, synthetic success log."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.fixture = json.loads((Path(__file__).with_name('fixtures')/'eks-resource-release-m30r2.json').read_text())
        self.run = pre.Run(self.root, pre.loop.TARGET)
        self.run.plan = copy.deepcopy(self.fixture['plan'])
        helper = self.root/'qualification';helper.write_bytes(b'hello')
        self.run.plan['helper'] = dict(path=str(helper),size=5,sha256=hashlib.sha256(b'hello').hexdigest())
        self.run.plan['objects'] = pre.objects(self.run.plan)
        self.run.template = self.run.plan['objects']['job']
        self.run.owned = copy.deepcopy(self.fixture['owned'])
        self.run.namespace = next(e for e in self.run.owned if e['kind']=='Namespace')
        self.run.job = next(e for e in self.run.owned if e['kind']=='Job')
        self.run.pod_entry = next(e for e in self.run.owned if e['kind']=='Pod')
        self.run.pod_ready = False
        self.run.runtime_evidence = False
        self.phase = 'running_pod'
        self.config = copy.deepcopy(self.run.plan['objects']['config'])
        self.config['metadata']['uid'] = next(e['uid'] for e in self.run.owned if e['kind']=='ConfigMap')
        ns = self.run.namespace
        self.namespace = dict(metadata=dict(name=ns['name'],uid=ns['uid'],labels={pre.common.LABEL:ns['owner']}))
        self.calls = []
        self.release_raw = '{"artifact":"released"}\n'
        self.release_code = 0
        self.release_script = None
        self.log = completion_log()
        self.mutate = None
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.addCleanup(signal.signal,sig,signal.getsignal(sig))
            def cancelled(signum, frame):
                raise KeyboardInterrupt('signal '+str(signum))
            signal.signal(sig, cancelled)

    def selected(self, kind, name):
        if kind == 'Namespace':return self.namespace
        if kind == 'Job':return self.fixture['job']
        if kind == 'ConfigMap':return self.config
        if kind == 'Pod':return self.fixture[self.phase]
        raise ValueError(kind)

    def command(self, container, argv):
        self.calls.append(argv[-1])
        if argv[-1].endswith('helper-upload.sh'):
            receipt = dict(artifact='uploaded',size=5,sha256=self.run.plan['helper']['sha256'])
            code = 'import sys; sys.stdin.buffer.read(); print('+repr(json.dumps(receipt))+')'
        else:
            self.assertTrue((self.root/'release.json').exists())
            self.phase = 'ready_pod'
            if self.mutate:self.mutate()
            code = self.release_script or ('import sys; sys.stdout.write('+repr(self.release_raw)+'); sys.stderr.write('+repr(self.fixture['release_stderr'] if self.release_code else '')+'); sys.exit('+str(self.release_code)+')')
        return [sys.executable,'-c',code]

    def transfer(self):
        with patch.object(self.run,'selected_object',side_effect=self.selected),patch.object(self.run,'job_events'),patch.object(self.run,'exec_command',side_effect=self.command),patch.object(self.run,'kube',return_value=self.log):
            self.run.transfer(time.monotonic()+30)

    def test_full_and_missing_reply_converge_on_same_completion(self):
        for mode in ('full','reset','empty'):
            with self.subTest(mode=mode):
                self.release_raw = '{"artifact":"released"}\n' if mode=='full' else ''
                self.release_code = int(mode=='reset')
                self.transfer()
                receipt = json.loads((self.root/'release.json').read_text())
                self.assertEqual(receipt['transport'],'failed' if mode=='reset' else 'exit0')
                self.assertIn('confirmed',receipt['remote_outcome'])
                if mode=='reset':self.assertEqual((self.root/'command-0001.err').read_text(),self.fixture['release_stderr'])
                self.assertEqual(self.calls.count('/qualification/helper-release.sh'),1)
                with self.assertRaisesRegex(ValueError,'already attempted'):self.transfer()
                self.assertEqual(len(self.calls),2)
                # Reset only this offline harness between independent scenarios.
                (self.root/'release.json').unlink();self.calls.clear();self.phase='running_pod';self.run.number=0

    def test_original_terminal_without_logs_stays_unknown(self):
        self.release_raw='';self.release_code=1;self.log=''
        with self.assertRaises(ValueError):self.transfer()
        receipt=json.loads((self.root/'release.json').read_text())
        self.assertEqual(receipt['remote_outcome'],'UNKNOWN')
        self.assertEqual(self.calls.count('/qualification/helper-release.sh'),1)

    def test_identity_terminal_configuration_and_restart_failures(self):
        baseline=copy.deepcopy(self.fixture)
        cases = ('pod','job','namespace','container','image','restart','lastState','failed','config','config-uid','mutable')
        for case in cases:
            with self.subTest(case=case):
                self.fixture=copy.deepcopy(baseline)
                self.phase='running_pod';self.calls=[]
                def mutate():
                    pod=self.fixture['ready_pod'];state=next(s for s in pod['status']['initContainerStatuses'] if s['name']=='bootstrap')
                    if case=='pod':pod['metadata']['uid']='foreign'
                    elif case=='job':self.fixture['job']['metadata']['uid']='foreign'
                    elif case=='namespace':self.namespace['metadata']['uid']='foreign'
                    elif case=='container':state['containerID']='containerd://foreign'
                    elif case=='image':state['imageID']='foreign'
                    elif case=='restart':state['restartCount']=1
                    elif case=='lastState':state['lastState']=dict(terminated=dict(exitCode=0))
                    elif case=='failed':state['state']['terminated']['exitCode']=1
                    elif case=='config':self.config['data']['helper-bootstrap.sh']='changed'
                    elif case=='config-uid':self.config['metadata']['uid']='foreign'
                    else:self.config['immutable']=False
                self.mutate=mutate
                with self.assertRaises(ValueError):self.transfer()
                self.assertEqual(json.loads((self.root/'release.json').read_text())['remote_outcome'],'UNKNOWN')
                self.assertEqual(self.calls.count('/qualification/helper-release.sh'),1)
                (self.root/'release.json').unlink()
                self.namespace['metadata']['uid']=self.run.namespace['uid']
                self.config=copy.deepcopy(self.run.plan['objects']['config'])
                self.config['metadata']['uid']=next(e['uid'] for e in self.run.owned if e['kind']=='ConfigMap')
                if hasattr(self.run,'identities'):del self.run.identities

    def test_missing_reordered_truncated_duplicate_and_contradictory_logs(self):
        raw=completion_log();lines=raw.splitlines(keepends=True)
        variants=['',''.join(lines[1:]),raw[:-1],raw[:50],raw+lines[-1],raw.replace('"completed":1','"completed":0'),
                  raw.replace(lines[1],''),raw.replace(lines[-3],''),raw.replace(lines[-2],''),
                  lines[1]+lines[0]+''.join(lines[2:]),raw.replace('read_status=1','read_status=0'),
                  raw.replace('socket-table-begin table=/proc/net/tcp ', 'socket-table-begin table=/proc/net/udp '),
                  raw.replace('local_address','missing'),raw+'x'*262144]
        for log in variants:
            with self.subTest(length=len(log)),self.assertRaises((ValueError,IndexError)):pre.bootstrap_receipt(log)
        self.log=raw.replace(lines[-2],'')
        with self.assertRaises(ValueError):self.transfer()
        self.assertEqual(json.loads((self.root/'release.json').read_text())['remote_outcome'],'UNKNOWN')

    def test_contradictory_reply_cancellation_and_local_timeout_stop(self):
        for mode in ('wrong','partial','SIGINT','SIGTERM','timeout','remote-deadline'):
            with self.subTest(mode=mode):
                self.calls=[];self.phase='running_pod';self.release_script=None
                self.release_raw='wrong\n' if mode=='wrong' else '{"artifact":' if mode=='partial' else ''
                if mode in ('SIGINT','SIGTERM'):
                    self.release_script='import os,signal,time; os.kill(os.getppid(),signal.'+mode+');time.sleep(30)'
                elif mode=='timeout':
                    self.release_script='import time;time.sleep(30)'
                    self.mutate=lambda:setattr(self.run,'deadline',time.monotonic()+.1)
                elif mode=='remote-deadline':
                    self.mutate=lambda:setattr(self.run,'deadline',time.monotonic()+.1)
                    self.release_script='import time;time.sleep(.2)'
                with self.assertRaises((ValueError,KeyboardInterrupt)):self.transfer()
                self.assertEqual(self.calls.count('/qualification/helper-release.sh'),1)
                self.assertEqual(json.loads((self.root/'release.json').read_text())['remote_outcome'],'UNKNOWN')
                records=[json.loads(p.read_text()) for p in self.root.glob('command-*.json')]
                self.assertTrue(all(r['exit'] is not None and 'end' in r for r in records))
                (self.root/'release.json').unlink();self.run.deadline=time.monotonic()+900;self.mutate=None


class Lifecycle(unittest.TestCase):
    def test_evidence_single_run_owner_path_and_create_only(self):
        with tempfile.TemporaryDirectory() as directory:
            repo=Path(directory).resolve()
            with patch.object(pre.common,'REPO',repo):
                root=repo/'.testdata'/'m30r2'/'native'
                pre.scope_check(root,'weir-qual-m30r2-offline')
                root.parent.mkdir(parents=True)
                root.mkdir(mode=0o700)
                with self.assertRaises(FileExistsError):root.mkdir(mode=0o700)
                for invalid in (repo/'other'/'m30r2'/'native',repo/'.testdata'/'..'/'native',repo/'.testdata'/'bad-name'/'native'):
                    with self.assertRaises(ValueError):pre.scope_check(invalid)
                for owner in ('weir-qual-m30-offline','weir-qual-m30r-offline','business'):
                    with self.assertRaises(ValueError):pre.scope_check(root,owner)
                alias=repo/'.testdata'/'alias';alias.symlink_to(root.parent,target_is_directory=True)
                with self.assertRaises(ValueError):pre.scope_check(alias/'native')

    def test_slow_static_checks_precede_one_frozen_resource_window(self):
        from eks_resources_test import node, pod
        for wait in (0, 121):
            with self.subTest(wait=wait), tempfile.TemporaryDirectory() as directory:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    self.addCleanup(signal.signal, sig, signal.getsignal(sig))
                root = Path(directory)/'m30r4'/'native';root.mkdir(parents=True)
                clock = [1000.0]
                events = []
                n = node();n.update(kernel='offline', kubelet='offline')
                n['allocatable']['ephemeral-storage'] = '20Gi'
                p = pod();p['containers'][0]['resources']['requests']['cpu'] = '0.5'
                for c in p['containers']:
                    c.update(allocatedResources=None, claims=False, resizePolicy=None)
                envelope = dict(kind='PodList', apiVersion='v1', itemsType='[]interface {}',
                                **{'continue':None}, remainingItemCount=None, items=[p])
                helper = plan()['helper']
                with patch.object(pre.time, 'monotonic', side_effect=lambda:clock[0]):
                    run = pre.Run(root, pre.loop.TARGET)
                    run.node_scope = dict(name='node', uid='node-uid')
                    def command(argv, timeout=25):
                        run.number += 1
                        dynamic = 'get' in argv and 'auth' not in argv and ('node' in argv or 'pods' in argv)
                        events.append(dict(argv=argv, at=clock[0], dynamic=dynamic,
                                           window=copy.deepcopy(run.resource_preflight), timeout=timeout))
                        if dynamic:
                            clock[0] += 2
                            return json.dumps(n if 'node' in argv else envelope)
                        if 'can-i' in argv or 'daemonset' in argv:
                            self.assertIsNone(run.resource_preflight)
                            clock[0] += 6  # 23 permissions plus CNI take 144 seconds.
                            return 'yes' if 'can-i' in argv else 'aws-node --enable-network-policy=false'
                        if argv[0] == 'aws':
                            cluster = dict(arn=pre.loop.TARGET['context'], status='ACTIVE', version='1.36')
                            return json.dumps(cluster)
                        if argv[:3] == ['kubectl', 'config', 'current-context']:
                            return pre.loop.TARGET['context']
                        return 'synthetic' if argv[:2] == ['git', 'rev-parse'] else ''
                    with patch.object(pre, 'scope_check'), patch.object(pre, 'verified_helper', return_value=helper), patch.object(run, 'run', side_effect=command):
                        pre.prepare(run, 'weir-qual-m30r4-offline')
                        frozen = json.loads((root/'plan.json').read_text())
                        state = copy.deepcopy(frozen['resource_preflight'])
                        self.assertEqual(state['started'], 1144)
                        self.assertEqual(state['deadline'], 1264)
                        self.assertEqual(sum('can-i' in e['argv'] for e in events), 23)
                        self.assertTrue(all(e['window'] is None for e in events if not e['dynamic']))
                        self.assertEqual([e['at'] for e in events if e['dynamic']], [1144, 1146])
                        clock[0] += wait
                        # Execute the real prewrite checks; stop at the first write
                        # seam. This is an offline process log, not EKS evidence.
                        cleanup_result = dict(confirmed=False, resources=[])
                        with patch.object(pre.loop, 'namespace_start', side_effect=ValueError('offline first write boundary')) as start, patch.object(run, 'cleanup', return_value=cleanup_result):
                            self.assertEqual(pre.execute(run, pre.common.digest(root/'plan.json')), 1)
                        self.assertEqual(run.resource_preflight, state)
                        self.assertEqual(start.call_count, 0 if wait else 1)
                        dynamic = [e for e in events if e['dynamic']]
                        self.assertEqual(len(dynamic), 2 if wait else 4)
                        self.assertTrue(all(e['timeout'] == 25 for e in dynamic))
                        self.assertTrue(all(e['window'] == state for e in dynamic))
                        result = json.loads((root/'result.json').read_text())
                        self.assertIn('资源窗口余额不足' if wait else 'offline first write boundary', result['errors'][0])

    def test_execute_cannot_start_missing_frozen_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'m30'/'native';root.mkdir(parents=True)
            p = plan();p.update(profile='m30-eks-no-load-resource-preflight',
                               evidence_root=str(root.resolve()), resource_preflight=None)
            (root/'plan.json').write_text(json.dumps(p))
            run = pre.Run(root, pre.loop.TARGET)
            with patch.object(pre, 'scope_check'), patch.object(run, 'run') as command:
                with self.assertRaisesRegex(ValueError, 'cannot start a new window'):
                    pre.execute(run, pre.common.digest(root/'plan.json'))
            command.assert_not_called()
            self.assertFalse((root.parent/'invocation.json').exists())

    def test_transfer_failure_cleans_owned_namespace_and_cannot_reinvoke(self):
        for sig in (signal.SIGINT,signal.SIGTERM):self.addCleanup(signal.signal,sig,signal.getsignal(sig))
        with tempfile.TemporaryDirectory() as directory:
            repo=Path(directory);root=repo/'.testdata'/'m30'/'native';root.mkdir(parents=True);p=plan()
            p['profile']='m30-eks-no-load-resource-preflight'
            p['resource_preflight']=dict(node=p['node'],started=time.monotonic(),deadline=time.monotonic()+120,recovery=None)
            p['evidence_root']=str(root.resolve())
            (root/'plan.json').write_text(json.dumps(p));sha=pre.common.digest(root/'plan.json')
            run=pre.Run(root,pre.loop.TARGET);calls=[]
            pod=live_pod();pod['metadata']['namespace']=p['namespace'];pod['metadata']['labels']=p['objects']['job']['metadata']['labels']
            pod['status']['initContainerStatuses'][1]['state']=dict(running=dict(startedAt='synthetic'))
            namespace=dict(kind='Namespace',name=p['namespace'],uid='ns',owner=p['owner'])
            job=dict(kind='Job',name='loopback',uid='synthetic-job',owner=p['owner'])
            def start(r):r.namespace=namespace;r.owned=[namespace]
            def current():
                run.pod_entry=dict(kind='Pod',name='synthetic-pod',uid='synthetic-pod-uid',owner=p['owner'])
                return pod
            def kube(args,namespace=None):
                if args[0]=='logs':return '{"bootstrap":"waiting-for-verified-helper"}\n'
                return ''
            def command(args,timeout=25):return 'synthetic' if args[:2]==['git','rev-parse'] else ''
            def transfer(until):calls.append('transfer failed');raise ValueError('injected upload failure')
            def cleanup(**kwargs):
                calls.append('cleanup');self.assertLessEqual(run.deadline-time.monotonic(),180)
                result=dict(confirmed=True,resources=[])
                return result
            with patch.object(pre,'scope_check'),patch.object(pre,'verified_helper',return_value=p['helper']),patch.object(run,'run',side_effect=command),patch.object(run,'kube',side_effect=kube),patch.object(run,'check_node'),patch.object(pre.loop,'namespace_start',side_effect=start),patch.object(run,'create',return_value=job),patch.object(run,'current_pod',side_effect=current),patch.object(run,'transfer',side_effect=transfer),patch.object(run,'selected_object',return_value=pod),patch.object(run,'cleanup',side_effect=cleanup):
                self.assertEqual(pre.execute(run,sha),1)
                self.assertEqual(calls,['transfer failed','cleanup'])
                result=json.loads((root/'result.json').read_text())
                self.assertTrue(result['cleanup']['confirmed']);self.assertEqual(result['planned'],0)
                self.assertIsNone(result['empty_index_put_completed'])
                with self.assertRaises(FileExistsError):pre.execute(run,sha)
                self.assertEqual(calls,['transfer failed','cleanup'])


class Raw(unittest.TestCase):
    def test_original_m28_raw_fields_and_realistic_eks_limits(self):
        for role in ('weir','es'):
            original=pre.read_stream(Path('scripts/fixtures/m28r-resource')/(role+'.jsonl'))
            samples=[x for x in original if 'files' in x]
            for old in samples:
                s=copy.deepcopy(old)
                s['observer']['identity']['exe_sha256']=pre.IMAGES['tool']['binary']
                if role=='weir':s['process']['identity']['exe_sha256']=pre.IMAGES['version']['binary']
                s['files']['limits']=s['files']['limits'].replace('4096', '1048576')
                s['files']['pids.max']='37697\n'
                result=pre.sample_check(s,role)
                self.assertEqual(result['fd_soft'],1048576);self.assertEqual(result['pids_max'],37697)
                for mutation in ('errors','missing','duplicate','negative','oom','identity'):
                    broken=copy.deepcopy(s)
                    if mutation=='errors':broken['errors']=['retained failure']
                    if mutation=='missing':del broken['files']['io.stat']
                    if mutation=='duplicate':broken['files']['limits']+='Max open files 1048576 1048576 files\n'
                    if mutation=='negative':broken['files']['pids.max']='-1\n'
                    if mutation=='oom':broken['files']['memory.events']=broken['files']['memory.events'].replace('oom 0','oom 1')
                    if mutation=='identity':broken['observer']['identity']['uid']=1234
                    with self.assertRaises((ValueError,KeyError)):pre.sample_check(broken,role)

    def test_full_six_sample_series_and_missing_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);complete_fixture(root)
            native=(root/'native-identity.txt').read_text().splitlines()
            for role in ('weir','es'):
                entries=pre.read_stream(root/(role+'.jsonl'));entries=entries[:7]+[entries[-1]];entries[-1]['samples']=6
                for entry in entries:
                    if entry.get('type')=='identity':
                        entry['observer']['exe_sha256']=pre.IMAGES['tool']['binary']
                        if role=='weir':entry['target']['exe_sha256']=entry['exe_sha256']=pre.IMAGES['version']['binary']
                    elif 'files' in entry:
                        entry['observer']['identity']['exe_sha256']=pre.IMAGES['tool']['binary']
                        if role=='weir':entry['process']['identity']['exe_sha256']=pre.IMAGES['version']['binary']
                        entry['files']['net/tcp']=tcp_table()
                (root/(role+'.jsonl')).write_text(''.join(json.dumps(e)+'\n' for e in entries))
            trial=pre.read_stream(root/'through.jsonl');client=next(e['sample'] for e in trial if e['type']=='client_start')
            for who in ('process','observer'):client[who]['identity']['exe_sha256']=pre.IMAGES['tool']['binary']
            client['files']['net/tcp']=tcp_table();(root/'client-snapshot.json').write_text(json.dumps(client))
            result=pre.observation_report(root,native);self.assertEqual(result['weir']['samples'],6)
            path=root/'es.jsonl';path.write_text('\n'.join(path.read_text().splitlines()[:-1])+'\n')
            with self.assertRaises(ValueError):pre.observation_report(root,native)


class Observation(unittest.TestCase):
    def command(self, role, *, seconds=0, ending=True):
        records = [dict(type='identity', role=role)]
        records += [dict(type='sample', sequence=i, padding='x'*48000) for i in range(6)]
        if ending:
            records += [dict(type='observer_end', role=role, samples=6)]
        code = ('import json,sys,time; records='+repr(records)+'; start=time.monotonic(); '
                '\nfor record in records:\n'
                ' if record["type"]=="sample":time.sleep(max(0,start+record["sequence"]*'+str(seconds)+'/5-time.monotonic()))\n'
                ' print(json.dumps(record),flush=True)\n'
                ' sys.stderr.write("e"*48000);sys.stderr.flush()\n')
        return [sys.executable, '-c', code]

    def test_real_six_samples_then_join_before_slow_identity_and_next_role(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory);run = pre.Run(root, pre.loop.TARGET)
            original_deadline = run.deadline
            calls = []
            def monitor():
                for record in root.glob('*-exec.json'):
                    saved = json.loads(record.read_text())
                    self.assertIsNotNone(saved['exit'])
                    self.assertIsNotNone(saved['stopped_monotonic'])
                    self.assertEqual(saved['eof'], [True, True])
                calls.append('monitor');time.sleep(.03)
            def command(container, argv):
                role = argv[argv.index('-role')+1]
                calls.append(role)
                return self.command(role, seconds=10 if role=='weir' else 0)
            with patch.object(run, 'monitor', side_effect=monitor), patch.object(run, 'exec_command', side_effect=command):
                run.observe('weir');run.observe('es')
                with self.assertRaises(FileExistsError):run.observe('weir')
            self.assertEqual(calls, ['monitor', 'weir', 'monitor', 'monitor', 'es', 'monitor'])
            self.assertEqual(run.deadline, original_deadline)
            self.assertEqual(len(pre.read_stream(root/'weir.jsonl')), 8)

    def test_missing_end_and_nonzero_do_not_reach_later_monitor(self):
        for failure in ('end', 'exit', 'partial', 'sequence'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);run=pre.Run(root,pre.loop.TARGET)
                command=self.command('weir',ending=failure!='end')
                if failure=='exit':command[-1]+='\nsys.exit(7)'
                if failure=='partial':command[-1]+='\nsys.stdout.write("{")'
                if failure=='sequence':command[-1]=command[-1].replace("'sequence': 5", "'sequence': 4")
                with patch.object(run,'monitor') as monitor,patch.object(run,'exec_command',return_value=command):
                    with self.assertRaises((ValueError,RuntimeError)):run.observe('weir')
                self.assertEqual(monitor.call_count,1)
                saved=json.loads((root/'weir-exec.json').read_text())
                self.assertIsNotNone(saved['stopped_monotonic'])
                self.assertIsNotNone(saved['exit'])

    def test_identity_failure_and_exhausted_budget_spawn_nothing(self):
        for failure in ('identity', 'expired', 'slow-monitor'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                run=pre.Run(Path(directory),pre.loop.TARGET)
                clock=[1000.0];run.deadline=1030 if failure!='expired' else 1017.99
                def monitor():
                    if failure=='identity':raise ValueError('runtime identity changed')
                    clock[0]+=13
                with patch.object(pre.time,'monotonic',side_effect=lambda:clock[0]),patch.object(run,'monitor',side_effect=monitor),patch.object(run,'exec_command') as command:
                    with self.assertRaises(ValueError):run.observe('weir')
                command.assert_not_called()
                self.assertFalse((run.root/'weir-exec.json').exists())

    def test_deadline_and_signal_close_stdin_and_join_before_diagnostic_failure(self):
        for failure in ('deadline','cancel'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                run=pre.Run(Path(directory),pre.loop.TARGET)
                command=[sys.executable,'-c','import sys;print("{}",flush=True);sys.stdin.read();print("{\\"cancelled\\":true}",flush=True)']
                original=Observer.poll
                def poll(observer):
                    original(observer)
                    if observer.entries and observer.stop_requested is None:
                        if failure=='cancel':raise KeyboardInterrupt('cancelled')
                        run.deadline=time.monotonic()-1
                    return observer.entries
                with patch.object(run,'monitor'),patch.object(run,'exec_command',return_value=command),patch.object(Observer,'poll',poll):
                    with self.assertRaises((ValueError,KeyboardInterrupt)):run.observe('weir')
                saved=json.loads((run.root/'weir-exec.json').read_text())
                self.assertEqual(saved['exit'],0)
                self.assertEqual(saved['eof'],[True,True])
                self.assertIsNotNone(saved['stopped_monotonic'])
                self.assertTrue(pre.read_stream(run.root/'weir.jsonl')[-1]['cancelled'])
                # This represents a later blocking/failing diagnostic; ownership
                # has already ended before it can use the control-plane path.
                with self.assertRaisesRegex(ValueError,'diagnostic'):
                    time.sleep(.02);raise ValueError('diagnostic failed')

    def test_post_observation_identity_drift_occurs_after_wait(self):
        with tempfile.TemporaryDirectory() as directory:
            run=pre.Run(Path(directory),pre.loop.TARGET)
            command=self.command('weir')
            with patch.object(run,'monitor',side_effect=[None,ValueError('runtime identity changed')]),patch.object(run,'exec_command',return_value=command):
                with self.assertRaisesRegex(ValueError,'identity changed'):run.observe('weir')
            saved=json.loads((run.root/'weir-exec.json').read_text())
            self.assertEqual(saved['exit'],0)
            self.assertIsNotNone(saved['stopped_monotonic'])


if __name__=='__main__':unittest.main()
