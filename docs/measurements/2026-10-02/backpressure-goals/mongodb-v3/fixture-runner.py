import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
STATE = json.loads((ROOT/'state.json').read_text())
OWNER = STATE['owner']
LABEL = 'weir.load-comparison.owner'
ENV = dict(os.environ,KUBECTL_REMOTE_COMMAND_WEBSOCKETS='false')
HTTP = 'http://127.0.0.1:19184'


def kube(args,role=None,timeout=60,check=True):
    command=['kubectl','-n',OWNER]
    if role:
        command+=['exec','fixture','-c',role,'--']
    value=subprocess.run(command+args,env=ENV,capture_output=True,text=True,timeout=timeout,check=check)
    return value


def shell(script,role='client'):
    return kube(['sh','-c',script],role).stdout.strip()


def remote(name):
    with urllib.request.urlopen(HTTP+'/'+name,timeout=10) as response:
        data=response.read((32<<20)+1)
        if len(data)>32<<20:
            raise RuntimeError('owned evidence response exceeds32MiB')
        return data


def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/(name+'.py'))
    result=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def owned():
    ns=json.loads(kube(['get','namespace',OWNER,'-o','json']).stdout)
    if ns['metadata']['labels'].get(LABEL)!=OWNER:
        raise RuntimeError('namespace owner mismatch')
    return ns


def bootstrap(opts):
    for name in ('namespace','policy','pod'):
        source=ROOT/(name+'.json')
        value=json.loads(source.read_text())
        expected=value['metadata'].get('namespace',value['metadata'].get('name'))
        if expected!=OWNER or value['metadata']['labels'].get(LABEL)!=OWNER:
            raise RuntimeError('prepared manifest owner mismatch')
    node=STATE['planned_node']['kubernetes.io/hostname']
    (ROOT/'node-before.json').write_text(kube(['get','node',node,'-o','json']).stdout)
    (ROOT/'node-before-top.txt').write_text(kube(['top','node',node],check=False).stdout)
    prior=kube(['get','namespace',OWNER,'-o','json'],check=False)
    if prior.returncode==0 or 'NotFound' not in prior.stderr:
        raise RuntimeError('fresh namespace is not known absent')
    STATE['create_attempted']=True
    (ROOT/'state.json').write_text(json.dumps(STATE,indent=2))
    kube(['create','-f',str(ROOT/'namespace.json')])
    STATE['created']=True
    STATE['namespace_uid']=owned()['metadata']['uid']
    (ROOT/'state.json').write_text(json.dumps(STATE,indent=2))
    kube(['create','-f',str(ROOT/'policy.json')])
    kube(['create','-f',str(ROOT/'pod.json')])
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        pod=json.loads(kube(['get','pod','fixture','-o','json']).stdout)
        (ROOT/'scheduled-pod.json').write_text(json.dumps(pod,indent=2))
        if pod['status'].get('phase')=='Running' and all(c.get('ready') for c in pod['status'].get('containerStatuses',[])):
            break
        if pod['status'].get('phase') in ('Failed','Succeeded'):
            raise RuntimeError('fixture Pod did not stay Running')
        time.sleep(1)
    else:
        raise RuntimeError('fixture scheduling/startup timed out')
    if pod['spec']['nodeName']!=STATE['planned_node']['kubernetes.io/hostname']:
        raise RuntimeError('fixture scheduled outside isolated planned node')
    (ROOT/'pod-before-trials.json').write_text(kube(['get','pod','fixture','-o','json']).stdout)
    (ROOT/'node-during-top.txt').write_text(kube(['top','node',pod['spec']['nodeName']],check=False).stdout)
    kube(['cp',str(opts.archive),'fixture:/bench/binaries.tar','-c','client'],timeout=300)
    shell('tar -xof /bench/binaries.tar -C /bench; chmod 755 /bench/client-final /bench/weir-final /bench/sampler /bench/sampler-metrics')
    hashes=shell('sha256sum /bench/client-final /bench/weir-final /bench/sampler /bench/sampler-metrics')
    actual={Path(line.split()[1]).name:line.split()[0] for line in hashes.splitlines()}
    if actual!=opts.binary_hashes:
        raise RuntimeError('remote frozen binary hash mismatch')
    (ROOT/'remote-binary-hashes.json').write_text(json.dumps(actual,indent=2))
    units=shell('getconf CLK_TCK; getconf PAGESIZE; cat /proc/1/comm','database').splitlines()
    if units!=['100','4096','mongod']:
        raise RuntimeError('archived sampler analysis units do not match fixture')
    (ROOT/'sampler-units.json').write_text(json.dumps({'clock_ticks_per_second':100,'page_size_bytes':4096,'database_pid1':'mongod'},indent=2))
    script='rs.initiate({_id:"weir_load_test",members:[{_id:0,host:"127.0.0.1:27017"}]}); for(let i=0;i<120;i++){if(db.hello().isWritablePrimary){print(JSON.stringify({version:db.version(),hello:db.hello()}));quit(0);} sleep(500);}quit(1);'
    result=kube(['mongosh','--quiet','--norc','--eval',script],'database',timeout=90)
    (ROOT/'database-bootstrap.jsonl').write_text(result.stdout)
    shell('busybox httpd -f -p 8080 -h /bench > /bench/fixture-http.log 2>&1 < /dev/null & echo $! > /bench/fixture-http.pid')
    return pod


def partial_export():
    errors=[]
    for tag in ('mongo-v3-1','mongo-v3-2'):
        try:
            result=shell('test -d /bench/'+tag+' && tar -czf /bench/'+tag+'-partial.tar.gz -C /bench '+tag)
            raw=remote(tag+'-partial.tar.gz')
            output=ROOT/tag
            output.mkdir(exist_ok=True)
            (output/'partial-raw.tar.gz').write_bytes(raw)
        except Exception as error:
            errors.append(str(error))
    (ROOT/'partial-export-errors.json').write_text(json.dumps(errors,indent=2))


def cleanup(pf):
    value={'owner':OWNER,'namespace_uid':STATE.get('namespace_uid'),'port_forward_pid':pf.pid if pf else None}
    errors=[]
    try:
        if STATE.get('create_attempted'):
            probe=kube(['get','namespace',OWNER,'-o','json'],check=False)
            if probe.returncode and 'NotFound' in probe.stderr:
                value['namespace_not_found']=probe.stderr.strip()
            else:
                probe.check_returncode()
                ns=json.loads(probe.stdout)
                if ns['metadata']['labels'].get(LABEL)!=OWNER or (STATE.get('namespace_uid') and ns['metadata']['uid']!=STATE['namespace_uid']):
                    raise RuntimeError('namespace owner/UID changed; refuse deletion')
                value['namespace_uid']=ns['metadata']['uid']
                value['delete']=kube(['delete','namespace',OWNER,'--wait=false']).stdout.strip()
                deadline=time.monotonic()+90
                while time.monotonic()<deadline:
                    result=kube(['get','namespace',OWNER,'-o','json'],check=False)
                    if result.returncode and 'NotFound' in result.stderr:
                        value['namespace_not_found']=result.stderr.strip()
                        break
                    time.sleep(1)
                if 'namespace_not_found' not in value:
                    raise RuntimeError('owned namespace cleanup missingNotFound')
    except Exception as error:
        errors.append(str(error))
    finally:
        if pf:
            if pf.poll() is None:
                pf.terminate()
            try:
                value['port_forward_exit']=pf.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pf.kill();value['port_forward_exit']=pf.wait(timeout=5)
            value['port_forward_command']=pf.args
            value['port_forward_pid_absent']=subprocess.run(['ps','-p',str(pf.pid),'-o','pid='],capture_output=True,text=True).returncode!=0
        value['errors']=errors
        (ROOT/'cleanup.json').write_text(json.dumps(value,indent=2))
        print(json.dumps(value),flush=True)
    if errors:
        raise RuntimeError('owned fixture cleanup failed: '+str(errors))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--binaries',type=Path,required=True)
    parser.add_argument('--expected-hashes',type=Path,required=True)
    opts=parser.parse_args()
    opts.archive=opts.binaries.resolve()
    opts.binary_hashes=json.loads(opts.expected_hashes.read_text())
    (ROOT/'frozen-binaries.json').write_text(json.dumps({'archive_sha256':hashlib.sha256(opts.archive.read_bytes()).hexdigest(),'binary_sha256':opts.binary_hashes},indent=2))
    with tarfile.open(opts.archive) as archive:
        members=archive.getmembers()
        names={member.name.lstrip('./') for member in members if member.isfile()}
        if names!=set(opts.binary_hashes) or any(not member.isfile() or member.name.startswith('/') or '..' in Path(member.name).parts for member in members):
            raise RuntimeError('frozen binary archive must contain exactly declared regular files')
        for member in members:
            if hashlib.sha256(archive.extractfile(member).read()).hexdigest()!=opts.binary_hashes[member.name.lstrip('./')]:
                raise RuntimeError('local frozen archive checksum mismatch')
    pf=None;pf_log=None
    try:
        bootstrap(opts)
        pf_log=(ROOT/'portforward.log').open('w')
        command=['kubectl','-n',OWNER,'port-forward','pod/fixture','19184:8080']
        pf=subprocess.Popen(command,env=ENV,stdout=pf_log,stderr=pf_log)
        (ROOT/'portforward.pid').write_text(str(pf.pid))
        for _ in range(40):
            if pf.poll() is not None:
                raise RuntimeError('owned port-forward exited')
            try:
                remote('fixture-http.pid');break
            except Exception:
                time.sleep(.25)
        else:
            raise RuntimeError('owned HTTP not ready')
        phase=module('phase-runner')
        analyze=module('analyze')
        for tag in ('mongo-v3-1','mongo-v3-2'):
            run_opts=argparse.Namespace(mode='current',tag=tag,rate=2500,batch=8,client_queue=32,seconds=90,write_every=10,healthy_cpu=500,slow_cpu=50)
            phase.run(run_opts)
            result=analyze.analyze(ROOT/tag)
            print(json.dumps({'tag':tag,'run_error':result['run_error'],'audit':result['audit'],'window_changes':result['window_changes']}),flush=True)
        (ROOT/'pod-after-trials.json').write_text(kube(['get','pod','fixture','-o','json']).stdout)
        (ROOT/'database-final-disk.txt').write_text(shell('df -h /data/db; du -sh /data/db','database'))
        (ROOT/'final-processes-client.txt').write_text(shell('ps','client'))
        (ROOT/'final-processes-weir.txt').write_text(shell('ps','weir'))
    except Exception:
        if pf and pf.poll() is None:
            partial_export()
        raise
    finally:
        cleanup(pf)
        if pf_log:pf_log.close()


if __name__=='__main__':
    main()
