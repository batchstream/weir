#!/usr/bin/env python3
"""Frozen M28 local native short fixture; no EKS, image build, or capacity search."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import time

import eks_pacing as common
import eks_loopback as loop
from local_es_prerequisite import INSPECT, local_check
from capacity_fixture import Observer
from resource_report import report, require, LIMITS, stream_report

ENVIRONMENT = 'sha256:ef36debc338afa91481a64a435dbe23400f6252742ff63aaca45cfeedcaebdd9'
LABEL = 'qualification.weir.io/owner'


ES_START = """set -euo pipefail
umask 077
mkdir -p /tmp/weir-es-config/jvm.options.d
for name in elasticsearch.yml jvm.options log4j2.properties; do
  source=/usr/share/elasticsearch/config/$name
  [[ $(stat -c %s "$source") -le 262144 ]]
  cp "$source" /tmp/weir-es-config/$name
  sha256sum "$source" /tmp/weir-es-config/$name
done
export ES_PATH_CONF=/tmp/weir-es-config
exec /usr/local/bin/docker-entrypoint.sh "$@"
"""


def inventories():
    result=dict(containers=['docker','ps','-aq','--no-trunc'],
                networks=['docker','network','ls','--no-trunc','--format','{{json .}}'])
    return result


def inventory_check(before, after, name):
    if name=='containers':
        require(set(before.splitlines())==set(after.splitlines()), 'container inventory changed')
        return []
    old={row['Name']:row['ID'] for row in map(json.loads,before.splitlines())}
    new={row['Name']:row['ID'] for row in map(json.loads,after.splitlines())}
    # Docker may recreate its built-in bridge on container start. This is
    # recorded separately; every nondefault network must retain its exact ID.
    previous_bridge=old.pop('bridge',None);current_bridge=new.pop('bridge',None)
    require(old==new, 'nondefault network inventory changed')
    return [] if previous_bridge==current_bridge else ['default bridge ID changed: '+str(previous_bridge)+' -> '+str(current_bridge)]


def commands(options):
    root, artifacts, owner = (options[k] for k in ('root','artifacts','owner'))
    result={}
    for role in ('es','weir','client'):
        cpus,mib,pids=LIMITS[role]
        uid='1000:0' if role=='es' else '65532:65532'
        argv=['docker','create','--pull=never','--name',owner+'-'+role,'--label',LABEL+'='+owner,
              '--platform','linux/arm64','--network','none' if role=='es' else 'container:{es}',
              '--user',uid,'--read-only','--cap-drop=ALL','--security-opt','no-new-privileges=true',
              '--cpus',str(cpus),'--memory',str(mib)+'m','--memory-swap',str(mib)+'m','--pids-limit',str(pids),
              '--ulimit','nofile=4096:4096','--log-driver','local','--log-opt','max-size=4m',
              '--log-opt','max-file=1','--log-opt','compress=false',
              '--tmpfs','/tmp:rw,nosuid,nodev,size=268435456,mode=1777',
              '--mount',f'type=bind,src={artifacts},dst=/qualification,readonly',
              '--env','WEIR_CAPACITY_INTEGRATION=1','--env','GOMAXPROCS='+('2' if role=='weir' else '1')]
        if role=='es':
            seed=dict(owner=owner,namespace=owner,node=dict(name='local-only'))
            es=loop.objects(seed)['job']['spec']['template']['spec']['initContainers'][0]
            argv+=['--hostname',owner+'-es','--add-host',owner+'-es:127.0.0.1',
                   '--env','ES_JAVA_OPTS=-Xms1024m -Xmx1024m','--env','AWS_EC2_METADATA_DISABLED=true',
                   '--tmpfs','/usr/share/elasticsearch/data:rw,nosuid,nodev,size=1073741824,uid=1000,gid=0,mode=0770',
                   '--tmpfs','/usr/share/elasticsearch/logs:rw,nosuid,nodev,size=67108864,uid=1000,gid=0,mode=0770',
                   '--mount',f'type=bind,src={root}/local-check.sh,dst=/local-check.sh,readonly',
                   '--mount',f'type=bind,src={root}/es-start.sh,dst=/es-start.sh,readonly',
                   '--entrypoint','/bin/tini',loop.ES['reference'],'--','/bin/bash','--noprofile','--norc','/es-start.sh']+es['args']
        elif role=='weir':
            argv+=['--mount',f'type=bind,src={root}/node.json,dst=/node.json,readonly',
                   '--entrypoint','/qualification/weir',ENVIRONMENT,'-config','/node.json']
        else:
            argv+=['--entrypoint','/qualification/client',ENVIRONMENT,'-mode','idle']
        result[role]=argv
    return result


def inspect(run, cid):
    return json.loads(run.run(['docker','inspect',cid,'--format',INSPECT]))


def owned(obj, options):
    require(obj['Id']==options['cid'] and obj['Name']=='/'+options['owner']+'-'+options['role'] and
            obj['Config']['Labels'].get(LABEL)==options['owner'], 'container owner/ID drift')


def verify(obj, options):
    owned(obj,options)
    role=options['role'];host=obj['HostConfig'];cpus,mib,pids=LIMITS[role]
    require(host['NanoCpus']==cpus*10**9 and host['Memory']==host['MemorySwap']==mib*1024**2 and host['PidsLimit']==pids,'container limits')
    require(host['CapDrop']==['ALL'] and not host['CapAdd'] and host['SecurityOpt']==['no-new-privileges=true'] and
            not host['Privileged'] and host['ReadonlyRootfs'] and not host['PidMode'] and
            not host['PublishAllPorts'] and not host['PortBindings'] and host['IpcMode']=='private','container isolation')
    require(host['NetworkMode']==('none' if role=='es' else 'container:'+options['es']), 'network namespace drift')
    require(obj['Config']['User']==('1000:0' if role=='es' else '65532:65532'), 'container UID')
    require(obj['Image']==(loop.ES['config'] if role=='es' else ENVIRONMENT), 'image identity')
    require(all(m['Type']=='bind' and not m['RW'] and m['Source'].startswith(str(common.REPO/'.testdata/m28')+'/') for m in obj['Mounts']),'static mounts')


def execute(run, plan):
    containers={};observers={};result=dict(passed=False,cleanup=False,errors=[])
    started=time.monotonic();run.deadline=plan["fixture_deadline_monotonic"]
    def monitor():
        for role,observer in observers.items():
            series=[sample for sample in observer.poll() if 'files' in sample]
            if len(series)>=2:stream_report(series,role)
    run.monitor=monitor
    try:
        require(all(common.digest(p)==value for p,value in plan['inputs'].items()),'frozen inputs changed')
        for name,command in inventories().items():run.save(name+'-before.txt',run.run(command))
        for image in (ENVIRONMENT,loop.ES['reference']):
            raw=run.run(['docker','image','inspect',image,'--format','{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}}}'])
            obj=json.loads(raw);require(obj['Os']=='linux' and obj['Architecture']=='arm64','native image architecture')
            run.save('image-'+('es' if image==loop.ES['reference'] else 'environment')+'.json',obj)
        run.save('docker-version.json',run.run(['docker','version','--format','{{json .Server}}']))
        run.save('host-uname.txt',run.run(['uname','-smr']))
        for role in ('es','client','weir'):
            if role=='weir':
                # Only empty index metadata before product startup; no extra seed.
                run.run(['docker','exec',containers['es']]+loop.CURL+['-X','PUT','-H','Content-Type: application/json',
                    '--data','{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"enabled":false}}','http://127.0.0.1:9200/records'])
            argv=[part.replace('{es}',containers.get('es','')) for part in plan['commands'][role]]
            cid=run.run(argv).strip();require(re.fullmatch('[0-9a-f]{64}',cid),'create ID')
            containers[role]=cid;run.save('owned.json',containers)
            options=dict(cid=cid,owner=plan['owner'],role=role,es=containers['es'])
            actual=inspect(run,cid);verify(actual,options);run.save(role+'-created.json',actual)
            run.run(['docker','start',cid])
            if role=='es':
                run.save('native-identity.txt',run.run(['docker','exec','--user','1000:0',cid,'/bin/bash','--noprofile','--norc','-c',
                    'set -eu; uname -smr; id; getconf CLK_TCK; sha256sum /usr/share/elasticsearch/jdk/bin/java; getent --no-addrconfig ahostsv4 "$HOSTNAME"; cat /etc/hosts']))
                until=time.monotonic()+120
                while True:
                    actual=inspect(run,cid);run.save('es-starting.json',actual)
                    require(actual['State']['Running'] and not actual['State']['OOMKilled'],'ES exited/OOM during startup')
                    try:raw=run.run(['docker','exec',cid]+loop.CURL+['http://127.0.0.1:9200/'],8)
                    except ValueError:
                        command=json.loads((run.root/f'command-{run.number:04d}.json').read_text())
                        require(command['exit'] in (7,28) and time.monotonic()<until,'ES readiness failure')
                        time.sleep(2);continue
                    require(json.loads(raw)['version']['number']=='8.19.22','ES version');run.save('es-version.json',raw);break
                run.save('socket-before.log',run.run(['docker','exec',cid,'/usr/bin/timeout','20','/bin/bash','--noprofile','--norc','/local-check.sh'],25))
            if role=='client':
                run.save('native-tests.log',run.run(['docker','exec',cid,'/qualification/observe.test','-test.v','-test.run','^(TestNativeSelfObservation|TestNativeObservationExitedTarget|TestProcessParsing|TestObservation|TestBoundedObservation|TestTargetIdentity|TestEvidence)'],45))
                run.save('native-runtime-test.log',run.run(['docker','exec',cid,'/qualification/app.test','-test.v','-test.run','^TestStandardRuntimeCollectors$'],15))
        run.run(['docker','exec',containers['weir'],'/qualification/weir','-probe','ready'],10)
        for role in ('weir','es'):
            command=['docker','exec','-i','--user','65532:65532' if role=='weir' else '1000:0',containers[role],
                     '/qualification/client','-mode','observe','-role',role,'-pid','1' if role=='weir' else 'java','-seconds','140']
            options=dict(root=run.root,role=role,command=command)
            observers[role]=Observer(options)
        until=time.monotonic()+6
        while time.monotonic()<until:monitor();time.sleep(.05)
        require(all(len(o.poll())>=3 for o in observers.values()),'preload observer unavailable')
        for name in ('through','direct'):
            command=['docker','exec',containers['client'],'/qualification/client','-mode','trial','-backend','http://127.0.0.1:9200',
                     '-prefix','m28-'+name,'-rate','50','-warm','20','-seconds','20','-mutation-reservation','1200']
            if name=='through':command+=['-target','127.0.0.1:7447']
            raw=run.run(command,65,monitor=True);run.save(name+'.jsonl',raw)
            entries=[json.loads(line) for line in raw.splitlines()]
            trials=[entry['trial'] for entry in entries if entry.get('type')=='trial'];require(len(trials)==1,'trial missing')
            for window in (trials[0]['warm'],trials[0]['measure']):require(not window['all']['failures'] and not window['all']['unknown'],'real safety/data/transport failure; no retry')
            require(any(entry.get('type')=='audit' and entry['error']=='<nil>' for entry in entries),'DB audit missing/error')
            run.save('socket-'+name+'.log',run.run(['docker','exec',containers['es'],'/usr/bin/timeout','20','/bin/bash','--noprofile','--norc','/local-check.sh','main'],25))
        while any(o.child.poll() is None for o in observers.values()):
            require(time.monotonic()<started+300,'observer completion deadline');monitor();time.sleep(.05)
        monitor()
        for role,cid in containers.items():
            actual=inspect(run,cid);run.save(role+'-running.json',actual)
            require(actual['State']['Running'] and not actual['State']['OOMKilled'] and actual['RestartCount']==0,'runtime exit/OOM/restart')
        result['report']=report(run.root);run.save('resource-report.json',result['report'])
        require(result['report']['resource_evidence']=='complete-for-declared-visible-leaf-profile','resource evidence partial')
        result['passed']=True
    except BaseException as exc:result['errors'].append(str(exc))
    finally:
        result['fixture_seconds']=time.monotonic()-started
        cleanup_start=time.monotonic();run.deadline=cleanup_start+120;run.cleaning=True
        cleanup_errors=[]
        for role,observer in observers.items():
            try:observer.stop()
            except BaseException as exc:cleanup_errors.append(role+' exec: '+str(exc))
        for role,cid in reversed(list(containers.items())):
            try:
                actual=inspect(run,cid);options=dict(cid=cid,owner=plan['owner'],role=role);owned(actual,options)
                try:run.save(role+'.log',run.run(['docker','logs',cid],10))
                except BaseException as exc:cleanup_errors.append(role+' log: '+str(exc))
                run.run(['docker','stop','--time','10',cid],16);run.save(role+'-wait.txt',run.run(['docker','wait',cid],5))
                actual=inspect(run,cid);owned(actual,options);run.save(role+'-stopped.json',actual)
                require(not actual['State']['Running'],'container still running')
                run.run(['docker','rm',cid],10)
                require(not run.run(['docker','ps','-aq','--no-trunc','--filter','id='+cid]).strip(),'ID remains')
            except BaseException as exc:cleanup_errors.append(role+': '+str(exc))
        try:
            remaining=run.run(['docker','ps','-aq','--no-trunc','--filter','label='+LABEL+'='+plan['owner']]);require(not remaining.strip(),'owner remains')
            run.save('owner-after.txt',remaining)
            for name,command in inventories().items():
                after=run.run(command);run.save(name+'-after.txt',after)
                changes=inventory_check((run.root/(name+'-before.txt')).read_text(),after,name)
                result.setdefault('environment_changes',[]).extend(changes)
        except BaseException as exc:cleanup_errors.append(str(exc))
        result.update(cleanup_seconds=time.monotonic()-cleanup_start,cleanup_errors=cleanup_errors,cleanup=not cleanup_errors)
        run.save('result.json',result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--owner',required=True)
    args=parser.parse_args();root=args.evidence.absolute()
    require(os.environ.get('WEIR_CAPACITY_INTEGRATION')=='1','explicit integration opt-in')
    require(root.parent==common.REPO/'.testdata/m28' and not root.exists() and re.fullmatch('weir-m28-[a-z0-9-]{1,32}',args.owner),'new owned evidence path')
    prior=list(root.parent.glob('fixture-*'));require(len(prior)<3,'wiring corrections exhausted')
    for attempt in prior:
        previous=json.loads((attempt/'result.json').read_text())
        require(previous['cleanup'] or (attempt/'owned-cleanup-proof.json').is_file(), 'previous owned cleanup unconfirmed')
    if prior:require((root.parent/('correction-'+str(len(prior))+'.json')).is_file(),'explicit documented wiring correction required')
    window=root.parent/'native-window.json'
    if not window.exists():window.write_text(json.dumps(dict(start_utc=time.time(),deadline_monotonic=time.monotonic()+900)))
    deadline=json.loads(window.read_text())['deadline_monotonic'];require(time.monotonic()<deadline,'total native window exhausted')
    root.mkdir(mode=0o700);(root/'docker-config').mkdir(mode=0o700);(root/'docker-config/config.json').write_text('{}\n')
    os.environ['DOCKER_CONFIG']=str(root/'docker-config');os.environ['DOCKER_HOST']='unix:///var/run/docker.sock'
    run=common.Run(root,None);run.save('node.json',loop.configuration());run.save('local-check.sh',local_check());run.save('es-start.sh',ES_START)
    artifacts=common.REPO/'.testdata/m28/artifacts'
    options=dict(root=root,artifacts=artifacts,owner=args.owner)
    files=[p for p in artifacts.iterdir() if p.is_file()]+[root/'node.json',root/'local-check.sh',root/'es-start.sh',Path(__file__),common.REPO/'scripts/resource_report.py',common.REPO/'scripts/capacity_fixture.py',root.parent/'source-inputs.json']
    plan=dict(fixture_deadline_monotonic=deadline,owner=args.owner,source=os.environ['WEIR_M28_SOURCE'],commands=commands(options),inputs={str(p):common.digest(p) for p in files},
              budget=json.loads((root.parent/'scope.json').read_text()),runtime='same-architecture native Linux arm64 Docker VM; binaries built on Darwin Go1.27.1 CGO0',
              observer_seconds=140,observer_samples=71,observer_expected_CLK_TCK=100,planned=6000,document_mutations=2400,seconds=900,cleanup_seconds=120)
    run.save('plan.json',plan);(root/'plan.json').chmod(0o400);run.save('plan.sha256',common.digest(root/'plan.json'))
    def interrupted(signum,frame):raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,interrupted);signal.signal(signal.SIGTERM,interrupted)
    result=execute(run,plan);print(json.dumps(result),flush=True)
    return 0 if result['passed'] and result['cleanup'] else 1


if __name__=='__main__':raise SystemExit(main())
