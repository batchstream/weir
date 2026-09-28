#!/usr/bin/env python3
"""Frozen native short fixture in an explicitly owned stage and shared budget."""
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
from resource_report import report, require, LIMITS, stream_report, trial_report, observer_identity
from observer_completion import observation_samples, finish_observation, abort_observation

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
              '--cgroupns','private',
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
            not host['PublishAllPorts'] and not host['PortBindings'] and host['IpcMode']=='private' and
            host['CgroupnsMode']=='private','container isolation')
    require(host['NetworkMode']==('none' if role=='es' else 'container:'+options['es']), 'network namespace drift')
    require(obj['Config']['User']==('1000:0' if role=='es' else '65532:65532'), 'container UID')
    require(obj['Image']==(loop.ES['config'] if role=='es' else ENVIRONMENT), 'image identity')
    root=options['root'];artifacts=root.parent/'artifacts'
    mounts={str(artifacts):'/qualification'}
    if role=='es':mounts.update({str(root/name):'/'+name for name in ('local-check.sh','es-start.sh')})
    if role=='weir':mounts[str(root/'node.json')]='/node.json'
    require(len(obj['Mounts'])==len(mounts) and all(m['Type']=='bind' and not m['RW'] and
            mounts.get(m['Source'])==m['Destination'] for m in obj['Mounts']),'static mounts')


def monitor_observations(observers, profiles, deadline):
    for role, observer in observers.items():
        series, complete = observation_samples(observer, profiles[role])
        if len(series) >= 2:
            stream_report(series, role)
        if complete and observer.stopped is None:
            finish_observation(observer, profiles[role], deadline)


def execute(run, plan):
    containers={};observers={};result=dict(passed=False,cleanup=False,load_started=False,errors=[])
    started=time.monotonic();run.deadline=plan["fixture_deadline_monotonic"]
    profiles={}
    def monitor():
        monitor_observations(observers, profiles, min(started+300, run.deadline))
    run.monitor=monitor
    try:
        require(all(common.digest(p)==value for p,value in plan['inputs'].items()),'frozen inputs changed')
        source=json.loads(Path(plan['source_manifest']).read_text())
        require(source['source']==plan['source'] and all(common.digest(common.REPO/p)==value for p,value in source['inputs'].items()), 'source input drift')
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
                raw=run.run(['docker','exec',containers['es']]+loop.CURL+['-X','PUT','-H','Content-Type: application/json',
                    '--data','{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"enabled":false}}','http://127.0.0.1:9200/records'])
                require(json.loads(raw)['acknowledged'] is True, 'preload index management PUT')
                management=dict(index_create=1,document_mutations=0,response=json.loads(raw))
                run.save('preload-management.json',management)
            argv=[part.replace('{es}',containers.get('es','')) for part in plan['commands'][role]]
            cid=run.run(argv).strip();require(re.fullmatch('[0-9a-f]{64}',cid),'create ID')
            containers[role]=cid;run.save('owned.json',containers)
            options=dict(cid=cid,owner=plan['owner'],role=role,es=containers['es'],root=run.root)
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
                run.save('native-tests.log',run.run(['docker','exec',cid,'/qualification/observe.test','-test.v','-test.run','^(TestNativeSelfObservation|TestNativeObservationExitedTarget|TestProcessParsing|TestObservation|TestBoundedObservation|TestTargetIdentity|TestJVMModule|TestEvidence)'],45))
                run.save('native-runtime-test.log',run.run(['docker','exec',cid,'/qualification/app.test','-test.v','-test.run','^TestStandardRuntimeCollectors$'],15))
        ready_until=time.monotonic()+5
        while True:
            try:
                run.run(['docker','exec',containers['weir'],'/qualification/weir','-probe','ready'],3)
                break
            except ValueError:
                record=json.loads((run.root/f'command-{run.number:04d}.json').read_text())
                actual=inspect(run,containers['weir'])
                require(record['exit']==1 and actual['State']['Running'] and not actual['State']['OOMKilled'] and
                        time.monotonic()<ready_until, 'Weir startup readiness deadline/exit')
                time.sleep(.1)
        hashes={Path(p).name:v for p,v in plan['inputs'].items()}
        native=(run.root/'native-identity.txt').read_text().splitlines()
        for role in ('weir','es'):
            command=['docker','exec','-i','--user','65532:65532' if role=='weir' else '1000:0',containers[role],
                     '/qualification/client','-mode','observe','-role',role,'-pid','1' if role=='weir' else 'java','-seconds','140']
            options=dict(root=run.root,role=role,command=command)
            observers[role]=Observer(options)
            profiles[role]=dict(role=role,hashes=hashes,native=native,seconds=140,samples=71)
        until=time.monotonic()+6
        while time.monotonic()<until:monitor();time.sleep(.05)
        for role,observer in observers.items():
            entries=observer.poll();series=[e for e in entries if 'files' in e]
            require(len(series)>=2,'preload observer unavailable')
            stream_report(series,role)
            identity_options=dict(role=role,hashes=hashes,native=native)
            observer_identity(entries[0],series[0],identity_options)
        for name in ('through','direct'):
            # Persist before spawning: even an interrupted seed rules out retry.
            result['load_started']=True
            load_started=dict(trial=name,time=time.time())
            run.save('load-started.json',load_started)
            command=['docker','exec',containers['client'],'/qualification/client','-mode','trial','-backend','http://127.0.0.1:9200',
                     '-prefix','m28r-'+name,'-rate','50','-warm','20','-seconds','20','-mutation-reservation','1200']
            if name=='through':command+=['-target','127.0.0.1:7447']
            raw=run.run(command,65,monitor=True);run.save(name+'.jsonl',raw)
            entries=[json.loads(line) for line in raw.splitlines()]
            _,client,_,_=trial_report(entries,name)
            stream_report(client,'client')
            run.save('socket-'+name+'.log',run.run(['docker','exec',containers['es'],'/usr/bin/timeout','20','/bin/bash','--noprofile','--norc','/local-check.sh','main'],25))
        while any(o.stopped is None for o in observers.values()):
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
            try:abort_observation(observer)
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


def stage_window(args):
    root=args.evidence.resolve();stage=root.parent
    require(root==args.evidence.absolute() and stage.parent==common.REPO.resolve()/'.testdata' and
            stage.name not in ('m28','m28r') and not root.exists(), 'new resolved stage evidence path')
    require(re.fullmatch('[0-9a-f]{40}',args.source),'implementation source SHA')
    budget_path=stage/'stage-budget.json'
    require(budget_path.resolve()==budget_path,'budget symlink')
    budget=json.loads(budget_path.read_text())
    require(budget['evidence_root']==str(stage) and budget['attempts']==['fixture-1','fixture-2'] and
            budget['seconds']==900 and budget['cleanup_seconds']==120 and
            budget['planned']==6000 and budget['document_mutations']==2400,'frozen stage budget')
    require(re.fullmatch('weir-[a-z0-9-]{1,40}',budget['owner']) and
            root.name in budget['attempts'] and args.owner==budget['owner']+'-'+root.name,'frozen attempt/owner')
    for name in ('artifacts','artifacts.json','source-inputs-'+args.source+'.json'):
        target=stage/name
        require(target.exists() and target.resolve()==target,'missing/aliased stage input')
    require(all(p.is_file() and not p.is_symlink() for p in (stage/'artifacts').iterdir()),'artifact path')
    window=stage/'native-window.json'
    require(window.resolve()==window,'window symlink')
    if root.name=='fixture-1':
        native_window=dict(start_utc=time.time(),start_monotonic=time.monotonic(),
                           deadline_monotonic=time.monotonic()+900,budget_sha256=common.digest(budget_path))
        with window.open('x') as stream:json.dump(native_window,stream)
    native_window=json.loads(window.read_text())
    require(native_window['budget_sha256']==common.digest(budget_path),'stage budget changed')
    deadline=native_window['deadline_monotonic']
    require(native_window['start_monotonic']<=time.monotonic()<deadline and
            0<=time.time()-native_window['start_utc']<900,'total native window exhausted')
    if root.name=='fixture-2':
        previous=stage/'fixture-1'
        for target in (previous,previous/'result.json',previous/'plan.json',stage/'retry-review.json',stage/'fixture-1.claim'):
            require(target.resolve()==target,'retry evidence symlink')
        require((stage/'fixture-1.claim').is_file() and not (previous/'load-started.json').exists(),'prior attempt/zero load required')
        result=json.loads((previous/'result.json').read_text())
        require(result['passed'] is False and result['cleanup'] is True and result['load_started'] is False,'retry prohibited after load/unclean/success')
        review=json.loads((stage/'retry-review.json').read_text())
        require(review['classification']=='fixture-text-wiring' and review['previous_plan_sha256']==common.digest(previous/'plan.json') and
                review['new_source']==args.source and json.loads((previous/'plan.json').read_text())['source']!=args.source,
                'retry needs evidence review and new implementation')
        replay=stage/'retry-raw-replay.json'
        require(replay.resolve()==replay and common.digest(replay)==review['replay_sha256'],'retry replay evidence')
    # Claims survive moved failed directories; a used attempt cannot be reclaimed.
    claim=dict(owner=args.owner,source=args.source)
    with (stage/(root.name+'.claim')).open('x') as stream:json.dump(claim,stream)
    return root, budget, deadline


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--owner',required=True)
    parser.add_argument('--source',required=True)
    args=parser.parse_args()
    require(os.environ.get('WEIR_CAPACITY_INTEGRATION')=='1','explicit integration opt-in')
    root,budget,deadline=stage_window(args)
    root.mkdir(mode=0o700);(root/'docker-config').mkdir(mode=0o700);(root/'docker-config/config.json').write_text('{}\n')
    os.environ['DOCKER_CONFIG']=str(root/'docker-config');os.environ['DOCKER_HOST']='unix:///var/run/docker.sock'
    run=common.Run(root,None);run.save('node.json',loop.configuration());run.save('local-check.sh',local_check());run.save('es-start.sh',ES_START)
    artifacts=root.parent/'artifacts'
    options=dict(root=root,artifacts=artifacts,owner=args.owner)
    source_manifest=root.parent/('source-inputs-'+args.source+'.json')
    files=list(artifacts.iterdir())+[root/'node.json',root/'local-check.sh',root/'es-start.sh',source_manifest,root.parent/'artifacts.json',root.parent/'stage-budget.json']
    files += sorted((common.REPO/'scripts').glob('*.py'))+sorted((common.REPO/'scripts').glob('*.sh'))+sorted((common.REPO/'scripts').glob('*.json'))
    plan=dict(fixture_deadline_monotonic=deadline,owner=args.owner,source=args.source,source_manifest=str(source_manifest),
              artifact_source=json.loads((root.parent/'artifacts.json').read_text())['source'],commands=commands(options),inputs={str(p):common.digest(p) for p in files},
              budget=budget,runtime='same-architecture native Linux arm64 Docker VM; binaries built on Darwin Go1.27.1 CGO0',
              observer_seconds=140,observer_samples=71,observer_expected_CLK_TCK=100,planned=6000,document_mutations=2400,seconds=900,cleanup_seconds=120)
    run.save('plan.json',plan);(root/'plan.json').chmod(0o400);run.save('plan.sha256',common.digest(root/'plan.json'))
    def interrupted(signum,frame):raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,interrupted);signal.signal(signal.SIGTERM,interrupted)
    result=execute(run,plan);print(json.dumps(result),flush=True)
    return 0 if result['passed'] and result['cleanup'] else 1


if __name__=='__main__':raise SystemExit(main())
