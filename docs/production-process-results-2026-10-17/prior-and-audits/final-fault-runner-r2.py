import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback
import urllib.parse

sys.dont_write_bytecode = True
ROOT = Path('/Users/liran/Projects/liran/go/weir')
BASE = ROOT / '.testdata/reliability-2026-10-16'
spec = importlib.util.spec_from_file_location('qualified_helpers', BASE / 'lua-repair-runner-r3.py')
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
out = BASE / 'final-fault-r2'
out.mkdir()
source = h.git(['rev-parse', 'HEAD'])
before = h.clean_source(source)
h.write_json(out / 'source-before.json', before)
old = json.loads((BASE / 'final-process-r1/source-before.json').read_text())
changed = sorted(k for k in set(old['files_sha256']) | set(before['files_sha256'])
                 if old['files_sha256'].get(k) != before['files_sha256'].get(k))
h.require(changed == ['internal/app/production_fault_integration_test.go', 'internal/testutil/testmongo/proxy.go'],
          'sustained qualified runtime changed')
h.write_json(out / 'sustained-to-fault-source-projection.json', dict(
    sustained_source=old['revision'], fault_source=source, changed_files=changed,
    unchanged_files=len(before['files_sha256'])-len(changed),
    meaning='Only opt-in fault driver and test-only Mongo observer changed; shipping CLI/runtime/package/dependency inputs unchanged.',
    sustained_result='../final-process-r1/formal600-corrected-validation.json'))
env = h.environment(out)
opts = dict(output=out, env=env, source=source, before=before)
h.write_json(out / 'runner.json', dict(source=source, started=h.now(),
    runner_sha256=h.digest(Path(__file__).read_bytes()),
    helper_sha256=h.digest(Path(spec.origin).read_bytes()), retry=False,
    corrected_gates='20s fixture timeout, original3/5/8s business gates, credit0 before manual proxy release'))

try:
    h.preflight(opts)
    cli, driver = out / 'weir-linux-arm64', out / 'app-race.test'
    cli_env = dict(env, GOOS='linux', GOARCH='arm64', CGO_ENABLED='0')
    command = [h.GO,'build','-trimpath','-buildvcs=true','-mod=readonly',
               '-ldflags=-buildid= -X main.sourceRevision=' + source,'-o',str(cli),'./cmd/weir']
    h.stage(dict(opts,name='CLI-build',command=command,env=cli_env,timeout=180))
    cli_info = h.build_info(dict(opts,name='CLI-buildinfo',binary=cli,
        required={'GOOS':'linux','GOARCH':'arm64','CGO_ENABLED':'0','-trimpath':'true'}))
    driver_env = dict(env,GOOS='darwin',GOARCH='arm64',CGO_ENABLED='1')
    command = [h.GO,'test','-c','-race','-trimpath','-buildvcs=true','-mod=readonly',
               '-tags=integration','-o',str(driver),'./internal/app']
    h.stage(dict(opts,name='driver-build',command=command,env=driver_env,timeout=180))
    driver_info = h.build_info(dict(opts,name='driver-buildinfo',binary=driver,
        required={'GOOS':'darwin','GOARCH':'arm64','CGO_ENABLED':'1','-race':'true','-trimpath':'true'}))
    h.stage(dict(opts,name='proxy-race',command=[h.GO,'test','-race','-count=1','./internal/testutil/testmongo'],env=driver_env,timeout=120))
    h.stage(dict(opts,name='integration-vet',command=[h.GO,'vet','-tags=integration','./...'],env=driver_env,timeout=120))
    h.write_json(out / 'runtime-started.json',dict(source=source,at=h.now(),no_retry=True))
    run_env = dict(driver_env,WEIR_INTEGRATION='1',WEIR_SEARCH_INTEGRATION='elasticsearch',
                   WEIR_QUALIFICATION_BINARY=str(cli),WEIR_QUALIFICATION_SOURCE=source,
                   WEIR_QUALIFICATION_SHA256=cli_info['binary_sha256'],WEIR_PRODUCTION_FAULT_QUALIFICATION='1')
    command = [str(driver),'-test.v','-test.failfast','-test.count=1',
               '-test.run=^TestProductionProcessFaultQualification$','-test.timeout=2m']
    record = h.run_stage(dict(opts,name='fault',command=command,env=run_env,timeout=150))
    raw = Path(record['raw']).read_text()
    problems = []
    if record['exit_code'] != 0 or record['timed_out']:
        problems.append('driver failed or timed out')
    for marker,count in [('baseline backend=',4),('ack_loss backend=',4),
                         ('cancel_after_ack backend=',2),('one_held_IO backend=',4),
                         ('held_IO_cancel backend=',2),('proxy_recovered backend=',2),
                         ('fault_SIGTERM elapsed=',1),('CLI exit=false 0',1),
                         ('qualification=true',1),('--- PASS: TestProductionProcessFaultQualification (',1)]:
        if raw.count(marker) != count:
            problems.append('fault matrix mismatch: ' + marker)
    if any(m in raw for m in ['--- FAIL:', '--- SKIP:', 'WARNING: DATA RACE']):
        problems.append('failed / skipped / raced')
    name = 'weir_test_' + str(record['pid']) + '_1'
    index = 'weir_search_' + str(record['pid']) + '_1'
    javascript = 'const n=' + json.dumps(name) + ';print(JSON.stringify({name:n,absent:db.adminCommand({listDatabases:1,nameOnly:true,filter:{name:n}}).databases.length===0}));'
    mongo = h.mongo_read(dict(opts,name='fault-mongo-absence',javascript=javascript))
    search = h.search_get('/' + index)
    filters = urllib.parse.quote(json.dumps({'name':['^/weir-memory-recovery-']}))
    containers = h.docker_get(dict(env=env,path='/containers/json?all=1&filters='+filters))
    independent = dict(mongo=mongo,search_index=index,search=search,
                       owned_containers_status=containers['status'],owned_containers=containers['body'])
    h.write_json(out / 'independent-absence.json',independent)
    if not (mongo == dict(name=name,absent=True) and search['status']==404 and containers['status']==200 and containers['body']==[]):
        problems.append('owned fixture cleanup not independently confirmed')
    after = h.clean_source(source)
    h.write_json(out / 'source-after.json',after)
    h.require(after['files_sha256']==before['files_sha256'],'source changed during qualification')
    h.write_json(out / 'validation.json',dict(status='PASS' if not problems else 'FAIL',
        source=source,CLI=cli_info,race_driver=driver_info,record=record,
        independent=independent,problems=problems))
    h.require(not problems,'final fault failed: '+str(problems))
    print('PASS final formal fault',flush=True)
except Exception as exc:
    h.write_json(out / 'failure.json',dict(at=h.now(),source=source,error=str(exc),traceback=traceback.format_exc()))
    raise
