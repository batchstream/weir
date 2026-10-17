import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import traceback
import urllib.parse

sys.dont_write_bytecode = True
ROOT = Path('/Users/liran/Projects/liran/go/weir')
BASE = ROOT / '.testdata/reliability-2026-10-16'
spec = importlib.util.spec_from_file_location('qualified_helpers', BASE / 'lua-repair-runner-r3.py')
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
out = BASE / 'final-process-r1'
out.mkdir()
source = h.git(['rev-parse', 'HEAD'])
before = h.clean_source(source)
h.write_json(out / 'source-before.json', before)
env = h.environment(out)
opts = dict(output=out, env=env, source=source, before=before)
h.write_json(out / 'runner.json', dict(source=source, started=h.now(),
    runner_sha256=h.digest(Path(__file__).read_bytes()),
    helper_sha256=h.digest(Path(spec.origin).read_bytes()), retry=False,
    formal_seconds=600, warmup_seconds=30, fixed_arrival=200,
    scope='Default actual Linux CLI, real MongoDB and Elasticsearch; no smoke or exploratory opt-ins'))


def unchanged(label):
    after = h.clean_source(source)
    h.write_json(out / (label + '-source-after.json'), after)
    h.require(after['files_sha256'] == before['files_sha256'], 'tracked qualification source changed')


def absent(label, record):
    name = 'weir_test_' + str(record['pid']) + '_1'
    index = 'weir_search_' + str(record['pid']) + '_1'
    javascript = 'const n=' + json.dumps(name) + ';print(JSON.stringify({name:n,absent:db.adminCommand({listDatabases:1,nameOnly:true,filter:{name:n}}).databases.length===0}));'
    mongo = h.mongo_read(dict(opts, name=label + '-mongo-absence', javascript=javascript))
    search = h.search_get('/' + index)
    filters = urllib.parse.quote(json.dumps({'name': ['^/weir-memory-recovery-']}))
    containers = h.docker_get(dict(env=env, path='/containers/json?all=1&filters=' + filters))
    result = dict(mongo=mongo, search_index=index, search=search,
                  owned_containers_status=containers['status'], owned_containers=containers['body'])
    h.write_json(out / (label + '-independent-absence.json'), result)
    h.require(mongo == dict(name=name, absent=True) and search['status'] == 404 and
              containers['status'] == 200 and containers['body'] == [], 'owned fixture cleanup not independently confirmed')
    return result


def validate(label, record):
    raw = Path(record['raw']).read_text()
    problems = []
    def check(ok, message):
        if not ok:
            problems.append(message)
    check(record['exit_code'] == 0 and not record['timed_out'], 'driver failed or timed out')
    check('qualification=true' in raw and '--- PASS: TestProductionProcess' in raw, 'formal mode / test PASS missing')
    check('CLI exit=false 0' in raw, 'normal no-OOM CLI exit missing')
    check('--- SKIP:' not in raw and '--- FAIL:' not in raw and 'WARNING: DATA RACE' not in raw, 'skipped / failed / raced')
    distributions = []
    if label == 'formal600':
        for line in raw.splitlines():
            m = re.search(r'distribution ((?:steady|warmup|burst)/\S+) offered=(\d+) completed=(\d+)', line)
            if m and 'minute=' not in line:
                distributions.append(dict(key=m[1], offered=int(m[2]), completed=int(m[3]), raw_line=line.strip()))
        check(len(distributions) == 26, 'all 26 backend/method/phase distributions missing')
        for phase, want in [('steady',120000),('warmup',6000),('burst',256)]:
            rows = [r for r in distributions if r['key'].startswith(phase + '/')]
            check(sum(r['offered'] for r in rows) == want and all(r['offered']==r['completed'] for r in rows), 'fixed completed offer mismatch: ' + phase)
        for marker in ['duration=10m0s warmup=30s fixed_arrival=200RPC/s',
                       'offered burst=256', 'all 32 slow consumers canceled and joined',
                       'final all queue/execution/publication/session/byte gauges=0',
                       'oracle backend=mongo', 'oracle backend=search']:
            check(marker in raw, 'missing oracle: ' + marker)
    else:
        for marker, count in [('baseline backend=',4), ('ack_loss backend=',4),
                              ('cancel_after_ack backend=',2), ('one_held_IO backend=',4),
                              ('held_IO_cancel backend=',2), ('proxy_recovered backend=',2),
                              ('fault_SIGTERM elapsed=',1)]:
            check(raw.count(marker) == count, 'fault matrix mismatch: ' + marker)
    independent = absent(label, record)
    unchanged(label)
    result = dict(status='PASS' if not problems else 'FAIL', source=source,
                  raw=record['raw'], raw_sha256=record['raw_sha256'], record=record,
                  distributions=distributions, independent=independent, problems=problems)
    h.write_json(out / (label + '-validation.json'), result)
    h.require(not problems, label + ' did not qualify: ' + str(problems))
    return result


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
    unchanged('offline')
    h.write_json(out / 'runtime-started.json',dict(source=source,at=h.now(),no_retry=True))
    run_env = dict(driver_env,WEIR_INTEGRATION='1',WEIR_SEARCH_INTEGRATION='elasticsearch',
                   WEIR_QUALIFICATION_BINARY=str(cli),WEIR_QUALIFICATION_SOURCE=source,
                   WEIR_QUALIFICATION_SHA256=cli_info['binary_sha256'])
    results = []
    for label,test,flag,timeout in [('formal600','TestProductionProcessQualification','WEIR_PRODUCTION_QUALIFICATION',780),
                                   ('fault','TestProductionProcessFaultQualification','WEIR_PRODUCTION_FAULT_QUALIFICATION',150)]:
        unchanged(label + '-before')
        command = [str(driver),'-test.v','-test.failfast','-test.count=1','-test.run=^'+test+'$',
                   '-test.timeout=' + ('12m' if label=='formal600' else '2m')]
        record = h.run_stage(dict(opts,name=label,command=command,env=dict(run_env,**{flag:'1'}),timeout=timeout))
        results.append(validate(label,record))
    h.write_json(out / 'validation.json',dict(status='PASS',source=source,CLI=cli_info,race_driver=driver_info,runs=results))
    print('PASS actual final600 and fault',flush=True)
except Exception as exc:
    h.write_json(out / 'failure.json',dict(at=h.now(),source=source,error=str(exc),traceback=traceback.format_exc()))
    raise
