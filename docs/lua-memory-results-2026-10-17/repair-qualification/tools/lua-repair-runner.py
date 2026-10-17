#!/usr/bin/env python3
"""Single-attempt, clean-commit local Lua qualification or offline packaging.

Preparation does not run this module. Qualification creates only the fixtures
already owned by the unchanged integration tests; extra backend checks are reads.
"""
import argparse
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shlex
import socket
import stat
import subprocess
import sys
import tarfile
import threading
import time
import urllib.parse
import urllib.request


ROOT = Path('/Users/liran/Projects/liran/go/weir')
EVIDENCE = ROOT / '.testdata/reliability-2026-10-16'
TOOLCHAIN = '/Users/liran/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64'
GO = TOOLCHAIN + '/bin/go'
GRPC_MANIFEST_SHA = '43e88ff7f4928ddaaadb1a7379b77c23f603208a7402bc3a3327c187d6043987'
ORIGINAL_SHA = '36cd530f20b03a2e72f6c913bf583910cde2b163b594556480592261ca252012'
PATHS_SHA = 'a12c37954a9272a27d6e298bb092859bdc18fd45a034b62e73fb5aa496a6ed61'
IDLE_HELPER_SHA = '4eda4766b936b16bb405a17c68cca1195858e8ed9501bc284277c7e680701113'
BUSYBOX_ID = 'sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b'
SEARCH_ID = '0a8037715b19fd64c43641f9fc448bf917a608410b252b1ddb591c51bb91349b'
SEARCH_IMAGE = 'docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237'
PINS = {'google.golang.org/grpc': ('v1.83.2', './third_party/grpc'),
        'github.com/iceisfun/golua': ('v1.1.1', './third_party/golua')}
PATH_CASES = {'increment': ('APPLIED', ''),
              'pcall-concat': ('NOT_APPLIED', 'RESOURCE_EXHAUSTED'),
              'shared-dag': ('NOT_APPLIED', 'RESOURCE_EXHAUSTED'),
              'large-error': ('NOT_APPLIED', 'INVALID_ARGUMENT'),
              'same-key-comments': ('APPLIED', ''),
              'control-quote': ('APPLIED', '')}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def write_json(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def git(args):
    return subprocess.check_output(['git', *args], cwd=ROOT).decode().strip()


def clean_source(revision):
    require(git(['rev-parse', 'HEAD']) == revision, 'HEAD differs from explicit immutable H')
    require(not git(['status', '--porcelain', '--untracked-files=normal']),
            'requires a clean committed tree, including untracked files')
    manifest_path = ROOT / 'third_party/grpc/WEIR_UPSTREAM.json'
    manifest_raw = manifest_path.read_bytes()
    require(digest(manifest_raw) == GRPC_MANIFEST_SHA, 'public gRPC provenance pin changed')
    grpc = json.loads(manifest_raw)
    names = git(['ls-files', '-z']).split('\0')
    selected = []
    for name in filter(None, names):
        parts = Path(name).parts
        sensitive = any(part.lower() in ('.ssh', 'credentials', 'keyfile') or
                        part.lower().startswith('.env') or
                        part.lower().endswith(('.pem', '.key')) for part in parts)
        public = (name.startswith('third_party/grpc/') and name.endswith('.go') and
                  name.removeprefix('third_party/grpc/') in grpc['files'])
        require(not sensitive or public, 'refused sensitive tracked path before opening: ' + name)
        include = (name.endswith(('.go', '.py', '.lua', '.yaml', '.yml')) or
                   Path(name).name in ('go.mod', 'go.sum', 'LICENSE', 'AUTHORS',
                                       'Dockerfile', 'WEIR_UPSTREAM.json',
                                       'WEIR_PATCH.md', 'WEIR_PATCH.diff'))
        if include:
            selected.append((name, sensitive))
    files = {}
    for name, sensitive in sorted(selected):
        path = ROOT / name
        require(path.is_file() and not path.is_symlink(), 'non-regular source refused: ' + name)
        raw = path.read_bytes()
        value = digest(raw)
        if sensitive:
            expected = grpc['files'][name.removeprefix('third_party/grpc/')]
            require(value == expected, 'public credentials Go source differs from exact pin: ' + name)
        files[name] = value
    required = {'internal/app/lua_memory_budget_integration_test.go': ORIGINAL_SHA,
                'internal/app/lua_memory_paths_integration_test.go': PATHS_SHA,
                'internal/app/ingress_memory_budget_integration_test.go': IDLE_HELPER_SHA}
    for name, expected in required.items():
        require(files.get(name) == expected, 'fixed qualification/ownership contract changed: ' + name)
    for module, pin in PINS.items():
        version, relative = pin
        directory = ROOT / relative
        manifest = json.loads((directory / 'WEIR_UPSTREAM.json').read_text())
        require(manifest['module'] == module and manifest['version'] == version,
                'unexpected patched upstream identity: ' + module)
        patched, new = manifest.get('patchedFiles'), manifest.get('newFiles')
        require(isinstance(patched, list) and patched and isinstance(new, list) and new,
                'patch manifest is not finalized: ' + module)
        expected = dict(manifest['files'])
        for record in patched:
            name = record['path']
            require(name in expected and record['upstream_sha256'] == expected[name],
                    'patch upstream preimage differs: ' + name)
            expected[name] = record['sha256']
        for record in new:
            name = record['path']
            require(name not in expected, 'duplicate new patch source: ' + name)
            expected[name] = record['sha256']
        for name, expected_digest in expected.items():
            key = str(Path(relative) / name)
            require(files.get(key) == expected_digest, 'patch manifest source mismatch: ' + key)
    result = {'at': now(), 'revision': revision, 'status': 'clean',
              'collection': 'All tracked Go/Python/Lua/YAML and recursive module/provenance/license files',
              'files_count': len(files), 'files_sha256': files}
    return result


def environment(output):
    keep = ('HOME', 'PATH', 'TMPDIR', 'SYSTEMROOT', 'GOMODCACHE')
    env = {key: os.environ[key] for key in keep if key in os.environ}
    env.update(GOROOT=TOOLCHAIN, GOENV='off', GOTOOLCHAIN='local', GOPROXY='off',
               GOSUMDB='off', GOWORK='off', GOFLAGS='', GOEXPERIMENT='',
               GOAMD64='v1', GOARM64='v8.0', PYTHONDONTWRITEBYTECODE='1',
               MONGOSH_DISABLE_TELEMETRY='1')
    env['PATH'] = TOOLCHAIN + '/bin:' + env.get('PATH', '')
    socket_url = os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock')
    parsed = urllib.parse.urlsplit(socket_url)
    require(parsed.scheme == 'unix' and not parsed.netloc and parsed.path.startswith('/') and
            not parsed.query and not parsed.fragment, 'requires only the local Docker Unix socket')
    require(stat.S_ISSOCK(os.stat(parsed.path).st_mode), 'Docker endpoint is not a Unix socket')
    config = output / 'docker-client'
    config.mkdir()
    write_json(config / 'config.json', {'auths': {}})
    env.update(DOCKER_HOST=socket_url, DOCKER_CONFIG=str(config))
    return env


def run_stage(options):
    name, command, output, env = options['name'], options['command'], options['output'], options['env']
    timeout = options.get('timeout', 120)
    record = {'started_utc': now(), 'cwd': str(ROOT), 'args': command,
              'env': {key: env[key] for key in sorted(env) if key not in ('HOME', 'PATH', 'TMPDIR')},
              'timeout_seconds': timeout, 'retry': False}
    write_json(output / (name + '-command.json'), record)
    print('START ' + name + ' ' + record['started_utc'], flush=True)
    log = output / (name + '-process.txt')
    started = time.monotonic()
    expired = threading.Event()
    with log.open('xb') as stream:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT)
        write_json(output / (name + '-pid.json'), {'pid': process.pid, 'at': now()})
        def expire():
            expired.set()
            process.kill()
        timer = threading.Timer(timeout, expire)
        timer.start()
        try:
            code = process.wait()
        finally:
            timer.cancel()
    (output / (name + '-process.exit')).write_text(str(code) + '\n')
    record.update(finished_utc=now(), elapsed_seconds=time.monotonic() - started,
                  exit_code=code, pid=process.pid, timed_out=expired.is_set(),
                  raw=str(log), raw_bytes=log.stat().st_size, raw_sha256=digest(log.read_bytes()))
    write_json(output / (name + '-receipt.json'), record)
    print('END ' + name + ' exit=' + str(code), flush=True)
    return record


def stage(options):
    record = run_stage(options)
    require(record['exit_code'] == 0 and not record['timed_out'], 'stage failed: ' + options['name'])
    return record


def docker_get(options):
    parsed = urllib.parse.urlsplit(options['env']['DOCKER_HOST'])
    connection = http.client.HTTPConnection('localhost', timeout=3)
    connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.sock.settimeout(3)
    connection.sock.connect(parsed.path)
    try:
        connection.request('GET', options['path'], headers={'Connection': 'close'})
        response = connection.getresponse()
        raw = response.read((2 << 20) + 1)
        require(len(raw) <= 2 << 20, 'Docker response exceeds bounded 2 MiB')
        value = json.loads(raw) if raw else None
        result = {'status': response.status, 'body': value}
        return result
    finally:
        connection.close()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def search_get(path):
    require(path.startswith('/') and '?' not in path, 'invalid fixed Search read path')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    request = urllib.request.Request('http://127.0.0.1:19200' + path, method='GET')
    try:
        response = opener.open(request, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read((2 << 20) + 1)
        require(len(raw) <= 2 << 20, 'Search response exceeds bounded 2 MiB')
        result = {'status': response.status, 'body': json.loads(raw),
                  'raw_bytes': len(raw), 'raw_sha256': digest(raw)}
        return result


def mongo_read(options):
    uri = ('mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll&'
           'serverSelectionTimeoutMS=3000&retryReads=false&retryWrites=false')
    command = ['mongosh', '--norc', '--quiet', uri, '--eval', options['javascript']]
    step = {'name': options['name'], 'command': command, 'output': options['output'],
            'env': options['env'], 'timeout': 12}
    record = stage(step)
    raw = Path(record['raw']).read_text().strip()
    value = json.loads(raw)
    return value


def preflight(options):
    output, env = options['output'], options['env']
    owner = ROOT / '.testdata/mongo-local/.weir-owner'
    require(owner.read_text().strip() == 'weir-mongo-local mongodb-8.0.32 loopback-27028',
            'MongoDB owner marker differs')
    pid = (ROOT / '.testdata/mongo-local/mongod.pid').read_text().strip()
    require(pid.isdecimal(), 'MongoDB PID marker is invalid')
    actual = subprocess.check_output(['ps', '-p', pid, '-o', 'args='], text=True).strip()
    expected = [str(ROOT / '.tools/mongodb-macos-aarch64--8.0.32/bin/mongod'),
                '--dbpath', str(ROOT / '.testdata/mongo-local'), '--bind_ip', '127.0.0.1',
                '--port', '27028', '--replSet', 'weir_local', '--setParameter',
                'enableTestCommands=1', '--logpath', str(ROOT / '.testdata/mongo-local/mongod.log'),
                '--pidfilepath', str(ROOT / '.testdata/mongo-local/mongod.pid'), '--fork']
    require(shlex.split(actual) == expected, 'MongoDB process identity/loopback ownership differs')
    javascript = ('const h=db.adminCommand({hello:1});const b=db.adminCommand({buildInfo:1});'
                  'print(JSON.stringify({hello_ok:h.ok,set:h.setName,primary:h.isWritablePrimary,'
                  'build_ok:b.ok,version:b.version}));')
    mongo_opts = dict(options, name='preflight-mongo-read', javascript=javascript)
    mongo = mongo_read(mongo_opts)
    require(mongo == {'hello_ok': 1, 'set': 'weir_local', 'primary': True,
                      'build_ok': 1, 'version': '8.0.32'}, 'wrong isolated MongoDB server')
    info = docker_get({'env': env, 'path': '/info'})
    require(info['status'] == 200, 'Docker info unavailable')
    info = info['body']
    require(info.get('OSType') == 'linux' and info.get('Architecture') == 'aarch64' and
            info.get('CgroupVersion') == '2', 'requires native Linux arm64 cgroup v2')
    image = docker_get({'env': env, 'path': '/images/busybox:1.37.0/json'})
    require(image['status'] == 200 and image['body']['Id'] == BUSYBOX_ID and
            image['body']['Os'] == 'linux' and image['body']['Architecture'] == 'arm64',
            'pinned native BusyBox fixture changed or missing')
    search = docker_get({'env': env, 'path': '/containers/weir-local-elasticsearch/json'})
    require(search['status'] == 200, 'owned Search fixture missing')
    search = search['body']
    require(search['Id'] == SEARCH_ID and search['Config']['Labels'].get('weir.owner') ==
            'weir-search-local' and search['Config']['Image'] == SEARCH_IMAGE and
            search['State']['Running'] is True, 'owned pinned Search fixture differs')
    version = search_get('/')
    require(version['status'] == 200 and version['body']['cluster_name'] == 'weir-local-elasticsearch'
            and version['body']['version']['number'] == '8.19.22', 'wrong isolated Search server')
    filters = urllib.parse.quote(json.dumps({'name': ['^/weir-memory-recovery-']}))
    containers = docker_get({'env': env, 'path': '/containers/json?all=1&filters=' + filters})
    require(containers['status'] == 200 and containers['body'] == [],
            'existing owned qualification container must be reconciled before a new run')
    result = {'at': now(), 'MongoDB': mongo, 'MongoDB_pid': int(pid),
              'MongoDB_command': actual, 'MongoDB_owner_sha256': digest(owner.read_bytes()),
              'Docker': {key: info.get(key) for key in
                         ('ServerVersion', 'OSType', 'Architecture', 'CgroupVersion', 'KernelVersion')},
              'BusyBox': BUSYBOX_ID, 'Search_container': SEARCH_ID,
              'Search_image': SEARCH_IMAGE, 'Search': version,
              'existing_owned_qualification_containers': 0, 'scope': 'read-only local fixture checks'}
    write_json(output / 'preflight.json', result)
    return result


def build_info(options):
    step = dict(options, command=[GO, 'version', '-m', str(options['binary'])], timeout=30)
    record = stage(step)
    raw = Path(record['raw']).read_text()
    require(raw.splitlines()[0].endswith(': go1.27.1'), 'binary compiler is not Go 1.27.1')
    settings, patches, dependency = {}, {}, None
    for line in raw.splitlines()[1:]:
        fields = line.strip().split('\t')
        if fields[0] == 'build':
            key, value = fields[1].split('=', 1)
            settings[key] = value
        elif fields[0] == 'dep':
            dependency = fields[1:3]
        elif fields[0] == '=>':
            module = dependency[0] if dependency else None
            require(module in PINS and module not in patches and
                    dependency[1] == PINS[module][0] and fields[1:] == [PINS[module][1], '(devel)'],
                    'unexpected linked replacement dependency')
            patches[module] = fields[1]
    required = dict(options['required'], **{'vcs.revision': options['source'], 'vcs.modified': 'false'})
    require(all(settings.get(key) == value for key, value in required.items()),
            'actual binary target/race/VCS settings differ: ' + str(settings))
    require(set(patches) == set(PINS), 'binary is missing one of the two pinned local forks')
    result = {'source': options['source'], 'binary': str(options['binary']),
              'binary_sha256': digest(options['binary'].read_bytes()),
              'settings': settings, 'patches': patches, 'raw_sha256': record['raw_sha256']}
    write_json(options['output'] / (options['name'] + '-validated.json'), result)
    return result


def evidence_lines(raw, marker):
    result = [{'raw_line': number, 'text': line.strip()}
              for number, line in enumerate(raw.splitlines(), 1) if marker in line]
    return result


def validate_runtime(options):
    record, output, source = options['record'], options['output'], options['source']
    raw = Path(record['raw']).read_text()
    supplemental = options['supplemental']
    problems = []
    def check(condition, message):
        if not condition:
            problems.append(message)
    expected_test = 'TestCLILuaMemoryPaths' if supplemental else 'TestCLILuaMemoryBudget'
    check(record['exit_code'] == 0 and not record['timed_out'], 'actual race-driver exit was not zero')
    check(len(evidence_lines(raw, '=== RUN   ' + expected_test)) == 1 and
          len(evidence_lines(raw, '--- PASS: ' + expected_test + ' (')) == 1 and
          not re.search(r'(^--- FAIL:|^panic:|^WARNING: DATA RACE|^FAIL$|^--- SKIP:)', raw, re.M),
          'selected test did not run and pass exactly once without failure/skip/race')
    identity_marker = 'Lua_paths fixed512MiB/2CPU' if supplemental else 'Lua fixed512MiB/2CPU'
    identities = evidence_lines(raw, identity_marker)
    check(len(identities) == 1 and 'source=' + source in identities[0]['text'] and
          'binary_SHA256=' + options['cli_sha256'] in identities[0]['text'] and
          'single_process=true' in identities[0]['text'] and
          '"state":"clean-commit"' in identities[0]['text'] and
          '"dirty":"false"' in identities[0]['text'], 'exact runtime CLI identity missing')
    docker_states = evidence_lines(raw, 'ingress_Docker at=')
    owners = []
    for line in docker_states:
        match = re.search(r'state=(weir-memory-recovery-[0-9]+-[0-9]+) (true|false) (true|false) (-?\d+) error=(.*)$', line['text'])
        check(match is not None, 'unknown/malformed independent Docker observation')
        if match:
            owners.append(match[1])
            check(match.groups()[1:] == ('true', 'false', '0', '<nil>'), 'Docker observed stopped/OOM/nonzero/unknown state')
    owner_set = sorted(set(owners))
    check(len(docker_states) >= 2 and len(owner_set) == 1, 'missing unique independently observed owned CLI')
    check(len(evidence_lines(raw, 'CLI exit=false 0')) == 1, 'normal CLI exit zero was not confirmed')
    closed = evidence_lines(raw, 'backend_connections_closed ')
    closed_backends = set()
    for line in closed:
        fields = dict(re.findall(r'([a-z_]+)=([^\s]+)', line['text']))
        closed_backends.add(fields.get('backend'))
        check(fields.get('owned') == '0' and fields.get('acquired') == fields.get('released'),
              'backend connection accounting was not fully closed')
        if fields.get('backend') == 'mongo':
            check(fields.get('dialing') == '0' and fields.get('closing') == '0',
                  'MongoDB retained a dialing/closing connection')
    check(closed_backends == ({'mongo', 'search'} if supplemental else {'mongo'}),
          'missing final backend-owned connection accounting')
    idle_zero = evidence_lines(raw, 'ingress_final5s temporary_Node/Store/RPC/ingress/wire/Q/I_ownership0 latch0 connections=0 connection_bytes=0')
    check(len(idle_zero) == 1, 'all owners / Q / I / connection bytes were not confirmed zero after SDK closure')
    cgroups = evidence_lines(raw, 'cgroup_memory_events phase=')
    expected_phases = ({'Lua_paths_initial', 'Lua_paths_final', 'Lua_paths_cleanup'} if supplemental
                       else {'Lua_initial', 'Lua_after_probe', 'Lua_final_cleanup'})
    phases = set()
    for line in cgroups:
        match = re.search(r'phase=(\S+) value=(".*") error=(.*)$', line['text'])
        check(match is not None, 'malformed cgroup memory.events record')
        if not match:
            continue
        phases.add(match[1])
        try:
            values = dict(item.split() for item in json.loads(match[2]).splitlines())
        except (ValueError, TypeError):
            values = {}
        check(match[3] == '<nil>' and all(values.get(name) == '0' for name in ('oom', 'oom_kill', 'oom_group_kill')),
              'cgroup OOM counters were nonzero or unobservable')
    check(phases == expected_phases, 'missing initial/final/cleanup cgroup observations')
    metrics = []
    for line in evidence_lines(raw, 'ingress_sample at='):
        match = re.search(r'error=<nil> metrics=(\{.*\})$', line['text'])
        check(match is not None, 'metrics scrape was unknown/failed')
        if match:
            metrics.append(json.loads(match[1]))
    check(bool(metrics), 'no successful process-memory observations')
    for sample in metrics:
        check(sample.get('weir_memory_cgroup_valid') == 1 and
              sample.get('weir_memory_cgroup_limit_bytes') == 512 << 20 and
              sample.get('weir_memory_unknown') == 0 and
              sample.get('weir_memory_go_runtime_managed') == 1,
              'sampled default automatic/cgroup policy was unknown or changed')
    details = {}
    if supplemental:
        starts, outcomes = evidence_lines(raw, 'Lua_path_start store='), evidence_lines(raw, 'Lua_path_outcome case=')
        check(len(starts) == 12 and len(outcomes) == 12, 'supplement did not execute all 12 cases exactly once')
        seen = set()
        for line in outcomes:
            match = re.search(r'case=(mongo|search)/(\S+) outcome=(\S+) failure=(\S*) RPC=(\S+) error=(.*?) returned=(\S+) within_original_deadline=(\S+).* failure_message_bytes=(\d+) ', line['text'])
            check(match is not None, 'malformed Lua path outcome')
            if not match:
                continue
            key = (match[1], match[2])
            check(key not in seen, 'duplicate Lua path outcome')
            seen.add(key)
            expected = PATH_CASES.get(match[2])
            check(expected == (match[3], match[4]) and match.groups()[4:8] == ('OK', '<nil>', 'true', 'true') and
                  int(match[9]) <= 4096, 'Lua path lacked its exact timely bounded business outcome')
        check(seen == {(backend, name) for backend in ('mongo', 'search') for name in PATH_CASES},
              'Lua path case set differs from the fixed contract')
        effects = evidence_lines(raw, 'Lua_path_independent_effect store=')
        check(len(effects) == 36, 'missing before/after/recovery full-raw document oracles')
        for line in effects:
            check('full_raw_equal=true error=<nil>' in line['text'], 'a full-raw document oracle differed or was unknown')
        recoveries = evidence_lines(raw, 'Lua_path_same_SDK_recovery store=')
        check(len(recoveries) == 12 and all(' error=<nil> ' in item['text'] for item in recoveries),
              'same-original-SDK recovery failed or incomplete')
        summaries = evidence_lines(raw, 'independent_oracle store=')
        check(len(summaries) == 2 and all('expected=13 seen=13 every_ID_n_pad_checked=true' in item['text'] for item in summaries),
              'final full-backend cardinality/content oracle incomplete')
        details.update(outcomes=outcomes, effects=effects, same_SDK_recovery=recoveries, final_oracles=summaries)
    else:
        check(len(evidence_lines(raw, 'Lua_probe tiny_source_bytes=96 source_SHA256=5056d1b223d893e1ae6ea8f2a80aa80af78b75b1610aae5f65bce6883fdf6730 iterations=29 final_string_bytes=536870912')) == 1,
              'original fixed 96-byte / 29-doubling probe differs')
        control = evidence_lines(raw, 'Lua_normal_control Transform_APPLIED=true ID_n_pad_valid=true bytes=1071 SHA256=e4c35b4ae559db91a7df884b3adb80e5260301dcb722c2047a49e7a9d254ed1c caller3s')
        check(len(control) == 1, 'normal Lua control lacked APPLIED and exact independent BSON hash')
        outcomes = evidence_lines(raw, 'Lua_probe_outcome outcome=')
        check(len(outcomes) == 1, 'original tiny-source probe outcome missing/duplicated')
        if len(outcomes) == 1:
            text = outcomes[0]['text']
            check('RPC=OK error=<nil> returned=true' in text and 'within_original_deadline=true' in text and
                  ('confirmed_successful_Keep=true safe_capacity_rejection=false' in text or
                   'confirmed_successful_Keep=false safe_capacity_rejection=true' in text),
                  'original probe lacked timely complete Keep or explicit safe capacity rejection')
        oracle = evidence_lines(raw, 'Lua_independent_unchanged ID=lua-bound-target bytes_before=1076 bytes_after=1076 before_SHA256=897d51bafa1d9a23ef9728feb897efaa527bccd013f709c60395140f8795da6c after_SHA256=897d51bafa1d9a23ef9728feb897efaa527bccd013f709c60395140f8795da6c unchanged=true error=<nil>')
        check(len(oracle) == 1, 'original target full raw BSON was changed or unknown')
        check(len(evidence_lines(raw, 'same_running_process_and_SDK fresh small Put_APPLIED=true Read_n_pad_valid=true caller3s')) == 1,
              'same original SDK failed fresh business Put/Read recovery')
        details.update(control=control, outcomes=outcomes, independent_unchanged=oracle)
    health = evidence_lines(raw, 'ingress_health SDK_client=')
    check(len(health) == (24 if supplemental else 2) and
          all(item['text'].endswith('error=<nil>') for item in health), 'fresh independent Read/Native health failed/incomplete')
    # Exactly one namespace per driver process, determined by the fixed test fixture.
    db = 'weir_test_' + str(record['pid']) + '_1'
    javascript = ('const r=db.adminCommand({listDatabases:1,nameOnly:true,filter:{name:' +
                  json.dumps(db) + '}});print(JSON.stringify({database:' + json.dumps(db) +
                  ',ok:r.ok,absent:r.ok===1&&Array.isArray(r.databases)&&r.databases.length===0}));')
    cleanup_opts = dict(options, name=options['name'] + '-mongo-absence', javascript=javascript)
    try:
        mongo = mongo_read(cleanup_opts)
        check(mongo == {'database': db, 'ok': 1, 'absent': True}, 'original MongoDB fixture Drop did not leave exact namespace absent')
    except Exception as error:
        mongo = {'error': type(error).__name__ + ': ' + str(error)}
        check(False, 'could not independently establish original MongoDB fixture namespace absence')
    search = None
    if supplemental:
        index = 'weir_search_' + str(record['pid']) + '_1'
        try:
            search = search_get('/' + index)
            check(search['status'] == 404 and search['body'].get('status') == 404 and
                  search['body'].get('error', {}).get('type') == 'index_not_found_exception',
                  'original Search fixture Drop did not leave exact index absent')
        except Exception as error:
            search = {'error': type(error).__name__ + ': ' + str(error)}
            check(False, 'could not independently establish original Search index absence')
    absence = []
    for owner in owner_set:
        observed = docker_get({'env': options['env'], 'path': '/containers/' + owner + '/json'})
        absent = observed['status'] == 404 and observed['body'].get('message') == 'No such container: ' + owner
        check(absent, 'owned qualification CLI container was not independently absent after cleanup')
        absence.append({'owner': owner, 'status': observed['status'], 'absent': absent})
    result = {'created_utc': now(), 'status': 'PASS' if not problems else 'FAIL',
              'source': source, 'runtime_run_count': 1, 'retry': False,
              'profile': {'capacity_MiB': 512, 'CPUs': 2, 'caller_seconds': 3,
                          'single_Weir_process': True, 'default_Store_policy': True},
              'actual_driver_exit': record['exit_code'], 'driver_PID': record['pid'],
              'raw_sha256': record['raw_sha256'], 'source_hash_verified_all_owner_assertions': IDLE_HELPER_SHA,
              'final_zero_owners_evidence': idle_zero, 'independent_Docker_observations': docker_states,
              'normal_exit_evidence': evidence_lines(raw, 'CLI exit='), 'backend_connections': closed,
              'cgroup_events': cgroups, 'successful_metrics_observations': len(metrics),
              'metric_sample_peaks': {key: max(item.get(key, 0) for item in metrics) for key in
                                      ('process_resident_memory_bytes', 'weir_memory_cgroup_current_bytes',
                                       'go_memstats_heap_alloc_bytes', 'weir_execution_memory_used_bytes')} if metrics else {},
              'peak_scope': 'sampled maxima at separate times; not a physical transient peak proof',
              'Mongo_fixture_absence': mongo, 'Search_fixture_absence': search,
              'owned_CLI_container_absence': absence, 'details': details, 'problems': problems}
    write_json(output / (options['name'] + '-validation.json'), result)
    return result


def qualify(options):
    output, env, source = options['output'], options['env'], options['source']
    for attempt in EVIDENCE.glob('lua-repair-*/runtime-started.json'):
        value = json.loads(attempt.read_text())
        require(value.get('source') != source, 'this H already started actual qualification; retain first result instead of retrying')
    preflight(options)
    cli, driver = output / 'weir-linux-arm64', output / 'app-race.test'
    cli_env = dict(env, GOOS='linux', GOARCH='arm64', CGO_ENABLED='0')
    command = [GO, 'build', '-trimpath', '-buildvcs=true', '-mod=readonly',
               '-ldflags=-buildid= -X main.sourceRevision=' + source, '-o', str(cli), './cmd/weir']
    step = dict(options, name='CLI-build', command=command, env=cli_env, timeout=180)
    stage(step)
    required = {'GOOS': 'linux', 'GOARCH': 'arm64', 'CGO_ENABLED': '0', '-trimpath': 'true'}
    info_opts = dict(options, name='CLI-buildinfo', binary=cli, required=required)
    cli_info = build_info(info_opts)
    driver_env = dict(env, GOOS='darwin', GOARCH='arm64', CGO_ENABLED='1')
    command = [GO, 'test', '-c', '-race', '-trimpath', '-buildvcs=true', '-mod=readonly',
               '-tags=integration', '-o', str(driver), './internal/app']
    stage(dict(options, name='driver-build', command=command, env=driver_env, timeout=180))
    required = {'GOOS': 'darwin', 'GOARCH': 'arm64', 'CGO_ENABLED': '1', '-race': 'true', '-trimpath': 'true'}
    driver_info = build_info(dict(options, name='driver-buildinfo', binary=driver, required=required))
    write_json(output / 'runtime-started.json', {'source': source, 'at': now(), 'first_original_probe_attempt': True})
    results = []
    for name, test, supplemental in [('original', 'TestCLILuaMemoryBudget', False),
                                      ('paths', 'TestCLILuaMemoryPaths', True)]:
        clean_source(source)
        run_env = dict(driver_env, WEIR_INTEGRATION='1', WEIR_LUA_MEMORY_BINARY=str(cli),
                       WEIR_LUA_MEMORY_SOURCE=source, WEIR_LUA_MEMORY_SHA256=cli_info['binary_sha256'])
        if supplemental:
            run_env.update(WEIR_LUA_MEMORY_PATHS_INTEGRATION='1', WEIR_SEARCH_INTEGRATION='elasticsearch')
        else:
            run_env['WEIR_LUA_MEMORY_INTEGRATION'] = '1'
        command = [str(driver), '-test.v', '-test.failfast', '-test.count=1',
                   '-test.run=^' + test + '$', '-test.timeout=2m']
        record = run_stage(dict(options, name=name, command=command, env=run_env, timeout=150))
        validation = validate_runtime(dict(options, name=name, record=record, supplemental=supplemental,
                                          cli_sha256=cli_info['binary_sha256']))
        after = clean_source(source)
        write_json(output / (name + '-source-after.json'), after)
        require(after['files_sha256'] == options['before']['files_sha256'], 'qualification source changed during actual run')
        results.append(validation)
        require(validation['status'] == 'PASS', name + ' actual qualification failed; supplemental/retries will not run')
    result = {'CLI': cli_info, 'race_driver': driver_info, 'runs': results}
    return result


def package(options):
    output = options['output']
    artifacts = output / 'artifacts'
    command = ['python3', 'scripts/package.py', '--output', str(artifacts)]
    stage(dict(options, name='package-two-exports-four-targets', command=command, timeout=900))
    comparison = json.loads((artifacts / 'comparison.json').read_text())
    require(comparison.get('source') == options['source'] and comparison.get('completed') is True and
            comparison.get('identical_binaries') == 4 and comparison.get('identical_archives') == 4 and
            comparison.get('oci_identical') is False, 'actual two-export / four-target reproducibility failed')
    checked = []
    for export in ('first', 'second'):
        for target in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64'):
            path = artifacts / export / ('weir-' + target + '.tar.gz')
            with tarfile.open(path, 'r:gz') as archive:
                names = archive.getnames()
                for fork in ('grpc', 'golua'):
                    for name in ('LICENSE', 'WEIR_UPSTREAM.json', 'WEIR_PATCH.md', 'WEIR_PATCH.diff'):
                        member = 'licenses/' + fork + '/' + name
                        require(names.count(member) == 1, 'archive omitted/duplicated patched-fork provenance: ' + member)
                        raw = archive.extractfile(member).read()
                        require(digest(raw) == digest((ROOT / 'third_party' / fork / name).read_bytes()),
                                'archive patched-fork provenance differs: ' + member)
            checked.append({'export': export, 'target': target, 'archive': str(path),
                            'archive_sha256': digest(path.read_bytes()), 'both_fork_license_provenance': True})
    result = {'comparison': comparison, 'independent_archive_provenance': checked,
              'scope': 'actual two independent clean exports / fresh caches / four targets; no OCI, publish, or deploy'}
    write_json(output / 'package-validation.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, help='exact full clean candidate H')
    parser.add_argument('--label', required=True, help='new lowercase evidence suffix; existing output always refused')
    parser.add_argument('--mode', required=True, choices=('qualify', 'package'))
    args = parser.parse_args()
    require(re.fullmatch('[0-9a-f]{40}', args.source) is not None, 'requires full immutable source SHA')
    require(re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', args.label) is not None, 'invalid safe output label')
    require(Path.cwd().resolve() == ROOT, 'run from the primary Weir checkout')
    output = EVIDENCE / ('lua-repair-' + args.label)
    output.mkdir(exist_ok=False)
    started = now()
    receipt = {'source': args.source, 'mode': args.mode, 'started_utc': started,
               'retry': False, 'status': 'INCOMPLETE', 'runner_sha256': digest(Path(__file__).read_bytes())}
    write_json(output / 'attempt.json', receipt)
    before = None
    try:
        before = clean_source(args.source)
        write_json(output / 'source-before.json', before)
        env = environment(output)
        options = {'output': output, 'env': env, 'source': args.source, 'before': before}
        version_step = dict(options, name='toolchain-version', command=[GO, 'version'], timeout=30)
        version = stage(version_step)
        require(Path(version['raw']).read_text().strip() == 'go version go1.27.1 darwin/arm64',
                'unexpected host/toolchain version')
        result = qualify(options) if args.mode == 'qualify' else package(options)
        receipt.update(status='PASS', result=result)
    except BaseException as error:
        receipt.update(status='FAIL', error=type(error).__name__ + ': ' + str(error))
    finally:
        try:
            after = clean_source(args.source)
            write_json(output / 'source-after.json', after)
            unchanged = before is not None and before['files_sha256'] == after['files_sha256']
            receipt['source_before_after_clean_unchanged'] = unchanged
            if not unchanged:
                receipt['status'] = 'FAIL'
        except BaseException as error:
            receipt.update(status='FAIL', source_after_error=type(error).__name__ + ': ' + str(error))
        receipt.update(finished_utc=now(), output_directory=str(output))
        write_json(output / 'validation.json', receipt)
        print(json.dumps({'status': receipt['status'], 'source': args.source, 'mode': args.mode,
                          'validation': str(output / 'validation.json'), 'error': receipt.get('error')}, indent=2), flush=True)
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
