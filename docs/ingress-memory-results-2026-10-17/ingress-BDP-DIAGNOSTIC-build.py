#!/usr/bin/env python3
"""One trace-preserving BDP causal-comparison build, without running it."""
import datetime
import difflib
import hashlib
import json
import os
import pathlib
import subprocess
import time

BASE = pathlib.Path('/Users/liran/.codex/worktrees/native-burst-admission/weir')
PRIMARY = pathlib.Path('/Users/liran/Projects/liran/go/weir')
OUT = PRIMARY / '.testdata/reliability-2026-10-16'
GOROOT = '/Users/liran/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64'
PREFIX = 'ingress-BDP-DIAGNOSTIC-'
REVISION = '0288974d8b64c199da4f0d7fb7f03d8a33978eee'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def state(directory):
    names = subprocess.check_output(['git', 'ls-files', '-z'], cwd=directory).decode().split('\0')
    return {
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=directory, text=True).strip(),
        'porcelain': subprocess.check_output(['git', 'status', '--porcelain'], cwd=directory, text=True),
        'sources': {name: sha(directory / name) for name in names if (name.endswith('.go') and not name.startswith('docs/')) or name in ('go.mod', 'go.sum')},
    }

before = state(BASE)
primary_before = state(PRIMARY)
assert before['revision'] == REVISION
assert before['porcelain'] == primary_before['porcelain'] == ''
c0_receipt = OUT / 'ingress-DIAGNOSTIC-build-receipt.json'
c0 = json.loads(c0_receipt.read_text())
assert c0['binary_sha256'] == sha(pathlib.Path(c0['binary'])) == 'e5116920123a3c5b76ba4589b6cc2df4b772a9b593f472514f4f353307a00fae'
for source in c0['source_overlays']:
    assert sha(pathlib.Path(source['overlay'])) == source['overlay_sha256']
    assert sha(pathlib.Path(source['target'])) == source['original_sha256']

original_map = pathlib.Path(c0['overlay'])
replacement = json.loads(original_map.read_text())['Replace']
target = BASE / 'internal/server/server.go'
original_server = target.read_text()
window_lines = '\t\tgrpc.InitialWindowSize(64 << 10),\n\t\tgrpc.InitialConnWindowSize(64 << 10),\n'
assert original_server.count(window_lines) == 1
server = OUT / (PREFIX + 'weir-server.go.txt')
server.write_text(original_server.replace(window_lines, '', 1))
replacement[str(target)] = str(server)
version_target = BASE / 'cmd/weir/version.go'
original_version = pathlib.Path(replacement[str(version_target)]).read_text()
assert original_version.count('DIAGNOSTIC-ingress-stages') == 1
version = OUT / (PREFIX + 'weir-version.go.txt')
version.write_text(original_version.replace('DIAGNOSTIC-ingress-stages', 'DIAGNOSTIC-flow-control', 1))
replacement[str(version_target)] = str(version)
formatter = [GOROOT + '/bin/gofmt', '-w', str(server), str(version)]
subprocess.run(formatter, check=True)
overlay = OUT / (PREFIX + 'server-module-overlay.json')
overlay.write_text(json.dumps({'Replace': replacement}, indent=2) + '\n')
changes = ''.join(difflib.unified_diff(original_server.splitlines(True), server.read_text().splitlines(True), fromfile=str(target), tofile=str(server)))
changes += ''.join(difflib.unified_diff(original_version.splitlines(True), version.read_text().splitlines(True), fromfile='original-DIAGNOSTIC-effective-version.go', tofile=str(version)))
(OUT / (PREFIX + 'relative-C0.diff')).write_text(changes)

modfile = OUT / 'ingress-BDP-DIAGNOSTIC.mod'
sumfile = OUT / 'ingress-BDP-DIAGNOSTIC.sum'
modfile.write_bytes((OUT / 'ingress-DIAGNOSTIC.mod').read_bytes())
sumfile.write_bytes((OUT / 'ingress-DIAGNOSTIC.sum').read_bytes())
assert sha(modfile) == c0['modfile_sha256']
env_delta = {'GOROOT': GOROOT, 'GOENV': 'off', 'GOTOOLCHAIN': 'local', 'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOWORK': 'off', 'GOFLAGS': '', 'CGO_ENABLED': '0', 'GOOS': 'linux', 'GOARCH': 'arm64'}
env = os.environ.copy()
env.update(env_delta)
binary = OUT / (PREFIX + 'weir-linux')
args = [GOROOT + '/bin/go', 'build', '-trimpath', '-buildvcs=true', '-mod=readonly', '-modfile=' + str(modfile), '-overlay=' + str(overlay), '-ldflags=-buildid=', '-o', str(binary), './cmd/weir']
command = {'cwd': str(BASE), 'env': env_delta, 'args': args, 'purpose': 'DIAGNOSTIC causal comparison only; no load started'}
(OUT / (PREFIX + 'build-command.json')).write_text(json.dumps(command, indent=2) + '\n')
started = time.monotonic()
with (OUT / (PREFIX + 'build.txt')).open('w') as output:
    result = subprocess.run(args, cwd=BASE, env=env, stdout=output, stderr=subprocess.STDOUT)
elapsed = time.monotonic() - started
(OUT / (PREFIX + 'build.exit')).write_text(str(result.returncode) + '\n')
after = state(BASE)
primary_after = state(PRIMARY)
receipt = {
    'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'purpose': 'DIAGNOSTIC-flow-control causal comparison; not production qualification',
    'actual_build_exit': result.returncode, 'build_seconds': elapsed,
    'C0_receipt': str(c0_receipt), 'C0_receipt_sha256': sha(c0_receipt),
    'C0_binary_sha256': c0['binary_sha256'],
    'binary': str(binary), 'binary_sha256': sha(binary) if result.returncode == 0 else None,
    'expected_version': {'version': 'DIAGNOSTIC-flow-control', 'state': 'DIAGNOSTIC-overlay', 'dirty': 'true', 'revision': REVISION, 'target': 'linux/arm64'},
    'runtime_version_inspection': 'NOT_RUN; must be asserted by the owned driver before diagnostic load',
    'command': command, 'overlay': str(overlay), 'overlay_sha256': sha(overlay),
    'relative_C0_diff': str(OUT / (PREFIX + 'relative-C0.diff')),
    'relative_C0_diff_sha256': sha(OUT / (PREFIX + 'relative-C0.diff')),
    'new_sources': [{'target': str(target), 'overlay': str(server), 'base_sha256': sha(target), 'overlay_sha256': sha(server)}, {'target': str(version_target), 'overlay': str(version), 'C0_effective_sha256': hashlib.sha256(original_version.encode()).hexdigest(), 'overlay_sha256': sha(version)}],
    'modfile': str(modfile), 'modfile_sha256': sha(modfile), 'sumfile': str(sumfile), 'sumfile_sha256': sha(sumfile),
    'same_version_grpc_copy_manifest': c0['grpc_copy_manifest'], 'same_version_grpc_copy_manifest_sha256': c0['grpc_copy_manifest_sha256'],
    'base_before': before, 'base_after': after,
    'base_clean_unchanged': before == after, 'primary_clean_unchanged': primary_before == primary_after,
    'original_driver_sha256': sha(PRIMARY / 'internal/app/ingress_memory_budget_integration_test.go'),
    'original_A_raw_sha256': sha(OUT / 'ingress-memory-main-A-process.txt'),
    'C0_diagnostic_raw_sha256': sha(OUT / 'ingress-DIAGNOSTIC-driver-process.txt'),
    'only_effective_delta': ['Remove grpc.InitialWindowSize(64<<10) and grpc.InitialConnWindowSize(64<<10) from server options, restoring upstream dynamic BDP defaults.', 'Change transparent version label from DIAGNOSTIC-ingress-stages to DIAGNOSTIC-flow-control; state remainsDIAGNOSTIC-overlay and dirty=true.'],
    'unchanged': ['All trace hooks and standard SDK/protobuf paths', 'All shared ExecutionMemory budgets/counts and business logic', 'Same grpc v1.83.2 exact public source copy and identical same-version modfile replace'],
    'planned_profile': {'capacity_MiB':512,'CPUs':2,'calls':32,'BSON_bytes':12<<20,'caller_seconds':5,'retries':0},
    'not_run': ['this diagnostic load', 'DB', 'Docker', 'CLI service or version process', '1024/B/Lua/formal600'],
    'limits': ['No causal outcome is asserted by this build. Faster ingress can expose existing predecode memory gaps and OOM; no reduced load or relaxed deadline is permitted.', 'Trace timestamps and logs can perturb timing; neither C0 nor this candidate is a qualification binary.'],
}
path = OUT / (PREFIX + 'build-receipt.json')
path.write_text(json.dumps(receipt, indent=2) + '\n')
assert result.returncode == 0, 'Build failed; preserved raw/exit/receipt'
assert before == after and primary_before == primary_after
assert receipt['original_driver_sha256'] == '941d8dbb37b1449dfdb0a8f6c33e5402eafc70b6ca97ab2c100b8c8f8df35c07'
assert receipt['original_A_raw_sha256'] == 'e9d7d060cbcaed89b1d7b74474aaf575db4489f17befe9dc4f9efe44ea0b23d2'
print(json.dumps({'receipt': str(path), 'receipt_sha256': sha(path), 'binary': str(binary), 'binary_sha256': receipt['binary_sha256'], 'exit': result.returncode, 'seconds': elapsed}))
