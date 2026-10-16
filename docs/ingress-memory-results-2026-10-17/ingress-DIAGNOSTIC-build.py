#!/usr/bin/env python3
"""Build a transparently instrumented diagnostic CLI; never starts a service."""
import datetime
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
GO = GOROOT + '/bin/go'
OVERLAY = OUT / 'ingress-DIAGNOSTIC-server-module-overlay.json'
BINARY = OUT / 'ingress-DIAGNOSTIC-weir-linux'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def state(directory):
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=directory, text=True).strip()
    status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=directory, text=True)
    files = subprocess.check_output(['git', 'ls-files', '-z'], cwd=directory).decode().split('\0')
    sources = {name: sha(directory / name) for name in files if (name.endswith('.go') and not name.startswith('docs/')) or name in ('go.mod', 'go.sum')}
    return {'revision': revision, 'porcelain': status, 'sources': sources}

before = state(BASE)
primary_before = state(PRIMARY)
assert before['revision'] == '0288974d8b64c199da4f0d7fb7f03d8a33978eee'
assert before['porcelain'] == primary_before['porcelain'] == ''
original_driver = PRIMARY / 'internal/app/ingress_memory_budget_integration_test.go'
original_raw = OUT / 'ingress-memory-main-A-process.txt'
assert sha(original_driver) == '941d8dbb37b1449dfdb0a8f6c33e5402eafc70b6ca97ab2c100b8c8f8df35c07'
assert sha(original_raw) == 'e9d7d060cbcaed89b1d7b74474aaf575db4489f17befe9dc4f9efe44ea0b23d2'
env_delta = {'GOROOT': GOROOT, 'GOENV': 'off', 'GOTOOLCHAIN': 'local', 'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOWORK': 'off', 'GOFLAGS': '', 'CGO_ENABLED': '0', 'GOOS': 'linux', 'GOARCH': 'arm64'}
env = os.environ.copy()
env.update(env_delta)
modfile = OUT / 'ingress-DIAGNOSTIC.mod'
args = [GO, 'build', '-trimpath', '-buildvcs=true', '-mod=readonly', '-modfile=' + str(modfile), '-overlay=' + str(OVERLAY), '-ldflags=-buildid=', '-o', str(BINARY), './cmd/weir']
command = {'cwd': str(BASE), 'env': env_delta, 'args': args, 'not_a_clean_commit_binary': True, 'starts_service': False}
(OUT / 'ingress-DIAGNOSTIC-build-command.json').write_text(json.dumps(command, indent=2) + '\n')
started = time.monotonic()
with (OUT / 'ingress-DIAGNOSTIC-build.txt').open('w') as output:
    result = subprocess.run(args, cwd=BASE, env=env, stdout=output, stderr=subprocess.STDOUT)
elapsed = time.monotonic() - started
(OUT / 'ingress-DIAGNOSTIC-build.exit').write_text(str(result.returncode) + '\n')
after = state(BASE)
primary_after = state(PRIMARY)
provenance_path = OUT / 'ingress-DIAGNOSTIC-server-module-provenance.json'
provenance = json.loads(provenance_path.read_text())
receipt = {
    'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'purpose': 'DIAGNOSTIC ONLY; not a production fix and not qualification',
    'actual_build_exit': result.returncode,
    'build_seconds': elapsed,
    'command': command,
    'binary': str(BINARY),
    'binary_sha256': sha(BINARY) if result.returncode == 0 else None,
    'base_before': before,
    'base_after': after,
    'primary_revision': primary_after['revision'],
    'primary_clean_unchanged': primary_before == primary_after,
    'controlled_base_clean_unchanged': before == after,
    'expected_version': provenance['expected_version'],
    'version_runtime_inspection': 'NOT_RUN: no Docker/CLI service or version process was started; expected identity is enforced by explicit version.go overlay',
    'overlay': str(OVERLAY),
    'overlay_sha256': sha(OVERLAY),
    'provenance': str(provenance_path),
    'provenance_sha256': sha(provenance_path),
    'source_overlays': provenance['sources'],
    'modfile': str(modfile),
    'modfile_sha256': sha(modfile),
    'grpc_copy_manifest': str(OUT / 'ingress-DIAGNOSTIC-grpc-copy-manifest.json'),
    'grpc_copy_manifest_sha256': sha(OUT / 'ingress-DIAGNOSTIC-grpc-copy-manifest.json'),
    'original_A_driver_sha256_after': sha(original_driver),
    'original_A_raw_sha256_after': sha(original_raw),
    'not_run': ['database access', 'CLI service load', 'Docker', 'A diagnostic profile', '1024MiB', 'B', 'Lua', 'formal600'],
    'limitations': provenance['semantics'],
}
receipt_path = OUT / 'ingress-DIAGNOSTIC-build-receipt.json'
receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
assert result.returncode == 0, 'Build failed; preserved raw output/exit/receipt'
assert before == after and primary_before == primary_after
print(json.dumps({'receipt': str(receipt_path), 'receipt_sha256': sha(receipt_path), 'binary': str(BINARY), 'binary_sha256': receipt['binary_sha256'], 'exit': result.returncode, 'seconds': elapsed}))
