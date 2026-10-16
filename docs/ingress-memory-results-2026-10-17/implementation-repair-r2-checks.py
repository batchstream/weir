from pathlib import Path
import subprocess, json, hashlib, os, time
from datetime import datetime, timezone
root = Path('/Users/liran/.codex/worktrees/native-burst-admission/weir')
evidence = root / '.testdata/ingress-offline'
go_root = '/Users/liran/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64'
go = go_root + '/bin/go'
env = dict(os.environ, GOROOT=go_root, GOENV='off', GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOWORK='off')
receipt = {'status': 'RUNNING', 'checks': [], 'qualification_profiles_unchanged': True,
    'unit_fixture_note': 'Wait-only fixture uses 18MiB instead of 24MiB under unchanged 192MiB allowance: the complete prepaid receive peak changed from 3N to 4N+frame slack; both individual messages fit and two cannot coexist. Formal A32x12MiB/512MiB/2CPU/5s is unchanged.'}
def inventory():
    result = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=root, check=True, stdout=subprocess.PIPE)
    names = [n.decode() for n in result.stdout.split(b'\0') if n]
    selected = [n for n in names if n.endswith('.go') or Path(n).name in {'go.mod', 'go.sum'} or n in {'third_party/grpc/WEIR_UPSTREAM.json', 'third_party/grpc/WEIR_PATCH.md', 'third_party/grpc/WEIR_PATCH.diff', 'third_party/grpc/LICENSE', 'third_party/grpc/AUTHORS'}]
    return {n: hashlib.sha256((root/n).read_bytes()).hexdigest() for n in sorted(set(selected))}
def save():
    (evidence/'repair-r2-final-receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
before=inventory()
(evidence/'repair-r2-source-before.json').write_text(json.dumps(before, indent=2)+'\n')
checks=[
    ('all-race', root, ['test', '-race', './...', '-count=1'], {}),
    ('nested-race', root/'third_party/grpc', ['test', '-mod=readonly', '-race', './', './mem', './internal/transport', '-run', '^TestReceive', '-count=5'], {}),
    ('observer-race', root, ['test', '-race', '-tags', 'integration', './internal/app', '-run', '^TestIngressMemoryDockerInspect', '-count=1'], {}),
    ('vet', root, ['vet', './...'], {}),
    ('integration-vet', root, ['vet', '-tags', 'integration', './internal/store', './internal/server', './internal/protowire', './internal/app'], {}),
    ('linux-build', root, ['build', '-trimpath', '-o', str(evidence/'weir-linux-repair-r2'), './cmd/weir'], {'GOOS': 'linux', 'GOARCH': 'amd64', 'CGO_ENABLED': '0'}),
]
save()
for name, cwd, args, extra in checks:
    log = evidence/('repair-r2-final-'+name+'.txt')
    item={'name': name, 'command': [go]+args, 'cwd':str(cwd), 'started':datetime.now(timezone.utc).isoformat(), 'log':str(log.relative_to(root))}
    start=time.monotonic()
    print('START '+name, flush=True)
    with log.open('wb') as output:
        process = subprocess.run([go]+args, cwd=cwd, env=dict(env, **extra), stdout=output, stderr=subprocess.STDOUT)
    item.update(exit=process.returncode, elapsed_seconds=time.monotonic()-start)
    receipt['checks'].append(item)
    print('END '+name+' exit='+str(process.returncode), flush=True)
    save()
    if process.returncode:
        receipt['status']='FAIL'
        break
after=inventory()
(evidence/'repair-r2-source-after.json').write_text(json.dumps(after, indent=2)+'\n')
receipt['source_files']=len(before)
receipt['source_unchanged']=before==after
receipt['changed_during_checks']=[n for n in sorted(set(before)|set(after)) if before.get(n)!=after.get(n)]
receipt['status']='PASS' if len(receipt['checks'])==len(checks) and all(v['exit']==0 for v in receipt['checks']) and before==after else 'FAIL'
if (evidence/'weir-linux-repair-r2').exists():
    receipt['binary_sha256']=hashlib.sha256((evidence/'weir-linux-repair-r2').read_bytes()).hexdigest()
save()
print(json.dumps({'status':receipt['status'], 'source_files':len(before), 'source_unchanged':before==after}), flush=True)
