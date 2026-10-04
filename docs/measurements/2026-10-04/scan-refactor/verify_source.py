from pathlib import Path
import hashlib,json,subprocess,sys
manifest=json.loads((Path(__file__).parent/'source-manifest.json').read_text())
if len(sys.argv)!=3 or sys.argv[1] not in ('baseline','current'):
    raise SystemExit('usage: python3 verify_source.py baseline|current /path/to/repository')
variant,repo=sys.argv[1],Path(sys.argv[2])
version=manifest['versions'][variant]
try:
    inventory=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=repo,stderr=subprocess.DEVNULL).decode().split('\0')
    paths=[repo/name for name in inventory if name]
except subprocess.CalledProcessError:
    paths=list(repo.rglob('*'))
selected={path.relative_to(repo).as_posix() for path in paths if path.is_file() and ((path.suffix=='.go' and not path.name.endswith('_test.go')) or path.name in ('go.mod','go.sum'))}
expected={entry['path'] for entry in version['files']}
assert selected==expected,{'added':sorted(selected-expected),'missing':sorted(expected-selected)}
digest=hashlib.sha256()
for entry in version['files']:
    raw=(repo/entry['path']).read_bytes()
    actual=hashlib.sha256(raw).digest()
    assert actual.hex()==entry['sha256'],entry['path']
    digest.update(entry['path'].encode()+b'\0'+actual)
assert digest.hexdigest()==version['tree_sha256'],digest.hexdigest()
print(json.dumps({'variant':variant,'verified_files':len(version['files']),'tree_sha256':digest.hexdigest()}))
