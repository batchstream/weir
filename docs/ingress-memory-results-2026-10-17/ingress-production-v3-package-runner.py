from pathlib import Path
import datetime, hashlib, json, os, subprocess, sys, time
root=Path('/Users/liran/.codex/worktrees/native-burst-admission/weir')
r=Path('/Users/liran/Projects/liran/go/weir/.testdata/reliability-2026-10-16')
expected='e264138ea0820a9664f3e5b6b7ee51925f3473fa'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==expected
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()
output=root/'.testdata/ingress-release-M3-e264138'
assert not output.exists()
toolchain='/Users/liran/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64'
env=dict(os.environ,GOROOT=toolchain,PATH=toolchain+'/bin:'+os.environ['PATH'],GOENV='off',GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off',GOWORK='off',PYTHONDONTWRITEBYTECODE='1')
args=['python3','scripts/package.py','--output',str(output)]
started=datetime.datetime.now(datetime.timezone.utc).isoformat();begin=time.monotonic()
command={'source':expected,'cwd':str(root),'args':args,'started_at':started,'environment':{k:env[k] for k in ['GOROOT','GOENV','GOTOOLCHAIN','GOPROXY','GOSUMDB','GOWORK']},'scope':'Actual two independent clean exports, separate fresh caches, four targets each. No OCI or publishing.'}
commandfile=r/'ingress-production-v3-package-command.json';assert not commandfile.exists();commandfile.write_text(json.dumps(command,indent=2,sort_keys=True)+'\n')
log=r/'ingress-production-v3-package-process.txt'
with log.open('x') as f:
 process=subprocess.Popen(args,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
 for line in process.stdout:
  f.write(line);f.flush();print(line,end='',flush=True)
 code=process.wait()
(r/'ingress-production-v3-package-process.exit').write_text(str(code)+'\n')
comparison=json.loads((output/'comparison.json').read_text()) if (output/'comparison.json').exists() else None
receipt={'source':expected,'status':'PASS' if code==0 and comparison and comparison.get('completed') else 'FAIL','actual_exit':code,'started_at':started,'finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-begin,'output_directory':str(output),'process_log_sha256':hashlib.sha256(log.read_bytes()).hexdigest(),'comparison':comparison,'clean_after':not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()}
(r/'ingress-production-v3-package-validation.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps(receipt,indent=2),flush=True)
sys.exit(code)
