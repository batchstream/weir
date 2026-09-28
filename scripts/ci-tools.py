#!/usr/bin/env python3
"""Install checksum-pinned public tools into a new job-owned directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile


def download(pin, destination):
    subprocess.run(['curl', '-q', '--fail', '--location', '--max-time', '180',
                    '--max-filesize', '134217728', '--output', str(destination), pin['url']], check=True, timeout=190)
    if hashlib.sha256(destination.read_bytes()).hexdigest() != pin['sha256']:
        raise ValueError('public tool checksum mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('test', 'publish', 'smoke'))
    args = parser.parse_args()
    if platform.system() != 'Linux':
        raise ValueError('this bootstrap requires a native Linux runner')
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64'}[platform.machine()]
    pins = json.loads(Path('scripts/ci-tools.json').read_text())
    root = Path('.tools/ci').resolve()
    root.mkdir(parents=True, exist_ok=False)
    bins = root / 'bin'
    bins.mkdir()
    env = dict(os.environ, GOENV='off', GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='-mod=readonly', GOEXPERIMENT='')
    updates = {}
    if args.mode != 'smoke':
        archive = root / 'go.tar.gz'
        download(pins['go'][arch], archive)
        with tarfile.open(archive) as bundle:
            bundle.extractall(root, filter='data')
        updates.update(GOROOT=str(root / 'go'), GOMODCACHE=str(root / 'modules'))
        env.update(updates, PATH=str(root / 'go/bin') + ':' + env['PATH'])
        go_version = subprocess.check_output(['go', 'version'], env=env, timeout=10).decode().strip()
        if go_version != 'go version go1.27.1 linux/' + arch:
            raise ValueError('native Go driver/compiler mismatch: ' + go_version)
        print(go_version, flush=True)
        env.update(GOPROXY='https://proxy.golang.org', GOSUMDB='sum.golang.org')
        subprocess.run(['go', 'mod', 'download'], env=env, check=True, timeout=300)
        env.update(GOPROXY='off', GOSUMDB='off')
        subprocess.run(['go', 'mod', 'verify'], env=env, check=True, timeout=60)
    if args.mode != 'test':
        dest = bins / 'regctl'
        download(pins['regctl']['linux-' + arch], dest)
        dest.chmod(0o755)
    if args.mode == 'publish':
        plugins = root / 'plugins'
        plugins.mkdir()
        dest = plugins / 'docker-buildx'
        download(pins['buildx']['linux-' + arch], dest)
        dest.chmod(0o755)
        updates['BUILDX_CONFIG'] = str(root / 'buildx')
    updates.update(GOENV='off', GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='-mod=readonly',
                   GOEXPERIMENT='', GOPROXY='off', GOSUMDB='off')
    with Path(os.environ['GITHUB_ENV']).open('a') as output:
        for key, value in updates.items():
            output.write(key + '=' + value + '\n')
    with Path(os.environ['GITHUB_PATH']).open('a') as output:
        output.write(str(bins) + '\n')
        if args.mode != 'smoke':
            output.write(str(root / 'go/bin') + '\n')
    # Runner labels select a moving image; record the actual machine separately.
    record = dict(architecture=arch, kernel=platform.release(), machine=platform.machine(),
                  logical_cpus=os.cpu_count(), memory_bytes=os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES'),
                  image_os=os.environ.get('ImageOS'), image_version=os.environ.get('ImageVersion'),
                  runner_arch=os.environ.get('RUNNER_ARCH'), runner_name=os.environ.get('RUNNER_NAME'),
                  source=os.environ.get('GITHUB_SHA'), run_id=os.environ.get('GITHUB_RUN_ID'), pins=pins)
    print('CI_ENVIRONMENT=' + json.dumps(record, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
