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
import tempfile

def download(pin, destination):
    subprocess.run(['curl', '-q', '--fail', '--location', '--max-time', '180',
                    '--max-filesize', '134217728', '--output', str(destination), pin['url']], check=True, timeout=190)
    if hashlib.sha256(destination.read_bytes()).hexdigest() != pin['sha256']:
        raise ValueError('public tool checksum mismatch')


def prepare_modules(source, work_root, env):
    inputs = {name: (source / name).read_bytes() for name in ('go.mod', 'go.sum')
              if name == 'go.mod' or (source / name).exists()}
    with tempfile.TemporaryDirectory(prefix='module-prepare-', dir=work_root) as work:
        for name, raw in inputs.items():
            (Path(work) / name).write_bytes(raw)
        # Test dependency versions must be cached before CI switches back to
        # its network-disabled test mode.
        subprocess.run(['go', 'mod', 'download', 'all'], cwd=work, env=env, check=True, timeout=300)
    if inputs != {name: (source / name).read_bytes() for name in inputs}:
        raise ValueError('module preparation changed fixed go.mod/go.sum')


def prepare_mongo(pin, root, bins):
    # Downloaded bytes stay in their own new directory. Only the verified regular
    # mongod member is copied; no archive paths or installer code are executed.
    work = root / 'mongo-download'
    work.mkdir()
    archive = work / 'mongo.tgz'
    download(pin, archive)
    if archive.stat().st_size != pin['size']:
        raise ValueError('MongoDB archive size mismatch')
    expected_size = pin['member_size']
    if not isinstance(expected_size, int) or not 0 < expected_size <= 256 * 1024 * 1024:
        raise ValueError('MongoDB member size outside extraction bound')
    destination = bins / 'mongod'
    with tarfile.open(archive) as bundle:
        matches = [member for member in bundle.getmembers() if member.name == pin['member']]
        if len(matches) != 1 or not matches[0].isfile() or matches[0].size != expected_size:
            raise ValueError('MongoDB archive member identity mismatch')
        digest = hashlib.sha256()
        count = 0
        with bundle.extractfile(matches[0]) as source, destination.open('xb') as output:
            destination.chmod(0o600)
            while chunk := source.read(1024 * 1024):
                count += len(chunk)
                if count > expected_size:
                    raise ValueError('MongoDB member exceeds extraction bound')
                digest.update(chunk)
                output.write(chunk)
    if count != expected_size or digest.hexdigest() != pin['member_sha256']:
        raise ValueError('MongoDB member checksum mismatch')
    destination.chmod(0o755)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('test', 'publish', 'smoke'))
    parser.add_argument('--mongo', action='store_true', help='prepare pinned native MongoDB for owned replica tests')
    args = parser.parse_args()
    if args.mongo and args.mode != 'test':
        raise ValueError('--mongo is only supported for test preparation')
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('this bootstrap requires a native Linux amd64 runner')
    arch = 'amd64'
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
        # The packager inventories the complete module graph, including lazy test dependencies.
        # Download in a temporary module copy: full downloads may add unused zip sums.
        prepare_modules(Path.cwd(), root, env)
        env.update(GOPROXY='off', GOSUMDB='off')
        subprocess.run(['go', 'mod', 'verify'], env=env, check=True, timeout=60)
        subprocess.run(['go', 'list', '-m', '-json', 'all'], env=env, check=True, timeout=60, stdout=subprocess.DEVNULL)
    if args.mongo:
        mongo = prepare_mongo(pins['mongo']['linux-amd64'], root, bins)
        updates['WEIR_MONGO_REPLICA_BINARY'] = str(mongo)
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


if __name__ == '__main__':
    main()
