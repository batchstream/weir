#!/usr/bin/env python3
"""Build release archives from a clean HEAD. Offline Go builds; optional local OCI image via BuildKit."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile

import check_dependencies as dependencies

ROOT = Path(__file__).resolve().parent.parent
TARGETS = tuple((system, arch) for system in ('linux', 'darwin') for arch in ('amd64', 'arm64'))
GO = 'go1.27.1'
PUREGO_LICENSES = ('purego-Apache-2.0.txt', 'purego-Go-BSD-3-Clause.txt', 'purego-NOTICE.txt')


def run(args, *, cwd=ROOT, env=None, timeout=300):
    result = subprocess.run(args, cwd=cwd, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout, check=True)
    return result.stdout


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def secret_path(name):
    parts = PurePosixPath(name).parts
    return any(p.lower() in ('.ssh', 'credentials', 'keyfile') or p.lower().startswith('.env')
               or p.lower().endswith(('.pem', '.key')) for p in parts)


def public_grpc_source(name):
    if not name.startswith('third_party/grpc/') or not name.endswith('.go') or name.endswith('_test.go'):
        return False
    parts = PurePosixPath(name).parts
    # The pinned public library has packages named credentials; these are Go source.
    return '..' not in parts and not secret_path('/'.join(part for part in parts if part != 'credentials'))


def allowed(name):
    return (
        name in ('go.mod', 'go.sum', 'README.md', 'config/weir.reference.yaml', 'config/routes.reference.yaml')
        or name in ('third_party/grpc/go.mod', 'third_party/grpc/go.sum', 'third_party/grpc/LICENSE',
                    'third_party/grpc/AUTHORS', 'third_party/grpc/WEIR_UPSTREAM.json',
                    'third_party/grpc/WEIR_PATCH.md', 'third_party/grpc/WEIR_PATCH.diff')
        or public_grpc_source(name)
        or (
            name.startswith('third_party/golua/') and not secret_path(name)
            and '..' not in PurePosixPath(name).parts
            and ((name.endswith('.go') and not name.endswith('_test.go'))
                 or name in ('third_party/golua/go.mod', 'third_party/golua/go.sum',
                             'third_party/golua/LICENSE', 'third_party/golua/WEIR_UPSTREAM.json',
                             'third_party/golua/WEIR_PATCH.md', 'third_party/golua/WEIR_PATCH.diff'))
        )
        or name in ('deploy/docker/Dockerfile', 'deploy/docker/base.json')
        or name in ('licenses/' + license_name for license_name in PUREGO_LICENSES)
        or (
            name.endswith('.go')
            and not name.endswith('_test.go')
            and name.startswith(('internal/', 'cmd/weir/'))
            and not name.startswith('internal/testutil/')
        )
    )


def source_files(root, revision):
    # Inspect every tracked path before opening any source blob, even excluded files.
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('expected immutable full source SHA')
    records = run(['git', 'ls-tree', '-rz', revision], cwd=root).split(b'\0')
    files = []
    for record in filter(None, records):
        metadata, encoded = record.split(b'\t', 1)
        name = encoded.decode('utf-8')
        mode, kind, oid = metadata.decode().split()
        if secret_path(name) and not public_grpc_source(name):
            raise ValueError('tracked secret-style path refused: ' + name)
        if allowed(name):
            if mode not in ('100644', '100755') or kind != 'blob' or '..' in PurePosixPath(name).parts:
                raise ValueError('non-regular build input refused: ' + name)
            files.append((name, oid))
    public_packages = [name for name, _ in files if secret_path(name)]
    if public_packages:
        manifest_name = 'third_party/grpc/WEIR_UPSTREAM.json'
        manifest_oid = dict(files).get(manifest_name)
        if manifest_oid is None:
            raise ValueError('public gRPC source requires its pinned upstream manifest')
        manifest = json.loads(run(['git', 'cat-file', 'blob', manifest_oid], cwd=root))
        if manifest.get('module') != dependencies.GRPC or manifest.get('version') != dependencies.GRPC_VERSION:
            raise ValueError('unexpected public gRPC upstream manifest')
        if any(name.removeprefix('third_party/grpc/') not in manifest.get('files', {}) for name in public_packages):
            raise ValueError('unlisted credentials package source refused')
        for name in public_packages:
            expected = manifest['files'][name.removeprefix('third_party/grpc/')]
            if sha(run(['git', 'cat-file', 'blob', dict(files)[name]], cwd=root)) != expected:
                raise ValueError('public credentials source differs from pinned upstream: ' + name)
    if not {'go.mod', 'go.sum', 'cmd/weir/main.go'} <= {name for name, _ in files}:
        raise ValueError('missing required build inputs')
    return files


def clean_head(root):
    if run(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=root).strip():
        raise ValueError('packaging requires a clean worktree, including untracked files')
    revision = run(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip()
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('expected full source SHA')
    return revision


def validate_patch_inputs(source, inputs):
    for module, pin in dependencies.LOCAL_PATCHES.items():
        validate_patch(source, inputs, module, pin)


def validate_patch(source, inputs, module, pin):
    version, relative = pin
    relative = relative.removeprefix('./')
    directory = source / relative
    manifest = json.loads((directory / 'WEIR_UPSTREAM.json').read_text())
    if manifest.get('module') != module or manifest.get('version') != version:
        raise ValueError('unexpected public upstream manifest: ' + module)
    expected = dict(manifest['files'])
    for record in manifest.get('patchedFiles', []):
        name = record['path']
        if name not in expected or record['upstream_sha256'] != manifest['files'].get(name):
            raise ValueError('patch does not match its recorded upstream source: ' + module)
        expected[name] = record['sha256']
    for record in manifest.get('newFiles', []):
        name = record['path']
        if name in expected:
            raise ValueError('duplicate source in patch manifest: ' + module)
        expected[name] = record['sha256']
    selected = {relative + '/' + name: digest for name, digest in expected.items()
                if allowed(relative + '/' + name)}
    actual = {name: digest for name, digest in inputs.items()
              if name.startswith(relative + '/') and name != relative + '/WEIR_UPSTREAM.json'}
    if actual != selected:
        raise ValueError('exported sources differ from the pinned patch manifest: ' + module)


def go_environment():
    # Ignore user Go settings, workspace, compiler overrides, proxies and experiments.
    # GOROOT is not inherited: the go command on PATH locates its own matching toolchain.
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'SYSTEMROOT', 'GOMODCACHE') if k in os.environ}
    env.update(GOENV='off', GOTOOLCHAIN='local', GOWORK='off', GOPROXY='off', GOSUMDB='off',
               GOFLAGS='', GOEXPERIMENT='', CGO_ENABLED='0', GOAMD64='v1', GOARM64='v8.0')
    actual = run(['go', 'env', 'GOVERSION'], env=env).decode().strip()
    if actual != GO:
        raise ValueError(f'requires {GO}, actual {actual}; no automatic toolchain download')
    return env


def json_stream(raw):
    decoder = json.JSONDecoder()
    text, values = raw.decode(), []
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        values.append(value)
        text = text.lstrip()[end:]
    return values


def archive(path, members, epoch):
    with path.open('wb') as output:
        with gzip.GzipFile(filename='', mode='wb', fileobj=output, mtime=0, compresslevel=9) as compressed:
            with tarfile.open(fileobj=compressed, mode='w', format=tarfile.USTAR_FORMAT) as bundle:
                for name, (data, mode) in sorted(members.items()):
                    entry = tarfile.TarInfo(name)
                    entry.size, entry.mode, entry.mtime = len(data), mode, epoch
                    entry.uid = entry.gid = 0
                    entry.uname = entry.gname = ''
                    bundle.addfile(entry, io.BytesIO(data))
    verify_archive(path, members, epoch)


def verify_archive(path, members, epoch):
    with tarfile.open(path) as bundle:
        entries = bundle.getmembers()
        if [e.name for e in entries] != sorted(members):
            raise ValueError('archive file set/order mismatch')
        for entry in entries:
            data, mode = members[entry.name]
            if not entry.isfile() or bundle.extractfile(entry).read() != data or (entry.mode, entry.mtime, entry.uid, entry.gid, entry.uname, entry.gname) != (mode, epoch, 0, 0, '', ''):
                raise ValueError('archive content/metadata mismatch')


def oci_index_digest(path):
    # Read the multi-platform image index digest from BuildKit OCI layout output.
    with tarfile.open(path) as bundle:
        outer = bundle.extractfile('index.json').read()
        manifests = json.loads(outer)['manifests']
        digest = 'sha256:' + sha(outer)
        if len(manifests) == 1 and 'index' in manifests[0]['mediaType']:
            digest = manifests[0]['digest']
            raw = bundle.extractfile('blobs/sha256/' + digest.removeprefix('sha256:')).read()
            if 'sha256:' + sha(raw) != digest:
                raise ValueError('OCI index digest mismatch')
            manifests = json.loads(raw)['manifests']
    platforms = sorted((m.get('platform', {}).get('os'), m.get('platform', {}).get('architecture')) for m in manifests)
    if platforms != [('linux', 'amd64'), ('linux', 'arm64')]:
        raise ValueError('OCI index must contain exactly linux/amd64 and linux/arm64')
    return digest


def archive_members(source, system):
    members = {
        'README.md': ((source / 'README.md').read_bytes(), 0o644),
        'config/weir.reference.yaml': ((source / 'config/weir.reference.yaml').read_bytes(), 0o644),
        'config/routes.reference.yaml': ((source / 'config/routes.reference.yaml').read_bytes(), 0o644),
    }
    for _, relative in dependencies.LOCAL_PATCHES.values():
        patch_source = source / relative
        names = ['LICENSE', 'WEIR_UPSTREAM.json', 'WEIR_PATCH.md', 'WEIR_PATCH.diff']
        if patch_source.name == 'grpc':
            names.append('AUTHORS')
        for name in names:
            members['licenses/' + patch_source.name + '/' + name] = ((patch_source / name).read_bytes(), 0o644)
    if system == 'darwin':
        # purego is linked only into Darwin binaries.
        for name in PUREGO_LICENSES:
            members['licenses/' + name] = ((source / 'licenses' / name).read_bytes(), 0o644)
    return members


def build_image(opts, binaries):
    # Build only from the already compiled Linux binaries; the image is never pushed here.
    source, work, epoch = opts['source'], opts['work'], opts['epoch']
    base = json.loads((source / 'deploy/docker/base.json').read_text())
    context = work / 'oci-context'
    context.mkdir()
    shutil.copyfile(source / 'deploy/docker/Dockerfile', context / 'Dockerfile')
    for arch in ('amd64', 'arm64'):
        dest = context / ('linux-' + arch)
        dest.mkdir()
        shutil.copyfile(binaries / ('linux-' + arch) / 'weir', dest / 'weir')
    for file in context.rglob('*'):
        os.utime(file, (epoch, epoch))
    oci = opts['output'] / 'weir-linux.oci.tar'
    args = ['docker', 'buildx', 'build', '--builder', opts['builder'], '--platform=linux/amd64,linux/arm64',
            '--no-cache', '--provenance=false', '--sbom=false', '--network=none',
            '--build-arg', 'BASE=' + base['image'] + '@' + base['index'],
            '--build-arg', 'SOURCE_REVISION=' + opts['revision'],
            '--build-arg', 'SOURCE_DATE_EPOCH=' + str(epoch),
            '--output', 'type=oci,dest=' + str(oci) + ',rewrite-timestamp=true', str(context)]
    result = subprocess.run(args, env=opts['docker_env'], text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=600)
    if result.returncode:
        raise RuntimeError('OCI build failed:\n' + result.stdout[-16000:])
    return oci


def build_source(opts):
    """Export the selected sources, cross-build every target and write archives plus SHA256SUMS."""
    work, output = opts['work'], opts['output']
    source = opts['source']
    env = dict(opts['env'], GOCACHE=str(work / 'cache'))
    for name, oid in opts['files']:
        dest = source / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(run(['git', 'cat-file', 'blob', oid]))
    inputs = {name: sha((source / name).read_bytes()) for name, _ in opts['files']}
    validate_patch_inputs(source, inputs)
    run(['go', 'mod', 'verify'], cwd=source, env=env)
    graph = json_stream(run(['go', 'list', '-m', '-json', 'all'], cwd=source, env=env))
    dependencies.check_modules(graph, source)
    binaries = work / 'binaries'
    flags = ['-trimpath', '-buildvcs=false', '-mod=readonly', '-ldflags=-buildid= -X main.revision=' + opts['revision']]
    archives = []
    for system, arch in TARGETS:
        target = system + '-' + arch
        print('build', target, flush=True)
        binary = binaries / target / 'weir'
        binary.parent.mkdir(parents=True)
        target_env = dict(env, GOOS=system, GOARCH=arch)
        run(['go', 'build', *flags, '-o', str(binary), './cmd/weir'], cwd=source, env=target_env)
        members = archive_members(source, system)
        members['weir'] = (binary.read_bytes(), 0o755)
        dest = output / ('weir-' + target + '.tar.gz')
        archive(dest, members, opts['epoch'])
        archives.append(dest)
    checksums = ''.join(sha(item.read_bytes()) + '  ' + item.name + '\n' for item in archives)
    (output / 'SHA256SUMS').write_text(checksums)
    if opts['oci']:
        build_image(opts, binaries)
    return archives


def build(opts):
    """Package the clean HEAD into opts['output'], a new directory."""
    revision = clean_head(ROOT)
    files, env = source_files(ROOT, revision), go_environment()
    epoch = int(run(['git', 'show', '-s', '--format=%ct', revision]).strip())
    output = opts['output']
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='weir-package-') as temporary:
        work = Path(temporary)
        source = work / 'source'
        source.mkdir()
        build_opts = dict(opts, work=work, source=source, env=env, files=files, revision=revision, epoch=epoch)
        archives = build_source(build_opts)
    return archives


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new output directory for archives and SHA256SUMS')
    parser.add_argument('--oci', action='store_true', help='also build the Linux image as an OCI tar with a local BuildKit builder; never push')
    parser.add_argument('--builder', default='default')
    args = parser.parse_args()
    output = args.output.resolve()
    with tempfile.TemporaryDirectory(prefix='weir-package-docker-') as config:
        # Use a blank client config; never read existing registry credentials.
        value = {'auths': {}}
        plugins = Path('/Applications/Docker.app/Contents/Resources/cli-plugins')
        if plugins.is_dir():
            value['cliPluginsExtraDirs'] = [str(plugins)]
        write_json(Path(config) / 'config.json', value)
        docker_env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'BUILDX_CONFIG') if k in os.environ}
        docker_env.update(DOCKER_CONFIG=config, DOCKER_HOST=os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock'))
        opts = dict(output=output, oci=args.oci, builder=args.builder, docker_env=docker_env)
        build(opts)
    print('Archives:', output, flush=True)


if __name__ == '__main__':
    main()
