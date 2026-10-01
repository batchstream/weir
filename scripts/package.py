#!/usr/bin/env python3
"""Build a clean HEAD twice. Offline Go builds; optional local-only OCI via BuildKit."""
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
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
TARGETS = tuple((system, arch) for system in ('linux', 'darwin', 'windows') for arch in ('amd64', 'arm64'))
GO = 'go1.27.1'


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


def allowed(name):
    return (
        name in ('go.mod', 'go.sum', 'README.md', 'config.example.yaml', 'routes.example.yaml')
        or name.startswith('deploy/docker/')
        or (
            name.endswith('.go')
            and not name.endswith('_test.go')
            and name.startswith(('api/', 'internal/', 'cmd/weir/'))
            and not name.startswith('internal/testutil/')
        )
    )


def source_files(root, revision, *, qualification=False):
    # Inspect every tracked path before opening any source blob, even excluded files.
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('expected immutable full source SHA')
    records = run(['git', 'ls-tree', '-rz', revision], cwd=root).split(b'\0')
    files = []
    for record in filter(None, records):
        metadata, encoded = record.split(b'\t', 1)
        name = encoded.decode('utf-8')
        mode, kind, oid = metadata.decode().split()
        if secret_path(name):
            raise ValueError('tracked secret-style path refused: ' + name)
        helper = qualification and (name == 'scripts/qualification.Dockerfile' or (
            name.startswith('internal/testutil/testcapacity/') and name.endswith('.go') and not name.endswith('_test.go')))
        if allowed(name) or helper:
            if mode not in ('100644', '100755') or kind != 'blob' or '..' in PurePosixPath(name).parts:
                raise ValueError('non-regular build input refused: ' + name)
            files.append((name, oid))
    if not {'go.mod', 'go.sum', 'cmd/weir/main.go'} <= {name for name, _ in files}:
        raise ValueError('missing required build inputs')
    return files


def clean_head(root):
    if run(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=root).strip():
        raise ValueError('formal packaging requires a clean worktree, including untracked files')
    revision = run(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip()
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('expected full source SHA')
    return revision


def go_environment():
    # Ignore user Go settings, workspace, compiler overrides, proxies and experiments.
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'SYSTEMROOT', 'GOROOT', 'GOMODCACHE') if k in os.environ}
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


def build_info(binary, target, env):
    lines = run(['go', 'version', '-m', str(binary)], env=env).decode().splitlines()
    if not lines or not lines[0].endswith(': ' + GO):
        raise ValueError('unexpected binary compiler')
    settings, modules = {}, []
    for line in lines[1:]:
        fields = line.strip().split('\t')
        if fields[0] == 'build':
            key, value = fields[1].split('=', 1)
            settings[key] = value
        elif fields[0] == 'dep':
            modules.append(fields[1:])
        elif fields[0] == '=>':
            raise ValueError('replacement dependency refused')
    system, arch = target
    required = {'GOOS': system, 'GOARCH': arch, 'CGO_ENABLED': '0', '-trimpath': 'true',
                'GOAMD64' if arch == 'amd64' else 'GOARM64': 'v1' if arch == 'amd64' else 'v8.0'}
    if any(settings.get(k) != v for k, v in required.items()):
        raise ValueError('binary target/build settings mismatch')
    result = {'go': GO, 'target': system + '/' + arch, 'settings': settings, 'linked_modules': modules}
    return result


def archive(path, members, epoch):
    if path.suffix == '.zip':
        with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as output:
            for name, (data, mode) in sorted(members.items()):
                entry = zipfile.ZipInfo(name, time.gmtime(epoch)[:6])
                entry.create_system = 3
                entry.external_attr = (0o100000 | mode) << 16
                output.writestr(entry, data)
    else:
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
    if path.suffix == '.zip':
        with zipfile.ZipFile(path) as bundle:
            entries = bundle.infolist()
            if [e.filename for e in entries] != sorted(members):
                raise ValueError('archive file set/order mismatch')
            for entry in entries:
                data, mode = members[entry.filename]
                stamp = time.gmtime(epoch)[:5] + (time.gmtime(epoch)[5] // 2 * 2,)
                if bundle.read(entry) != data or entry.date_time != stamp or entry.external_attr >> 16 != 0o100000 | mode:
                    raise ValueError('archive content/metadata mismatch')
    else:
        with tarfile.open(path) as bundle:
            entries = bundle.getmembers()
            if [e.name for e in entries] != sorted(members):
                raise ValueError('archive file set/order mismatch')
            for entry in entries:
                data, mode = members[entry.name]
                if not entry.isfile() or bundle.extractfile(entry).read() != data or (entry.mode, entry.mtime, entry.uid, entry.gid, entry.uname, entry.gname) != (mode, epoch, 0, 0, '', ''):
                    raise ValueError('archive content/metadata mismatch')


def oci_receipt(archive_path, binaries, options=None):
    # Inspect BuildKit output; never synthesize an OCI implementation or its tar layers.
    options = options or {}
    binary_name = options.get('binary_name', 'weir')
    revision, base_layers = options.get('revision'), options.get('base_layers')
    with tarfile.open(archive_path) as bundle:
        def blob(digest):
            data = bundle.extractfile('blobs/sha256/' + digest.removeprefix('sha256:')).read()
            if 'sha256:' + sha(data) != digest:
                raise ValueError('OCI blob digest mismatch')
            return data
        outer = bundle.extractfile('index.json').read()
        index = json.loads(outer)
        manifests = index['manifests']
        index_digest = 'sha256:' + sha(outer)
        if len(manifests) == 1 and 'index' in manifests[0]['mediaType']:
            index_digest = manifests[0]['digest']
            manifests = json.loads(blob(index_digest))['manifests']
        images = {}
        for descriptor in manifests:
            manifest = json.loads(blob(descriptor['digest']))
            config = json.loads(blob(manifest['config']['digest']))
            arch = config['architecture']
            runtime = config['config']
            if config['os'] != 'linux' or arch not in ('amd64', 'arm64') or runtime['User'] != '65532:65532' or runtime['Entrypoint'] != ['/' + binary_name]:
                raise ValueError('unexpected image runtime/target')
            if arch in images or descriptor.get('platform', {}).get('architecture') != arch:
                raise ValueError('duplicate or inconsistent OCI platform')
            if revision is not None:
                labels = runtime.get('Labels', {})
                if labels.get('org.opencontainers.image.revision') != revision or labels.get('org.opencontainers.image.source') != 'https://github.com/batchstream/weir':
                    raise ValueError('image source labels mismatch')
            layers = [layer['digest'] for layer in manifest['layers']]
            if base_layers is not None and layers[:-1] != base_layers[arch]:
                raise ValueError('image must contain exact base layers plus one binary layer')
            found = {}
            for number, layer in enumerate(manifest['layers']):
                raw = blob(layer['digest'])
                with tarfile.open(fileobj=io.BytesIO(raw), mode='r:*') as content:
                    for entry in content:
                        name = entry.name.removeprefix('./')
                        expected_names = {binary_name}
                        if base_layers is not None and number == len(layers) - 1:
                            if name not in expected_names or not entry.isfile() or entry.mode != 0o555:
                                raise ValueError('unexpected file in application layer')
                        if name in expected_names and entry.isfile():
                            found[name] = sha(content.extractfile(entry).read())
            if found.get(binary_name) != binaries['linux-' + arch]:
                raise ValueError('wrong binary in OCI image')
            image = {'manifest': descriptor['digest'], 'config': manifest['config']['digest'],
                     'layers': [layer['digest'] for layer in manifest['layers']], 'binary_sha256': found[binary_name]}
            images[arch] = image
        if set(images) != {'amd64', 'arm64'}:
            raise ValueError('OCI index must contain both architectures')
        receipt = {'index': index_digest, 'outer_index_sha256': sha(outer), 'images': images}
        return receipt


def build_once(opts):
    source, output, env = opts['source'], opts['output'], opts['env']
    output.mkdir()
    cache = source.parent / 'cache'
    env = dict(env, GOCACHE=str(cache))
    for name, oid in opts['files']:
        dest = source / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(run(['git', 'cat-file', 'blob', oid]))
    original = {name: sha((source / name).read_bytes()) for name, _ in opts['files']}
    run(['go', 'mod', 'verify'], cwd=source, env=env)
    graph = json_stream(run(['go', 'list', '-m', '-json', 'all'], cwd=source, env=env))
    modules = []
    for module in graph:
        if 'Replace' in module:
            raise ValueError('module replacements refused')
        modules.append({key: module[key] for key in ('Path', 'Version', 'Sum', 'GoModSum', 'GoVersion', 'Main') if key in module})
    write_json(output / 'module-graph.json', modules)
    binary_hashes, artifact_hashes = {}, {}
    qualification = opts.get('qualification', False)
    binary_name = 'qualification' if qualification else 'weir'
    targets = (('linux', 'amd64'), ('linux', 'arm64')) if qualification else TARGETS
    flags = ['-trimpath', '-buildvcs=false', '-mod=readonly', '-ldflags=-buildid= -X main.sourceRevision=' + opts['revision']]
    if qualification:
        flags.append('-tags=integration')
    for system, arch in targets:
        target = system + '-' + arch
        print('build', output.name, target, flush=True)
        dest = output / 'binaries' / target
        dest.mkdir(parents=True)
        binary = dest / (binary_name + ('.exe' if system == 'windows' else ''))
        target_env = dict(env, GOOS=system, GOARCH=arch)
        entry = './internal/testutil/testcapacity' if qualification else './cmd/weir'
        run(['go', 'build', *flags, '-o', str(binary), entry], cwd=source, env=target_env)
        info = build_info(binary, (system, arch), env)
        if run(['go', 'tool', 'buildid', str(binary)], env=env).strip():
            raise ValueError('binary build ID must be empty')
        info.update(source=opts['revision'], flags=flags, cpu_baseline='v1' if arch == 'amd64' else 'v8.0')
        write_json(output / (target + '-linked.json'), info)
        binary_hashes[target] = sha(binary.read_bytes())
        if qualification:
            continue
        members = {
            binary.name: (binary.read_bytes(), 0o755),
            'README.md': ((source / 'README.md').read_bytes(), 0o644),
            'node.example.yaml': ((source / 'deploy/docker/node.example.yaml').read_bytes(), 0o644),
            'routes.example.yaml': ((source / 'deploy/docker/routes.example.yaml').read_bytes(), 0o644),
            'reference/config.example.yaml': ((source / 'config.example.yaml').read_bytes(), 0o644),
            'reference/routes.example.yaml': ((source / 'routes.example.yaml').read_bytes(), 0o644),
        }
        if system == 'darwin':
            for license_file in sorted((source / 'deploy/docker/licenses').glob('purego-*.txt')):
                members['licenses/' + license_file.name] = (license_file.read_bytes(), 0o644)
        dest = output / ('weir-' + target + ('.zip' if system == 'windows' else '.tar.gz'))
        archive(dest, members, opts['epoch'])
        artifact_hashes[dest.name] = sha(dest.read_bytes())
    if original != {name: sha((source / name).read_bytes()) for name, _ in opts['files']}:
        raise ValueError('build modified exported inputs')
    receipt = {'source': opts['revision'], 'go': GO, 'flags': flags, 'epoch': opts['epoch'],
               'inputs': original, 'binaries': binary_hashes,
               'archives': artifact_hashes}
    if opts['oci']:
        base = json.loads((source / 'deploy/docker/base.json').read_text())
        context = source.parent / 'oci-context'
        context.mkdir()
        dockerfile = 'scripts/qualification.Dockerfile' if qualification else 'deploy/docker/Dockerfile'
        shutil.copyfile(source / dockerfile, context / 'Dockerfile')
        for arch in ('amd64', 'arm64'):
            dest = context / ('linux-' + arch)
            dest.mkdir()
            shutil.copyfile(output / 'binaries' / ('linux-' + arch) / binary_name, dest / binary_name)
        for file in context.rglob('*'):
            os.utime(file, (opts['epoch'], opts['epoch']))
        oci = output / (binary_name + '-linux.oci.tar')
        args = ['docker', 'buildx', 'build', '--builder', opts['builder'], '--platform=linux/amd64,linux/arm64',
                '--no-cache', '--provenance=false', '--sbom=false', '--network=none',
                '--build-arg', 'BASE=' + base['image'] + '@' + base['index'],
                '--build-arg', 'SOURCE_REVISION=' + opts['revision'],
                '--build-arg', 'SOURCE_DATE_EPOCH=' + str(opts['epoch']),
                '--output', 'type=oci,dest=' + str(oci) + ',rewrite-timestamp=true', str(context)]
        result = subprocess.run(args, env=opts['docker_env'], text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=600)
        (output / 'oci-build.log').write_text(result.stdout)
        if result.returncode:
            raise RuntimeError('OCI build failed; see ' + str(output / 'oci-build.log'))
        image_options = dict(binary_name=binary_name, revision=opts['revision'], base_layers=opts.get('base_layers'))
        receipt['oci'] = oci_receipt(oci, binary_hashes, image_options)
        receipt['base'] = base
    write_json(output / 'receipt.json', receipt)
    checksums = []
    for file in sorted(output.rglob('*')):
        if file.is_file() and not file.name.endswith('.log'):
            checksums.append(sha(file.read_bytes()) + '  ' + file.relative_to(output).as_posix())
    (output / 'SHA256SUMS').write_text('\n'.join(checksums) + '\n')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new owned output directory; keep deliverables here')
    parser.add_argument('--oci', action='store_true', help='also use public pinned base with an explicitly selected local builder; never push')
    parser.add_argument('--builder', default='default')
    args = parser.parse_args()
    revision = clean_head(ROOT)
    files, env = source_files(ROOT, revision), go_environment()
    epoch = int(run(['git', 'show', '-s', '--format=%ct', revision]).strip())
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / 'owner').write_text('weir-local-packaging ' + revision + '\n')
    docker_env = dict(env)
    if args.oci:
        # Require an explicitly owned blank client config; do not inspect existing credentials.
        config = output / 'docker'
        config.mkdir()
        value = {'auths': {}}
        plugins = Path('/Applications/Docker.app/Contents/Resources/cli-plugins')
        if plugins.is_dir():
            value['cliPluginsExtraDirs'] = [str(plugins)]
        write_json(config / 'config.json', value)
        docker_env.update(DOCKER_CONFIG=str(config), DOCKER_HOST=os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock'))
        # Buildx instance metadata, when needed, is passed separately from credential config.
        if os.environ.get('BUILDX_CONFIG'):
            docker_env['BUILDX_CONFIG'] = os.environ['BUILDX_CONFIG']
    evidence = {'source': revision, 'export_directories': [], 'completed': False}
    try:
        receipts = []
        for name in ('first', 'second'):
            with tempfile.TemporaryDirectory(prefix='weir-package-' + name + '-') as work:
                source = Path(work) / 'source'
                source.mkdir()
                evidence['export_directories'].append(str(source))
                opts = {'source': source, 'output': output / name, 'env': env, 'files': files,
                        'revision': revision, 'epoch': epoch, 'oci': args.oci,
                        'builder': args.builder, 'docker_env': docker_env}
                receipts.append(build_once(opts))
        if receipts[0] != receipts[1]:
            raise ValueError('reproducibility mismatch; preserve both receipts and artifacts')
        evidence.update(completed=True, identical_binaries=6, identical_archives=6,
                        oci_identical=bool(args.oci), receipt_sha256=sha((output / 'first/receipt.json').read_bytes()))
        print('Reproducible artifacts:', output, flush=True)
    except BaseException as error:
        evidence['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        write_json(output / 'comparison.json', evidence)


if __name__ == '__main__':
    main()
