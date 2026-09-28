#!/usr/bin/env python3
"""Fixed Weir/GHCR delivery: verified archives, exact copy, anonymous native smoke."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile

import package

ROOT = package.ROOT
OUT = ROOT / '.testdata/m24/ci'
IMAGES = {'weir': 'weir', 'qualification': 'weir-qualification'}


def client_environment(directory, *, plugins=False):
    directory.mkdir(parents=True, exist_ok=False)
    config = directory / 'docker'
    config.mkdir()
    value = {'auths': {}}
    if plugins:
        value['cliPluginsExtraDirs'] = [str(ROOT / '.tools/ci/plugins')]
    package.write_json(config / 'config.json', value)
    env = {k: os.environ[k] for k in ('PATH', 'TMPDIR', 'BUILDX_CONFIG') if k in os.environ}
    env.update(HOME=str(directory), DOCKER_CONFIG=str(config), DOCKER_HOST='unix:///var/run/docker.sock',
               REGCTL_CONFIG=str(directory / 'regctl.json'))
    return env


def manifest(reference, env):
    raw = package.run(['regctl', 'manifest', 'get', reference, '--format', 'raw-body'], env=env)
    digest = 'sha256:' + package.sha(raw)
    if '@' in reference and reference.split('@')[1] != digest:
        raise ValueError('registry manifest digest mismatch')
    return json.loads(raw)


def base_layers(env):
    base = json.loads((ROOT / 'packaging/base.json').read_text())
    index = manifest(base['image'] + '@' + base['index'], env)
    layers = {}
    for arch, digest in base['platforms'].items():
        selected = [m['digest'] for m in index['manifests'] if m.get('platform', {}).get('architecture') == arch and m['platform']['os'] == 'linux']
        if selected != [digest]:
            raise ValueError('base platform identity drift')
        value = manifest(base['image'] + '@' + digest, env)
        layers[arch] = [layer['digest'] for layer in value['layers']]
    return layers


def build():
    revision = package.clean_head(ROOT)
    OUT.mkdir(parents=True, exist_ok=False)
    env = client_environment(OUT / 'builder-client', plugins=True)
    pins = json.loads((ROOT / 'scripts/ci-tools.json').read_text())
    builder = 'weir-m24-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT']
    existing = package.run(['docker', 'buildx', 'ls', '--format', '{{.Name}}'], env=env).decode().splitlines()
    if builder in existing:
        raise ValueError('builder owner collision')
    package.run(['docker', 'buildx', 'version'], env=env)
    package.run(['docker', 'buildx', 'create', '--name', builder, '--driver', 'docker-container',
             '--driver-opt', 'image=' + pins['buildkit']['image']], env=env)
    try:
        print(package.run(['docker', 'buildx', 'inspect', '--bootstrap', builder], env=env, timeout=180).decode(), flush=True)
        layers = base_layers(env)
        go_env = package.go_environment()
        epoch = int(package.run(['git', 'show', '-s', '--format=%ct', revision], env=env).strip())
        delivery = dict(source=revision, base=json.loads((ROOT / 'packaging/base.json').read_text()), base_layers=layers, images={})
        for name in IMAGES:
            qualification = name == 'qualification'
            files = package.source_files(ROOT, revision, qualification=qualification)
            receipts = []
            for round_name in ('first', 'second'):
                with tempfile.TemporaryDirectory(prefix='weir-ci-' + name + '-') as work:
                    source = Path(work) / 'source'
                    source.mkdir()
                    opts = dict(source=source, output=OUT / (name + '-' + round_name), env=go_env,
                                files=files, revision=revision, epoch=epoch, oci=True, builder=builder,
                                docker_env=env, qualification=qualification, base_layers=layers)
                    receipts.append(package.build_once(opts))
            if receipts[0] != receipts[1]:
                raise ValueError('repeated build mismatch: ' + name)
            receipt = receipts[0]
            print('BUILD_RECEIPT=' + json.dumps(dict(name=name, receipt=receipt), sort_keys=True), flush=True)
            delivery['images'][name] = dict(oci=receipt['oci'], binaries=receipt['binaries'],
                                            input_sha256=package.sha(json.dumps(receipt['inputs'], sort_keys=True).encode()))
        package.write_json(OUT / 'delivery.json', delivery)
        print('DELIVERY=' + json.dumps(delivery, sort_keys=True), flush=True)
    except subprocess.CalledProcessError as error:
        print((error.stderr or b'')[-16000:].decode(errors='replace'), flush=True)
        raise
    except RuntimeError:
        for log in OUT.glob('*-*/oci-build.log'):
            print(log.name + '\n' + log.read_text()[-16000:], flush=True)
        raise
    finally:
        # This client directory and unique builder were created by this invocation.
        inspection = package.run(['docker', 'buildx', 'ls', '--format', '{{.Name}}'], env=env).decode().splitlines()
        if inspection.count(builder) != 1:
            raise ValueError('builder owner changed; refuse cleanup')
        package.run(['docker', 'buildx', 'rm', builder], env=env)
        print('BUILD_CLEANUP=' + builder, flush=True)


def check_existing(reference, expected, env):
    result = subprocess.run(['regctl', 'manifest', 'head', reference], env=env, capture_output=True, timeout=60)
    if result.returncode == 0:
        if result.stdout.decode().strip() != expected:
            raise ValueError('same source tag has different content; refusing overwrite: ' + reference)
        return True
    # A failed auth/transport check must never be interpreted as a missing tag.
    if 'not found' not in result.stderr.decode().lower() and '404' not in result.stderr.decode():
        raise RuntimeError('tag preflight failed: ' + result.stderr.decode()[:1000])
    return False


def verify_archive(path, name, delivery):
    info = delivery['images'][name]
    options = dict(binary_name=name, revision=delivery['source'], base_layers=delivery['base_layers'])
    receipt = package.oci_receipt(path, info['binaries'], options)
    expected = info['oci']
    # Export tools may add a local ref-name to the outer layout, never to published content.
    if receipt['index'] != expected['index'] or receipt['images'] != expected['images']:
        raise ValueError('registry content differs from verified build: ' + name)
    return receipt


def publish():
    delivery = json.loads((OUT / 'delivery.json').read_text())
    env = {k: os.environ[k] for k in ('PATH', 'TMPDIR') if k in os.environ}
    env.update(HOME=str(OUT / 'publisher'), DOCKER_CONFIG=str(OUT / 'publisher/docker'),
               REGCTL_CONFIG=str(OUT / 'publisher/regctl.json'))
    for name, repository in IMAGES.items():
        digest = delivery['images'][name]['oci']['index']
        image = 'ghcr.io/batchstream/' + repository
        reference = image + ':' + delivery['source']
        exists = check_existing(reference, digest, env)
        if not exists:
            # Import verified BuildKit output directly; no rebuild or handwritten OCI conversion.
            archive = OUT / (name + '-first') / (name + '-linux.oci.tar')
            package.run(['regctl', 'image', 'import', image + '@' + digest, str(archive)], env=env, timeout=300)
        downloaded = OUT / (name + '-published.tar')
        package.run(['regctl', 'image', 'export', image + '@' + digest, str(downloaded)], env=env, timeout=180)
        verify_archive(downloaded, name, delivery)
        if not exists:
            # Check again immediately before assigning the sole full-SHA tag.
            exists = check_existing(reference, digest, env)
            if not exists:
                package.run(['regctl', 'image', 'copy', image + '@' + digest, reference], env=env, timeout=180)
        if not check_existing(reference, digest, env):
            raise ValueError('published tag missing')
        print('PUBLISHED=' + json.dumps(dict(image=image, source=delivery['source'], digest=digest)), flush=True)
    with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
        output.write('delivery=' + json.dumps(delivery, separators=(',', ':')) + '\n')


def verify_snapshot(result, expected_hash):
    if result.get('errors') or result.get('role') != 'client' or result.get('sequence') != 0:
        raise ValueError('snapshot sampling failed')
    identity = result['process']['identity']
    if identity != result['observer']['identity'] or identity['pid'] != '1' or identity['uid'] != 65532 or identity['exe_sha256'] != expected_hash or identity['start_ticks'] <= 0 or identity['cgroup'] != '0::/\n':
        raise ValueError('snapshot self identity mismatch')
    if set(identity['namespaces']) != {'pid', 'mnt', 'cgroup', 'net', 'user'} or not all(identity['namespaces'].values()):
        raise ValueError('snapshot namespace identity missing')
    files = result['files']
    required = {'memory.current', 'memory.max', 'memory.swap.max', 'memory.events', 'cpu.max', 'cpu.stat',
                'pids.current', 'pids.max', 'cpuset.cpus.effective', 'io.stat', 'limits', 'net/tcp', 'net/tcp6', 'status', 'stat', 'cgroup'}
    if not required <= files.keys() or any(not isinstance(files[k], str) or (k != 'io.stat' and not files[k].strip()) for k in required):
        raise ValueError('snapshot raw fields missing')
    if files['memory.max'].strip() != '268435456' or files['memory.swap.max'].strip() != '0' or files['pids.max'].strip() != '128' or files['cpu.max'].split() != ['100000', '100000']:
        raise ValueError('snapshot smoke resource limits mismatch')
    if result['duration_ns'] <= 0 or result['duration_ns'] != result['end_monotonic_ns'] - result['monotonic_ns'] or result['gomaxprocs'] <= 0 or result['goroutines'] <= 0 or result['go_heap_alloc_bytes'] <= 0:
        raise ValueError('snapshot runtime fields missing')
    for name in ('process', 'observer'):
        process = result[name]
        if process['rss_bytes'] <= 0 or process['fd'] <= 0 or process['threads'] <= 0 or process['user_ticks'] < 0 or process['system_ticks'] < 0 or not process['status'] or not process['stat']:
            raise ValueError('snapshot process fields missing')


def smoke_container(options):
    env, reference, name = options['env'], options['reference'], options['name']
    mode = options.get('mode', 'pace' if name == 'qualification' else 'version')
    owner = 'weir-m24-' + os.environ['GITHUB_RUN_ID'] + '-' + name + '-' + mode
    args = ['docker', 'create', '--name', owner, '--label', 'weir.m24.owner=' + owner,
            '--network=none', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
            '--user=65532:65532', '--cgroupns=private',
            '--cpus=1', '--memory=256m', '--memory-swap=256m', '--pids-limit=128',
            '--log-driver=json-file', '--log-opt=max-size=1m', '--log-opt=max-file=1']
    if name == 'qualification':
        args += ['--env', 'WEIR_CAPACITY_INTEGRATION=1', reference, '-mode', mode]
        if mode == 'pace':
            args += ['-seconds', '1', '-rate', '50']
    else:
        args += [reference, '-version']
    cid = package.run(args, env=env).decode().strip()
    try:
        raw = package.run(['docker', 'start', '--attach', cid], env=env, timeout=20)
        print('NATIVE_SMOKE_RAW=' + json.dumps(dict(name=name, mode=mode, stdout=raw.decode())), flush=True)
        inspection = json.loads(package.run(['docker', 'inspect', cid], env=env))[0]
        host = inspection['HostConfig']
        record = dict(id=cid, mode=mode, image=inspection['Image'], state=inspection['State'],
                      user=inspection['Config']['User'], host=host, mounts=inspection['Mounts'],
                      restart_count=inspection['RestartCount'], linux=options['linux'])
        print('NATIVE_SMOKE_CONTAINER=' + json.dumps(record, sort_keys=True), flush=True)
        if inspection['Image'] != options['config'] or inspection['Config']['User'] != '65532:65532' or inspection['Mounts'] or inspection['RestartCount'] or inspection['State']['OOMKilled']:
            raise ValueError('native smoke image/isolation mismatch')
        if host['NetworkMode'] != 'none' or not host['ReadonlyRootfs'] or host['Privileged'] or host['PidMode'] or host['CgroupnsMode'] != 'private' or host['CapDrop'] != ['ALL'] or 'no-new-privileges' not in host['SecurityOpt'] or host['NanoCpus'] != 1000000000 or host['Memory'] != 268435456 or host['MemorySwap'] != 268435456 or host['PidsLimit'] != 128:
            raise ValueError('native smoke resource/isolation mismatch')
        if inspection['State']['Running'] or inspection['State']['ExitCode'] != 0:
            raise ValueError('native smoke did not exit successfully')
        result = json.loads(raw)
        if name == 'weir':
            if result.get('revision') != options['source'] or result.get('target') != 'linux/' + options['arch'] or result.get('go') != 'go1.27.1' or result.get('state') != 'clean-commit' or result.get('dirty') != 'false':
                raise ValueError('product version contract mismatch')
        elif mode == 'snapshot':
            verify_snapshot(result, options['hash'])
        elif result.get('exe_sha256') != options['hash'] or result.get('goarch') != options['arch'] or result.get('goos') != 'linux' or result.get('kind') != 'native pacing diagnostic only':
            raise ValueError('qualification output identity mismatch')
        if name == 'qualification' and mode == 'pace':
            trial = result['trial']
            metrics = trial['measure']['all']
            if trial['planned'] != 50 or not trial['options']['TimingOnly'] or trial['options']['Seconds'] != 1 or metrics['completed'] + metrics['client_drop'] != 50:
                raise ValueError('pure pacing output contract mismatch')
        # No latency threshold is evaluated by this one-second execution smoke.
        print('NATIVE_SMOKE=' + json.dumps(dict(name=name, mode=mode, result=result), sort_keys=True), flush=True)
    finally:
        inspection = json.loads(package.run(['docker', 'inspect', cid], env=env))[0]
        if inspection['Id'] != cid or inspection['Config']['Labels'].get('weir.m24.owner') != owner:
            raise ValueError('container owner changed; refuse cleanup')
        package.run(['docker', 'stop', '--time', '2', cid], env=env, timeout=10)
        package.run(['docker', 'wait', cid], env=env, timeout=10)
        package.run(['docker', 'rm', cid], env=env)
        if package.run(['docker', 'ps', '-aq', '--filter', 'id=' + cid], env=env).strip():
            raise ValueError('native smoke container remains after removal')
        print('SMOKE_CLEANUP=' + json.dumps(dict(id=cid, owner=owner, removed=True)), flush=True)


def anonymous(delivery):
    if not re.fullmatch('[0-9a-f]{40}', delivery['source']):
        raise ValueError('invalid source SHA')
    env = client_environment(OUT / 'anonymous')
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}[platform.machine()]
    native = platform.system() == 'Linux'
    for name, repository in IMAGES.items():
        digest = delivery['images'][name]['oci']['index']
        if not re.fullmatch('sha256:[0-9a-f]{64}', digest):
            raise ValueError('invalid index digest')
        reference = 'ghcr.io/batchstream/' + repository + '@' + digest
        downloaded = OUT / (name + '-anonymous.tar')
        package.run(['regctl', 'image', 'export', reference, str(downloaded)], env=env, timeout=180)
        receipt = verify_archive(downloaded, name, delivery)
        record = dict(image=reference, receipt=receipt)
        package.write_json(OUT / (name + '-anonymous.json'), record)
        print('ANONYMOUS_PULL=' + json.dumps(record, sort_keys=True), flush=True)
        if native:
            engine = json.loads(package.run(['docker', 'info', '--format', '{{json .}}'], env=env))
            engine_arch = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(engine['Architecture'])
            if engine['OSType'] != 'linux' or engine_arch != arch:
                raise ValueError('native Docker engine required')
            platform_ref = 'ghcr.io/batchstream/' + repository + '@' + receipt['images'][arch]['manifest']
            package.run(['docker', 'pull', '--platform', 'linux/' + arch, platform_ref], env=env, timeout=180)
            linux = dict(os=engine['OSType'], arch=engine_arch, kernel=engine['KernelVersion'], cpus=engine['NCPU'])
            options = dict(env=env, reference=platform_ref, name=name, source=delivery['source'], arch=arch,
                           hash=receipt['images'][arch]['binary_sha256'], config=receipt['images'][arch]['config'], linux=linux)
            smoke_container(options)
            if name == 'qualification':
                options = dict(options, mode='snapshot')
                smoke_container(options)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('build', 'publish', 'anonymous'))
    args = parser.parse_args()
    if args.mode != 'anonymous':
        if os.environ.get('GITHUB_REPOSITORY') != 'batchstream/weir' or os.environ.get('GITHUB_REF') != 'refs/heads/main':
            raise ValueError('publication is restricted to batchstream/weir main')
        revision = package.clean_head(ROOT)
        if revision != os.environ.get('GITHUB_SHA'):
            raise ValueError('checkout does not match Actions source SHA')
    if args.mode == 'build':
        build()
    elif args.mode == 'publish':
        publish()
    else:
        delivery = json.loads(os.environ['WEIR_DELIVERY']) if os.environ.get('WEIR_DELIVERY') else json.loads((OUT / 'delivery.json').read_text())
        anonymous(delivery)


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        # These commands never take a token argument or print login state.
        print((error.stderr or b'')[-16000:].decode(errors='replace'), flush=True)
        raise
