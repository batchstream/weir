#!/usr/bin/env python3
"""Build the Weir image once, publish it to GHCR under its source SHA and smoke-run it."""

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess

import package
import release

ROOT = package.ROOT
OUT = ROOT / '.testdata/images'
DIST = OUT / 'dist'
OCI = DIST / 'weir-linux.oci.tar'


def client_environment(directory, *, plugins=False):
    # A fresh Docker/regctl client state that never inherits registry credentials.
    directory.mkdir(parents=True, exist_ok=False)
    config = directory / 'docker'
    config.mkdir()
    value = {'auths': {}}
    if plugins:
        value['cliPluginsExtraDirs'] = [str(ROOT / '.tools/ci/plugins')]
    package.write_json(config / 'config.json', value)
    env = {k: os.environ[k] for k in ('PATH', 'TMPDIR', 'BUILDX_CONFIG') if k in os.environ}
    env.update(
        HOME=str(directory),
        DOCKER_CONFIG=str(config),
        DOCKER_HOST='unix:///var/run/docker.sock',
        REGCTL_CONFIG=str(directory / 'regctl.json'),
    )
    return env


def build():
    env = client_environment(OUT / 'builder-client', plugins=True)
    pins = json.loads((ROOT / 'scripts/ci-tools.json').read_text())
    builder = 'weir-images-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT']
    create = ['docker', 'buildx', 'create', '--name', builder, '--driver', 'docker-container',
              '--driver-opt', 'image=' + pins['buildkit']['image']]
    package.run(create, env=env)
    try:
        package.run(['docker', 'buildx', 'inspect', '--bootstrap', builder], env=env, timeout=180)
        opts = dict(output=DIST, oci=True, builder=builder, docker_env=env)
        package.build(opts)
        print('Built', package.oci_index_digest(OCI), flush=True)
    finally:
        package.run(['docker', 'buildx', 'rm', builder], env=env)


def check_existing(reference, expected, env):
    result = subprocess.run(
        ['regctl', 'manifest', 'head', reference], env=env, capture_output=True, timeout=60
    )
    if result.returncode == 0:
        if result.stdout.decode().strip() != expected:
            raise ValueError(
                'same source tag has different content; refusing overwrite: ' + reference
            )
        return True
    # A failed auth/transport check must never be interpreted as a missing tag.
    if 'not found' not in result.stderr.decode().lower() and '404' not in result.stderr.decode():
        raise RuntimeError('tag preflight failed: ' + result.stderr.decode()[:1000])
    return False


def publish():
    digest = package.oci_index_digest(OCI)
    env = {k: os.environ[k] for k in ('PATH', 'TMPDIR') if k in os.environ}
    env.update(
        HOME=str(OUT / 'publisher'),
        DOCKER_CONFIG=str(OUT / 'publisher/docker'),
        REGCTL_CONFIG=str(OUT / 'publisher/regctl.json'),
    )
    reference = release.IMAGE + ':' + os.environ['GITHUB_SHA']
    if not check_existing(reference, digest, env):
        # Push the built OCI layout as is; no rebuild.
        package.run(['regctl', 'image', 'import', release.IMAGE + '@' + digest, str(OCI)], env=env, timeout=300)
        package.run(['regctl', 'image', 'copy', release.IMAGE + '@' + digest, reference], env=env, timeout=180)
        if not check_existing(reference, digest, env):
            raise ValueError('published tag missing: ' + reference)
    print('Published', reference, digest, flush=True)
    with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
        output.write('digest=' + digest + '\n')


def smoke():
    # Pull the published image without credentials and run its version command.
    digest = package.oci_index_digest(OCI)
    env = client_environment(OUT / 'smoke')
    arch = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}[platform.machine().lower()]
    args = ['docker', 'run', '--rm', '--pull=always', '--platform=linux/' + arch, '--network=none',
            '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
            release.IMAGE + '@' + digest, 'version']
    raw = package.run(args, env=env, timeout=180).decode()
    identity = json.loads(raw)
    if identity.get('product') != 'weir' or identity.get('target') != 'linux/' + arch or identity.get('go') != package.GO:
        raise ValueError('unexpected weir version output: ' + raw)
    print('Smoke', raw.strip(), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('build', 'publish', 'smoke'))
    args = parser.parse_args()
    if args.mode != 'smoke':
        release.check_source(os.environ.get('RELEASE_VERSION', ''))
        if package.clean_head(ROOT) != os.environ.get('GITHUB_SHA'):
            raise ValueError('checkout does not match Actions source SHA')
    if args.mode == 'build':
        build()
    elif args.mode == 'publish':
        publish()
    else:
        smoke()


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        # These commands never take a token argument or print login state.
        print((error.stderr or b'')[-16000:].decode(errors='replace'), flush=True)
        raise
