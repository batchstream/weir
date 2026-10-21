#!/usr/bin/env python3
"""Publish a stable GitHub release for a merged-main source whose image is already on GHCR."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

IMAGE = 'ghcr.io/batchstream/weir'
ARCHIVES = tuple('weir-' + system + '-' + arch + '.tar.gz' for system in ('linux', 'darwin') for arch in ('amd64', 'arm64'))


def version_valid(value):
    return re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', value) is not None


def run(args):
    return subprocess.check_output(args, text=True, timeout=180).strip()


def check_source(version):
    if version and not version_valid(version):
        raise ValueError('release version must be vMAJOR.MINOR.PATCH')
    if os.environ.get('GITHUB_REPOSITORY') != 'batchstream/weir':
        raise ValueError('release requires batchstream/weir')
    event, ref = os.environ.get('GITHUB_EVENT_NAME'), os.environ.get('GITHUB_REF')
    if event == 'workflow_dispatch':
        if ref != 'refs/heads/main':
            raise ValueError('manual release requires main')
    elif event == 'release':
        if not version or ref != 'refs/tags/' + version:
            raise ValueError('published release requires its version tag')
    else:
        raise ValueError('unsupported release event')
    source = os.environ.get('GITHUB_SHA', '')
    if re.fullmatch('[0-9a-f]{40}', source) is None or run(['git', 'rev-parse', 'HEAD']) != source:
        raise ValueError('release source differs from workflow checkout')
    run(['git', 'fetch', '--no-tags', 'origin', 'main'])
    result = subprocess.run(['git', 'merge-base', '--is-ancestor', source, 'origin/main'],
                            capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise ValueError('release source must be merged into main')


def release_assets(directory):
    # The four platform archives plus the SHA256SUMS that package.py wrote for them.
    checksum_file = directory / 'SHA256SUMS'
    listed = {}
    for line in checksum_file.read_text().splitlines():
        digest, name = line.split('  ', 1)
        listed[name] = digest
    if set(listed) != set(ARCHIVES):
        raise ValueError('SHA256SUMS must list exactly the four platform archives')
    for name in ARCHIVES:
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != listed[name]:
            raise ValueError('release archive checksum mismatch: ' + name)
    assets = [directory / name for name in ARCHIVES]
    assets.append(checksum_file)
    return assets


def resume_release(version, assets):
    result = subprocess.run(['gh', 'release', 'view', version, '--repo', 'batchstream/weir', '--json', 'tagName,isDraft,assets'],
                            capture_output=True, text=True, timeout=60)
    if result.returncode:
        if 'not found' in result.stderr.lower() or '404' in result.stderr:
            return False
        raise RuntimeError('release lookup failed')
    release = json.loads(result.stdout)
    if release['tagName'] != version or release['isDraft']:
        raise ValueError('existing release identity or publication state differs')
    expected = {item.name: item for item in assets}
    with tempfile.TemporaryDirectory(prefix='weir-release-') as temporary:
        if release['assets']:
            run(['gh', 'release', 'download', version, '--repo', 'batchstream/weir', '--dir', temporary])
        existing = {item.name: item for item in Path(temporary).iterdir()}
        if not existing.keys() <= expected.keys():
            raise ValueError('existing release has unexpected assets')
        for name, item in existing.items():
            if hashlib.sha256(item.read_bytes()).digest() != hashlib.sha256(expected[name].read_bytes()).digest():
                raise ValueError('existing release asset differs: ' + name)
        missing = [str(item) for name, item in expected.items() if name not in existing]
        if missing:
            run(['gh', 'release', 'upload', version, '--repo', 'batchstream/weir', *missing])
    return True


def tag_image(version, source, digest):
    # Copy the published source image to its version tag; never retag different content.
    if re.fullmatch(r'sha256:[0-9a-f]{64}', digest) is None:
        raise ValueError('invalid image digest')
    if run(['regctl', 'manifest', 'head', IMAGE + ':' + source]) != digest:
        raise ValueError('published source image differs from the built image')
    reference = IMAGE + ':' + version
    existing = subprocess.run(['regctl', 'manifest', 'head', reference], capture_output=True, text=True, timeout=60)
    if existing.returncode == 0:
        if existing.stdout.strip() != digest:
            raise ValueError('version image already exists with different content')
    elif 'not found' not in existing.stderr.lower() and '404' not in existing.stderr:
        raise RuntimeError('version image preflight failed')
    else:
        run(['regctl', 'image', 'copy', IMAGE + '@' + digest, reference])
    if run(['regctl', 'manifest', 'head', reference]) != digest:
        raise ValueError('version image verification failed')
    return reference


def publish(version, directory):
    if not version_valid(version):
        raise ValueError('release version must be vMAJOR.MINOR.PATCH')
    check_source(version)
    source = os.environ['GITHUB_SHA']
    assets = release_assets(directory)
    # Check both lightweight and annotated tags; never retarget a version.
    tag = 'refs/tags/' + version
    refs = dict(line.split()[::-1] for line in run(['git', 'ls-remote', '--tags', 'origin', tag, tag + '^{}']).splitlines())
    if refs and refs.get(tag + '^{}', refs.get(tag)) != source:
        raise ValueError('release tag already points to a different source')
    digest = os.environ['WEIR_IMAGE_DIGEST']
    reference = tag_image(version, source, digest)
    if refs and resume_release(version, assets):
        return
    notes = directory / 'release-notes.md'
    notes.write_text(
        f'Weir {version}, built from `{source}`.\n\n'
        f'Image: `{reference}` (`{digest}`), for Linux amd64 and arm64.\n\n'
        'Archives are provided for Linux and macOS on amd64 and arm64. '
        'Windows server builds are unavailable because the Lua dependency uses Unix process primitives. '
        'Verify downloaded archives with `sha256sum -c SHA256SUMS`.\n\n'
        'Use the [Go SDK](https://github.com/batchstream/weir-go) and '
        '[Helm chart](https://github.com/batchstream/weir-charts); see '
        f'[the architecture](https://github.com/batchstream/weir/blob/{source}/docs/architecture.md).\n')
    run(['gh', 'release', 'create', version, '--repo', 'batchstream/weir', '--target', source,
         '--title', version, '--notes-file', str(notes), *map(str, assets)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-source', action='store_true', help='only validate the optional version and merged source')
    parser.add_argument('version')
    parser.add_argument('directory', nargs='?', type=Path, help='directory holding the archives and SHA256SUMS')
    args = parser.parse_args()
    if args.check_source:
        check_source(args.version)
        return
    if args.directory is None:
        parser.error('release archive directory is required')
    publish(args.version, args.directory)


if __name__ == '__main__':
    main()
