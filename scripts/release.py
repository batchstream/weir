#!/usr/bin/env python3
"""Publish a stable version of already verified, merged-main artifacts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def version_valid(value):
    return re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', value) is not None


def run(args):
    return subprocess.check_output(args, text=True, timeout=180).strip()


def verify_assets(directory, delivery):
    source = delivery['source']
    if re.fullmatch('[0-9a-f]{40}', source) is None:
        raise ValueError('invalid source revision')
    if json.loads((directory / 'delivery.json').read_text()) != delivery:
        raise ValueError('delivery receipt differs from verified workflow output')
    product = directory / 'weir-first'
    receipt = json.loads((product / 'receipt.json').read_text())
    if receipt['source'] != source or receipt['binaries'] != delivery['images']['weir']['binaries']:
        raise ValueError('release source or binary identity mismatch')
    expected = {'weir-' + target + ('.zip' if target.startswith('windows') else '.tar.gz')
                for target in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64', 'windows-amd64', 'windows-arm64')}
    if set(receipt['archives']) != expected:
        raise ValueError('release must contain exactly six platform archives')
    for name, digest in receipt['archives'].items():
        if hashlib.sha256((product / name).read_bytes()).hexdigest() != digest:
            raise ValueError('release archive checksum mismatch: ' + name)
    assets = [directory / 'delivery.json', product / 'receipt.json', product / 'module-graph.json']
    assets += [product / name for name in sorted(expected)]
    assets += [product / (target + '-linked.json') for target in sorted(receipt['binaries'])]
    checksums = ''.join(hashlib.sha256(item.read_bytes()).hexdigest() + '  ' + item.name + '\n' for item in assets)
    checksum_file = directory / 'SHA256SUMS'
    checksum_file.write_text(checksums)
    return assets + [checksum_file]


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
    with tempfile.TemporaryDirectory(prefix='weir-release-verify-') as temporary:
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


def publish(version, directory):
    if not version_valid(version):
        raise ValueError('release version must be vMAJOR.MINOR.PATCH')
    if os.environ.get('GITHUB_REPOSITORY') != 'batchstream/weir' or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('release requires batchstream/weir main')
    delivery = json.loads(os.environ['WEIR_DELIVERY'])
    source = delivery['source']
    if source != os.environ['GITHUB_SHA'] or run(['git', 'rev-parse', 'HEAD']) != source:
        raise ValueError('release source differs from workflow checkout')
    assets = verify_assets(directory, delivery)
    # Verify both lightweight and annotated tags; never retarget a version.
    tag = 'refs/tags/' + version
    refs = dict(line.split()[::-1] for line in run(['git', 'ls-remote', '--tags', 'origin', tag, tag + '^{}']).splitlines())
    if refs and refs.get(tag + '^{}', refs.get(tag)) != source:
        raise ValueError('release tag already points to a different source')
    image = 'ghcr.io/batchstream/weir'
    digest = delivery['images']['weir']['oci']['index']
    if re.fullmatch(r'sha256:[0-9a-f]{64}', digest) is None:
        raise ValueError('invalid image digest')
    if run(['regctl', 'manifest', 'head', image + ':' + source]) != digest:
        raise ValueError('published source image differs from verified artifact')
    reference = image + ':' + version
    existing = subprocess.run(['regctl', 'manifest', 'head', reference], capture_output=True, text=True, timeout=60)
    if existing.returncode == 0:
        if existing.stdout.strip() != digest:
            raise ValueError('version image already exists with different content')
    elif 'not found' not in existing.stderr.lower() and '404' not in existing.stderr:
        raise RuntimeError('version image preflight failed')
    else:
        run(['regctl', 'image', 'copy', image + '@' + digest, reference])
    if run(['regctl', 'manifest', 'head', reference]) != digest:
        raise ValueError('version image verification failed')
    if refs and resume_release(version, assets):
        return
    notes = directory / 'release-notes.md'
    notes.write_text(
        f'Source: `{source}` (merged main).\n\n'
        f'Image: `{reference}@{digest}`. Native Linux amd64 tests, race tests, '
        'reproducible builds and anonymous amd64 image smoke completed before this release. '
        'Archives are provided for six targets; build availability does not imply native production qualification.\n\n'
        'Verify downloaded assets with `sha256sum -c SHA256SUMS`. The binary reports its immutable source identity.\n\n'
        'Use the [Go SDK](https://github.com/batchstream/weir-go) and '
        '[Helm chart](https://github.com/batchstream/weir-charts). '
        'Deployment isolation and backend qualification remain required; see '
        f'[the architecture](https://github.com/batchstream/weir/blob/{source}/docs/architecture.md).\n')
    run(['gh', 'release', 'create', version, '--repo', 'batchstream/weir', '--target', source,
         '--title', version, '--notes-file', str(notes), *map(str, assets)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-version', action='store_true')
    parser.add_argument('version')
    parser.add_argument('directory', nargs='?', type=Path)
    args = parser.parse_args()
    if args.check_version:
        if args.version and not version_valid(args.version):
            raise ValueError('release version must be vMAJOR.MINOR.PATCH')
        return
    if args.directory is None:
        parser.error('release artifact directory is required')
    publish(args.version, args.directory)


if __name__ == '__main__':
    main()
