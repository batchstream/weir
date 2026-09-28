"""Offline negative delivery tests. Tiny synthetic OCI data is never published."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import package

spec = importlib.util.spec_from_file_location('ci_images', Path(__file__).with_name('ci-images.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


def fixture(path, change):
    blobs = {}

    def blob(raw):
        digest = 'sha256:' + package.sha(raw)
        blobs['blobs/sha256/' + digest[7:]] = raw
        value = dict(digest=digest, size=len(raw))
        return value

    manifests = []
    for arch in ('amd64', 'arm64'):
        layer = io.BytesIO()
        with tarfile.open(fileobj=layer, mode='w') as output:
            entries = ['qualification', 'unapproved-config'] if change == 'extra' else ['qualification']
            for name in entries:
                item = tarfile.TarInfo(name)
                item.size, item.mode = 6, 0o555
                output.addfile(item, io.BytesIO(b'binary'))
        layer_info = blob(layer.getvalue())
        runtime = dict(User='0' if change == 'root' else '65532:65532', Entrypoint=['/qualification'],
                       Labels={'org.opencontainers.image.revision': 'b' * 40 if change == 'revision' else 'a' * 40,
                               'org.opencontainers.image.source': 'https://github.com/batchstream/weir'})
        config = dict(architecture=arch, os='linux', config=runtime)
        config_info = blob(json.dumps(config).encode())
        value = dict(config=config_info, layers=[layer_info])
        descriptor = blob(json.dumps(value).encode())
        descriptor.update(mediaType='application/vnd.oci.image.manifest.v1+json', platform=dict(os='linux', architecture=arch))
        manifests.append(descriptor)
    if change == 'duplicate':
        manifests.append(manifests[0])
    index = dict(schemaVersion=2, manifests=manifests)
    blobs['index.json'] = json.dumps(index).encode()
    if change == 'corrupt':
        blobs[next(iter(blobs))] = b'corrupted layer'
    with tarfile.open(path, 'w') as bundle:
        for name, raw in blobs.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            bundle.addfile(member, io.BytesIO(raw))


class DeliveryTests(unittest.TestCase):
    def test_oci_rejects_unapproved_content_and_identity(self):
        hashes = {'linux-' + arch: package.sha(b'binary') for arch in ('amd64', 'arm64')}
        options = dict(binary_name='qualification', revision='a' * 40, base_layers=dict(amd64=[], arm64=[]))
        with tempfile.TemporaryDirectory() as temp:
            for change in ('none', 'extra', 'root', 'revision', 'duplicate', 'corrupt'):
                path = Path(temp) / (change + '.tar')
                fixture(path, change)
                if change == 'none':
                    result = package.oci_receipt(path, hashes, options)
                    self.assertEqual(set(result['images']), {'amd64', 'arm64'})
                    wrong_base = dict(options, base_layers=dict(amd64=['sha256:foreign'], arm64=[]))
                    with self.assertRaisesRegex(ValueError, 'base layers'):
                        package.oci_receipt(path, hashes, wrong_base)
                else:
                    with self.assertRaises(ValueError, msg=change):
                        package.oci_receipt(path, hashes, options)

    def test_existing_tag_never_overwritten_or_auth_failure_ignored(self):
        digest = 'sha256:' + 'a' * 64
        for code, stdout, stderr, expected in (
                (0, digest, '', True), (1, '', 'HTTP status: 404 Not Found', False)):
            result = subprocess.CompletedProcess([], code, stdout.encode(), stderr.encode())
            with patch.object(subprocess, 'run', return_value=result):
                self.assertEqual(ci.check_existing('example:sha', digest, {}), expected)
        for code, stdout, stderr in ((0, 'sha256:wrong', ''), (1, '', 'unauthorized'), (1, '', 'connection refused')):
            result = subprocess.CompletedProcess([], code, stdout.encode(), stderr.encode())
            with patch.object(subprocess, 'run', return_value=result):
                with self.assertRaises((ValueError, RuntimeError)):
                    ci.check_existing('example:sha', digest, {})

    def test_anonymous_environment_has_no_inherited_auth(self):
        with tempfile.TemporaryDirectory() as temp:
            inherited = dict(GITHUB_TOKEN='must-not-inherit', DOCKER_AUTH_CONFIG='must-not-inherit', REGCTL_CONFIG='foreign')
            with patch.dict(ci.os.environ, inherited):
                env = ci.client_environment(Path(temp) / 'client')
            self.assertNotIn('GITHUB_TOKEN', env)
            self.assertNotIn('DOCKER_AUTH_CONFIG', env)
            self.assertEqual(json.loads((Path(env['DOCKER_CONFIG']) / 'config.json').read_text()), {'auths': {}})
            self.assertTrue(env['REGCTL_CONFIG'].startswith(temp))
            with self.assertRaises(FileExistsError):
                ci.client_environment(Path(temp) / 'client')


if __name__ == '__main__':
    unittest.main()
