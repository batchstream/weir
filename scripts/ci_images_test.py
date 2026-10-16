"""Offline negative delivery tests. Tiny synthetic OCI data is never published."""
import copy
import importlib.util
import fnmatch
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import package
import release

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
            entries = ['weir', 'unapproved-config'] if change == 'extra' else ['weir']
            for name in entries:
                item = tarfile.TarInfo(name)
                item.size, item.mode = 6, 0o555
                output.addfile(item, io.BytesIO(b'binary'))
        layer_info = blob(layer.getvalue())
        runtime = dict(User='0' if change == 'root' else '65532:65532', Entrypoint=['/weir'],
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
    def test_entrypoint_accepts_verified_main_and_release_sources(self):
        source = 'a' * 40
        cases = (
            dict(GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REF='refs/heads/main', RELEASE_VERSION=''),
            dict(GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REF='refs/heads/main', RELEASE_VERSION='v1.2.3'),
            dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/tags/v1.2.3', RELEASE_VERSION='v1.2.3'),
        )
        for mode in ('build', 'publish'):
            for case in cases:
                environment = dict(GITHUB_REPOSITORY='batchstream/weir', GITHUB_SHA=source, **case)
                ancestry_result = subprocess.CompletedProcess([], 0)
                with self.subTest(mode=mode, case=case), patch.dict(os.environ, environment), \
                     patch.object(sys, 'argv', ['ci-images.py', mode]), \
                     patch.object(ci.package, 'clean_head', return_value=source), \
                     patch.object(release, 'run', side_effect=[source, '']) as git, \
                     patch.object(release.subprocess, 'run', return_value=ancestry_result) as ancestry, \
                     patch.object(ci, mode) as action:
                    ci.main()
                    action.assert_called_once_with()
                    self.assertEqual(git.call_args_list[0].args[0], ['git', 'rev-parse', 'HEAD'])
                    self.assertEqual(git.call_args_list[1].args[0], ['git', 'fetch', '--no-tags', 'origin', 'main'])
                    self.assertEqual(ancestry.call_args.args[0],
                                     ['git', 'merge-base', '--is-ancestor', source, 'origin/main'])

    def test_entrypoint_refuses_unqualified_sources_before_build_or_publish(self):
        source = 'a' * 40
        cases = (
            ('fork', dict(GITHUB_REPOSITORY='fork/weir'), source, 0),
            ('push', dict(GITHUB_EVENT_NAME='push'), source, 0),
            ('manual_branch', dict(GITHUB_REF='refs/heads/feature'), source, 0),
            ('manual_tag', dict(GITHUB_REF='refs/tags/v1.2.3'), source, 0),
            ('noncanonical_version', dict(RELEASE_VERSION='v01.2.3'), source, 0),
            ('release_without_version', dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/tags/v1.2.3', RELEASE_VERSION=''), source, 0),
            ('release_branch', dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/heads/main'), source, 0),
            ('release_wrong_tag', dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/tags/v1.2.4'), source, 0),
            ('invalid_source', dict(GITHUB_SHA='not-a-sha'), source, 0),
            ('checkout_mismatch', dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/tags/v1.2.3'), 'b' * 40, 0),
            ('unmerged_release', dict(GITHUB_EVENT_NAME='release', GITHUB_REF='refs/tags/v1.2.3'), source, 1),
        )
        for mode in ('build', 'publish'):
            for name, changed, checkout, ancestry_status in cases:
                environment = dict(GITHUB_REPOSITORY='batchstream/weir', GITHUB_EVENT_NAME='workflow_dispatch',
                                   GITHUB_REF='refs/heads/main', GITHUB_SHA=source, RELEASE_VERSION='v1.2.3')
                environment.update(changed)
                ancestry_result = subprocess.CompletedProcess([], ancestry_status)
                with self.subTest(mode=mode, case=name), patch.dict(os.environ, environment), \
                     patch.object(sys, 'argv', ['ci-images.py', mode]), \
                     patch.object(ci.package, 'clean_head', return_value=source), \
                     patch.object(release, 'run', side_effect=[checkout, '']), \
                     patch.object(release.subprocess, 'run', return_value=ancestry_result), \
                     patch.object(ci, mode) as action:
                    with self.assertRaises(ValueError):
                        ci.main()
                    action.assert_not_called()

    def test_entrypoint_requires_the_verified_source_to_remain_clean_and_unchanged(self):
        source = 'a' * 40
        environment = dict(GITHUB_REPOSITORY='batchstream/weir', GITHUB_EVENT_NAME='workflow_dispatch',
                           GITHUB_REF='refs/heads/main', GITHUB_SHA=source, RELEASE_VERSION='')
        for mode in ('build', 'publish'):
            for changed in (False, True):
                ancestry_result = subprocess.CompletedProcess([], 0)
                with self.subTest(mode=mode, changed=changed), patch.dict(os.environ, environment), \
                     patch.object(sys, 'argv', ['ci-images.py', mode]), \
                     patch.object(release, 'run', side_effect=[source, '']), \
                     patch.object(release.subprocess, 'run', return_value=ancestry_result), \
                     patch.object(ci.package, 'clean_head') as clean_head, \
                     patch.object(ci, mode) as action:
                    if changed:
                        clean_head.return_value = 'b' * 40
                    else:
                        clean_head.side_effect = ValueError('dirty worktree')
                    with self.assertRaises(ValueError):
                        ci.main()
                    action.assert_not_called()

    def test_workflow_discovers_all_offline_python_tests_in_both_modes(self):
        workflow = (package.ROOT / '.github/workflows/images.yml').read_text()
        for mode in ('', '-O '):
            self.assertIn("python3 " + mode + "-m unittest discover -v -s scripts -p '*_test.py'", workflow)
        tests = {p.name for p in Path(__file__).parent.glob('*_test.py')}
        self.assertTrue({'package_test.py', 'release_test.py', 'ci_images_test.py',
                         'test_memory_linux_test.py', 'check_dependencies_test.py', 'config_yaml_test.py'} <= tests)
        self.assertTrue(all(fnmatch.fnmatch(name, '*_test.py') for name in tests))

    def test_oci_rejects_unapproved_content_and_identity(self):
        hashes = {'linux-' + arch: package.sha(b'binary') for arch in ('amd64', 'arm64')}
        options = dict(binary_name='weir', revision='a' * 40, base_layers=dict(amd64=[], arm64=[]))
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
