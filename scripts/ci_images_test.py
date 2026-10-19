"""Offline negative delivery tests. Tiny synthetic OCI data is never published."""
import copy
import importlib.util
import fnmatch
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import package
import release
from release_test import asset_fixture

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

    def assert_candidate_retention(self, workflow):
        job = workflow.split('\n  publish:\n', 1)[1].split('\n  release:\n', 1)[0]
        self.assertIn('    needs: test\n', job)
        self.assertNotIn('continue-on-error:', job)
        blocks = re.findall(r'^      - name: ([^\n]+)\n(.*?)(?=^      - |\Z)', job, re.M | re.S)
        steps = dict(blocks)
        names = [name for name, _ in blocks]
        gates = {
            'Reproduce and validate the server image': 'timeout 24m python3 scripts/ci-images.py build',
            'Publish exact verified content': 'python3 scripts/ci-images.py publish',
            'Anonymous full pull and native amd64 smoke': 'python3 scripts/ci-images.py anonymous',
            'Verify candidate distribution bundle': 'python3 scripts/release.py --verify-only "$RELEASE_VERSION" .testdata/images',
        }
        upload_name = 'Preserve verified candidate and release assets'
        ordered = [*gates, upload_name]
        self.assertEqual([name for name in names if name in ordered], ordered)
        for name, command in gates.items():
            self.assertIn('        run: ' + command + '\n', steps[name])
        for name in ordered:
            # No release-only condition or always() shortcut: every prerequisite must succeed.
            self.assertNotIn('        if:', steps[name])
        verify = steps['Verify candidate distribution bundle']
        self.assertIn('          WEIR_DELIVERY: ${{ steps.publish.outputs.delivery }}\n', verify)
        upload = steps[upload_name]
        self.assertIn('actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02', upload)
        self.assertIn('          name: verified-release\n', upload)
        self.assertIn('          if-no-files-found: error\n', upload)
        self.assertEqual(job.count('uses: actions/upload-artifact@'), 1)
        paths = re.findall(r'^            (.+)$', upload, re.M)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            delivery = asset_fixture(root)
            verified = release.verify_assets(root, delivery)
            expected = {'.testdata/images/' + item.relative_to(root).as_posix() for item in verified}
        self.assertEqual(len(paths), len(expected))
        self.assertEqual(set(paths), expected)
        self.assertIn('          name: verified-release\n          path: .testdata/release\n', workflow)
        self.assertIn("  release:\n    if: github.event_name == 'release' || inputs.release_version != ''\n", workflow)

    def test_successful_nonrelease_candidate_retains_only_the_verified_bundle(self):
        workflow = (package.ROOT / '.github/workflows/images.yml').read_text()
        self.assert_candidate_retention(workflow)

    def test_candidate_retention_contract_rejects_missing_or_unsafe_uploads(self):
        workflow = (package.ROOT / '.github/workflows/images.yml').read_text()
        upload_name = '      - name: Preserve verified candidate and release assets\n'
        changes = [
            workflow.replace(upload_name, upload_name + "        if: inputs.release_version != ''\n"),
            workflow.replace('            .testdata/images/weir-first/weir-linux-arm64.tar.gz\n', ''),
            workflow.replace(upload_name, upload_name + '        if: always()\n'),
            workflow.replace('      - name: Verify candidate distribution bundle\n',
                             '      - name: Verify candidate distribution bundle\n        if: always()\n'),
            workflow.replace('      - name: Anonymous full pull and native amd64 smoke\n',
                             '      - name: Anonymous full pull and native amd64 smoke\n        continue-on-error: true\n'),
            workflow.replace('      - name: Reproduce and validate the server image\n',
                             '      - name: Reproduce and validate the server image\n        continue-on-error: true\n'),
        ]
        # Path strings only: never inspect actual job-owned login state or secret files.
        for extra in ('.testdata/images', '.testdata/images/**', '.testdata/images/publisher/docker',
                      '.testdata/images/.env', '.testdata/images/server.key', '.testdata/images/credentials'):
            changes.append(workflow.replace('          path: |\n', '          path: |\n            ' + extra + '\n'))
        for index, changed in enumerate(changes):
            with self.subTest(mutation=index), self.assertRaises(AssertionError):
                self.assert_candidate_retention(changed)

    def test_repeated_build_mismatch_cannot_complete_delivery(self):
        source = 'a' * 40
        builder = 'weir-images-fixture-1'
        listings = iter((b'', (builder + '\n').encode()))

        def command(args, **kwargs):
            if args[:3] == ['docker', 'buildx', 'ls']:
                return next(listings)
            if args[:2] == ['git', 'show']:
                return b'1700000001'
            return b''

        first = {'binaries': {'linux-amd64': 'a' * 64}}
        second = {'binaries': {'linux-amd64': 'b' * 64}}
        empty = {}
        environment = dict(GITHUB_RUN_ID='fixture', GITHUB_RUN_ATTEMPT='1')
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'images'
            with patch.dict(os.environ, environment), patch.object(ci, 'OUT', output), \
                 patch.object(ci, 'client_environment', return_value=empty), \
                 patch.object(ci, 'base_layers', return_value=empty), \
                 patch.object(package, 'clean_head', return_value=source), \
                 patch.object(package, 'source_files', return_value=[]), \
                 patch.object(package, 'go_environment', return_value=empty), \
                 patch.object(package, 'build_once', side_effect=[first, second]), \
                 patch.object(package, 'run', side_effect=command):
                with self.assertRaisesRegex(ValueError, 'repeated build mismatch'):
                    ci.build()
            self.assertFalse((output / 'delivery.json').exists())
            self.assertFalse((output / 'SHA256SUMS').exists())

    def test_anonymous_native_smoke_failure_propagates(self):
        digest = 'sha256:' + 'b' * 64
        image = {'oci': {'index': digest}}
        delivery = {'source': 'a' * 40, 'images': {'weir': image}}
        receipt = {'images': {'amd64': {'manifest': digest, 'binary_sha256': 'c' * 64, 'config': digest}}}
        engine = dict(Architecture='amd64', OSType='linux', KernelVersion='fixture', NCPU=1)
        environment = dict(WEIR_DELIVERY=json.dumps(delivery))
        empty = {}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with patch.dict(os.environ, environment), patch.object(sys, 'argv', ['ci-images.py', 'anonymous']), \
                 patch.object(ci, 'OUT', output), patch.object(ci, 'client_environment', return_value=empty), \
                 patch.object(ci.platform, 'machine', return_value='x86_64'), \
                 patch.object(ci.platform, 'system', return_value='Linux'), \
                 patch.object(ci, 'verify_archive', return_value=receipt), \
                 patch.object(package, 'run', side_effect=[b'', json.dumps(engine).encode(), b'']), \
                 patch.object(ci, 'smoke_container', side_effect=ValueError('native smoke failed')) as smoke:
                with self.assertRaisesRegex(ValueError, 'native smoke failed'):
                    ci.main()
                smoke.assert_called_once()
            self.assertFalse((output / 'SHA256SUMS').exists())

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
