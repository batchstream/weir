"""Offline release-input integrity tests; these never contact a registry or GitHub."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
import subprocess

import release


def asset_fixture(root):
    product = root / 'weir-first'
    product.mkdir()
    source = 'a' * 40
    archives, binaries = {}, {}
    for target in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64'):
        name = 'weir-' + target + '.tar.gz'
        raw = ('test-archive-' + target).encode()
        (product / name).write_bytes(raw)
        archives[name] = hashlib.sha256(raw).hexdigest()
        binaries[target] = 'b' * 64
        (product / (target + '-linked.json')).write_text('{}')
    (product / 'module-graph.json').write_text('[]')
    receipt = dict(source=source, archives=archives, binaries=binaries)
    delivery = {'source': source, 'images': {'weir': {'binaries': binaries}}}
    (root / 'delivery.json').write_text(json.dumps(delivery))
    (product / 'receipt.json').write_text(json.dumps(receipt))
    return delivery


class ReleaseTests(unittest.TestCase):
    def test_release_source_requires_merged_main_and_matching_checkout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, checkout = root / 'remote.git', root / 'checkout'
            subprocess.check_output(['git', 'init', '--bare', str(remote)], stderr=subprocess.STDOUT)
            subprocess.check_output(['git', 'init', '-b', 'main', str(checkout)], stderr=subprocess.STDOUT)
            subprocess.check_output(['git', '-c', 'user.name=Release Test', '-c', 'user.email=test@example.invalid',
                                     'commit', '--allow-empty', '-m', 'merged'], cwd=checkout, stderr=subprocess.STDOUT)
            subprocess.check_output(['git', 'remote', 'add', 'origin', str(remote)], cwd=checkout)
            subprocess.check_output(['git', 'push', 'origin', 'main'], cwd=checkout, stderr=subprocess.STDOUT)
            merged = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip()
            environment = dict(GITHUB_REPOSITORY='batchstream/weir', GITHUB_EVENT_NAME='release',
                               GITHUB_REF='refs/tags/v0.2.0', GITHUB_SHA=merged)
            previous = Path.cwd()
            try:
                os.chdir(checkout)
                with patch.dict(os.environ, environment):
                    release.check_source('v0.2.0')
                    with patch.dict(os.environ, GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REF='refs/heads/main'):
                        release.check_source('')
                    with patch.dict(os.environ, GITHUB_SHA='f' * 40):
                        with self.assertRaisesRegex(ValueError, 'workflow checkout'):
                            release.check_source('v0.2.0')
                subprocess.check_output(['git', 'switch', '-c', 'unmerged'], stderr=subprocess.STDOUT)
                subprocess.check_output(['git', '-c', 'user.name=Release Test', '-c', 'user.email=test@example.invalid',
                                         'commit', '--allow-empty', '-m', 'unmerged'], stderr=subprocess.STDOUT)
                unmerged = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
                with patch.dict(os.environ, environment), patch.dict(os.environ, GITHUB_SHA=unmerged):
                    with self.assertRaisesRegex(ValueError, 'merged into main'):
                        release.check_source('v0.2.0')
            finally:
                os.chdir(previous)

    def test_release_event_cannot_publish_another_ref_or_repository(self):
        environment = dict(GITHUB_REPOSITORY='batchstream/weir', GITHUB_EVENT_NAME='release',
                           GITHUB_REF='refs/tags/v0.2.0', GITHUB_SHA='a' * 40)
        for change in (dict(GITHUB_REPOSITORY='fork/weir'), dict(GITHUB_EVENT_NAME='push'),
                       dict(GITHUB_REF='refs/heads/main'), dict(GITHUB_REF='refs/tags/v0.3.0')):
            with self.subTest(change=change), patch.dict(os.environ, environment), patch.dict(os.environ, change):
                with patch.object(release.subprocess, 'check_output') as invoke:
                    with self.assertRaises(ValueError):
                        release.check_source('v0.2.0')
                    invoke.assert_not_called()
        with patch.dict(os.environ, environment), patch.dict(os.environ, GITHUB_EVENT_NAME='workflow_dispatch'):
            with self.assertRaisesRegex(ValueError, 'requires main'):
                release.check_source('v0.2.0')

    def test_resume_preserves_existing_assets_and_only_adds_missing(self):
        with tempfile.TemporaryDirectory() as temporary:
            first = Path(temporary) / 'first'
            second = Path(temporary) / 'second'
            first.write_bytes(b'first archive')
            second.write_bytes(b'second archive')
            value = dict(tagName='v0.1.0', isDraft=False, assets=[dict(name='first')])
            response = subprocess.CompletedProcess([], 0, json.dumps(value), '')
            def command(args, **kwargs):
                if args[2] == 'download':
                    (Path(args[-1]) / 'first').write_bytes(first.read_bytes())
                return ''
            with patch.object(release.subprocess, 'run', return_value=response), patch.object(release.subprocess, 'check_output', side_effect=command) as invoke:
                self.assertTrue(release.resume_release('v0.1.0', [first, second]))
                self.assertEqual(invoke.call_args_list[-1].args[0],
                                 ['gh', 'release', 'upload', 'v0.1.0', '--repo', 'batchstream/weir', str(second)])
            def corrupt(args, **kwargs):
                (Path(args[-1]) / 'first').write_bytes(b'changed')
                return ''
            with patch.object(release.subprocess, 'run', return_value=response), patch.object(release.subprocess, 'check_output', side_effect=corrupt) as invoke:
                with self.assertRaisesRegex(ValueError, 'asset differs'):
                    release.resume_release('v0.1.0', [first, second])
                self.assertEqual(invoke.call_count, 1)

    def test_resume_never_treats_access_failure_as_missing(self):
        for code, error, absent in ((1, 'release not found', True), (1, 'HTTP 403 denied', False), (1, 'connection reset', False)):
            response = subprocess.CompletedProcess([], code, '', error)
            with patch.object(release.subprocess, 'run', return_value=response):
                if absent:
                    self.assertFalse(release.resume_release('v0.1.0', []))
                else:
                    with self.assertRaisesRegex(RuntimeError, 'lookup failed'):
                        release.resume_release('v0.1.0', [])

    def test_version_is_a_canonical_stable_tag(self):
        for value in ('v0.1.0', 'v1.2.30'):
            self.assertTrue(release.version_valid(value))
        for value in ('', 'latest', '1.0.0', 'v01.0.0', 'v1.0.0-rc.1', 'v1.0.0\n', '../v1.0.0', 'v1.0.0;echo unexpected'):
            self.assertFalse(release.version_valid(value))

    def test_assets_include_the_complete_bundle_and_checksums(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            delivery = asset_fixture(root)
            assets = release.verify_assets(root, delivery)
            self.assertEqual(len(assets), 12)
            checksums = (root / 'SHA256SUMS').read_text().splitlines()
            self.assertEqual(len(checksums), 11)
            self.assertEqual(checksums, [hashlib.sha256(item.read_bytes()).hexdigest() + '  ' + item.name
                                         for item in assets[:-1]])
            self.assertEqual(len({item.name for item in assets}), len(assets))

    def verify_candidate(self, root, delivery):
        environment = dict(WEIR_DELIVERY=json.dumps(delivery), GITHUB_SHA=delivery['source'])
        # Empty version is the ordinary, non-release main candidate path.
        argv = ['release.py', '--verify-only', '', str(root)]
        with patch.dict(os.environ, environment), patch.object(sys, 'argv', argv), \
             patch.object(release, 'run', return_value=delivery['source']) as command, \
             patch.object(release, 'verify_assets', wraps=release.verify_assets) as verify, \
             patch.object(release, 'check_source') as source_gate, \
             patch.object(release, 'publish') as publish, \
             patch.object(release, 'resume_release') as resume, \
             patch.object(release.subprocess, 'check_output') as external, \
             patch.object(release.subprocess, 'run') as invoke:
            try:
                release.main()
            finally:
                command.assert_called_once_with(['git', 'rev-parse', 'HEAD'])
                verify.assert_called_once_with(root, delivery, checksum_layout='artifact')
                source_gate.assert_not_called()
                publish.assert_not_called()
                resume.assert_not_called()
                external.assert_not_called()
                invoke.assert_not_called()

    def test_verify_only_accepts_nonrelease_candidate_without_publishing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            delivery = asset_fixture(root)
            self.verify_candidate(root, delivery)
            self.assertTrue((root / 'SHA256SUMS').is_file())

    def assert_downloaded_checksums(self, directory, expected_paths):
        entries = (directory / 'SHA256SUMS').read_text().splitlines()
        self.assertEqual(len(entries), 11)
        paths = []
        for entry in entries:
            expected_digest, relative = entry.split('  ', 1)
            # Consume the manifest against downloaded bytes, not the generator's path calculation.
            actual_digest = hashlib.sha256((directory / relative).read_bytes()).hexdigest()
            self.assertEqual(actual_digest, expected_digest, relative)
            paths.append(relative)
        self.assertEqual(set(paths), expected_paths - {'SHA256SUMS'})

    def test_candidate_checksums_verify_the_downloaded_artifact_layout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            built, downloaded = root / 'built', root / 'downloaded'
            built.mkdir()
            downloaded.mkdir()
            delivery = asset_fixture(built)
            self.verify_candidate(built, delivery)
            workflow = (Path(__file__).resolve().parent.parent / '.github/workflows/images.yml').read_text()
            paths = re.findall(r'^            \.testdata/images/(.+)$', workflow, re.M)
            self.assertEqual(len(paths), 12)
            # Model upload/download-artifact's retained common-root directory structure.
            for relative in paths:
                destination = downloaded / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(built / relative, destination)
            self.assert_downloaded_checksums(downloaded, set(paths))

    def test_release_checksums_verify_flat_downloaded_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            built, downloaded = root / 'built', root / 'downloaded'
            built.mkdir()
            downloaded.mkdir()
            delivery = asset_fixture(built)
            self.verify_candidate(built, delivery)
            environment = dict(GITHUB_SHA=delivery['source'])
            with patch.dict(os.environ, environment), patch.object(release, 'run', return_value=delivery['source']):
                # Ordinary publish replaces the retained candidate manifest with the flat release layout.
                assets = release.verify_workflow_assets(built, delivery)
            for item in assets:
                shutil.copyfile(item, downloaded / item.name)
            self.assert_downloaded_checksums(downloaded, {item.name for item in assets})

    def test_verify_only_refuses_incomplete_or_changed_candidate_assets(self):
        targets = ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64')
        missing = ['weir-first/weir-' + target + '.tar.gz' for target in targets]
        missing += ['weir-first/' + target + '-linked.json' for target in targets]
        missing += ['weir-first/module-graph.json']
        cases = missing + ['digest', 'source', 'binaries', 'archive_set', 'binary_set', 'delivery']
        for change in cases:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                delivery = asset_fixture(root)
                receipt_path = root / 'weir-first/receipt.json'
                receipt = json.loads(receipt_path.read_text())
                if change in missing:
                    (root / change).unlink()
                elif change == 'digest':
                    (root / 'weir-first/weir-linux-amd64.tar.gz').write_bytes(b'corrupt')
                elif change == 'source':
                    receipt['source'] = 'c' * 40
                elif change == 'binaries':
                    receipt['binaries']['linux-amd64'] = 'c' * 64
                elif change == 'archive_set':
                    del receipt['archives']['weir-darwin-arm64.tar.gz']
                elif change == 'binary_set':
                    del receipt['binaries']['darwin-arm64']
                    delivery['images']['weir']['binaries'] = receipt['binaries']
                    (root / 'delivery.json').write_text(json.dumps(delivery))
                else:
                    (root / 'delivery.json').write_text('{}')
                receipt_path.write_text(json.dumps(receipt))
                with self.assertRaises((ValueError, FileNotFoundError)):
                    self.verify_candidate(root, delivery)
                self.assertFalse((root / 'SHA256SUMS').exists())

    def test_verify_only_requires_workflow_and_checkout_source_identity(self):
        for workflow_source, checkout in (('b' * 40, 'a' * 40), ('a' * 40, 'b' * 40)):
            with self.subTest(workflow=workflow_source, checkout=checkout), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                delivery = asset_fixture(root)
                environment = dict(WEIR_DELIVERY=json.dumps(delivery), GITHUB_SHA=workflow_source)
                argv = ['release.py', '--verify-only', '', str(root)]
                with patch.dict(os.environ, environment), patch.object(sys, 'argv', argv), \
                     patch.object(release, 'run', return_value=checkout), \
                     patch.object(release, 'verify_assets') as verify, patch.object(release, 'publish') as publish:
                    with self.assertRaisesRegex(ValueError, 'workflow checkout'):
                        release.main()
                    verify.assert_not_called()
                    publish.assert_not_called()
                self.assertFalse((root / 'SHA256SUMS').exists())


if __name__ == '__main__':
    unittest.main()
