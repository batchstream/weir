"""Offline release tests; these never contact a registry or GitHub."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

import release


def asset_fixture(root):
    checksums = ''
    for name in release.ARCHIVES:
        raw = ('test-archive-' + name).encode()
        (root / name).write_bytes(raw)
        checksums += hashlib.sha256(raw).hexdigest() + '  ' + name + '\n'
    (root / 'SHA256SUMS').write_text(checksums)


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

    def test_assets_are_the_four_archives_and_their_checksums(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            asset_fixture(root)
            assets = release.release_assets(root)
            self.assertEqual([item.name for item in assets], [*release.ARCHIVES, 'SHA256SUMS'])

    def test_assets_refuse_missing_extra_or_changed_archives(self):
        for change in ('corrupt', 'missing', 'unlisted', 'extra'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                asset_fixture(root)
                checksums = root / 'SHA256SUMS'
                if change == 'corrupt':
                    (root / release.ARCHIVES[0]).write_bytes(b'corrupt')
                elif change == 'missing':
                    (root / release.ARCHIVES[0]).unlink()
                elif change == 'unlisted':
                    checksums.write_text(''.join(checksums.read_text().splitlines(keepends=True)[1:]))
                else:
                    checksums.write_text(checksums.read_text() + '0' * 64 + '  weir-windows-amd64.tar.gz\n')
                with self.assertRaises((ValueError, FileNotFoundError)):
                    release.release_assets(root)

    def test_version_image_is_never_retagged_to_different_content(self):
        digest = 'sha256:' + 'a' * 64
        source = 'b' * 40
        for code, stdout, stderr in ((0, 'sha256:' + 'c' * 64, ''), (1, '', 'unauthorized')):
            existing = subprocess.CompletedProcess([], code, stdout, stderr)
            with self.subTest(stderr=stderr), patch.object(release, 'run', return_value=digest) as command, \
                 patch.object(release.subprocess, 'run', return_value=existing):
                with self.assertRaises((ValueError, RuntimeError)):
                    release.tag_image('v1.2.3', source, digest)
                self.assertEqual(command.call_count, 1)
        missing = subprocess.CompletedProcess([], 1, '', 'manifest not found')
        with patch.object(release, 'run', return_value=digest) as command, \
             patch.object(release.subprocess, 'run', return_value=missing):
            self.assertEqual(release.tag_image('v1.2.3', source, digest), release.IMAGE + ':v1.2.3')
            self.assertIn(['regctl', 'image', 'copy', release.IMAGE + '@' + digest, release.IMAGE + ':v1.2.3'],
                          [call.args[0] for call in command.call_args_list])
        with patch.object(release, 'run', return_value='sha256:' + 'd' * 64):
            with self.assertRaisesRegex(ValueError, 'source image differs'):
                release.tag_image('v1.2.3', source, digest)


if __name__ == '__main__':
    unittest.main()
