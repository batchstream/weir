"""Offline image workflow script tests; nothing is built, pulled or published."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import release

spec = importlib.util.spec_from_file_location('ci_images', Path(__file__).with_name('ci-images.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class ImageTests(unittest.TestCase):
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

    def test_entrypoint_refuses_other_sources_before_build_or_publish(self):
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

    def test_client_environment_has_no_inherited_auth(self):
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

    def test_smoke_rejects_unexpected_version_output(self):
        digest = 'sha256:' + 'a' * 64
        expected = dict(product='weir', version='dev', go=ci.package.GO, target='linux/amd64')
        for change in ({}, dict(product='other'), dict(target='linux/arm64'), dict(go='go1.0')):
            output = json.dumps(dict(expected, **change)).encode()
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary, \
                 patch.object(ci, 'OUT', Path(temporary)), \
                 patch.object(ci.package, 'oci_index_digest', return_value=digest), \
                 patch.object(ci.platform, 'machine', return_value='x86_64'), \
                 patch.object(ci.package, 'run', return_value=output) as command:
                if change:
                    with self.assertRaisesRegex(ValueError, 'unexpected weir version output'):
                        ci.smoke()
                else:
                    ci.smoke()
                args = command.call_args.args[0]
                self.assertEqual(args[-2:], [release.IMAGE + '@' + digest, 'version'])
                self.assertIn('--network=none', args)


if __name__ == '__main__':
    unittest.main()
