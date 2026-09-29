"""Offline release-input integrity tests; these never contact a registry or GitHub."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

import release


class ReleaseTests(unittest.TestCase):
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

    def test_assets_require_source_and_exact_archive_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            product = root / 'weir-first'
            product.mkdir()
            source = 'a' * 40
            archives, binaries = {}, {}
            for target in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64', 'windows-amd64', 'windows-arm64'):
                name = 'weir-' + target + ('.zip' if target.startswith('windows') else '.tar.gz')
                raw = ('test-archive-' + target).encode()
                (product / name).write_bytes(raw)
                archives[name] = hashlib.sha256(raw).hexdigest()
                binaries[target] = 'b' * 64
                (product / (target + '-linked.json')).write_text('{}')
            (product / 'module-graph.json').write_text('[]')
            receipt = dict(source=source, archives=archives, binaries=binaries)
            delivery = dict(source=source, images=dict(weir=dict(binaries=binaries)))
            (root / 'delivery.json').write_text(json.dumps(delivery))
            (product / 'receipt.json').write_text(json.dumps(receipt))
            assets = release.verify_assets(root, delivery)
            self.assertEqual(len(assets), 16)
            self.assertEqual(len((root / 'SHA256SUMS').read_text().splitlines()), 15)
            (product / 'weir-linux-amd64.tar.gz').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'archive checksum'):
                release.verify_assets(root, delivery)
            receipt['source'] = 'c' * 40
            (product / 'receipt.json').write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'source or binary identity'):
                release.verify_assets(root, delivery)
            receipt['source'] = source
            del receipt['archives']['weir-windows-arm64.zip']
            (product / 'receipt.json').write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'exactly six'):
                release.verify_assets(root, delivery)


if __name__ == '__main__':
    unittest.main()
