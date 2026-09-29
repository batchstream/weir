"""Offline release-input integrity tests; these never contact a registry or GitHub."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import release


class ReleaseTests(unittest.TestCase):
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
