"""Offline packaging input/archive tests; no Docker, registry or database."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('package', Path(__file__).with_name('package.py'))
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


class PackageTests(unittest.TestCase):
    def test_archives_reproducible_and_strict(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            members = {'weir': (b'binary', 0o755), 'README.md': (b'docs', 0o644)}
            epoch = 1700000001
            for suffix in ('.tar.gz', '.zip'):
                a, b = root / ('a' + suffix), root / ('b' + suffix)
                package.archive(a, members, epoch)
                package.archive(b, members, epoch)
                self.assertEqual(a.read_bytes(), b.read_bytes())
                for wrong in ({'weir': members['weir']}, dict(members, weir=(b'changed', 0o755)), dict(members, weir=(b'binary', 0o644))):
                    with self.assertRaises(ValueError):
                        package.verify_archive(a, wrong, epoch)
                with self.assertRaises(ValueError):
                    package.verify_archive(a, members, epoch + 4)

    def test_inputs_exclude_tests_local_and_secrets(self):
        for name in ('secret/.env.production', '.ssh/config', 'keys/server.pem', 'server.key', 'credentials', 'keyfile'):
            self.assertTrue(package.secret_path(name), name)
        for name in ('.tools/weir', '.testdata/test.go', '.git/config', 'experiments/probe.go', 'cmd/weir/main_test.go', 'internal/testutil/root.go'):
            self.assertFalse(package.allowed(name), name)
        self.assertTrue(package.allowed('cmd/weir-lua-worker/main.go'))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def git(*args):
                return package.run(['git', *args], cwd=root)
            git('init', '-q')
            for name in ('go.mod', 'go.sum', 'cmd/weir/main.go', 'cmd/weir-lua-worker/main.go'):
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('fixture')
            git('add', '.')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture')
            revision = package.clean_head(root)
            self.assertEqual(len(revision), 40)
            self.assertEqual(len(package.source_files(root, revision)), 4)
            (root / 'extra').write_text('dirty')
            with self.assertRaises(ValueError):
                package.clean_head(root)
            # A tracked secret-style symlink is rejected by pathname without opening its target.
            (root / 'private.pem').symlink_to('/missing-do-not-open')
            git('add', 'private.pem')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'path only')
            with self.assertRaisesRegex(ValueError, 'private.pem'):
                package.source_files(root, git('rev-parse', 'HEAD').decode().strip())

    def test_captured_revision_survives_head_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def git(*args):
                return package.run(['git', *args], cwd=root)
            git('init', '-q')
            for name in ('go.mod', 'go.sum', 'cmd/weir/main.go', 'cmd/weir-lua-worker/main.go'):
                dest = root / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text('original')
            git('add', '.')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'original')
            revision = package.clean_head(root)
            (root / 'cmd/weir/main.go').write_text('changed')
            (root / 'cmd/weir/added.go').write_text('new input')
            git('add', '.')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'moved HEAD')
            self.assertNotEqual(package.clean_head(root), revision)
            files = package.source_files(root, revision)
            self.assertEqual({name for name, _ in files}, {'go.mod', 'go.sum', 'cmd/weir/main.go', 'cmd/weir-lua-worker/main.go'})
            for _, oid in files:
                self.assertEqual(git('cat-file', 'blob', oid), b'original')
            with self.assertRaisesRegex(ValueError, 'immutable full source SHA'):
                package.source_files(root, 'HEAD')

    def test_toolchain_drift_rejected(self):
        result = subprocess.CompletedProcess([], 0, b'go1.27.0\n', b'')
        with patch.object(subprocess, 'run', return_value=result):
            with self.assertRaisesRegex(ValueError, 'requires go1.27.1'):
                package.go_environment()

    def test_binary_target_and_linked_dependency_validation(self):
        lines = 'binary: go1.27.1\n\tbuild\tGOOS=linux\n\tbuild\tGOARCH=arm64\n\tbuild\tCGO_ENABLED=0\n\tbuild\t-trimpath=true\n\tbuild\tGOARM64=v8.0\n'
        for extra, target, success in (('', ('linux', 'arm64'), True), ('', ('linux', 'amd64'), False), ('\tdep\tgithub.com/arnodel/golua\tv0.3.0\th1:fixture\n', ('linux', 'arm64'), False)):
            result = subprocess.CompletedProcess([], 0, (lines + extra).encode(), b'')
            with patch.object(subprocess, 'run', return_value=result):
                if success:
                    self.assertEqual(package.build_info(Path('binary'), target, {})['linked_modules'], [])
                else:
                    with self.assertRaises(ValueError):
                        package.build_info(Path('binary'), target, {})


if __name__ == '__main__':
    unittest.main()
