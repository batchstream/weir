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
        for name in ('.tools/weir', '.testdata/test.go', '.git/config', 'examples/basic/main.go', 'examples/native/main.go', 'cmd/weir/main_test.go', 'internal/testutil/root.go'):
            self.assertFalse(package.allowed(name), name)
        self.assertFalse(package.allowed('cmd/weir-lua-worker/main.go'))
        included = (
            'README.md',
            'config/weir.yaml',
            'config/routes.yaml',
            'deploy/docker/Dockerfile',
            'deploy/docker/weir.yaml',
            'deploy/docker/routes.yaml',
            'deploy/docker/licenses/purego-NOTICE.txt',
        )
        for name in included:
            self.assertTrue(package.allowed(name), name)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def git(*args):
                return package.run(['git', *args], cwd=root)
            git('init', '-q')
            for name in ('go.mod', 'go.sum', 'cmd/weir/main.go'):
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('fixture')
            git('add', '.')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture')
            revision = package.clean_head(root)
            self.assertEqual(len(revision), 40)
            self.assertEqual(len(package.source_files(root, revision)), 3)
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
            for name in ('go.mod', 'go.sum', 'cmd/weir/main.go'):
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
            self.assertEqual({name for name, _ in files}, {'go.mod', 'go.sum', 'cmd/weir/main.go'})
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
        for extra, target, success in (('', ('linux', 'arm64'), True), ('', ('linux', 'amd64'), False), ('\tdep\tgithub.com/yuin/gopher-lua\tv1.1.1\th1:fixture\n', ('linux', 'arm64'), True)):
            result = subprocess.CompletedProcess([], 0, (lines + extra).encode(), b'')
            with patch.object(subprocess, 'run', return_value=result):
                if success:
                    self.assertEqual(package.build_info(Path('binary'), target, {})['linked_modules'],
                                     [['github.com/yuin/gopher-lua', 'v1.1.1', 'h1:fixture']] if extra else [])
                else:
                    with self.assertRaises(ValueError):
                        package.build_info(Path('binary'), target, {})

    def test_oci_build_context_contains_only_built_binaries(self):
        for qualification in (False, True):
            with self.subTest(qualification=qualification), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                source = root / 'source'
                source.mkdir()
                base = {'image': 'fixture', 'index': 'sha256:' + '0' * 64}
                inputs = {
                    'deploy/docker/base.json': json.dumps(base).encode(),
                    'deploy/docker/Dockerfile': b'product Dockerfile',
                    'scripts/qualification.Dockerfile': b'qualification Dockerfile',
                    'README.md': b'fixture docs',
                    'config/weir.yaml': b'# Complete process reference\nlisteners:\n  application: 127.0.0.1:7447\nrouting:\n  file: routes.yaml\n',
                    'config/routes.yaml': b'# Complete routing reference\nservices: []\nroutes: []\n',
                    'deploy/docker/weir.yaml': b'listeners:\n  application: 127.0.0.1:7447\nrouting:\n  file: routes.yaml\n',
                    'deploy/docker/routes.yaml': b'services: []\nroutes: []\n',
                }

                def run(args, *, cwd=None, env=None):
                    if args[:3] == ['git', 'cat-file', 'blob']:
                        return inputs[args[3]]
                    if args == ['go', 'mod', 'verify'] or args[:3] == ['go', 'tool', 'buildid']:
                        return b''
                    if args == ['go', 'list', '-m', '-json', 'all']:
                        return b'{}'
                    if args[:2] == ['go', 'build']:
                        binary = Path(args[args.index('-o') + 1])
                        binary.write_bytes(f'{args[-1]} {env["GOOS"]}/{env["GOARCH"]}'.encode())
                        return b''
                    self.fail('unexpected command: ' + repr(args))

                opts = dict(source=source, output=root / 'output', env={},
                            files=[(name, name) for name in inputs], revision='0' * 40,
                            epoch=1700000001, qualification=qualification, oci=True,
                            builder='fixture', docker_env={})
                linked = {}
                oci = {'index': 'sha256:' + '1' * 64}
                completed = subprocess.CompletedProcess([], 0, stdout='fixture OCI build')
                with (patch.object(package, 'run', side_effect=run),
                      patch.object(package, 'build_info', return_value=linked),
                      patch.object(package, 'oci_receipt', return_value=oci) as receipt,
                      patch.object(package.subprocess, 'run', return_value=completed) as docker):
                    result = package.build_once(opts)

                name = 'qualification' if qualification else 'weir'
                expected = {name}
                for arch in ('amd64', 'arm64'):
                    context = root / 'oci-context' / ('linux-' + arch)
                    self.assertEqual({item.name for item in context.iterdir()}, expected)
                    for binary in expected:
                        built = opts['output'] / 'binaries' / ('linux-' + arch) / binary
                        self.assertEqual((context / binary).read_bytes(), built.read_bytes())
                dockerfile = 'scripts/qualification.Dockerfile' if qualification else 'deploy/docker/Dockerfile'
                self.assertEqual((root / 'oci-context' / 'Dockerfile').read_bytes(), inputs[dockerfile])
                if not qualification:
                    archive_path = opts['output'] / 'weir-linux-arm64.tar.gz'
                    with tarfile.open(archive_path) as archive:
                        configurations = {
                            'config/weir.yaml': 'config/weir.yaml',
                            'config/routes.yaml': 'config/routes.yaml',
                        }
                        yaml_files = {name for name in archive.getnames() if name.endswith('.yaml')}
                        self.assertEqual(yaml_files, set(configurations))
                        for filename, source_path in configurations.items():
                            self.assertEqual(archive.extractfile(filename).read(), inputs[source_path])
                receipt.assert_called_once()
                docker.assert_called_once()


if __name__ == '__main__':
    unittest.main()
