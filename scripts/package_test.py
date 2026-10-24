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
    def test_supported_server_targets(self):
        self.assertEqual(package.TARGETS, (('linux', 'amd64'), ('linux', 'arm64'),
                                          ('darwin', 'amd64'), ('darwin', 'arm64')))

    def test_archives_reproducible_and_strict(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            members = {'weir': (b'binary', 0o755), 'README.md': (b'docs', 0o644)}
            epoch = 1700000001
            a, b = root / 'a.tar.gz', root / 'b.tar.gz'
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
        for name in ('.tools/weir', '.testdata/test.go', '.git/config', 'examples/basic/main.go', 'examples/native/main.go', 'cmd/weir/main_test.go', 'internal/testutil/root.go', 'api/v1/store.go', 'licenses/other.txt'):
            self.assertFalse(package.allowed(name), name)
        self.assertFalse(package.allowed('cmd/other-product/main.go'))
        included = (
            'README.md',
            'config/weir.reference.yaml',
            'deploy/docker/Dockerfile',
            'deploy/docker/base.json',
            'licenses/purego-NOTICE.txt',
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

    def test_build_writes_archives_checksums_and_oci_context(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'source'
            source.mkdir()
            output = root / 'output'
            output.mkdir()
            base = {'image': 'fixture', 'index': 'sha256:' + '0' * 64}
            inputs = {
                'deploy/docker/base.json': json.dumps(base).encode(),
                'deploy/docker/Dockerfile': b'product Dockerfile',
                'README.md': b'fixture docs',
                'config/weir.reference.yaml': b'# Configuration reference\nlisteners:\n  application: 127.0.0.1:7447\n',
            }
            for license_name in package.PUREGO_LICENSES:
                inputs['licenses/' + license_name] = license_name.encode()

            def run(args, *, cwd=None, env=None):
                if args[:3] == ['git', 'cat-file', 'blob']:
                    return inputs[args[3]]
                if args == ['go', 'mod', 'verify']:
                    return b''
                if args == ['go', 'list', '-m', '-json', 'all']:
                    return b'{"Path":"github.com/batchstream/weir","Main":true}'
                if args[:2] == ['go', 'build']:
                    self.assertIn('-ldflags=-buildid= -X main.revision=' + '0' * 40, args)
                    binary = Path(args[args.index('-o') + 1])
                    binary.write_bytes(f'{args[-1]} {env["GOOS"]}/{env["GOARCH"]}'.encode())
                    return b''
                self.fail('unexpected command: ' + repr(args))

            opts = dict(work=root, source=source, output=output, env={},
                        files=[(name, name) for name in inputs], revision='0' * 40,
                        epoch=1700000001, oci=True, builder='fixture', docker_env={})
            completed = subprocess.CompletedProcess([], 0, stdout='fixture OCI build')
            with (patch.object(package, 'run', side_effect=run),
                  patch.object(package.subprocess, 'run', return_value=completed) as docker):
                archives = package.build_source(opts)

            names = ['weir-' + system + '-' + arch + '.tar.gz' for system, arch in package.TARGETS]
            self.assertEqual([item.name for item in archives], names)
            self.assertEqual(sorted(item.name for item in output.iterdir()), sorted(names + ['SHA256SUMS']))
            checksums = (output / 'SHA256SUMS').read_text().splitlines()
            self.assertEqual(checksums, [package.sha((output / name).read_bytes()) + '  ' + name for name in names])
            for arch in ('amd64', 'arm64'):
                context = root / 'oci-context' / ('linux-' + arch)
                self.assertEqual({item.name for item in context.iterdir()}, {'weir'})
                built = root / 'binaries' / ('linux-' + arch) / 'weir'
                self.assertEqual((context / 'weir').read_bytes(), built.read_bytes())
            self.assertEqual((root / 'oci-context' / 'Dockerfile').read_bytes(), inputs['deploy/docker/Dockerfile'])
            with tarfile.open(output / 'weir-linux-arm64.tar.gz') as archive:
                configurations = ('config/weir.reference.yaml',)
                yaml_files = {name for name in archive.getnames() if name.endswith('.yaml')}
                self.assertEqual(yaml_files, set(configurations))
                for filename in configurations:
                    self.assertEqual(archive.extractfile(filename).read(), inputs[filename])
                self.assertEqual(archive.extractfile('weir').read(), b'./cmd/weir linux/arm64')
                self.assertFalse(any(name.startswith('licenses/') for name in archive.getnames()))
            with tarfile.open(output / 'weir-darwin-arm64.tar.gz') as archive:
                licenses = {name for name in archive.getnames() if name.startswith('licenses/')}
                purego = {'licenses/' + name for name in package.PUREGO_LICENSES}
                self.assertEqual(licenses, purego)
            docker.assert_called_once()

    def test_oci_index_digest_requires_both_linux_platforms(self):
        def layout(path, platforms, *, nested):
            blobs = {}
            manifests = [{'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                          'digest': 'sha256:' + '1' * 64, 'platform': {'os': 'linux', 'architecture': arch}}
                         for arch in platforms]
            index = {'schemaVersion': 2, 'manifests': manifests}
            inner = json.dumps(index).encode()
            digest = 'sha256:' + package.sha(inner)
            if nested:
                blobs['blobs/sha256/' + digest[7:]] = inner
                descriptor = {'mediaType': 'application/vnd.oci.image.index.v1+json', 'digest': digest}
                outer = {'schemaVersion': 2, 'manifests': [descriptor]}
                blobs['index.json'] = json.dumps(outer).encode()
            else:
                blobs['index.json'] = inner
            with tarfile.open(path, 'w') as bundle:
                for name, raw in blobs.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(raw)
                    bundle.addfile(member, io.BytesIO(raw))
            return digest

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'image.tar'
            for nested in (True, False):
                expected = layout(path, ('amd64', 'arm64'), nested=nested)
                self.assertEqual(package.oci_index_digest(path), expected)
                layout(path, ('amd64',), nested=nested)
                with self.assertRaisesRegex(ValueError, 'linux/amd64 and linux/arm64'):
                    package.oci_index_digest(path)


if __name__ == '__main__':
    unittest.main()
