"""Exercise offline module bootstrap with the real local-replacement graph."""
import importlib.util
import hashlib
import io
import tarfile
from unittest import mock
import os
from pathlib import Path
import shutil
import tempfile
import unittest


spec = importlib.util.spec_from_file_location('ci_tools', Path(__file__).with_name('ci-tools.py'))
ci_tools = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci_tools)


class ModulePreparationTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('go'), 'requires the native Go driver')
    def test_relative_replacement_and_nested_graph_are_prepared_without_source_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'source'
            nested = source / 'third_party/grpc'
            nested.mkdir(parents=True)
            lua = source / 'third_party/golua'
            lua.mkdir(parents=True)
            (source / 'go.mod').write_text(
                'module example.com/bootstrap\n\ngo 1.20\n\n'
                'require (\n google.golang.org/grpc v1.83.2\n github.com/iceisfun/golua v1.1.1\n)\n\n'
                'replace google.golang.org/grpc => ./third_party/grpc\n'
                'replace github.com/iceisfun/golua => ./third_party/golua\n')
            (source / 'go.sum').write_text('')
            (nested / 'go.mod').write_text('module google.golang.org/grpc\n\ngo 1.20\n')
            (nested / 'go.sum').write_text('')
            (lua / 'go.mod').write_text('module github.com/iceisfun/golua\n\ngo 1.20\n')
            inputs = {item.relative_to(source): item.read_bytes()
                      for item in source.rglob('go.*')}
            work_root = Path(temporary) / 'work'
            work_root.mkdir()
            env = dict(os.environ, GOENV='off', GOWORK='off', GOTOOLCHAIN='local',
                       GOPROXY='off', GOSUMDB='off', GOFLAGS='-mod=readonly')
            ci_tools.prepare_modules(source, work_root, env)
            self.assertEqual(inputs, {relative: (source / relative).read_bytes() for relative in inputs})
            self.assertEqual([], list(work_root.iterdir()))


class MongoPreparationTests(unittest.TestCase):
    def archive(self, directory, kind='regular'):
        archive = directory / 'input.tgz'
        data = b'verified test-only native member'
        with tarfile.open(archive, 'w:gz') as bundle:
            member = tarfile.TarInfo('mongo/bin/mongod')
            member.size = len(data)
            if kind == 'symlink':
                member.type, member.linkname, member.size = tarfile.SYMTYPE, '/outside', 0
            bundle.addfile(member, io.BytesIO(data) if kind != 'symlink' else None)
            if kind == 'duplicate':
                bundle.addfile(member, io.BytesIO(data))
            # Nothing except the exact requested regular member may be extracted.
            other = tarfile.TarInfo('../../outside')
            other.size = 4
            bundle.addfile(other, io.BytesIO(b'nope'))
        pin = dict(url='unused', sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                   size=archive.stat().st_size, member='mongo/bin/mongod',
                   member_size=len(data), member_sha256=hashlib.sha256(data).hexdigest())
        return archive, pin, data

    def test_extracts_only_exact_verified_regular_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive, pin, data = self.archive(root)
            bins = root / 'bin'
            bins.mkdir()
            with mock.patch.object(ci_tools, 'download', side_effect=lambda _, dest: shutil.copyfile(archive, dest)):
                result = ci_tools.prepare_mongo(pin, root, bins)
            self.assertEqual(result.read_bytes(), data)
            self.assertEqual(result.stat().st_mode & 0o777, 0o755)
            self.assertEqual([result], list(bins.iterdir()))
            self.assertFalse((root / 'outside').exists())

    def test_rejects_wrong_sizes_digests_duplicates_and_links(self):
        for mode in ('size', 'member_size', 'member_sha256', 'duplicate', 'symlink', 'oversize'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive, pin, _ = self.archive(root, mode)
                if mode in ('size', 'member_size'):
                    pin[mode] += 1
                elif mode == 'member_sha256':
                    pin[mode] = '0' * 64
                elif mode == 'oversize':
                    pin['member_size'] = 256 * 1024 * 1024 + 1
                bins = root / 'bin'
                bins.mkdir()
                with mock.patch.object(ci_tools, 'download', side_effect=lambda _, dest: shutil.copyfile(archive, dest)):
                    with self.assertRaises(ValueError):
                        ci_tools.prepare_mongo(pin, root, bins)
                destination = bins / 'mongod'
                if destination.exists():
                    self.assertEqual(destination.stat().st_mode & 0o111, 0)


if __name__ == '__main__':
    unittest.main()
