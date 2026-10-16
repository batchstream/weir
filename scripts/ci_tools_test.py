"""Exercise offline module bootstrap with the real local-replacement graph."""
import importlib.util
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
            (source / 'go.mod').write_text(
                'module example.com/bootstrap\n\ngo 1.20\n\n'
                'require google.golang.org/grpc v1.83.2\n\n'
                'replace google.golang.org/grpc => ./third_party/grpc\n')
            (source / 'go.sum').write_text('')
            (nested / 'go.mod').write_text('module google.golang.org/grpc\n\ngo 1.20\n')
            (nested / 'go.sum').write_text('')
            inputs = {item.relative_to(source): item.read_bytes()
                      for item in source.rglob('go.*')}
            work_root = Path(temporary) / 'work'
            work_root.mkdir()
            env = dict(os.environ, GOENV='off', GOWORK='off', GOTOOLCHAIN='local',
                       GOPROXY='off', GOSUMDB='off', GOFLAGS='-mod=readonly')
            ci_tools.prepare_modules(source, work_root, env)
            self.assertEqual(inputs, {relative: (source / relative).read_bytes() for relative in inputs})
            self.assertEqual([], list(work_root.iterdir()))


if __name__ == '__main__':
    unittest.main()
