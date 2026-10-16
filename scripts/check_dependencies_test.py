"""Dependency regressions cover module edges omitted by production package lists."""
import unittest
from pathlib import Path
import tempfile

import check_dependencies as dependencies


class DependencyTests(unittest.TestCase):
    def test_server_sdk_protocol_form_a_dag(self):
        graph = '\n'.join((
            f'{dependencies.SERVER} {dependencies.SDK}@v0.2.0',
            f'{dependencies.SERVER} {dependencies.PROTOCOL}@v0.1.0',
            f'{dependencies.SDK}@v0.2.0 {dependencies.PROTOCOL}@v0.1.0',
            f'{dependencies.PROTOCOL}@v0.1.0 google.golang.org/grpc@v1.83.2',
        ))
        dependencies.check_graph(graph)

    def test_reverse_edges_and_self_dependencies_are_rejected(self):
        for source, target in (
            (dependencies.SDK, dependencies.SERVER),
            (dependencies.PROTOCOL, dependencies.SERVER),
            (dependencies.PROTOCOL, dependencies.SDK),
            (dependencies.SERVER, dependencies.SERVER),
        ):
            graph = f'{source}@v0.1.0 {target}@v0.1.0'
            with self.assertRaisesRegex(ValueError, 'points backwards'):
                dependencies.check_graph(graph)

    def test_local_replacements_cannot_hide_a_cycle(self):
        replacement = dict(Dir='../weir-protocol')
        module = dict(Path=dependencies.PROTOCOL, Replace=replacement)
        with self.assertRaisesRegex(ValueError, 'replacement hides'):
            dependencies.check_modules([module])

    def test_only_the_pinned_bounded_grpc_copy_is_allowed(self):
        directory = Path(__file__).resolve().parent.parent / 'third_party/grpc'
        replacement = dict(Path='./third_party/grpc', Dir=str(directory))
        module = dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION, Replace=replacement)
        dependencies.check_modules([module])
        other_path = dict(Path='../grpc', Dir=str(directory))
        other_directory = dict(Path='./third_party/grpc', Dir='/tmp/unrelated-grpc')
        published_replacement = dict(Path='./third_party/grpc', Dir=str(directory), Version='v1.83.2')
        variants = (
            dict(Path=dependencies.GRPC, Version='v1.83.1', Replace=replacement),
            dict(Path=dependencies.PROTOCOL, Version='v0.14.0', Replace=replacement),
            dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION,
                 Replace=other_path),
            dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION,
                 Replace=other_directory),
            dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION,
                 Replace=published_replacement),
        )
        for variant in variants:
            with self.subTest(module=variant):
                with self.assertRaisesRegex(ValueError, 'replacement hides'):
                    dependencies.check_modules([variant])

    def test_release_export_is_checked_against_its_own_source_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            replacement = dict(Path='./third_party/grpc', Dir=str(root / 'third_party/grpc'))
            module = dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION, Replace=replacement)
            dependencies.check_modules([module], root)
            with self.assertRaisesRegex(ValueError, 'replacement hides'):
                dependencies.check_modules([module])

    def test_only_the_pinned_lua_allocation_patch_is_allowed(self):
        directory = Path(__file__).resolve().parent.parent / 'third_party/golua'
        replacement = dict(Path='./third_party/golua', Dir=str(directory))
        module = dict(Path=dependencies.LUA, Version=dependencies.LUA_VERSION, Replace=replacement)
        dependencies.check_modules([module])
        variants = (
            dict(Path=dependencies.LUA, Version='v1.1.0', Replace=replacement),
            dict(Path=dependencies.LUA, Version=dependencies.LUA_VERSION,
                 Replace=dict(Path='../golua', Dir=str(directory))),
            dict(Path=dependencies.LUA, Version=dependencies.LUA_VERSION,
                 Replace=dict(Path='./third_party/golua', Dir='/tmp/unrelated-lua')),
            dict(Path=dependencies.LUA, Version=dependencies.LUA_VERSION,
                 Replace=dict(Path='./third_party/golua', Dir=str(directory), Version='v1.1.1')),
        )
        for variant in variants:
            with self.subTest(module=variant):
                with self.assertRaisesRegex(ValueError, 'replacement hides'):
                    dependencies.check_modules([variant])

    def test_the_patch_directory_cannot_escape_through_a_symlink(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'source'
            external = Path(temp) / 'external'
            external.mkdir()
            (root / 'third_party').mkdir(parents=True)
            (root / 'third_party/grpc').symlink_to(external, target_is_directory=True)
            replacement = dict(Path='./third_party/grpc', Dir=str(root / 'third_party/grpc'))
            module = dict(Path=dependencies.GRPC, Version=dependencies.GRPC_VERSION, Replace=replacement)
            with self.assertRaisesRegex(ValueError, 'replacement hides'):
                dependencies.check_modules([module], root)

    def test_project_dependencies_use_specific_stable_releases(self):
        modules = [dict(Path=dependencies.SDK, Version='v0.2.0'), dict(Path=dependencies.PROTOCOL, Version='v0.1.0')]
        dependencies.check_modules(modules)
        for path in (dependencies.SDK, dependencies.PROTOCOL):
            for version in ('', 'main', 'latest', 'v0.2.0-0.20261003000000-abcdef123456', 'v0.2.0-rc.1', 'v00.2.0'):
                with self.subTest(path=path, version=version):
                    module = dict(Path=path, Version=version)
                    with self.assertRaisesRegex(ValueError, 'stable release'):
                        dependencies.check_modules([module])

    def test_transitive_unused_and_test_edges_are_rejected(self):
        for source, target in (
            (dependencies.SDK, dependencies.SERVER),
            (dependencies.PROTOCOL, dependencies.SDK),
        ):
            graph = '\n'.join((
                f'{source}@v0.1.0 example.com/fixture@v0.1.0',
                f'example.com/fixture@v0.1.0 {target}@v0.1.0',
            ))
            with self.assertRaisesRegex(ValueError, 'points backwards'):
                dependencies.check_graph(graph)

    def test_server_sdk_is_test_only(self):
        package = dict(ImportPath=dependencies.SDK, Module=dict(Path=dependencies.SDK))
        with self.assertRaisesRegex(ValueError, 'binary imports the SDK'):
            dependencies.check_production([package])
        protocol = dict(ImportPath=dependencies.PROTOCOL + '/api/protocol', Module=dict(Path=dependencies.PROTOCOL))
        dependencies.check_production([dict(ImportPath='context'), protocol])


if __name__ == '__main__':
    unittest.main()
