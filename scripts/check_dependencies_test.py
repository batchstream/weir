"""Dependency regressions cover module edges omitted by production package lists."""
import unittest

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
        module = dict(Path=dependencies.PROTOCOL, Replace=dict(Dir='../weir-protocol'))
        with self.assertRaisesRegex(ValueError, 'replacement hides'):
            dependencies.check_modules([module])

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
