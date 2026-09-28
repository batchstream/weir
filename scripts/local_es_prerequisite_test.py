"""Recorded Docker shape plus executed shell and exact-ID cleanup regressions."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local_es_prerequisite as local
import eks_socket_diagnostics_test as diagnostics


class LocalES(unittest.TestCase):
    def setUp(self):
        self.actual = json.loads((Path(__file__).parent/'fixtures/local-es-container-m26r4.json').read_text())
        owner = 'weir-m26r5-es-test'
        self.actual['Config']['Hostname'] = owner
        self.actual['Name'] = '/'+owner
        self.actual['Config']['Labels'][local.common.LABEL] = owner
        self.actual['ExtraHosts'] = [owner+':127.0.0.1']
        self.actual['HostConfig']['LogConfig'] = dict(Type='json-file', Config={'max-size':'4m','max-file':'1'})
        mount = dict(Type='bind', RW=False, Source='/test/LocalHostname.java', Destination='/qualification/LocalHostname.java')
        self.actual['Mounts'].append(mount)
        self.plan = dict(owner=owner, extra_hosts=[owner+':127.0.0.1'], es=dict(args=self.actual['Config']['Cmd']),
                         mounts={m['Source']:m['Destination'] for m in self.actual['Mounts']})

    def test_recorded_configuration_and_named_mapping_rejections(self):
        local.configuration_check(self.actual, self.plan)
        for hosts in (None, [], ['other:127.0.0.1'], [self.plan['owner']+':10.0.0.1']):
            actual = copy.deepcopy(self.actual)
            actual['ExtraHosts'] = hosts
            with self.subTest(hosts=hosts), self.assertRaisesRegex(ValueError, 'mapping'):
                local.configuration_check(actual, self.plan)
        self.actual['Config']['Hostname'] = 'other'
        with self.assertRaisesRegex(ValueError, 'hostname'):
            local.configuration_check(self.actual, self.plan)

    def test_actual_shell_requires_http_and_unchanged_guard(self):
        shell = diagnostics.ShellDiagnostics('runTest')
        shell.setUp()
        self.addCleanup(shell.doCleanups)
        source = local.local_check()
        prelude = 'export AWS_EC2_METADATA_DISABLED=true\n'
        result = shell.execute(source=source, prelude=prelude)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.endswith(local.RECEIPT))
        (shell.root/'settings.json').write_text('{}')
        result = shell.execute(source=source, prelude=prelude)
        self.assertEqual(result.returncode, 22)
        self.assertNotIn(local.RECEIPT, result.stdout)
        shell.setUp()
        shell.tables['tcp'] += diagnostics.row('0A00000A:AAAA', 'FEA9FEA9:0050', '05')
        result = shell.execute(source=source, prelude=prelude)
        self.assertEqual(result.returncode, 23)
        self.assertIn('state=05', result.stdout)
        self.assertNotIn(local.RECEIPT, result.stdout)
        (shell.root/'curl').write_text('#!/bin/sh\nexit 7\n')
        result = shell.execute(source=source, prelude=prelude)
        self.assertEqual(result.returncode, 7)
        self.assertNotIn(local.RECEIPT, result.stdout)

    def test_log_failure_preserves_cleanup_and_rejects_foreign_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('containers', 'networks'):
                (root/(name+'-before.txt')).write_text('')
            run = local.common.Run(root, local.loop.TARGET)
            commands = []
            def docker(argv, timeout=25):
                commands.append(argv)
                if argv[1] == 'inspect':
                    if 'ExtraHosts' in argv[-1]:
                        return json.dumps(self.actual['ExtraHosts'])
                    return json.dumps(self.actual)
                if argv[1] == 'logs':
                    raise ValueError('original log failure')
                if argv[1] == 'wait':
                    return '1\n'
                return ''
            with patch.object(run, 'run', side_effect=docker):
                errors = local.cleanup(run, self.plan, self.actual['Id'])
                self.assertEqual(errors, ['logs: original log failure'])
                actions = [argv[1] for argv in commands]
                self.assertLess(actions.index('stop'), actions.index('wait'))
                self.assertLess(actions.index('wait'), actions.index('rm'))
                self.assertEqual([argv[-1] for argv in commands if argv[1] in ('stop','wait','rm')], [self.actual['Id']]*3)
                commands.clear()
                self.actual['Config']['Labels'][local.common.LABEL] = 'foreign'
                errors = local.cleanup(run, self.plan, self.actual['Id'])
                self.assertTrue(errors[0].startswith('ownership:'))
                self.assertFalse(any(argv[1] in ('logs','stop','wait','rm') for argv in commands))


if __name__ == '__main__':
    unittest.main()
