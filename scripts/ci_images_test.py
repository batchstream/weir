"""Offline negative delivery tests. Tiny synthetic OCI data is never published."""
import copy
import importlib.util
import fnmatch
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import package

spec = importlib.util.spec_from_file_location('ci_images', Path(__file__).with_name('ci-images.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


def fixture(path, change):
    blobs = {}

    def blob(raw):
        digest = 'sha256:' + package.sha(raw)
        blobs['blobs/sha256/' + digest[7:]] = raw
        value = dict(digest=digest, size=len(raw))
        return value

    manifests = []
    for arch in ('amd64', 'arm64'):
        layer = io.BytesIO()
        with tarfile.open(fileobj=layer, mode='w') as output:
            entries = ['qualification', 'unapproved-config'] if change == 'extra' else ['qualification']
            for name in entries:
                item = tarfile.TarInfo(name)
                item.size, item.mode = 6, 0o555
                output.addfile(item, io.BytesIO(b'binary'))
        layer_info = blob(layer.getvalue())
        runtime = dict(User='0' if change == 'root' else '65532:65532', Entrypoint=['/qualification'],
                       Labels={'org.opencontainers.image.revision': 'b' * 40 if change == 'revision' else 'a' * 40,
                               'org.opencontainers.image.source': 'https://github.com/batchstream/weir'})
        config = dict(architecture=arch, os='linux', config=runtime)
        config_info = blob(json.dumps(config).encode())
        value = dict(config=config_info, layers=[layer_info])
        descriptor = blob(json.dumps(value).encode())
        descriptor.update(mediaType='application/vnd.oci.image.manifest.v1+json', platform=dict(os='linux', architecture=arch))
        manifests.append(descriptor)
    if change == 'duplicate':
        manifests.append(manifests[0])
    index = dict(schemaVersion=2, manifests=manifests)
    blobs['index.json'] = json.dumps(index).encode()
    if change == 'corrupt':
        blobs[next(iter(blobs))] = b'corrupted layer'
    with tarfile.open(path, 'w') as bundle:
        for name, raw in blobs.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            bundle.addfile(member, io.BytesIO(raw))


class DeliveryTests(unittest.TestCase):
    def test_workflow_discovers_all_offline_python_tests_in_both_modes(self):
        workflow = (package.ROOT / '.github/workflows/images.yml').read_text()
        for mode in ('', '-O '):
            self.assertIn("python3 " + mode + "-m unittest discover -v -s scripts -p '*_test.py'", workflow)
        tests = {p.name for p in Path(__file__).parent.glob('*_test.py')}
        self.assertTrue({'test_capacity_test.py', 'resource_report_test.py', 'resource_local_test.py',
                         'eks_loopback_test.py', 'eks_resources_test.py', 'local_es_prerequisite_test.py'} <= tests)
        self.assertTrue(all(fnmatch.fnmatch(name, '*_test.py') for name in tests))

    def test_snapshot_requires_self_identity_raw_fields_and_no_errors(self):
        namespaces = {name: name + ':[123]' for name in ('pid', 'mnt', 'cgroup', 'net', 'user')}
        identity = dict(pid='1', uid=65532, start_ticks=123, exe_sha256='a' * 64, cgroup='0::/\n', namespaces=namespaces)
        process = dict(identity=identity, rss_bytes=4096, fd=3, threads=2, user_ticks=0, system_ticks=0,
                       status='synthetic status', stat='synthetic stat')
        files = {name: 'synthetic raw' for name in ('memory.current', 'memory.events', 'cpu.stat',
                 'pids.current', 'cpuset.cpus.effective', 'limits', 'net/tcp', 'net/tcp6', 'status', 'stat', 'cgroup')}
        limits = {'memory.max': '268435456\n', 'memory.swap.max': '0\n', 'pids.max': '128\n',
                  'cpu.max': '100000 100000\n', 'io.stat': ''}
        files.update(limits)
        sample = dict(role='client', sequence=0, process=process, observer=copy.deepcopy(process), files=files,
                      duration_ns=2, monotonic_ns=1, end_monotonic_ns=3, gomaxprocs=1, goroutines=2, go_heap_alloc_bytes=4096)
        ci.verify_snapshot(sample, 'a' * 64)
        mutations = [lambda s: s.update(errors=['missing sampling prerequisite']),
                     lambda s: s['observer']['identity'].update(pid='2'),
                     lambda s: s['process']['identity'].update(exe_sha256='b' * 64),
                     lambda s: s['process']['identity'].update(uid=0),
                     lambda s: s['process']['identity']['namespaces'].pop('net'),
                     lambda s: s['files'].pop('io.stat'),
                     lambda s: s['files'].__setitem__('cpu.max', 'max 100000'),
                     lambda s: s['files'].__setitem__('memory.max', '0'),
                     lambda s: s['files'].__setitem__('stat', ''),
                     lambda s: s.update(duration_ns=0),
                     lambda s: s['observer'].update(rss_bytes=0)]
        for mutate in mutations:
            value = copy.deepcopy(sample)
            mutate(value)
            with self.assertRaises(ValueError):
                ci.verify_snapshot(value, 'a' * 64)

    def test_oci_rejects_unapproved_content_and_identity(self):
        hashes = {'linux-' + arch: package.sha(b'binary') for arch in ('amd64', 'arm64')}
        options = dict(binary_name='qualification', revision='a' * 40, base_layers=dict(amd64=[], arm64=[]))
        with tempfile.TemporaryDirectory() as temp:
            for change in ('none', 'extra', 'root', 'revision', 'duplicate', 'corrupt'):
                path = Path(temp) / (change + '.tar')
                fixture(path, change)
                if change == 'none':
                    result = package.oci_receipt(path, hashes, options)
                    self.assertEqual(set(result['images']), {'amd64', 'arm64'})
                    wrong_base = dict(options, base_layers=dict(amd64=['sha256:foreign'], arm64=[]))
                    with self.assertRaisesRegex(ValueError, 'base layers'):
                        package.oci_receipt(path, hashes, wrong_base)
                else:
                    with self.assertRaises(ValueError, msg=change):
                        package.oci_receipt(path, hashes, options)

    def test_existing_tag_never_overwritten_or_auth_failure_ignored(self):
        digest = 'sha256:' + 'a' * 64
        for code, stdout, stderr, expected in (
                (0, digest, '', True), (1, '', 'HTTP status: 404 Not Found', False)):
            result = subprocess.CompletedProcess([], code, stdout.encode(), stderr.encode())
            with patch.object(subprocess, 'run', return_value=result):
                self.assertEqual(ci.check_existing('example:sha', digest, {}), expected)
        for code, stdout, stderr in ((0, 'sha256:wrong', ''), (1, '', 'unauthorized'), (1, '', 'connection refused')):
            result = subprocess.CompletedProcess([], code, stdout.encode(), stderr.encode())
            with patch.object(subprocess, 'run', return_value=result):
                with self.assertRaises((ValueError, RuntimeError)):
                    ci.check_existing('example:sha', digest, {})

    def test_anonymous_environment_has_no_inherited_auth(self):
        with tempfile.TemporaryDirectory() as temp:
            inherited = dict(GITHUB_TOKEN='must-not-inherit', DOCKER_AUTH_CONFIG='must-not-inherit', REGCTL_CONFIG='foreign')
            with patch.dict(ci.os.environ, inherited):
                env = ci.client_environment(Path(temp) / 'client')
            self.assertNotIn('GITHUB_TOKEN', env)
            self.assertNotIn('DOCKER_AUTH_CONFIG', env)
            self.assertEqual(json.loads((Path(env['DOCKER_CONFIG']) / 'config.json').read_text()), {'auths': {}})
            self.assertTrue(env['REGCTL_CONFIG'].startswith(temp))
            with self.assertRaises(FileExistsError):
                ci.client_environment(Path(temp) / 'client')


if __name__ == '__main__':
    unittest.main()
