#!/usr/bin/env python3
"""Qualify the byte-identical shipping archive artifact after candidate PASS."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tarfile

BASE = Path('/Users/liran/Projects/liran/go/weir/.testdata/reliability-2026-10-16')
SOURCE = 'a01d523e6fab728d0e48f3ff84d908710559aa65'
SHIPPING_SHA = '24cb1abf6d5ea9c8623c470cf2ffacccb517e6e1e8a4ab16898ad46807524126'
DRIVER_SHA = '6087dc99267acb7136f98ea61a23e04fb81085477dcd716446d72cb1bb2d2b35'
HELPER_SHA = 'c7430cf568e968244d1b88a9a9f35d1870ae2d13947cc35c131475af1cba6236'
spec = importlib.util.spec_from_file_location('lua_repair_r2', BASE / 'lua-repair-runner-r2.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def read(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', default='release-r1')
    args = parser.parse_args()
    helper.require(helper.re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', args.label) is not None, 'invalid new evidence label')
    helper.require(Path.cwd().resolve() == helper.ROOT, 'requires the primary checkout')
    helper.require(helper.digest(Path(spec.origin).read_bytes()) == HELPER_SHA, 'R2 helper changed')
    output = BASE / ('lua-repair-' + args.label)
    output.mkdir(exist_ok=False)
    receipt = {'source': SOURCE, 'started_utc': helper.now(), 'status': 'INCOMPLETE', 'retry': False,
               'scope': 'First qualification of the distinct reproducible shipping artifact; prior candidate result retained',
               'shipping_SHA256': SHIPPING_SHA, 'runner_sha256': helper.digest(Path(__file__).read_bytes())}
    helper.write_json(output / 'attempt.json', receipt)
    before = None
    try:
        before = helper.clean_source(SOURCE)
        helper.write_json(output / 'source-before.json', before)
        candidate_dir, package_dir = BASE / 'lua-repair-candidate-r2', BASE / 'lua-repair-package-r2'
        candidate, packaged = read(candidate_dir / 'validation.json'), read(package_dir / 'validation.json')
        helper.require(all(value.get('status') == 'PASS' and value.get('source') == SOURCE and
                           value.get('source_before_after_clean_unchanged') is True for value in (candidate, packaged)),
                       'requires original R2 candidate and actual packaging PASS at exact clean H')
        helper.require(len(candidate['result']['runs']) == 2 and all(run['status'] == 'PASS' for run in candidate['result']['runs']),
                       'candidate original and supplementary runs were not both PASS')
        helper.require(before['files_sha256'] == read(candidate_dir / 'source-before.json')['files_sha256'],
                       'shipping harness/source differs from passing candidate H')
        artifacts = package_dir / 'artifacts'
        comparison = read(artifacts / 'comparison.json')
        helper.require(comparison.get('source') == SOURCE and comparison.get('completed') is True and
                       comparison.get('identical_archives') == 4 and comparison.get('identical_binaries') == 4,
                       'actual two exports / four targets did not reproduce')
        first, second = artifacts / 'first/receipt.json', artifacts / 'second/receipt.json'
        helper.require(first.read_bytes() == second.read_bytes() and
                       helper.digest(first.read_bytes()) == comparison['receipt_sha256'], 'build receipts differ or changed')
        build = read(first)
        helper.require(build['source'] == SOURCE and build['go'] == 'go1.27.1' and
                       build['binaries']['linux-arm64'] == SHIPPING_SHA and '-buildvcs=false' in build['flags'] and
                       '-ldflags=-buildid= -X main.sourceRevision=' + SOURCE in build['flags'],
                       'shipping source/compiler/artifact flags differ from reproducible build')
        archive_proofs = []
        binary = output / 'weir-linux-arm64'
        for export in ('first', 'second'):
            archive = artifacts / export / 'weir-linux-arm64.tar.gz'
            archive_sha = helper.digest(archive.read_bytes())
            helper.require(archive_sha == build['archives']['weir-linux-arm64.tar.gz'], 'shipping archive changed')
            with tarfile.open(archive, 'r:gz') as bundle:
                members = [member for member in bundle.getmembers() if member.name == 'weir']
                helper.require(len(members) == 1 and members[0].isfile() and 0 < members[0].size <= 64 << 20,
                               'archive has no unique regular bounded weir member')
                raw = bundle.extractfile(members[0]).read()
            helper.require(helper.digest(raw) == SHIPPING_SHA, 'archive weir bytes differ from shipping SHA')
            if export == 'first':
                with binary.open('xb') as stream:
                    stream.write(raw)
                binary.chmod(0o755)
            archive_proofs.append({'export': export, 'archive_sha256': archive_sha, 'weir_sha256': helper.digest(raw)})
        driver = read(candidate_dir / 'driver-buildinfo-validated.json')
        helper.require(driver['source'] == SOURCE and driver['binary_sha256'] == DRIVER_SHA and
                       helper.digest(Path(driver['binary']).read_bytes()) == DRIVER_SHA and
                       driver['settings'].get('vcs.revision') == SOURCE and driver['settings'].get('vcs.modified') == 'false' and
                       driver['settings'].get('-race') == 'true', 'passing exact-H race driver changed')
        provenance = {'source': SOURCE, 'archives': archive_proofs, 'race_driver': driver,
                      'candidate_validation_sha256': helper.digest((candidate_dir / 'validation.json').read_bytes()),
                      'package_validation_sha256': helper.digest((package_dir / 'validation.json').read_bytes()),
                      'reproducible_build_receipt_sha256': helper.digest(first.read_bytes()),
                      'identity_scope': 'Shipping export intentionally has no Go VCS settings; runtime version must assert exact clean H'}
        helper.write_json(output / 'shipping-provenance.json', provenance)
        env = helper.environment(output)
        options = {'output': output, 'env': env, 'source': SOURCE, 'before': before}
        helper.preflight(options)
        helper.write_json(output / 'runtime-started.json', {'source': SOURCE, 'artifact': SHIPPING_SHA,
                          'at': helper.now(), 'first_shipping_artifact_attempt': True, 'candidate_retry': False})
        runs = []
        for name, test, supplemental in [('original', 'TestCLILuaMemoryBudget', False), ('paths', 'TestCLILuaMemoryPaths', True)]:
            helper.clean_source(SOURCE)
            run_env = dict(env, GOOS='darwin', GOARCH='arm64', CGO_ENABLED='1', WEIR_INTEGRATION='1',
                           WEIR_LUA_MEMORY_BINARY=str(binary), WEIR_LUA_MEMORY_SOURCE=SOURCE, WEIR_LUA_MEMORY_SHA256=SHIPPING_SHA)
            if supplemental:
                run_env.update(WEIR_LUA_MEMORY_PATHS_INTEGRATION='1', WEIR_SEARCH_INTEGRATION='elasticsearch')
            else:
                run_env['WEIR_LUA_MEMORY_INTEGRATION'] = '1'
            command = [driver['binary'], '-test.v', '-test.failfast', '-test.count=1', '-test.run=^' + test + '$', '-test.timeout=2m']
            record = helper.run_stage(dict(options, name=name, command=command, env=run_env, timeout=150))
            result = helper.validate_runtime(dict(options, name=name, record=record, supplemental=supplemental, cli_sha256=SHIPPING_SHA))
            after = helper.clean_source(SOURCE)
            helper.write_json(output / (name + '-source-after.json'), after)
            helper.require(after['files_sha256'] == before['files_sha256'], 'source changed during shipping run')
            runs.append(result)
            helper.require(result['status'] == 'PASS', name + ' shipping artifact qualification failed; stopping without retry')
        receipt.update(status='PASS', provenance=provenance, runs=runs)
    except BaseException as error:
        receipt.update(status='FAIL', error=type(error).__name__ + ': ' + str(error))
    finally:
        try:
            after = helper.clean_source(SOURCE)
            helper.write_json(output / 'source-after.json', after)
            unchanged = before is not None and before['files_sha256'] == after['files_sha256']
            receipt['source_before_after_clean_unchanged'] = unchanged
            if not unchanged:
                receipt['status'] = 'FAIL'
        except BaseException as error:
            receipt.update(status='FAIL', source_after_error=str(error))
        receipt.update(finished_utc=helper.now(), output_directory=str(output))
        helper.write_json(output / 'validation.json', receipt)
        print(json.dumps({'status': receipt['status'], 'validation': str(output / 'validation.json'), 'error': receipt.get('error')}, indent=2), flush=True)
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
