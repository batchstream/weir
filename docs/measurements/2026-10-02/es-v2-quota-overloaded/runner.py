#!/usr/bin/env python3
"""Two v2 ES quota phases with fixed profile and a bounded client queue."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import time

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location('v2main', HERE / 'es-v2-runner.py')
v2main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2main)
comparison = v2main.comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--reference-raw', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    options = parser.parse_args()
    manifest = json.loads(options.manifest.read_text())
    for name in ('client-v2', 'weir-v2', 'baseline-weir'):
        actual = hashlib.sha256((options.bin_dir / name).read_bytes()).hexdigest()
        if actual != manifest['binaries'][name]:
            raise RuntimeError('frozen binary mismatch: ' + name)
    reference = json.loads((options.reference_raw / 'summary.json').read_text())
    values = reference['provenance']['options'].copy()
    values.update(client=options.bin_dir / 'client-v2', weir=options.bin_dir / 'weir-v2',
                  baseline_weir=options.bin_dir / 'baseline-weir', output=options.output,
                  workers=32, pool=4, batch_operations=8, collect_ms=3,
                  max_read_size='16KiB', seconds=90, warm=0, client_queue=32,
                  modes='direct,weir', rates='3200', write_every='10',
                  recovery_rate=0, recovery_seconds=0)
    args = argparse.Namespace(**values)
    fixture = v2main.PhaseFixture(args)
    profile = {'pool': 4, 'collect_ms': 3, 'max_read_size': '16KiB', 'workers': 32, 'batch': 8}
    declared = v2main.configure(fixture, profile)
    declared['client_queue'] = 32
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt('owned v2 phase fixture interrupted; cleanup follows')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        fixture.start()
        fixture.save('frozen-build-manifest.json', manifest)
        fixture.save('resource-system.json', {'clock_ticks': fixture.db_ticks, 'page_size': fixture.db_page_size,
                                             'database_container': fixture.db, 'database_pid': fixture.db_pid})
        fixture.summary['experiment'] = {'version': 'v2 final CFS hold fix', 'kind': 'quota-phase-only',
            'reference_raw': str(options.reference_raw), 'started': time.time(), 'profile': declared,
            'phase_schedule': 'first actual Execute: .25CPU20s -> .08CPU30s -> .25CPU30s; continuous90s',
            'learning_note': 'Common JVM prewarm uses this fixed read/batch profile. Each measured Weir starts fresh; its first20s .25CPU baseline is the controller learning interval. Report readiness before limited phase; no baseline is presumed.'}
        fixture.prewarm(10)
        fixture.args.seconds, fixture.args.warm = 90, 0
        fixture.phase_enabled = True
        for repetition in range(2):
            try:
                fixture.trial('weir', 3200, repetition, 10)
            finally:
                if getattr(fixture, 'phase_stop', None) is not None:
                    fixture.phase_stop.set()
                    fixture.phase_thread.join(15)
                fixture.save('phase-last-controls.json', getattr(fixture, 'phase_controls', []))
                fixture.save('phase-last-errors.json', getattr(fixture, 'phase_errors', []))
            if fixture.phase_errors or len(fixture.phase_controls) != 3:
                raise RuntimeError('phase controller incomplete: ' + str(fixture.phase_errors))
            record = fixture.summary['runs'][-1]
            record.update(variant='backpressure', version='v2', profile=declared, controls=fixture.phase_controls)
            for filename in ('node.yaml', 'routes.yaml'):
                fixture.save(record['prefix'] + '-' + filename, (fixture.root / filename).read_text())
            fixture.save(record['prefix'] + '-controls.json', fixture.phase_controls)
            fixture.save('summary.json', fixture.summary)
            print(json.dumps({'event': 'v2_phase_complete', 'prefix': record['prefix'], 'profile': declared,
                              'controls': [{k: c[k] for k in ('phase', 'cpu_quota', 'actual_cpu_max', 'database_pid', 'database_start_ticks')} for c in record['controls']]}), flush=True)
        fixture.phase_enabled = False
        fixture.summary['experiment']['completed'] = time.time()
        fixture.save('summary.json', fixture.summary)
    finally:
        if not fixture.cleanup():
            raise RuntimeError('fixture cleanup incomplete')


if __name__ == '__main__':
    main()
