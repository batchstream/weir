#!/usr/bin/env python3
"""Four-path final ES matrix using the owned compare-load fixture."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import sys
import threading
import time

REPO = Path('/Users/liran/Projects/liran/go/weir')
sys.path.insert(0, str(REPO / 'scripts'))
spec = importlib.util.spec_from_file_location('comparison', REPO / 'scripts/compare-load.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class PhaseFixture(comparison.Fixture):
    def start_weir(self, mode):
        super().start_weir(mode)
        if not getattr(self, 'phase_enabled', False):
            return
        self.phase_controls, self.phase_errors = [], []
        self.phase_stop = threading.Event()
        self.phase_thread = threading.Thread(target=self.control_phases)
        self.phase_thread.start()

    def control_phases(self):
        try:
            until = time.monotonic() + 30
            while time.monotonic() < until and not self.phase_stop.is_set():
                raw = comparison.http(self.weir_url + '/metrics')
                if comparison.metric(raw, 'weir_store_executions_total') > 0:
                    break
                self.phase_stop.wait(0.1)
            else:
                raise RuntimeError('phase load start not observed')
            for name, quota, duration in [('baseline', 0.25, 20), ('limited', 0.08, 30), ('restored', 0.25, 30)]:
                comparison.run(['docker', 'update', '--cpus', str(quota), self.db])
                limits = comparison.run(['docker', 'inspect', '--format', '{{.HostConfig.NanoCpus}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}', self.db]).stdout.split()
                if limits != [str(int(quota * 1e9)), '0', str(1536 * 1024**2)]:
                    raise RuntimeError('phase quota identity drift')
                control = {
                    'phase': name, 'cpu_quota': quota, 'duration_seconds': duration,
                    'wall_time': time.time(), 'monotonic': time.monotonic(),
                    'database_container': self.db, 'database_pid': self.db_pid,
                    'weir_container': self.weir, 'limits': limits,
                    'database_resources': comparison.database_resources(self.db, self.db_pid),
                    'metrics': comparison.http(self.weir_url + '/metrics'),
                }
                self.phase_controls.append(control)
                if self.phase_stop.wait(duration):
                    break
        except Exception as exc:
            self.phase_errors.append(str(exc))
        finally:
            comparison.run(['docker', 'update', '--cpus', '0.25', self.db], check=False)


def configure(fixture, variant, tuned):
    fixture.args.pool = 4
    fixture.args.batch_operations = 32
    fixture.args.collect_ms = None
    fixture.args.max_read_size = None
    if variant == 'new-tuned' or variant.startswith('screen-'):
        fixture.args.pool = tuned['pool']
        fixture.args.collect_ms = tuned['collect_ms']
        fixture.args.max_read_size = '16KiB'
    profile = {
        'variant': variant,
        'store_concurrency': fixture.args.pool,
        'batch_operations': 32,
        'batch_collect_ms': fixture.args.collect_ms if fixture.args.collect_ms is not None else 1,
        'max_read_size': fixture.args.max_read_size or '2MiB',
        'workers': fixture.workers,
        'weir_cpu': fixture.weir_cpu,
        'weir_memory_mib': fixture.weir_memory,
        'direct_http_pool': 4,
    }
    return profile


def run_variant(fixture, options):
    variant, tuned = options['variant'], options['tuned']
    profile = configure(fixture, variant, tuned)
    mode = 'baseline' if variant == 'old-baseline' else 'direct' if variant == 'direct' else 'weir'
    fixture.trial(mode, options['rate'], options['repetition'], options['write_every'])
    record = fixture.summary['runs'][-1]
    record['variant'] = variant
    record['profile'] = profile
    if mode != 'direct':
        for filename in ('node.yaml', 'routes.yaml'):
            fixture.save(record['prefix'] + '-' + filename, (fixture.root / filename).read_text())
    fixture.save('summary.json', fixture.summary)
    progress = {'event': 'variant_complete', 'prefix': record['prefix'], 'profile': profile}
    print(json.dumps(progress), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--screen-only', action='store_true')
    parser.add_argument('--skip-screen', action='store_true')
    parser.add_argument('--backpressure-only', action='store_true')
    parser.add_argument('--skip-backpressure', action='store_true')
    parser.add_argument('--tuned-pool', type=int, default=4)
    parser.add_argument('--tuned-collect-ms', type=int, default=1)
    options = parser.parse_args()
    manifest = json.loads((REPO / 'docs/measurements/2026-10-02/build-manifest.json').read_text())
    for name in ('client-final', 'weir-final', 'baseline-weir'):
        actual = hashlib.sha256((options.bin_dir / name).read_bytes()).hexdigest()
        if actual != manifest['binaries'][name]:
            raise RuntimeError('frozen binary mismatch: ' + name)
    args = argparse.Namespace(
        client=options.bin_dir / 'client-final', weir=options.bin_dir / 'weir-final',
        baseline_weir=options.bin_dir / 'baseline-weir',
        baseline_source='614eb443e298ffc531507a32743b15f5a4baea00',
        client_source='614eb443e298ffc531507a32743b15f5a4baea00',
        weir_source='614eb443e298ffc531507a32743b15f5a4baea00',
        output=options.output, rates='800,3200', write_every='10,1',
        modes='direct,baseline,weir', repetitions=3, seconds=20, warm=10,
        recovery_rate=0, recovery_seconds=20, pool=4, direct_pool=4,
        batch_operations=32, collect_ms=None, max_read_size=None,
        db_cpu=0.25, backend='elasticsearch',
        mongo_image='unused', weir_cpu=2, workers=32, db_queue=200,
        prewarm=True,
        es_image='sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160',
    )
    fixture = PhaseFixture(args)
    tuned = {'pool': options.tuned_pool, 'collect_ms': options.tuned_collect_ms}
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt('owned fixture interrupted; cleanup follows')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        fixture.start()
        fixture.save('frozen-build-manifest.json', manifest)
        fixture.summary['experiment'] = {
            'source_status': 'frozen uncommitted sources; binary identities in build-manifest.json',
            'four_paths': ['direct', 'old-baseline', 'new-default', 'new-tuned'],
            'default_meaning': 'unchanged 2MiB read budget and 1ms collect; batch32/session32 shared across baseline and new',
            'started': time.time(),
        }
        configure(fixture, 'new-default', tuned)
        fixture.prewarm(10)
        if not options.skip_screen and not options.backpressure_only:
            fixture.args.seconds, fixture.args.warm = 8, 3
            candidates = [{'pool': 4, 'collect_ms': 1}, {'pool': 2, 'collect_ms': 1}, {'pool': 4, 'collect_ms': 3}]
            scores = []
            for number, candidate in enumerate(candidates):
                variant = 'screen-' + str(number)
                trial = {'variant': variant, 'rate': 3200, 'repetition': number, 'write_every': 10, 'tuned': candidate}
                run_variant(fixture, trial)
                result = fixture.summary['runs'][-1]
                metrics = result['measure']['all']
                errors = sum((metrics['failures'] or {}).values())
                scores.append((metrics['client_drop'] + errors, metrics['success_arrival']['p95_us'], candidate))
            tuned = min(scores, key=lambda item: item[:2])[2]
            fixture.summary['selected_tuning'] = tuned
            fixture.save('summary.json', fixture.summary)
            selection = {'event': 'selected_tuning', 'tuned': tuned, 'scores': scores}
            print(json.dumps(selection), flush=True)
        if options.screen_only:
            return
        fixture.args.seconds, fixture.args.warm = 20, 10
        for write_every in (() if options.backpressure_only else (10, 1)):
            if write_every == 1:
                configure(fixture, 'new-default', tuned)
                fixture.prewarm(write_every)
            for rate in (800, 3200):
                variants = ['direct', 'old-baseline', 'new-default', 'new-tuned']
                for repetition in range(3):
                    order = variants[repetition:] + variants[:repetition]
                    for variant in order:
                        trial = {'variant': variant, 'rate': rate, 'repetition': repetition, 'write_every': write_every, 'tuned': tuned}
                        run_variant(fixture, trial)
        if not options.skip_backpressure:
            fixture.args.seconds, fixture.args.warm = 90, 0
            phase_tuning = {'pool': 4, 'collect_ms': 1}
            fixture.phase_enabled = True
            for repetition in range(2):
                try:
                    trial = {'variant': 'new-tuned', 'rate': 3200, 'repetition': repetition, 'write_every': 10, 'tuned': phase_tuning}
                    run_variant(fixture, trial)
                finally:
                    fixture.phase_stop.set()
                    fixture.phase_thread.join(15)
                if fixture.phase_errors or len(fixture.phase_controls) != 3:
                    raise RuntimeError('phase controller incomplete: ' + str(fixture.phase_errors))
                record = fixture.summary['runs'][-1]
                record['variant'] = 'backpressure'
                record['controls'] = fixture.phase_controls
                fixture.save(record['prefix'] + '-controls.json', fixture.phase_controls)
                fixture.save('summary.json', fixture.summary)
            fixture.phase_enabled = False
        fixture.summary['experiment']['completed'] = time.time()
        fixture.save('summary.json', fixture.summary)
    finally:
        if not fixture.cleanup():
            raise RuntimeError('fixture cleanup incomplete')


if __name__ == '__main__':
    main()
