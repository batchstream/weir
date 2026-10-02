#!/usr/bin/env python3
"""Final v2 confirmation and controlled quota phases in a new owned ES fixture."""
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
        initial = None
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
                limits = comparison.run(['docker', 'inspect', '--format', '{{.HostConfig.NanoCpus}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}} {{.RestartCount}}', self.db]).stdout.split()
                if limits != [str(int(quota * 1e9)), '0', str(1536 * 1024**2), '0']:
                    raise RuntimeError('phase quota or restart identity drift')
                cpu_max = comparison.run(['docker', 'exec', self.db, 'cat', '/sys/fs/cgroup/cpu.max']).stdout.strip()
                cpu_fields = cpu_max.split()
                if len(cpu_fields) != 2 or abs(int(cpu_fields[0]) / int(cpu_fields[1]) - quota) > 1e-6:
                    raise RuntimeError('actual cgroup quota mismatch')
                resources = comparison.database_resources(self.db, self.db_pid)
                process_stat = resources['raw'].split('process_stat\n')[1].split('process_statm\n')[0].strip()
                process_start = int(process_stat[process_stat.rfind(')') + 2:].split()[19])
                state = json.loads(comparison.run(['docker', 'inspect', '--format', '{{json .State}}', self.db]).stdout)
                identity = (self.db, self.db_pid, process_start, state['Pid'], state['StartedAt'])
                if initial is None:
                    initial = identity
                if identity != initial or not state['Running'] or state['OOMKilled']:
                    raise RuntimeError('database process changed during quota phase')
                control = {
                    'phase': name, 'cpu_quota': quota, 'actual_cpu_max': cpu_max,
                    'duration_seconds': duration, 'wall_time': time.time(), 'monotonic': time.monotonic(),
                    'database_container': self.db, 'database_pid': self.db_pid,
                    'database_start_ticks': process_start, 'docker_state': state,
                    'weir_container': self.weir, 'limits': limits,
                    'database_resources': resources, 'metrics': comparison.http(self.weir_url + '/metrics'),
                }
                self.phase_controls.append(control)
                self.save('phase-controls-live.json', self.phase_controls)
                if self.phase_stop.wait(duration):
                    break
        except Exception as exc:
            self.phase_errors.append(str(exc))
        finally:
            comparison.run(['docker', 'update', '--cpus', '0.25', self.db], check=False)


def configure(fixture, profile):
    fixture.args.pool = profile['pool']
    fixture.args.batch_operations = profile.get('batch', 32)
    fixture.args.collect_ms = profile.get('collect_ms')
    fixture.args.max_read_size = profile.get('max_read_size')
    fixture.workers = fixture.args.workers = profile.get('workers', 32)
    fixture.weir_memory = 2304 if fixture.workers == 32 else 4608
    declared = {
        'store_concurrency': fixture.args.pool, 'batch_operations': fixture.args.batch_operations,
        'batch_collect_ms': fixture.args.collect_ms if fixture.args.collect_ms is not None else 1,
        'max_read_size': fixture.args.max_read_size or '2MiB', 'workers': fixture.workers,
        'max_sessions': fixture.workers, 'weir_cpu': 2, 'weir_memory_mib': fixture.weir_memory,
        'direct_http_pool': 4, 'database_cpu': 0.25, 'database_memory_mib': 1536,
    }
    return declared


def trial(fixture, options):
    declared = configure(fixture, options['profile'])
    mode = 'direct' if options['variant'] == 'matched-direct' else 'weir'
    fixture.trial(mode, options['rate'], options['repetition'], options['write_every'])
    record = fixture.summary['runs'][-1]
    record['variant'], record['version'], record['profile'] = options['variant'], 'v2', declared
    if mode != 'direct':
        for filename in ('node.yaml', 'routes.yaml'):
            fixture.save(record['prefix'] + '-' + filename, (fixture.root / filename).read_text())
    fixture.save('summary.json', fixture.summary)
    progress = {'event': 'v2_complete', 'prefix': record['prefix'], 'variant': record['variant'], 'profile': declared}
    print(json.dumps(progress), flush=True)


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
                  workers=32, pool=4, batch_operations=32, collect_ms=None, max_read_size=None,
                  seconds=20, warm=10)
    args = argparse.Namespace(**values)
    fixture = PhaseFixture(args)
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt('owned v2 fixture interrupted; cleanup follows')
    signal.signal(signal.SIGTERM, interrupted)
    default = {'pool': 4, 'workers': 32, 'batch': 32}
    chosen = {'pool': 4, 'collect_ms': 3, 'max_read_size': '16KiB', 'workers': 32, 'batch': 32}
    try:
        fixture.start()
        fixture.save('frozen-build-manifest.json', manifest)
        resource_system = {'clock_ticks': fixture.db_ticks, 'page_size': fixture.db_page_size, 'database_container': fixture.db, 'database_pid': fixture.db_pid}
        fixture.save('resource-system.json', resource_system)
        fixture.summary['experiment'] = {
            'version': 'v2 final CFS hold fix', 'reference_raw': str(options.reference_raw),
            'reference_source': 'v1 separately timed fixture; no direct pairing claimed',
            'started': time.time(),
        }
        configure(fixture, default)
        fixture.prewarm(10)
        fixture.args.seconds, fixture.args.warm = 8, 3
        candidates = [chosen, {'pool': 2, 'collect_ms': 5, 'max_read_size': '16KiB', 'workers': 32, 'batch': 32}]
        scores = []
        for number, candidate in enumerate(candidates):
            case = {'variant': 'screen-' + str(number), 'profile': candidate, 'rate': 3200, 'repetition': number, 'write_every': 10}
            trial(fixture, case)
            metrics = fixture.summary['runs'][-1]['measure']['all']
            scores.append((metrics['client_drop'] + sum((metrics['failures'] or {}).values()), metrics['success_arrival']['p95_us'], candidate))
        if min(score[0] for score in scores) > 0:
            candidate = {'pool': 2, 'collect_ms': 5, 'max_read_size': '16KiB', 'workers': 64, 'batch': 32}
            case = {'variant': 'screen-64-workers', 'profile': candidate, 'rate': 3200, 'repetition': 2, 'write_every': 10}
            trial(fixture, case)
            metrics = fixture.summary['runs'][-1]['measure']['all']
            scores.append((metrics['client_drop'] + sum((metrics['failures'] or {}).values()), metrics['success_arrival']['p95_us'], candidate))
        chosen = min(scores, key=lambda score: score[:2])[2]
        fixture.summary['selected_tuning'], fixture.summary['screen_scores'] = chosen, scores
        fixture.summary['experiment']['per_run_profile_is_canonical'] = True
        fixture.save('summary.json', fixture.summary)
        selection = {'event': 'v2_tuning_selected', 'profile': chosen, 'scores': scores}
        print(json.dumps(selection), flush=True)
        fixture.args.seconds, fixture.args.warm = 20, 10
        for write_every in (10, 1):
            if write_every == 1:
                configure(fixture, default)
                fixture.prewarm(write_every)
            for rate in (800, 3200):
                for repetition in range(3):
                    case = {'variant': 'new-tuned', 'profile': chosen, 'rate': rate, 'repetition': repetition, 'write_every': write_every}
                    direct_case = {'variant': 'matched-direct', 'profile': chosen, 'rate': rate, 'repetition': repetition, 'write_every': write_every}
                    order = [direct_case, case] if repetition % 2 == 0 else [case, direct_case]
                    for item in order:
                        trial(fixture, item)
        fixture.args.seconds, fixture.args.warm = 90, 0
        phase_profile = {'pool': 4, 'collect_ms': 1, 'max_read_size': '16KiB', 'workers': 32, 'batch': 8}
        fixture.phase_enabled = True
        for repetition in range(2):
            try:
                case = {'variant': 'backpressure', 'profile': phase_profile, 'rate': 3200, 'repetition': repetition, 'write_every': 10}
                trial(fixture, case)
            finally:
                if getattr(fixture, 'phase_stop', None) is not None:
                    fixture.phase_stop.set()
                    fixture.phase_thread.join(15)
            if fixture.phase_errors or len(fixture.phase_controls) != 3:
                raise RuntimeError('phase controller incomplete: ' + str(fixture.phase_errors))
            record = fixture.summary['runs'][-1]
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
