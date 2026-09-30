"""M28 raw-stream completeness, deliberately separate from capacity qualification."""

import base64
import json
import re
from pathlib import Path

from capacity_report import (
    counter,
    timestamp,
    prom,
    weir_metrics,
    window_gate,
    integer,
    metrics_sum,
    tcp_connections,
)

ROLES = ('weir', 'es', 'client')
LIMITS = dict(weir=(2, 1024, 256), es=(3, 3072, 512), client=(1, 512, 256))


def require(value, message):
    if not value:
        raise ValueError(message)


def read_stream(path):
    with Path(path).open('rb') as stream:
        raw = stream.read((64 << 20) + 1)
    require(raw, 'empty resource stream: ' + Path(path).name)
    require(len(raw) <= 64 << 20 and raw.endswith(b'\n'), 'stream size/truncated tail')
    lines = raw.splitlines()
    require(len(lines) <= 2000 and all(len(line) <= 1 << 20 for line in lines), 'line/count bound')
    entries = [json.loads(line) for line in lines]
    require(all(isinstance(e, dict) for e in entries), 'record must be an object')
    return entries


def process(value):
    identity = value['identity']
    require(isinstance(identity, dict), 'process identity must be an object')
    require(re.fullmatch('[0-9a-f]{64}', identity['exe_sha256']), 'executable hash')
    require(
        isinstance(identity['pid'], str)
        and identity['pid'].isdigit()
        and str(integer(int(identity['pid']), True)) == identity['pid'],
        'PID syntax',
    )
    integer(identity['start_ticks'], True)
    integer(identity['uid'], True)

    require(identity['cgroup'] == '0::/\n', 'unsupported cgroup profile')
    require(
        isinstance(identity['namespaces'], dict)
        and set(identity['namespaces']) == {'pid', 'mnt', 'cgroup', 'net', 'user'},
        'namespace identity missing',
    )
    for key, ns in identity['namespaces'].items():
        require(re.fullmatch(key + r':\[\d+\]', ns), 'namespace value')

    require(
        isinstance(value['stat'], str) and isinstance(value['status'], str),
        'raw process text missing',
    )
    require(value['stat'].split(' ', 1)[0] == identity['pid'], 'raw PID mismatch')
    fields = value['stat'].rsplit(') ', 1)[1].split()
    require(
        fields[0] not in ('Z', 'X') and int(fields[19]) == identity['start_ticks'],
        'exited/reused PID',
    )
    for key, index in (('user_ticks', 11), ('system_ticks', 12)):
        require(integer(value[key]) == int(fields[index]), 'raw process CPU mismatch')

    status = dict(line.split(':', 1) for line in value['status'].splitlines() if ':' in line)
    rss = status['VmRSS'].split()
    require(
        len(rss) == 2
        and rss[1] == 'kB'
        and int(rss[0]) * 1024 == integer(value['rss_bytes'], True),
        'RSS unit/value',
    )
    require(int(status['Threads']) == integer(value['threads'], True), 'thread value')
    require(list(map(int, status['Uid'].split())) == [identity['uid']] * 4, 'UID mismatch')
    require(
        int(status['CapEff'], 16) == 0
        and int(status['NoNewPrivs']) == 1
        and int(status['Seccomp']) == 2,
        'runtime privilege profile',
    )
    require(0 < integer(value['fd']) <= 4096, 'FD bound')
    return identity


def stream_report(samples, role):
    require(2 <= len(samples) <= 450, 'sample count/role missing: ' + role)
    maximum = dict(
        rss_bytes=0,
        fd=0,
        threads=0,
        cgroup_bytes=0,
        sample_duration_ns=0,
        observer_rss_bytes=0,
        observer_user_ticks=0,
        observer_system_ticks=0,
    )
    previous = None
    previous_native = None
    first_identity = None
    max_gap = 0
    max_observer_cpu = 0

    for sequence, sample in enumerate(samples):
        require(integer(sample["sequence"]) == sequence, "omitted/duplicate sample sequence")
        require(sample['role'] == role and not sample.get('errors'), 'sample role/errors')
        begin, end = timestamp(sample['time']), timestamp(sample['end'])
        mono, mono_end = integer(sample['monotonic_ns'], True), integer(
            sample['end_monotonic_ns'], True
        )
        duration = integer(sample['duration_ns'], True)
        require(
            mono_end - mono == duration
            and 0 < duration <= 2e9
            and abs(end - begin - duration / 1e9) < 0.05,
            'sample clocks/duration',
        )
        target, observer = process(sample['process']), process(sample['observer'])
        require(
            target['uid'] == observer['uid']
            and target['namespaces'] == observer['namespaces']
            and target['cgroup'] == observer['cgroup'],
            'observer not same container/UID',
        )
        identity = (target, observer)
        if first_identity is None:
            first_identity = identity
        require(identity == first_identity, 'process/observer identity changed')
        files = sample['files']
        require(
            isinstance(files, dict)
            and all(isinstance(v, str) and len(v.encode()) <= 262144 for v in files.values()),
            'raw files must contain bounded text',
        )
        tcp_connections(files)  # Shared network namespace, not process-owned connections.
        # Kernel columns are padded; only the field tokens carry meaning.
        limits = [line.split() for line in files['limits'].splitlines()]
        require(
            limits and limits[0] == ['Limit', 'Soft', 'Limit', 'Hard', 'Limit', 'Units'],
            'raw limits header',
        )
        fd_limits = [fields[3:] for fields in limits[1:] if fields[:3] == ['Max', 'open', 'files']]
        require(
            len(fd_limits) == 1 and len(fd_limits[0]) == 3,
            'missing/duplicate/extra FD limit fields',
        )
        soft, hard, unit = fd_limits[0]
        require(
            soft.isascii()
            and soft.isdecimal()
            and hard.isascii()
            and hard.isdecimal()
            and integer(int(soft), True) == integer(int(hard), True) == 4096
            and unit == 'files',
            'raw limits/FD profile',
        )
        require(
            files['status'] == sample['process']['status']
            and files['stat'] == sample['process']['stat']
            and files['cgroup'] == target['cgroup'],
            'raw process mismatch',
        )
        cpus, mib, pids = LIMITS[role]
        require(
            int(files['memory.max']) == mib * 1024**2
            and int(files['memory.swap.max']) == 0
            and int(files['pids.max']) == pids,
            'resource limit drift',
        )
        quota, period = map(int, files['cpu.max'].split())
        require(
            period > 0 and quota / period == cpus and 0 < int(files['pids.current']) <= pids,
            'CPU/pids profile',
        )
        require(
            re.fullmatch(r'\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*\n?', files['cpuset.cpus.effective']),
            'cpuset missing/invalid',
        )
        require(
            0 < int(files['memory.current']) <= mib * 1024**2
            and 0 < sample['rss_bytes'] <= mib * 1024**2,
            'memory resource bound',
        )
        require(
            integer(sample['rss_bytes'], True) == sample['process']['rss_bytes']
            and integer(sample['fd'], True) == sample['process']['fd'],
            'process summary mismatch',
        )
        require('io.stat' in files, 'io.stat missing')
        io_counters = {}
        for line in files['io.stat'].splitlines():
            fields = line.split()
            require(re.fullmatch(r'\d+:\d+', fields[0]) and len(fields) >= 2, 'io.stat device')
            for pair in fields[1:]:
                key, raw = pair.split('=')
                name = fields[0] + ':' + key
                require(name not in io_counters, 'duplicate io counter')
                io_counters[name] = integer(int(raw))
        cpu, memory = counter(files['cpu.stat']), counter(files['memory.events'])
        for key in (
            'usage_usec',
            'user_usec',
            'system_usec',
            'nr_periods',
            'nr_throttled',
            'throttled_usec',
        ):
            integer(cpu[key])
        for key in ('low', 'high', 'max', 'oom', 'oom_kill', 'oom_group_kill'):
            integer(memory[key])
        require(not any(memory[k] for k in ('oom', 'oom_kill', 'oom_group_kill')), 'OOM observed')
        values = {
            **{'cpu:' + k: v for k, v in cpu.items()},
            **{'memory:' + k: v for k, v in memory.items()},
            **io_counters,
        }
        for who in ('process', 'observer'):
            for key in ('user_ticks', 'system_ticks'):
                values[who + ':' + key] = sample[who][key]
        if previous:
            delta = (mono - previous[0]) / 1e9
            require(
                0 < delta <= 6 and abs(begin - previous[1] - delta) < 0.1,
                'sample gap/UTC clock drift',
            )
            max_gap = max(max_gap, delta)
            require(
                all(k in values and values[k] >= v for k, v in previous[2].items()),
                'counter decreased/disappeared',
            )
            if role != 'client':
                ticks = sum(
                    values['observer:' + k] - previous[2]['observer:' + k]
                    for k in ('user_ticks', 'system_ticks')
                )
                max_observer_cpu = max(max_observer_cpu, ticks / 100 / delta)
                require(max_observer_cpu <= 0.25, 'observer CPU budget at verified CLK_TCK=100')
        previous = (mono, begin, values)
        if role == 'weir':
            reasons, _, _ = weir_metrics(sample['metrics'], True)
            require(not reasons, 'Weir guard/ledger safety')
            metrics = prom(sample['metrics'])
            for name in (
                'go_goroutines',
                'go_memstats_heap_alloc_bytes',
                'process_resident_memory_bytes',
                'process_cpu_seconds_total',
                'process_open_fds',
            ):
                require(
                    name in metrics and metrics[name] >= 0,
                    'runtime metric missing/invalid: ' + name,
                )
            require(
                0 < metrics['go_goroutines'] == int(metrics['go_goroutines'])
                and 0 < metrics['process_open_fds'] <= 4096
                and 0 < metrics['process_resident_memory_bytes'] <= mib * 1024**2
                and 0 <= metrics['go_memstats_heap_alloc_bytes'] <= mib * 1024**2,
                'runtime resource boundary',
            )
            require(
                previous_native is None or metrics['process_cpu_seconds_total'] >= previous_native,
                'standard process CPU counter decreased',
            )
            previous_native = metrics['process_cpu_seconds_total']
            maximum['go_goroutines'] = max(
                maximum.get('go_goroutines', 0), metrics['go_goroutines']
            )
            maximum['go_heap_alloc_bytes'] = max(
                maximum.get('go_heap_alloc_bytes', 0), metrics['go_memstats_heap_alloc_bytes']
            )
        elif role == 'es':
            # fs.io_stats can be absent for tmpfs; io.stat remains a valid empty file.
            nodes = json.loads(sample['db'])['nodes']
            require(
                isinstance(nodes, dict) and len(nodes) == 1,
                'ES nodes must contain exactly one node',
            )
            node = next(iter(nodes.values()))
            require(isinstance(node, dict), 'ES node must be an object')
            require(
                abs(begin - integer(node['process']['timestamp'], True) / 1000) <= 2,
                'ES stats clock',
            )
            integer(node['process']['open_file_descriptors'], True)
            process_cpu = integer(node['process']['cpu']['total_in_millis'])
            for key in ('heap_used_in_bytes', 'heap_max_in_bytes'):
                integer(node['jvm']['mem'][key], True)
            integer(node['jvm']['threads']['count'], True)
            for key in ('current_open', 'total_opened'):
                integer(node['http'][key])
            for pool in ('write', 'get'):
                for key in ('active', 'queue', 'rejected', 'completed'):
                    integer(node['thread_pool'][pool][key])
                require(node['thread_pool'][pool]['rejected'] == 0, 'ES rejected work')
            native_counts = {
                'http_total_opened': node['http']['total_opened'],
                'process_cpu_millis': process_cpu,
            }
            for pool in ('write', 'get'):
                for key in ('rejected', 'completed'):
                    native_counts[pool + key] = node['thread_pool'][pool][key]
            if previous_native:
                require(
                    all(native_counts[k] >= v for k, v in previous_native.items()),
                    'ES native counter decreased',
                )
            previous_native = native_counts
            require(node['jvm']['mem']['heap_max_in_bytes'] == 1024**3, 'ES heap limit drift')
            require(
                node['jvm']['mem']['heap_used_in_bytes'] <= node['jvm']['mem']['heap_max_in_bytes'],
                'ES heap resource boundary',
            )
            maximum['jvm_heap_bytes'] = max(
                maximum.get('jvm_heap_bytes', 0), node['jvm']['mem']['heap_used_in_bytes']
            )
        else:
            integer(sample['goroutines'], True)
            integer(sample['gomaxprocs'], True)
            integer(sample['go_heap_alloc_bytes'], True)
            maximum['go_goroutines'] = max(maximum.get('go_goroutines', 0), sample['goroutines'])
        observed = dict(
            rss_bytes=sample['rss_bytes'],
            fd=sample['fd'],
            threads=sample['process']['threads'],
            cgroup_bytes=int(files['memory.current']),
            sample_duration_ns=duration,
            observer_rss_bytes=sample['observer']['rss_bytes'],
            observer_user_ticks=sample['observer']['user_ticks'],
            observer_system_ticks=sample['observer']['system_ticks'],
        )
        for key, value in observed.items():
            maximum[key] = max(maximum[key], value)
        if role != 'client':
            require(sample['observer']['rss_bytes'] <= 96 * 1024**2, 'observer RSS budget')
    seconds = (samples[-1]['monotonic_ns'] - samples[0]['monotonic_ns']) / 1e9
    tick_delta = sum(
        samples[-1]['observer'][k] - samples[0]['observer'][k]
        for k in ('user_ticks', 'system_ticks')
    )
    # Native fixture verifies getconf CLK_TCK=100 separately; report raw counts here.
    result = dict(
        samples=len(samples),
        identity=first_identity,
        max_gap_seconds=max_gap,
        sampled_maximum=maximum,
        observer_cpu_ticks_delta=tick_delta,
        seconds=seconds,
        process_cpu_unit='USER_HZ ticks; no guessed percentage',
        cgroup_cpu_usage_usec_delta=counter(samples[-1]['files']['cpu.stat'])['usage_usec']
        - counter(samples[0]['files']['cpu.stat'])['usage_usec'],
        hidden_ancestors='unknown',
        io_stat=(
            'valid empty'
            if all(s['files']['io.stat'] == '' for s in samples)
            else 'device counters'
        ),
    )
    if role != 'client':
        result.update(observer_max_interval_cpu_cores=max_observer_cpu, verified_CLK_TCK=100)
    return result


def coverage(samples, start, end):
    before = [s for s in samples if timestamp(s['end']) <= start]
    after = [s for s in samples if timestamp(s['time']) >= end]
    require(
        before
        and after
        and start - timestamp(before[-1]['end']) <= 6
        and timestamp(after[0]['time']) - end <= 6,
        'missing before/after load coverage',
    )


def trial_report(entries, name):
    """The actual short helper stream, including setup and terminal receipts."""
    kinds = [e['type'] for e in entries]
    require(
        kinds[:1000] == ['setup_progress'] * 1000
        and kinds[1000:1002] == ['client_start', 'trial']
        and kinds[-3:] == ['audit', 'client_end', 'mutation_receipt']
        and len(kinds[1002:-3]) >= 2
        and set(kinds[1002:-3]) == {'client_sample'},
        'trial record order/completion/setup',
    )
    seeds = [integer(e['started'], True) for e in entries[:1000]]
    require(seeds == list(range(1, 1001)), 'setup progress missing/duplicate/nonmonotonic')
    row = entries[1001]
    require(row['run_error'] == '<nil>', 'trial error')
    trial = row['trial']
    options = trial['options']
    for key, expected in dict(Rate=50, WarmSeconds=20, Seconds=20, Workers=64).items():
        require(integer(options[key], True) == expected, 'frozen option drift: ' + key)
    require(
        options['TimingOnly'] is False
        and options['LegacyExpiry'] is False
        and options['Prefix'] == 'm28r-' + name,
        'frozen trial mode/prefix',
    )
    require(integer(trial['planned']) == 2000, 'trial planned count')
    start = timestamp(trial['start'])
    end = start + 40
    require(start + 39 <= timestamp(trial['end']) <= end + 1, 'trial completion clock')
    windows = trial['ten_second_windows']
    require(isinstance(windows, list) and len(windows) == 2, 'measurement ten-second windows')
    reasons = []
    for window, planned in [(trial['warm'], 1000), (trial['measure'], 1000)] + [
        (w, 500) for w in windows
    ]:
        for kind in ('all', 'read', 'put'):
            m = window[kind]
            for key in ('due', 'cancelled_future', 'worker_expired'):
                integer(m[key])
            for key in (
                'arrival',
                'dispatch',
                'lag',
                'wake',
                'decision',
                'construct',
                'handoff',
                'worker_start',
            ):
                integer(m[key]['max_ns'])
            require(m['cancelled_future'] == 0, 'unfinished schedule')
        reasons += window_gate(window)
        require(
            window['all']['planned'] == planned and window['put']['planned'] == planned // 10,
            'fixed read/put plan',
        )
    for kind in ('all', 'read', 'put'):
        metrics_sum(trial['measure'][kind], [w[kind] for w in windows])
    audit_row = entries[-3]
    require(
        audit_row['error'] == '<nil>' and audit_row['prefix'] == options['Prefix'],
        'audit error/prefix',
    )
    ledger = base64.b64decode(audit_row['ledger'], validate=True)
    require(len(ledger) == 200 and set(ledger) <= {0, 1, 2, 3, 4}, 'mutation ledger')
    for window, segment in [
        (trial['warm'], ledger[:100]),
        (trial['measure'], ledger[100:]),
        (windows[0], ledger[100:150]),
        (windows[1], ledger[150:]),
    ]:
        put = window['put']
        require(
            segment.count(0) == put['client_drop']
            and segment.count(1) == put['success']
            and segment.count(2) == put['unknown']
            and len(segment) - segment.count(0) == put['started'],
            'ledger/window conservation',
        )
    audit = audit_row['audit']
    for key in ('planned_writes', 'applied', 'found_version1', 'unknown_found', 'absent', 'pages'):
        integer(audit[key])
    require(
        audit['planned_writes'] == len(ledger)
        and audit['applied'] == ledger.count(1)
        and audit['found_version1'] == audit['applied'] + audit['unknown_found']
        and audit['unknown_found'] <= ledger.count(2)
        and audit['found_version1'] + audit['absent'] == len(ledger)
        and audit['pages'] == 2,
        'DB audit/ledger conservation',
    )
    started = integer(entries[-1]['started'])
    put_started = trial['warm']['put']['started'] + trial['measure']['put']['started']
    require(
        started == len(seeds) + put_started and started <= 1200,
        'mutation receipt/setup/window conservation',
    )
    for window in (trial['warm'], trial['measure']):
        require(
            not window['all']['unknown'] and not window['all']['failures'], 'data/transport failure'
        )
    client = [
        e['sample'] for e in entries if e['type'] in ('client_start', 'client_sample', 'client_end')
    ]
    require(timestamp(client[-1]['time']) >= timestamp(trial['end']), 'client ends before trial')
    result = dict(
        name=name,
        counts=[trial['warm']['all'], trial['measure']['all']],
        timing_reasons=sorted(set(reasons)),
        audit=audit,
        seed_started=len(seeds),
        trial_put_started=put_started,
        mutation_started=started,
    )
    return result, client, start, end


def observer_identity(first, sample, options):
    role, hashes, native = (options[k] for k in ('role', 'hashes', 'native'))
    require(
        first['type'] == 'identity'
        and first['role'] == role
        and first['target'] == sample['process']['identity']
        and first['observer'] == sample['observer']['identity']
        and first['exe_sha256'] == first['target']['exe_sha256'],
        'initial stream identity',
    )
    require(
        first['goos'] == 'linux'
        and first['goarch'] == 'arm64'
        and first['go'] == 'go1.27.1'
        and isinstance(first['kernel'], str)
        and first['kernel'].startswith('Linux version ')
        and first['process_cpu_unit'] == 'USER_HZ ticks, no percentage conversion',
        'native observer identity/unit',
    )
    require(
        native[2] == '100' and native[0].startswith('Linux ') and native[0].endswith(' aarch64'),
        'native CLK_TCK/architecture',
    )
    identity = first['target']
    require(
        identity['exe_sha256'] == (hashes['weir'] if role == 'weir' else native[3].split()[0]),
        'target artifact drift',
    )
    require(identity['uid'] == (65532 if role == 'weir' else 1000), 'role UID drift')
    require(first['observer']['exe_sha256'] == hashes['client'], 'observer artifact drift')


def report(root):
    root = Path(root)
    result = dict(
        resource_evidence='partial',
        scope='visible cgroup-v2 leaf on local native Linux arm64 only',
        candidate=None,
        capacity_qualification='not-run',
        timing_qualification='not-run',
        errors=[],
        roles={},
        trials=[],
    )
    try:
        plan = json.loads((root / 'plan.json').read_text())
        hashes = {Path(name).name: value for name, value in plan['inputs'].items()}
        streams = {role: read_stream(root / (role + '.jsonl')) for role in ('weir', 'es')}
        native = (root / 'native-identity.txt').read_text().splitlines()
        samples = {}
        require(
            integer(plan['observer_seconds']) == 140
            and integer(plan['observer_samples']) == 71
            and integer(plan['planned']) == 6000
            and integer(plan['document_mutations']) == 2400,
            'frozen resource budget',
        )
        for role, stream in streams.items():
            samples[role] = stream[1:-1]
            first, last = stream[0], stream[-1]
            identity_options = dict(role=role, hashes=hashes, native=native)
            observer_identity(first, samples[role][0], identity_options)
            require(
                last['type'] == 'observer_end'
                and last['role'] == role
                and integer(last['samples']) == len(samples[role]) == plan['observer_samples'],
                'missing observer completion/count',
            )
            require(
                0 <= timestamp(last['ended_at']) - timestamp(samples[role][-1]['end']) <= 2,
                'observer completion clock',
            )
            result['roles'][role] = stream_report(samples[role], role)
            require(
                abs(result['roles'][role]['seconds'] - plan['observer_seconds']) <= 2,
                'observer duration coverage',
            )
        totals = dict(planned=0, mutations=0, seed_started=0, trial_planned=0)
        timing = []
        previous_end = None
        for name in ('through', 'direct'):
            entries = read_stream(root / (name + '.jsonl'))
            result['timing_qualification'] = 'incomplete'
            trial, client, start, end = trial_report(entries, name)
            result['roles']['client-' + name] = stream_report(client, 'client')
            identity = client[0]['process']['identity']
            require(
                identity['exe_sha256'] == hashes['client']
                and identity['uid'] == 65532
                and all(
                    s['observer']['identity'] == s['process']['identity'] and s['gomaxprocs'] == 1
                    for s in client
                ),
                'client artifact/UID/self sampling',
            )
            ns = identity['namespaces']
            for role in ('weir', 'es'):
                other = samples[role][0]['process']['identity']['namespaces']
                require(
                    ns['net'] == other['net']
                    and all(ns[key] != other[key] for key in ('pid', 'mnt', 'cgroup')),
                    'role namespaces not independent',
                )
            require(previous_end is None or start > previous_end, 'trials overlap/out of order')
            previous_end = timestamp(client[-1]['end'])
            for series in [samples['weir'], samples['es'], client]:
                coverage(series, start, end)
            totals['seed_started'] += trial['seed_started']
            totals['trial_planned'] += sum(w['planned'] for w in trial['counts'])
            totals['mutations'] += trial['mutation_started']
            timing += trial['timing_reasons']
            result['trials'].append(trial)
        totals['planned'] = totals['seed_started'] + totals['trial_planned']
        require(
            totals['planned'] == 6000 and totals['mutations'] <= 2400, 'global operation budget'
        )
        # Successful setup reaches client_start only after these management calls.
        result['trial_management_requests'] = dict(
            index_delete=2,
            index_create=2,
            seed_refresh=2,
            evidence='inferred from complete setup and fixed helper source; excludes fixture preload index PUT',
        )
        require(
            samples['weir'][0]['process']['identity']['namespaces']['net']
            == samples['es'][0]['process']['identity']['namespaces']['net'],
            'cross-stream network identity',
        )
        require(
            all(
                samples['weir'][0]['process']['identity']['namespaces'][key]
                != samples['es'][0]['process']['identity']['namespaces'][key]
                for key in ('pid', 'mnt', 'cgroup')
            ),
            'shared process/mount/cgroup namespace',
        )
        result.update(
            resource_evidence='complete-for-declared-visible-leaf-profile',
            timing_qualification='NO-GO' if timing else 'short-trial-pass-only',
            totals=totals,
        )
    except (
        ValueError,
        KeyError,
        TypeError,
        IndexError,
        RuntimeError,
        OSError,
        OverflowError,
    ) as exc:
        result['errors'].append(str(exc))
    return result
