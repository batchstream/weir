"""M28 raw-stream completeness, deliberately separate from capacity qualification."""
import base64
import json
import re
from pathlib import Path

from capacity_report import counter, timestamp, prom, weir_metrics, window_gate, db_gate

ROLES = ('weir', 'es', 'client')
LIMITS = dict(weir=(2, 1024, 256), es=(3, 3072, 512), client=(1, 512, 256))


def require(value, message):
    if not value:
        raise ValueError(message)


def integer(value, positive=False):
    require(type(value) is int and (0 < value if positive else 0 <= value) and value <= 2**64-1, 'invalid integer')
    return value


def read_stream(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= 64 << 20 and raw.endswith(b'\n'), 'stream size/truncated tail')
    lines = raw.splitlines()
    require(len(lines) <= 2000 and all(len(line) <= 1 << 20 for line in lines), 'line/count bound')
    return [json.loads(line) for line in lines]


def process(value):
    identity = value['identity']
    require(re.fullmatch('[0-9a-f]{64}', identity['exe_sha256']), 'executable hash')
    require(str(int(identity['pid'])) == identity['pid'], 'PID syntax')
    integer(identity['start_ticks'], True); integer(identity['uid'], True)
    require(identity['cgroup'] == '0::/\n', 'unsupported cgroup profile')
    require(set(identity['namespaces']) == {'pid', 'mnt', 'cgroup', 'net', 'user'}, 'namespace identity missing')
    for key, ns in identity['namespaces'].items():
        require(re.fullmatch(key+r':\[\d+\]', ns), 'namespace value')
    fields = value['stat'].rsplit(') ', 1)[1].split()
    require(fields[0] not in ('Z', 'X') and int(fields[19]) == identity['start_ticks'], 'exited/reused PID')
    for key, index in (('user_ticks', 11), ('system_ticks', 12)):
        require(integer(value[key]) == int(fields[index]), 'raw process CPU mismatch')
    status = dict(line.split(':', 1) for line in value['status'].splitlines() if ':' in line)
    rss = status['VmRSS'].split()
    require(len(rss) == 2 and rss[1] == 'kB' and int(rss[0])*1024 == integer(value['rss_bytes'], True), 'RSS unit/value')
    require(int(status['Threads']) == integer(value['threads'], True), 'thread value')
    require(list(map(int, status['Uid'].split())) == [identity['uid']]*4, 'UID mismatch')
    require(int(status['CapEff'],16)==0 and int(status['NoNewPrivs'])==1 and int(status['Seccomp'])==2, 'runtime privilege profile')
    require(0 < integer(value['fd']) <= 4096, 'FD bound')
    return identity


def stream_report(samples, role):
    require(2 <= len(samples) <= 450, 'sample count/role missing: '+role)
    maximum = dict(rss_bytes=0, fd=0, threads=0, cgroup_bytes=0, sample_duration_ns=0,
                   observer_rss_bytes=0, observer_user_ticks=0, observer_system_ticks=0)
    previous = None
    previous_native = None
    first_identity = None
    max_gap = 0
    observer_tick_delta = 0
    for sequence, sample in enumerate(samples):
        require(sample["sequence"] == sequence, "omitted/duplicate sample sequence")
        require(sample['role'] == role and not sample.get('errors'), 'sample role/errors')
        begin, end = timestamp(sample['time']), timestamp(sample['end'])
        mono, mono_end = integer(sample['monotonic_ns'], True), integer(sample['end_monotonic_ns'], True)
        duration = integer(sample['duration_ns'], True)
        require(mono_end-mono == duration and 0 < duration <= 2e9 and abs(end-begin-duration/1e9) < .05, 'sample clocks/duration')
        target, observer = process(sample['process']), process(sample['observer'])
        require(target['uid'] == observer['uid'] and target['namespaces'] == observer['namespaces'] and target['cgroup'] == observer['cgroup'], 'observer not same container/UID')
        identity = (target, observer)
        if first_identity is None:
            first_identity = identity
        require(identity == first_identity, 'process/observer identity changed')
        files = sample['files']
        require(files['status'] == sample['process']['status'] and files['stat'] == sample['process']['stat'] and files['cgroup'] == target['cgroup'], 'raw process mismatch')
        cpus, mib, pids = LIMITS[role]
        require(int(files['memory.max']) == mib*1024**2 and int(files['memory.swap.max']) == 0 and int(files['pids.max']) == pids, 'resource limit drift')
        quota, period = map(int, files['cpu.max'].split())
        require(period > 0 and quota/period == cpus and 0 < int(files['pids.current']) <= pids, 'CPU/pids profile')
        require(re.fullmatch(r'\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*\n?', files['cpuset.cpus.effective']), 'cpuset missing/invalid')
        require(0 < int(files['memory.current']) <= mib*1024**2 and 0 < sample['rss_bytes'] <= mib*1024**2, 'memory resource bound')
        require(sample['rss_bytes'] == sample['process']['rss_bytes'] and sample['fd'] == sample['process']['fd'], 'process summary mismatch')
        require('io.stat' in files, 'io.stat missing')
        io_counters = {}
        for line in files['io.stat'].splitlines():
            fields = line.split(); require(re.fullmatch(r'\d+:\d+', fields[0]) and len(fields) >= 2, 'io.stat device')
            for pair in fields[1:]:
                key, raw = pair.split('='); name = fields[0]+':'+key
                require(name not in io_counters, 'duplicate io counter');io_counters[name] = integer(int(raw))
        cpu, memory = counter(files['cpu.stat']), counter(files['memory.events'])
        for key in ('usage_usec', 'user_usec', 'system_usec', 'nr_periods', 'nr_throttled', 'throttled_usec'):
            integer(cpu[key])
        for key in ('low', 'high', 'max', 'oom', 'oom_kill', 'oom_group_kill'):
            integer(memory[key])
        require(not any(memory[k] for k in ('oom', 'oom_kill', 'oom_group_kill')), 'OOM observed')
        values = {**{'cpu:'+k:v for k,v in cpu.items()}, **{'memory:'+k:v for k,v in memory.items()}, **io_counters}
        for who in ('process', 'observer'):
            for key in ('user_ticks','system_ticks'):
                values[who+':'+key] = sample[who][key]
        if previous:
            delta = (mono-previous[0])/1e9
            require(0 < delta <= 6 and abs(begin-previous[1]-delta) < .1, 'sample gap/UTC clock drift')
            max_gap = max(max_gap, delta)
            require(all(k in values and values[k] >= v for k,v in previous[2].items()), 'counter decreased/disappeared')
        previous = (mono, begin, values)
        if role == 'weir':
            reasons, _, _ = weir_metrics(sample['metrics'], True)
            require(not reasons, 'Weir guard/ledger safety')
            metrics = prom(sample['metrics'])
            for name in ('go_goroutines','go_memstats_heap_alloc_bytes','process_resident_memory_bytes','process_cpu_seconds_total','process_open_fds'):
                require(name in metrics and metrics[name] >= 0, 'runtime metric missing/invalid: '+name)
            maximum['go_goroutines'] = max(maximum.get('go_goroutines', 0), metrics['go_goroutines'])
            maximum['go_heap_alloc_bytes'] = max(maximum.get('go_heap_alloc_bytes', 0), metrics['go_memstats_heap_alloc_bytes'])
        elif role == 'es':
            # fs.io_stats can be absent for tmpfs; io.stat remains a valid empty file.
            node = next(iter(json.loads(sample['db'])['nodes'].values()))
            require(len(json.loads(sample['db'])['nodes']) == 1, 'ES node count')
            require(abs(begin-node['process']['timestamp']/1000) <= 2, 'ES stats clock')
            for key in ('heap_used_in_bytes','heap_max_in_bytes'):
                integer(node['jvm']['mem'][key], True)
            integer(node['jvm']['threads']['count'], True)
            for key in ('current_open','total_opened'):
                integer(node['http'][key])
            for pool in ('write','get'):
                for key in ('active','queue','rejected','completed'):
                    integer(node['thread_pool'][pool][key])
                require(node['thread_pool'][pool]['rejected'] == 0, 'ES rejected work')
            native_counts = {'http_total_opened':node['http']['total_opened']}
            for pool in ('write','get'):
                for key in ('rejected','completed'):
                    native_counts[pool+key]=node['thread_pool'][pool][key]
            if previous_native:
                require(all(native_counts[k]>=v for k,v in previous_native.items()), 'ES native counter decreased')
            previous_native=native_counts
            require(node['jvm']['mem']['heap_max_in_bytes']==1024**3, 'ES heap limit drift')
            maximum['jvm_heap_bytes'] = max(maximum.get('jvm_heap_bytes',0),node['jvm']['mem']['heap_used_in_bytes'])
        else:
            integer(sample['goroutines'], True);integer(sample['gomaxprocs'], True);integer(sample['go_heap_alloc_bytes'], True)
            maximum['go_goroutines'] = max(maximum.get('go_goroutines',0),sample['goroutines'])
        observed = dict(rss_bytes=sample['rss_bytes'],fd=sample['fd'],threads=sample['process']['threads'],
                        cgroup_bytes=int(files['memory.current']),sample_duration_ns=duration,
                        observer_rss_bytes=sample['observer']['rss_bytes'],observer_user_ticks=sample['observer']['user_ticks'],
                        observer_system_ticks=sample['observer']['system_ticks'])
        for key,value in observed.items():
            maximum[key] = max(maximum[key],value)
        if role != 'client':
            require(sample['observer']['rss_bytes'] <= 96*1024**2, 'observer RSS budget')
    seconds = (samples[-1]['monotonic_ns']-samples[0]['monotonic_ns'])/1e9
    tick_delta = sum(samples[-1]['observer'][k]-samples[0]['observer'][k] for k in ('user_ticks','system_ticks'))
    # Native fixture verifies getconf CLK_TCK=100 separately; report raw counts here.
    result = dict(samples=len(samples),identity=first_identity,max_gap_seconds=max_gap,sampled_maximum=maximum,
                  observer_cpu_ticks_delta=tick_delta,seconds=seconds,process_cpu_unit='USER_HZ ticks; no guessed percentage',
                  cgroup_cpu_usage_usec_delta=counter(samples[-1]['files']['cpu.stat'])['usage_usec']-counter(samples[0]['files']['cpu.stat'])['usage_usec'],
                  hidden_ancestors='unknown',io_stat='valid empty' if all(s['files']['io.stat']=='' for s in samples) else 'device counters')
    return result


def coverage(samples, start, end):
    before = [s for s in samples if timestamp(s['end']) <= start]
    after = [s for s in samples if timestamp(s['time']) >= end]
    require(before and after and start-timestamp(before[-1]['end']) <= 6 and timestamp(after[0]['time'])-end <= 6, 'missing before/after load coverage')


def report(root):
    root=Path(root)
    result=dict(resource_evidence='partial',scope='visible cgroup-v2 leaf on local native Linux arm64 only',candidate=None,
                capacity_qualification='not-run',timing_qualification='NO-GO',errors=[],roles={},trials=[])
    try:
        plan=json.loads((root/'plan.json').read_text())
        hashes={Path(name).name:value for name,value in plan['inputs'].items()}
        streams={role:read_stream(root/(role+'.jsonl')) for role in ('weir','es')}
        samples={role:[v for v in stream if 'time' in v] for role,stream in streams.items()}
        for role, stream in streams.items():
            require(stream[-1].get('type')=='observer_end' and stream[-1]['samples']==len(samples[role]), 'missing observer completion/count')
            require(stream[0]['type']=='identity' and stream[0]['role']==role and stream[0]['target']==samples[role][0]['process']['identity'], 'initial stream identity')
            result['roles'][role]=stream_report(samples[role],role)
        native=(root/'native-identity.txt').read_text().splitlines()
        require(native[2]=='100', 'native CLK_TCK verification missing/wrong unit')
        java_hash=native[3].split()[0]
        for role in ('weir','es'):
            identity=samples[role][0]['process']['identity']
            require(identity['exe_sha256']==(hashes['weir'] if role=='weir' else java_hash), 'target artifact drift')
            require(identity['uid']==(65532 if role=='weir' else 1000), 'role UID drift')
            require(samples[role][0]['observer']['identity']['exe_sha256']==hashes['client'], 'observer artifact drift')
        for role in ('weir','es'):
            intervals=[]
            for before,after in zip(samples[role],samples[role][1:]):
                delta=sum(after['observer'][k]-before['observer'][k] for k in ('user_ticks','system_ticks'))
                intervals.append(delta/100/((after['monotonic_ns']-before['monotonic_ns'])/1e9))
            require(max(intervals)<=.25, 'observer CPU budget')
            result['roles'][role]['observer_max_interval_cpu_cores']=max(intervals)
            result['roles'][role]['verified_CLK_TCK']=100
        totals=dict(planned=0,mutations=0)
        timing=[]
        for name in ('through','direct'):
            entries=read_stream(root/(name+'.jsonl'))
            trials=[e for e in entries if e.get('type')=='trial'];require(len(trials)==1 and trials[0]['run_error']=='<nil>', 'trial error/count')
            trial=trials[0]['trial'];options=trial['options']
            require(options['Rate']==50 and options['WarmSeconds']==20 and options['Seconds']==20 and trial['planned']==2000, 'frozen load drift')
            client=[e['sample'] for e in entries if e.get('type') in ('client_start','client_sample','client_end')]
            result['roles']['client-'+name]=stream_report(client,'client')
            identity=client[0]['process']['identity']
            require(identity['exe_sha256']==hashes['client'] and identity['uid']==65532, 'client artifact/UID')
            ns=identity['namespaces']
            for role in ('weir','es'):
                other=samples[role][0]['process']['identity']['namespaces']
                require(ns['net']==other['net'] and all(ns[key]!=other[key] for key in ('pid','mnt','cgroup')), 'role namespaces not independent')
            start=timestamp(trial['start']);end=start+40
            for series in [samples['weir'],samples['es'],client]:coverage(series,start,end)
            reasons=[]
            for window in [trial['warm'],trial['measure']]+trial['ten_second_windows']:reasons+=window_gate(window)
            audits=[e for e in entries if e.get('type')=='audit'];require(len(audits)==1 and audits[0]['error']=='<nil>', 'DB audit missing/error')
            audit=audits[0];ledger=base64.b64decode(audit['ledger'],validate=True)
            require(len(ledger)==200 and set(ledger)<={0,1,2,3,4}, 'mutation ledger')
            require(audit['audit']['planned_writes']==200 and audit['audit']['applied']==ledger.count(1) and audit['audit']['found_version1']==ledger.count(1) and audit['audit']['unknown_found']==0, 'DB audit/ledger conservation')
            receipts=[e for e in entries if e.get('type')=='mutation_receipt'];require(len(receipts)==1 and receipts[0]['started']<=1200, 'mutation receipt')
            totals['planned']+=1000+trial['planned'];totals['mutations']+=receipts[0]['started']
            for window in (trial['warm'],trial['measure']):require(not window['all']['unknown'] and not window['all']['failures'], 'data/transport failure')
            timing+=reasons
            result['trials'].append(dict(name=name,counts=[trial['warm']['all'],trial['measure']['all']],timing_reasons=sorted(set(reasons)),audit=audit['audit']))
        require(totals['planned']==6000 and totals['mutations']<=2400, 'global operation budget')
        require(samples['weir'][0]['process']['identity']['namespaces']['net']==samples['es'][0]['process']['identity']['namespaces']['net'], 'cross-stream network identity')
        require(all(samples['weir'][0]['process']['identity']['namespaces'][key]!=samples['es'][0]['process']['identity']['namespaces'][key] for key in ('pid','mnt','cgroup')), 'shared process/mount/cgroup namespace')
        result.update(resource_evidence='complete-for-declared-visible-leaf-profile',timing_qualification='NO-GO' if timing else 'short-trial-pass-only',totals=totals)
    except (ValueError,KeyError,TypeError,IndexError,RuntimeError,OSError) as exc:
        result['errors'].append(str(exc))
    return result
