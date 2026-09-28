import base64
import datetime
import copy
import json
from pathlib import Path
import tempfile
import unittest

from capacity_report_test import sample as old_sample, metrics, BASE
from resource_report import stream_report, coverage, read_stream, report


def sample(role, second, sequence):
    value=old_sample(role,second)
    status='VmRSS: 42 kB\nThreads: 2\nUid: 1000 1000 1000 1000\nCapEff: 0000000000000000\nNoNewPrivs: 1\nSeccomp: 2\n'
    fields=['0']*24;fields[0]='S';fields[11]=str(3+sequence);fields[12]='5';fields[19]='123'
    stat='12 (java) '+' '.join(fields)+'\n'
    identity=dict(pid='12',start_ticks=123,exe_sha256='a'*64,uid=1000,cgroup='0::/\n',
                  namespaces={key:key+':[1]' for key in ('pid','mnt','cgroup','net','user')})
    process=dict(identity=identity,rss_bytes=43008,fd=3,threads=2,user_ticks=3+sequence,system_ticks=5,status=status,stat=stat)
    value.update(sequence=sequence,end=value['time'],end_monotonic_ns=value['monotonic_ns']+1000,duration_ns=1000,
                 process=process,observer=copy.deepcopy(process),rss_bytes=43008,fd=3,go_heap_alloc_bytes=1024)
    value['files'].update(status=status,stat=stat,cgroup='0::/\n',limits='Limit Soft Limit Hard Limit Units\nMax open files 4096 4096 files\n',**{'io.stat':''})
    if role=='weir':value['metrics']+='\ngo_goroutines 8\ngo_memstats_heap_alloc_bytes 1024\nprocess_resident_memory_bytes 43008\nprocess_cpu_seconds_total 1.2\nprocess_open_fds 3\n'
    return value


def full_window(planned, drops=0, slow=False):
    result = {}
    for kind, count, drop in [('all',planned,drops), ('read',planned*9//10,0), ('put',planned//10,drops)]:
        m = metrics(count-drop)
        m.update(planned=count,client_drop=drop,client_late=drop,due=count,cancelled_future=0,
                 worker_expired=0,drop_reasons={'expired':drop} if drop else None,failures=None)
        for name in ('wake','decision','construct'):
            m[name] = metrics(count)['arrival']
        for name in ('handoff','worker_start'):
            m[name] = metrics(count-drop)['arrival']
        for name in ('arrival','dispatch','lag','wake','decision','construct','handoff','worker_start'):
            h = m[name]
            h['max_ns'] = 1000000 if h['buckets'] else 0
            if slow and name == 'lag' and h['buckets']:
                h['buckets'][0]['upper_us'] = 6000
                h.update(p50_us=6000,p95_us=6000,p99_us=6000,max_ns=6000000)
        result[kind] = m
    return result


def complete_fixture(root, drops=0, slow=False):
    """Full serialized helper shapes. Synthetic validation only, never native evidence."""
    hashes = dict(weir='a'*64,client='b'*64,es='c'*64)
    def series(role, times):
        values = []
        uid = 1000 if role == 'es' else 65532
        for i,t in enumerate(times):
            s = sample(role,t,i)
            s['end'] = datetime.datetime.fromtimestamp(BASE+t+.000001,datetime.timezone.utc).isoformat()
            for who in ('process','observer'):
                p = s[who]; ident = p['identity']
                ident.update(uid=uid,pid='1' if who=='process' else '77',exe_sha256=hashes[role] if who=='process' else hashes['client'])
                ident['namespaces'] = {k:k+':['+str(100 if k in ('net','user') else {'weir':11,'es':12,'client':13}[role])+']' for k in ('pid','mnt','cgroup','net','user')}
                p['status'] = p['status'].replace('1000 1000 1000 1000',' '.join([str(uid)]*4))
                p['stat'] = p['stat'].replace('12 (java)',ident['pid']+' (target)')
            if role=='client':
                # The same PID is read twice; counters need not be an atomic snapshot.
                s['observer']=copy.deepcopy(s['process'])
                s['observer']['user_ticks']+=1
                fields=s['observer']['stat'].rsplit(') ',1)[1].split()
                fields[11]=str(s['observer']['user_ticks'])
                s['observer']['stat']='1 (target) '+' '.join(fields)+'\n'
            s['files'].update(status=s['process']['status'],stat=s['process']['stat'])
            if role=='es':
                node=json.loads(s['db'])['nodes']['node']
                node['jvm']['mem']['heap_max_in_bytes']=1024**3
                node['jvm']['threads']={'count':2};node['http']['total_opened']=2
                node['process']['cpu']={'total_in_millis':i*10}
                s['db']=json.dumps({'nodes':{'node':node}})
            values.append(s)
        return values
    plan = dict(inputs={'/owned/'+k:v for k,v in hashes.items()},observer_seconds=140,observer_samples=71,planned=6000,document_mutations=2400)
    (root/'plan.json').write_text(json.dumps(plan))
    (root/'native-identity.txt').write_text('Linux synthetic aarch64\nuid=1000\n100\n'+hashes['es']+' /own/java\n')
    streams = {}
    for role in ('weir','es'):
        ss = series(role,range(0,141,2))
        first = dict(type='identity',role=role,target=ss[0]['process']['identity'],observer=ss[0]['observer']['identity'],
                     exe_sha256=hashes[role],goos='linux',goarch='arm64',go='go1.27.1',kernel='Linux version synthetic',
                     process_cpu_unit='USER_HZ ticks, no percentage conversion')
        streams[role] = [first]+ss+[dict(type='observer_end',role=role,samples=len(ss),ended_at=ss[-1]['end'])]
    for name,start in (('through',11),('direct',67)):
        ss = series('client',range(start-1,start+42,2))
        options = dict(Rate=50,WarmSeconds=20,Seconds=20,Workers=64,Prefix='m28r-'+name,TimingOnly=False,LegacyExpiry=False)
        trial = dict(options=options,planned=2000,start=datetime.datetime.fromtimestamp(BASE+start,datetime.timezone.utc).isoformat(),
                     end=datetime.datetime.fromtimestamp(BASE+start+40,datetime.timezone.utc).isoformat(),
                     warm=full_window(1000,drops,slow),measure=full_window(1000,0,slow),
                     ten_second_windows=[full_window(500,0,slow),full_window(500,0,slow)])
        ledger = bytes([0]*drops+[1]*(200-drops))
        audit = dict(planned_writes=200,applied=200-drops,found_version1=200-drops,unknown_found=0,absent=drops,pages=2)
        streams[name] = [dict(type='setup_progress',started=i) for i in range(1,1001)]
        streams[name] += [dict(type='client_start',sample=ss[0]),dict(type='trial',trial=trial,run_error='<nil>')]
        streams[name] += [dict(type='client_sample',sample=s) for s in ss[1:-1]]
        streams[name] += [dict(type='audit',prefix=options['Prefix'],error='<nil>',audit=audit,ledger=base64.b64encode(ledger).decode()),
                          dict(type='client_end',sample=ss[-1]),dict(type='mutation_receipt',started=1200-drops)]
    for name,values in streams.items():
        (root/(name+'.jsonl')).write_text(''.join(json.dumps(v)+'\n' for v in values))
    return streams


class ResourceEvidence(unittest.TestCase):
    def test_client_and_weir(self):
        for role in ('client','weir'):
            values=[sample(role,t,i) for i,t in enumerate((0,2,4))]
            result=stream_report(values,role)
            self.assertEqual(result['samples'],3)
            self.assertEqual(result['sampled_maximum']['rss_bytes'],43008)
            self.assertEqual(result['io_stat'],'valid empty')

    def test_missing_corrupt_identity_units_and_gaps(self):
        values=[sample('client',t,i) for i,t in enumerate((0,2,4))]
        edits=[lambda v:v[1].update(sequence=2),lambda v:v[1]['process']['identity'].update(start_ticks=124),
               lambda v:v[1].update(role='es'),lambda v:v[1].update(errors=['permission denied']),
               lambda v:v[1]['files'].pop('io.stat'),lambda v:v[1]['files'].update(**{'cpu.stat':'usage_usec 0\n'}),
               lambda v:v[1]['process'].update(status=v[1]['process']['status'].replace('kB','MB')),
               lambda v:v[1]['observer']['identity'].update(uid=65532),
               lambda v:v[1].update(monotonic_ns=v[0]['monotonic_ns']),
               lambda v:v[1].update(duration_ns=2000000001),
               lambda v:v[1]['files'].update(**{'io.stat':'8:0 rbytes=-1\n'})]
        for edit in edits:
            bad=copy.deepcopy(values);edit(bad)
            with self.assertRaises((ValueError,KeyError)):stream_report(bad,'client')
        with self.assertRaises(ValueError):stream_report([sample('client',0,0),sample('client',7,1)],'client')
        with self.assertRaises(ValueError):stream_report([], 'client')

    def test_counter_decrease_and_clock_domains(self):
        values=[sample('client',t,i) for i,t in enumerate((0,2,4))]
        # Independent observer epochs can be offset; no cross-process subtraction.
        for value in values:
            value['monotonic_ns']+=987000000000
            value['end_monotonic_ns']+=987000000000
        stream_report(values,'client')
        values[1]['time']=values[0]['time']
        with self.assertRaises(ValueError):stream_report(values,'client')
        values=[sample('client',t,i) for i,t in enumerate((0,2,4))]
        values[1]['files']['memory.events']=values[1]['files']['memory.events'].replace('high 0','high 1')
        with self.assertRaises(ValueError):stream_report(values,'client')

    def test_coverage_and_truncated_stream(self):
        values=[sample('client',t,i) for i,t in enumerate((0,2,4))]
        from capacity_report import timestamp
        start=timestamp(values[0]['time'])+.1;end=timestamp(values[-1]['time'])-.1
        coverage(values,start,end)
        with self.assertRaises(ValueError):coverage(values[1:],start,end)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'stream';path.write_text('{"role":"client"}')
            with self.assertRaises(ValueError):read_stream(path)
            self.assertEqual(report(directory)['resource_evidence'],'partial')


class CompleteReport(unittest.TestCase):
    def test_positive_and_complete_timing_no_go(self):
        for drops,slow in ((0,False),(1,False),(0,True)):
            with self.subTest(drops=drops,slow=slow), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);complete_fixture(root,drops,slow)
                result=report(root)
                self.assertEqual(result['resource_evidence'],'complete-for-declared-visible-leaf-profile',result)
                self.assertEqual(result['timing_qualification'],'NO-GO' if drops or slow else 'short-trial-pass-only')
                self.assertEqual(result['totals']['mutations'],2400-2*drops)
                self.assertEqual(result['totals']['planned'],6000)
                self.assertIsNone(result['candidate'])

    def test_full_report_rejects_each_corruption(self):
        def contradicted_ledger(stream):
            stream[-3]['ledger']=base64.b64encode(bytes(200)).decode()
            stream[-3]['audit'].update(applied=0,found_version1=0,absent=200)
        cases = [
            ('negative receipt','through',lambda s:s[-1].update(started=-1)),
            ('under receipt','through',lambda s:s[-1].update(started=1199)),
            ('ledger contradicts successful Put','through',contradicted_ledger),
            ('missing tcp','weir',lambda s:s[2]['files'].pop('net/tcp')),
            ('missing tcp6','es',lambda s:s[2]['files'].pop('net/tcp6')),
            ('malformed tcp','es',lambda s:s[2]['files'].update(**{'net/tcp':'broken'})),
            ('missing limits','weir',lambda s:s[2]['files'].pop('limits')),
            ('missing cgroup file','weir',lambda s:s[2]['files'].pop('memory.events')),
            ('missing standard metric','weir',lambda s:s[2].update(metrics=s[2]['metrics'].replace('go_goroutines 8\n',''))),
            ('missing role','es',lambda s:s.clear()),
            ('missing end','weir',lambda s:s.pop()),
            ('end role','es',lambda s:s[-1].update(role='weir')),
            ('wrong identity','weir',lambda s:s[0].update(goarch='amd64')),
            ('sample gap','weir',lambda s:s.__delitem__(slice(10,14))),
            ('wrong sequence','weir',lambda s:s[3].update(sequence=9)),
            ('short complete observer','es',lambda s:s.__delitem__(-2)),
            ('missing setup','through',lambda s:s.pop(17)),
            ('setup drift','through',lambda s:s[17].update(started=17)),
            ('missing client start','through',lambda s:s.pop(1000)),
            ('missing client end','through',lambda s:s.pop(-2)),
            ('extra trial','through',lambda s:s.insert(1002,copy.deepcopy(s[1001]))),
            ('missing histogram','through',lambda s:s[1001]['trial']['warm']['all'].pop('decision')),
            ('window drift','through',lambda s:s[1001]['trial']['ten_second_windows'].pop()),
            ('audit pages','through',lambda s:s[-3]['audit'].update(pages=1)),
            ('audit absent','through',lambda s:s[-3]['audit'].update(absent=1)),
            ('ledger misplaced warm drop','through',lambda s:s[-3].update(ledger=base64.b64encode(bytes([0]+[1]*199)).decode())),
            ('frozen prefix','through',lambda s:s[1001]['trial']['options'].update(Prefix='other')),
            ('missing frozen option','through',lambda s:s[1001]['trial']['options'].pop('TimingOnly')),
        ]
        for raw in ('{}','{"nodes":{}}','{"nodes":[]}','{"nodes":null}','{"nodes":1}',
                    '{"nodes":{"a":{},"b":{}}}','{"nodes":{"a":null}}','{"nodes":{"a":{}}}'):
            cases.append(('ES shape '+raw,'es',lambda s,raw=raw:s[2].update(db=raw)))
        for value in (True,-1,1.5,'1',None,float('nan'),float('inf')):
            cases += [
                ('receipt type '+str(value),'through',lambda s,v=value:s[-1].update(started=v)),
                ('setup type '+str(value),'through',lambda s,v=value:s[0].update(started=v)),
                ('window type '+str(value),'through',lambda s,v=value:s[1001]['trial']['warm']['put'].update(unknown=v)),
                ('histogram type '+str(value),'through',lambda s,v=value:s[1001]['trial']['warm']['put']['lag']['buckets'][0].update(count=v)),
                ('audit type '+str(value),'through',lambda s,v=value:s[-3]['audit'].update(unknown_found=v)),
                ('sequence type '+str(value),'weir',lambda s,v=value:s[2].update(sequence=v)),
            ]
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);original=complete_fixture(root)
            for label,name,edit in cases:
                with self.subTest(label=label):
                    values=copy.deepcopy(original[name]);edit(values)
                    target=root/(name+'.jsonl');target.write_text(''.join(json.dumps(v)+'\n' for v in values))
                    result=report(root)
                    self.assertEqual(result['resource_evidence'],'partial',result)
                    self.assertTrue(result['errors'])
                    target.write_text(''.join(json.dumps(v)+'\n' for v in original[name]))


if __name__=='__main__':unittest.main()
