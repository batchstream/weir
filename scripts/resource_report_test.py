import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from capacity_report_test import sample as old_sample
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
    value['files'].update(status=status,stat=stat,cgroup='0::/\n',**{'io.stat':''})
    if role=='weir':value['metrics']+='\ngo_goroutines 8\ngo_memstats_heap_alloc_bytes 1024\nprocess_resident_memory_bytes 43008\nprocess_cpu_seconds_total 1.2\nprocess_open_fds 3\n'
    return value


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


if __name__=='__main__':unittest.main()
