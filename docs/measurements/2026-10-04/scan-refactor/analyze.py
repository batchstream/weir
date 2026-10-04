from pathlib import Path
import collections,hashlib,json,statistics,sys
root=Path(__file__).parent
log=Path(sys.argv[1]) if len(sys.argv)>1 else root/'timed.log'
rows=[]
for line in log.read_text().splitlines():
    if line.startswith('WEIR_SCAN_DIAGNOSTIC '):
        rows.append(json.loads(line[len('WEIR_SCAN_DIAGNOSTIC '):]))
assert len(rows)==24,len(rows)
groups=collections.defaultdict(list)
for row in rows:
    assert row['documents']==1024,row
    assert row['rpcs']==5,row
    assert row['elapsed_ns']>0,row
    counts=row['native_observation']['commands']
    name='find' if row['backend']=='mongodb' else 'POST /_search'
    want=1025 if row['variant']=='baseline' else 9
    assert counts[name]==want,(row,want)
    groups[(row['backend'],row['variant'])].append(row)
summary=[]
for backend in ['mongodb','search']:
    for variant in ['baseline','current']:
        samples=groups[(backend,variant)]
        assert len(samples)==6,(backend,variant,len(samples))
        values=[r['documents_per_second'] for r in samples]
        elapsed=[r['elapsed_ns']/1e6 for r in samples]
        summary.append({'backend':backend,'variant':variant,'samples':len(samples),'median_documents_per_second':statistics.median(values),'min_documents_per_second':min(values),'max_documents_per_second':max(values),'median_traversal_ms':statistics.median(elapsed),'backend_queries_per_traversal':1025 if variant=='baseline' else 9})
comparisons=[]
for backend in ['mongodb','search']:
    before=next(x for x in summary if x['backend']==backend and x['variant']=='baseline')
    after=next(x for x in summary if x['backend']==backend and x['variant']=='current')
    ratio=after['median_documents_per_second']/before['median_documents_per_second']
    comparisons.append({'backend':backend,'ratio':ratio,'increase_percent':(ratio-1)*100,'native_query_reduction_percent':(1-9/1025)*100})
receipts={}
archived_receipts=json.loads((root/'build-receipts.json').read_text())
for variant in ['baseline','current']:
    binary=root/(variant+'.test')
    digest=archived_receipts[variant]['sha256']
    if binary.exists():
        assert hashlib.sha256(binary.read_bytes()).hexdigest()==digest,variant
    receipts[variant]={'binary_sha256':digest}
result={'method':{'documents':1024,'pad_bytes_per_document':1024,'page_size':256,'client_concurrency':1,'store_concurrency':1,'pool':2,'gomaxprocs':4,'warmup_traversals_per_process':1,'timed_traversals_per_process':3,'order_per_backend':['baseline','current','current','baseline'],'same_fixture_per_backend':True,'measurement':'public Execute Scan microdiagnostic through command-observation loopback proxies; not database saturation capacity'},'source':json.loads((root/'source.json').read_text()),'receipts':receipts,'summary':summary,'comparisons':comparisons,'samples':rows}
(root/'report.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'summary':summary,'comparisons':comparisons},indent=2))
