#!/usr/bin/env python3
"""Prepare trace-only overlays. Does not run services or change base sources."""
import hashlib
import json
import pathlib
import subprocess

BASE = pathlib.Path('/Users/liran/.codex/worktrees/native-burst-admission/weir')
PRIMARY = pathlib.Path('/Users/liran/Projects/liran/go/weir')
OUT = PRIMARY / '.testdata/reliability-2026-10-16'
GRPC_ORIGINAL = pathlib.Path('/Users/liran/go/pkg/mod/google.golang.org/grpc@v1.83.2')
GRPC = OUT / 'ingress-DIAGNOSTIC-grpc-v1.83.2'
REVISION = '0288974d8b64c199da4f0d7fb7f03d8a33978eee'
REPLACEMENTS = {}
SOURCES = []

def digest(data):
    return hashlib.sha256(data).hexdigest()

def change_once(text, old, new):
    assert text.count(old) == 1, (old[:100], text.count(old))
    return text.replace(old, new, 1)

def add_import(text, package):
    assert '\t"' + package + '"\n' not in text
    return change_once(text, 'import (\n', 'import (\n\t"' + package + '"\n')

def save(target, label, text):
    original = target.read_bytes()
    destination = OUT / ('ingress-DIAGNOSTIC-' + label + '.go.txt')
    destination.write_text(text)
    REPLACEMENTS[str(target)] = str(destination)
    SOURCES.append({'target': str(target), 'original_sha256': digest(original), 'overlay': str(destination)})

assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=BASE, text=True).strip() == REVISION
assert subprocess.check_output(['git', 'status', '--porcelain'], cwd=BASE, text=True) == ''
assert digest((PRIMARY / 'internal/app/ingress_memory_budget_integration_test.go').read_bytes()) == '941d8dbb37b1449dfdb0a8f6c33e5402eafc70b6ca97ab2c100b8c8f8df35c07'

# Go1.27 rejects overlays in GOMODCACHE. A diagnostic modfile redirects only
# this same grpc version to exact public Go-source copies outside the cache.
copied_sources = {}
for source in sorted(GRPC_ORIGINAL.rglob('*.go')) + [GRPC_ORIGINAL / 'go.mod', GRPC_ORIGINAL / 'go.sum']:
    relative = source.relative_to(GRPC_ORIGINAL)
    destination = GRPC / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    data = source.read_bytes()
    if destination.exists():
        assert destination.read_bytes() == data
    else:
        destination.write_bytes(data)
    copied_sources[str(relative)] = digest(data)
modfile = OUT / 'ingress-DIAGNOSTIC.mod'
sumfile = OUT / 'ingress-DIAGNOSTIC.sum'
original_mod = (BASE / 'go.mod').read_bytes()
original_sum = (BASE / 'go.sum').read_bytes()
modfile.write_bytes(original_mod + ('\nreplace google.golang.org/grpc v1.83.2 => ' + str(GRPC) + '\n').encode())
sumfile.write_bytes(original_sum)
copy_manifest = {
    'original': str(GRPC_ORIGINAL), 'copy': str(GRPC),
    'copied_go_mod_sum_sha256': copied_sources,
    'scope': 'Only public .go sources and module go.mod/go.sum; no secrets/certificates/env files read or copied; no go:embed directives exist in grpc Go sources.',
    'all_copies_equal_original': True,
    'modfile': str(modfile), 'modfile_sha256': digest(modfile.read_bytes()),
    'base_mod_sha256': digest(original_mod),
    'modfile_only_delta': 'Append a same-version grpc v1.83.2 replace to the exact public source copy; require versions unchanged.',
    'sumfile': str(sumfile), 'sumfile_sha256': digest(sumfile.read_bytes()),
    'sumfile_equals_base': sumfile.read_bytes() == original_sum,
}
(OUT / 'ingress-DIAGNOSTIC-grpc-copy-manifest.json').write_text(json.dumps(copy_manifest, indent=2) + '\n')

# Trace native stream reads without changing the requested byte count, buffers,
# flow-control calls, or return values. ID/peer methods expose observations only.
target = GRPC / 'internal/transport/transport.go'
text = add_import(add_import(target.read_text(), 'log'), 'encoding/binary')
text = change_once(text, 'func (s *Stream) ReadMessageHeader(header []byte) (err error) {\n', '''func (s *Stream) ReadMessageHeader(header []byte) (err error) {
	diagnosticHeader := header
	_, diagnosticServer := s.readRequester.(*ServerStream)
	diagnosticStarted := time.Now()
	defer func() {
		if diagnosticServer && len(diagnosticHeader) == 5 {
			declared := uint32(0)
			if err == nil {
				declared = binary.BigEndian.Uint32(diagnosticHeader[1:])
			}
			log.Printf("INGRESS_DIAGNOSTIC phase=server_header_done at=%s remote=%s stream_id=%d declared_bytes=%d elapsed_ns=%d error_type=%T", time.Now().UTC().Format(time.RFC3339Nano), s.IngressDiagnosticRemoteAddr(), s.id, declared, time.Since(diagnosticStarted).Nanoseconds(), err)
		}
	}()
''')
text = change_once(text, 'func (s *Stream) read(n int) (data mem.BufferSlice, err error) {\n', '''func (s *Stream) read(n int) (data mem.BufferSlice, err error) {
	diagnosticRequested := n
	diagnosticNext := 4 << 20
	diagnosticStarted := time.Now()
	_, diagnosticServer := s.readRequester.(*ServerStream)
	if diagnosticServer {
		log.Printf("INGRESS_DIAGNOSTIC phase=server_body_begin at=%s remote=%s stream_id=%d requested_bytes=%d", diagnosticStarted.UTC().Format(time.RFC3339Nano), s.IngressDiagnosticRemoteAddr(), s.id, diagnosticRequested)
		defer func() {
			log.Printf("INGRESS_DIAGNOSTIC phase=server_body_done at=%s remote=%s stream_id=%d requested_bytes=%d received_bytes=%d elapsed_ns=%d error_type=%T", time.Now().UTC().Format(time.RFC3339Nano), s.IngressDiagnosticRemoteAddr(), s.id, diagnosticRequested, diagnosticRequested-n, time.Since(diagnosticStarted).Nanoseconds(), err)
		}()
	}
''')
text = change_once(text, '\t\tn -= bufLen\n\t\tif n == 0 {', '''		n -= bufLen
		if diagnosticServer && diagnosticRequested-n >= diagnosticNext {
			log.Printf("INGRESS_DIAGNOSTIC phase=server_body_progress at=%s remote=%s stream_id=%d requested_bytes=%d received_bytes=%d", time.Now().UTC().Format(time.RFC3339Nano), s.IngressDiagnosticRemoteAddr(), s.id, diagnosticRequested, diagnosticRequested-n)
			diagnosticNext += 4 << 20
		}
		if n == 0 {''')
text += '''
// Diagnostic-only observations; not a supported grpc transport extension.
func (s *Stream) IngressDiagnosticID() uint32 { return s.id }

func (s *Stream) IngressDiagnosticRemoteAddr() string {
	if remote, ok := peer.FromContext(s.ctx); ok && remote.Addr != nil {
		return remote.Addr.String()
	}
	return "unknown"
}
'''
save(target, 'grpc-transport', text)

# Preserve the original standard proto codec implementation and allocations.
# CodecV2 has no stream/context: stream_id=0 is explicitly unknown here.
target = GRPC / 'encoding/proto/proto.go'
text = add_import(add_import(target.read_text(), 'log'), 'time')
text = change_once(text, 'func (c *codecV2) Marshal(v any) (data mem.BufferSlice, err error) {\n', '''func (c *codecV2) Marshal(v any) (data mem.BufferSlice, err error) {
	diagnosticStarted := time.Now()
	log.Printf("INGRESS_DIAGNOSTIC phase=standard_proto_marshal_begin at=%s stream_id=0", diagnosticStarted.UTC().Format(time.RFC3339Nano))
	defer func() {
		log.Printf("INGRESS_DIAGNOSTIC phase=standard_proto_marshal_done at=%s stream_id=0 bytes=%d elapsed_ns=%d error_type=%T", time.Now().UTC().Format(time.RFC3339Nano), data.Len(), time.Since(diagnosticStarted).Nanoseconds(), err)
	}()
''')
save(target, 'grpc-proto', text)

# Per-stream SendMsg surrounds the exact existing prepareMsg call. A brief
# existing stream mutex is used only to snapshot attempt identity safely.
target = GRPC / 'stream.go'
text = add_import(target.read_text(), 'log')
text = change_once(text, '\t// load hdr, payload, data\n\thdr, data, payload, pf, err := prepareMsg(m, cs.codec, cs.compressorV0, cs.compressorV1, cs.cc.dopts.copts.BufferPool)\n', '''	diagnosticID := uint32(0)
	diagnosticRemote := "unknown"
	cs.mu.Lock()
	if cs.attempt != nil && cs.attempt.transportStream != nil {
		diagnosticID = cs.attempt.transportStream.IngressDiagnosticID()
		diagnosticRemote = cs.attempt.transportStream.IngressDiagnosticRemoteAddr()
	}
	cs.mu.Unlock()
	diagnosticStarted := time.Now()
	log.Printf("INGRESS_DIAGNOSTIC phase=client_prepare_begin at=%s remote=%s stream_id=%d", diagnosticStarted.UTC().Format(time.RFC3339Nano), diagnosticRemote, diagnosticID)
	// load hdr, payload, data
	hdr, data, payload, pf, err := prepareMsg(m, cs.codec, cs.compressorV0, cs.compressorV1, cs.cc.dopts.copts.BufferPool)
	log.Printf("INGRESS_DIAGNOSTIC phase=client_prepare_done at=%s remote=%s stream_id=%d bytes=%d elapsed_ns=%d error_type=%T", time.Now().UTC().Format(time.RFC3339Nano), diagnosticRemote, diagnosticID, data.Len(), time.Since(diagnosticStarted).Nanoseconds(), err)
''')
text = change_once(text, '\tif a.statsHandler != nil {\n\t\ta.statsHandler.HandleRPC(a.ctx, outPayload(true, m, dataLength, payloadLength, time.Now()))\n\t}', '''	log.Printf("INGRESS_DIAGNOSTIC phase=client_OutPayload_point at=%s remote=%s stream_id=%d bytes=%d payload_bytes=%d", time.Now().UTC().Format(time.RFC3339Nano), a.transportStream.IngressDiagnosticRemoteAddr(), a.transportStream.IngressDiagnosticID(), dataLength, payloadLength)
	if a.statsHandler != nil {
		a.statsHandler.HandleRPC(a.ctx, outPayload(true, m, dataLength, payloadLength, time.Now()))
	}''')
save(target, 'grpc-stream', text)

target = GRPC / 'internal/transport/http2_client.go'
text = add_import(target.read_text(), 'log')
text = change_once(text, '\tif s.statsHandler != nil {\n\t\theader, ok := metadata.FromOutgoingContext(ctx)', '''	log.Printf("INGRESS_DIAGNOSTIC phase=client_OutHeader_point at=%s remote=%s stream_id=%d", time.Now().UTC().Format(time.RFC3339Nano), t.remoteAddr.String(), s.id)
	if s.statsHandler != nil {
		header, ok := metadata.FromOutgoingContext(ctx)''')
save(target, 'grpc-http2-client', text)

# Weir decode and admission observations follow the real existing path.
target = BASE / 'internal/server/codec.go'
text = add_import(target.read_text(), 'log')
text = change_once(text, 'func (*responseCodec) Unmarshal(data mem.BufferSlice, value any) error {\n', '''func (*responseCodec) Unmarshal(data mem.BufferSlice, value any) (diagnosticErr error) {
	diagnosticStarted := time.Now()
	log.Printf("INGRESS_DIAGNOSTIC phase=weir_codec_begin at=%s stream_id=0 bytes=%d", diagnosticStarted.UTC().Format(time.RFC3339Nano), data.Len())
	defer func() {
		log.Printf("INGRESS_DIAGNOSTIC phase=weir_codec_done at=%s stream_id=0 bytes=%d elapsed_ns=%d error_type=%T", time.Now().UTC().Format(time.RFC3339Nano), data.Len(), time.Since(diagnosticStarted).Nanoseconds(), diagnosticErr)
	}()
''')
save(target, 'weir-codec', text)

target = BASE / 'internal/store/runtime.go'
text = add_import(target.read_text(), 'log')
text = change_once(text, 'func (r *Runtime) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {\n', '''func (r *Runtime) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	log.Printf("INGRESS_DIAGNOSTIC phase=weir_prepare_record at=%s stream_id=0 bytes=%d", time.Now().UTC().Format(time.RFC3339Nano), record.Bytes())
''')
text = change_once(text, 'func (r *Runtime) Submit(ctx context.Context, plan *execution.Plan, session *Session) (*Ticket, *pb.Failure, <-chan struct{}) {\n', '''func (r *Runtime) Submit(ctx context.Context, plan *execution.Plan, session *Session) (*Ticket, *pb.Failure, <-chan struct{}) {
	diagnosticBytes := 0
	if plan != nil {
		diagnosticBytes = plan.Bytes
	}
	log.Printf("INGRESS_DIAGNOSTIC phase=weir_submit at=%s stream_id=0 bytes=%d", time.Now().UTC().Format(time.RFC3339Nano), diagnosticBytes)
''')
save(target, 'weir-runtime', text)

target = BASE / 'cmd/weir/version.go'
text = target.read_text()
text = change_once(text, '\treturn json.NewEncoder(output).Encode(identity)\n', '''	identity.Version = "DIAGNOSTIC-ingress-stages"
	identity.Revision = "0288974d8b64c199da4f0d7fb7f03d8a33978eee"
	identity.State = "DIAGNOSTIC-overlay"
	identity.Dirty = "true"
	return json.NewEncoder(output).Encode(identity)
''')
save(target, 'weir-version', text)

target = BASE / 'cmd/weir/main.go'
text = add_import(add_import(add_import(target.read_text(), 'log'), 'runtime/metrics'), 'strings')
text = change_once(text, 'func serve(ctx context.Context, cfg app.Config, output io.Writer) (resultErr error) {\n', '''func serve(ctx context.Context, cfg app.Config, output io.Writer) (resultErr error) {
	diagnosticContext, diagnosticCancel := context.WithCancel(ctx)
	diagnosticDone := make(chan struct{})
	go func() {
		defer close(diagnosticDone)
		ingressDiagnosticRuntime(diagnosticContext)
	}()
	defer func() {
		diagnosticCancel()
		<-diagnosticDone
	}()
''')
text += '''
// Diagnostic runtime counters do not request a GC or change the GC policy.
func ingressDiagnosticRuntime(ctx context.Context) {
	names := []string{
		"/cpu/classes/gc/mark/assist:cpu-seconds",
		"/cpu/classes/gc/mark/dedicated:cpu-seconds",
		"/cpu/classes/gc/mark/idle:cpu-seconds",
		"/cpu/classes/gc/pause:cpu-seconds",
		"/cpu/classes/gc/total:cpu-seconds",
		"/cpu/classes/user:cpu-seconds",
		"/cpu/classes/total:cpu-seconds",
		"/gc/cycles/total:gc-cycles",
		"/gc/gomemlimit:bytes",
		"/gc/heap/goal:bytes",
		"/gc/heap/live:bytes",
		"/memory/classes/heap/objects:bytes",
		"/sched/gomaxprocs:threads",
	}
	samples := make([]metrics.Sample, len(names))
	for index, name := range names {
		samples[index].Name = name
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		metrics.Read(samples)
		var line strings.Builder
		fmt.Fprintf(&line, "INGRESS_DIAGNOSTIC phase=cli_runtime at=%s stream_id=0", time.Now().UTC().Format(time.RFC3339Nano))
		for _, sample := range samples {
			switch sample.Value.Kind() {
			case metrics.KindUint64:
				fmt.Fprintf(&line, " %s=%d", sample.Name, sample.Value.Uint64())
			case metrics.KindFloat64:
				fmt.Fprintf(&line, " %s=%.9f", sample.Name, sample.Value.Float64())
			default:
				fmt.Fprintf(&line, " %s=unsupported", sample.Name)
			}
		}
		log.Print(line.String())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
'''
save(target, 'weir-main', text)

overlay = OUT / 'ingress-DIAGNOSTIC-server-module-overlay.json'
overlay.write_text(json.dumps({'Replace': REPLACEMENTS}, indent=2) + '\n')
provenance = {
    'purpose': 'DIAGNOSTIC ONLY; not production qualification or a production fix',
    'base_revision': REVISION,
    'driver_original_sha256': '941d8dbb37b1449dfdb0a8f6c33e5402eafc70b6ca97ab2c100b8c8f8df35c07',
    'original_A_raw_sha256': 'e9d7d060cbcaed89b1d7b74474aaf575db4489f17befe9dc4f9efe44ea0b23d2',
    'overlay': str(overlay),
    'modfile': str(modfile),
    'grpc_copy_manifest': str(OUT / 'ingress-DIAGNOSTIC-grpc-copy-manifest.json'),
    'sources': SOURCES,
    'expected_version': {'version': 'DIAGNOSTIC-ingress-stages', 'revision': REVISION, 'state': 'DIAGNOSTIC-overlay', 'dirty': 'true', 'target': 'linux/arm64'},
    'semantics': [
        'Original public SDK, validations, standard protobuf codec, MarshalAppend, read byte counts, frame buffers, and flow-control calls remain unchanged.',
        'Standard proto codec and Weir codec have no context; stream_id=0 explicitly means unknown, not a particular stream.',
        'Client prepare begin/end, transport OutHeader and OutPayload points correlate real stream IDs; output payload point means queued, not socket completion.',
        'Server native ReadMessageHeader logs declared length after actual five-byte read; body progress is actual bytes returned by native stream reader, every four MiB.',
        'Remote addresses are transport peer addresses only; no metadata, resource names, documents, request bodies, credentials or error messages are logged.',
        'CLI runtime/metrics is sampled every250ms for heap goal/live/limit and GC assist/dedicated/idle CPU; no forced GC or changed memory policy.',
        'Logs, timestamps, one short identity mutex snapshot and runtime sampling can perturb scheduling and performance; results are diagnostic only.',
        'Only512MiB/2CPU/32x12MiB/common5s diagnostic profile is authorized; no1024/B/Lua/formal load is started by this preparation.',
    ],
}
(OUT / 'ingress-DIAGNOSTIC-server-module-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(json.dumps({'overlay': str(overlay), 'files': len(SOURCES), 'provenance': str(OUT / 'ingress-DIAGNOSTIC-server-module-provenance.json')}))
