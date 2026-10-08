//go:build integration

package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type process struct {
	command    *exec.Cmd
	address    string
	addresses  []string
	diagnostic string
	stderr     *bytes.Buffer
	done       chan error
	once       sync.Once
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		_ = p.command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-p.done:
			if err != nil {
				t.Errorf("Weir process exit: %v %s", err, p.stderr.String())
			}
		case <-time.After(8 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			t.Error("Weir process drain exceeded bound")
		}
	})
}

func startProcess(t *testing.T, binary string, cfg Config) *process {
	t.Helper()
	name := filepath.Join(t.TempDir(), "node.yaml")
	routingFilename := writeConfigFiles(t, name, cfg, 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	args := []string{"serve", "--config", name, "--routes", routingFilename}
	command := exec.CommandContext(ctx, binary, args...)
	return watchProcess(t, command, cfg.Basic.Diagnostics.Address != "")
}

func watchProcess(t *testing.T, command *exec.Cmd, diagnostics bool) *process {
	t.Helper()
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := new(bytes.Buffer)
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{command: command, stderr: stderr, done: make(chan error, 1)}
	line := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		if scanner.Scan() {
			message := scanner.Text()
			if diagnostics && scanner.Scan() {
				message += "\n" + scanner.Text()
			}
			line <- message
		} else {
			line <- ""
		}
	}()
	t.Cleanup(func() { p.stop(t) })
	select {
	case message := <-line:
		start, end := strings.Index(message, "["), strings.Index(message, "]")
		if start < 0 || end <= start {
			go func() { p.done <- command.Wait() }()
			t.Fatal("no process readiness", message)
		}
		p.addresses = strings.Fields(message[start+1 : end])
		p.address = p.addresses[0]
		if diagnostics {
			parts := strings.Split(message, "\n")
			if len(parts) != 2 {
				t.Fatal("missing diagnostic address")
			}
			p.diagnostic = strings.TrimPrefix(parts[1], "Diagnostics listening on ")
		}
	case <-time.After(10 * time.Second):
		go func() { p.done <- command.Wait() }()
		t.Fatal("process startup timeout")
	}
	go func() { p.done <- command.Wait() }()
	return p
}

type processSmokeOptions struct {
	client      pb.StoreServiceClient
	store, root string
	search      *testsearch.Backend
	initialized bool
}

func processDocument(t *testing.T, store, id string, n int32) *pb.Document {
	t.Helper()
	document := &pb.Document{ContentType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if store == "mongo" {
		record := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
		raw, err := bson.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		document.ContentType, document.Data = "application/bson", raw
	}
	return document
}

func processRecordNumber(t *testing.T, document *pb.Document) int32 {
	t.Helper()
	if document == nil {
		t.Fatal("record document missing")
	}
	var record struct {
		N int32 `json:"n" bson:"n"`
	}
	var err error
	if document.ContentType == "application/bson" {
		err = bson.Unmarshal(document.Data, &record)
	} else if document.ContentType == "application/json" {
		err = json.Unmarshal(document.Data, &record)
	} else {
		t.Fatal("unexpected record content type", document.ContentType)
	}
	if err != nil {
		t.Fatal(err)
	}
	return record.N
}

func processPublicSmoke(t *testing.T, opts processSmokeOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resource := opts.root + "/s:example"
	document := processDocument(t, opts.store, "example", 1)
	put := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: resource, Action: put}
	recordResult, err := testutil.ExecuteRecord(ctx, opts.client, testutil.RecordRequest(opts.store, mutation))
	result := recordResult.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil {
		t.Fatal("direct mutation", result, err)
	}
	read := &pb.ReadRequest{Resource: resource}
	recordResult2, err := testutil.ExecuteRecord(ctx, opts.client, testutil.RecordRequest(opts.store, read))
	found := recordResult2.GetReadResult()
	if err != nil || processRecordNumber(t, found.GetDocument()) != 1 {
		t.Fatal("direct read", found, err)
	}
	readBatch := []*pb.ReadRequest{read, read}
	reads, err := testutil.ReadRecords(ctx, opts.client, opts.store, readBatch)
	if err != nil || len(reads) != 2 {
		t.Fatal("batch read", reads, err)
	}
	for _, item := range reads {
		if item.GetFailure() != nil || processRecordNumber(t, item.GetDocument()) != 1 {
			t.Fatal("batch read changed record", item)
		}
	}
	requests := make([]*pb.MutateRequest, 0, 2)
	for i := range 2 {
		id := fmt.Sprintf("bulk-example-%d", i)
		document := processDocument(t, opts.store, id, int32(i+2))
		create := &pb.MutateRequest_Create{Create: document}
		mutation := &pb.MutateRequest{Resource: opts.root + "/s:" + id, Action: create}
		requests = append(requests, mutation)
	}
	mutations, err := testutil.MutateRecords(ctx, opts.client, opts.store, requests)
	if err != nil || len(mutations) != len(requests) {
		t.Fatal("batch create", mutations, err)
	}
	for i, item := range mutations {
		if item.GetOutcome() != pb.MutationOutcome_APPLIED || item.GetFailure() != nil {
			t.Fatal("batch create", i, item)
		}
	}
	verification := make([]*pb.ReadRequest, 0, len(requests))
	for _, item := range requests {
		request := &pb.ReadRequest{Resource: item.Resource}
		verification = append(verification, request)
	}
	verified, err := testutil.ReadRecords(ctx, opts.client, opts.store, verification)
	if err != nil || len(verified) != len(requests) {
		t.Fatal("batch create readback", verified, err)
	}
	for i, item := range verified {
		if item.GetFailure() != nil || processRecordNumber(t, item.GetDocument()) != int32(i+2) {
			t.Fatal("batch input order/readback", i, item)
		}
	}
	processNativeSmoke(t, ctx, opts)
	if opts.store == "search" {
		status, _ := opts.search.Do(t, "POST", "/"+opts.search.Index+"/_refresh", "")
		if status != http.StatusOK {
			t.Fatal("owned search index refresh", status)
		}
	}
	processScanSmoke(t, ctx, opts)
	t.Logf("three independent Weir processes, %s: batch reads and mutations, Native and Scan completed", opts.store)
}

func processNativeSmoke(t *testing.T, ctx context.Context, opts processSmokeOptions) {
	t.Helper()
	var request *pb.NativeRequest
	if opts.store == "mongo" {
		query := bson.D{{Key: "_id", Value: "example"}}
		command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
		body, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		variant := &pb.Document{ContentType: "application/bson", Data: body}
		request = &pb.NativeRequest{Resource: opts.root, Request: variant}
	} else {
		httpRequest, err := http.NewRequest(http.MethodGet, "http://ignored.invalid/_doc/example?realtime=true", nil)
		if err != nil {
			t.Fatal(err)
		}
		request, err = weirclient.NewHTTPNativeRequest(opts.root, httpRequest)
		if err != nil {
			t.Fatal(err)
		}
	}
	nativeCall := request
	nativeVariant := &pb.Command_Native{Native: nativeCall}
	call := &pb.Command{Operation: nativeVariant}
	stream, err := testutil.ExecuteEvents(ctx, opts.client, opts.store, call)
	if err != nil {
		t.Fatal(err)
	}
	var response []byte
	headSeen := false
	for {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatal("Native response", err)
		}
		if head := frame.GetHead(); head != nil {
			if headSeen || len(response) != 0 {
				t.Fatal("Native Head order or duplicate")
			}
			headSeen = true
			if opts.store == "search" {
				metadata, err := weirclient.ParseHTTPNativeResponse(head)
				if err != nil || metadata.StatusCode != http.StatusOK {
					t.Fatal("Native HTTP metadata", head)
				}
			}
		}
		if chunk := frame.GetChunk(); len(chunk) != 0 {
			if !headSeen {
				t.Fatal("Native body preceded Head")
			}
			response = append(response, chunk...)
		}
		if end := frame.GetNativeEnd(); end != nil {
			if !headSeen || end.Failure != nil || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
				t.Fatal("Native terminal evidence", end)
			}
			break
		}
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("Native final status", err)
	}
	if opts.store == "mongo" {
		raw := bson.Raw(response)
		if raw.Lookup("ok").AsInt64() != 1 || raw.Lookup("n").AsInt64() != 1 {
			t.Fatal("Native count response", raw)
		}
	} else {
		var record struct {
			ID     string            `json:"_id"`
			Found  bool              `json:"found"`
			Source struct{ N int32 } `json:"_source"`
		}
		if json.Unmarshal(response, &record) != nil || record.ID != "example" || !record.Found || record.Source.N != 1 {
			t.Fatal("Native realtime document response", string(response))
		}
	}
}

func processScanSmoke(t *testing.T, ctx context.Context, opts processSmokeOptions) {
	t.Helper()
	request := &pb.ScanRequest{Resource: opts.root}
	scanVariant := &pb.Command_Scan{Scan: request}
	scanCall := &pb.Command{Operation: scanVariant}
	stream, err := testutil.ExecuteEvents(ctx, opts.client, opts.store, scanCall)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int32]bool)
	values := map[int32]bool{1: true, 2: true, 3: true}
	if opts.initialized {
		values[7] = true
	}
	expectedDocuments := uint64(len(values))
	for {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatal("Scan response", err)
		}
		if end := frame.GetScanEnd(); end != nil {
			if end.Failure != nil || end.DocumentCount != expectedDocuments || len(seen) != int(expectedDocuments) {
				t.Fatal("Scan terminal evidence", end, seen)
			}
			break
		}
		document := frame.GetDocument()
		if document == nil {
			t.Fatal("unexpected Scan frame", frame)
		}
		var n int32
		if opts.store == "mongo" {
			raw := bson.Raw(document.Data)
			n = raw.Lookup("n").Int32()
		} else {
			var record struct{ N int32 }
			if json.Unmarshal(document.Data, &record) != nil {
				t.Fatal("invalid Scan document", string(document.Data))
			}
			n = record.N
		}
		if seen[n] || !values[n] {
			t.Fatal("Scan repeated or changed records", n)
		}
		seen[n] = true
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("Scan final status", err)
	}
}

func TestIndependentWeirProcesses(t *testing.T) {
	mongoFixture := testmongo.Open(t)
	search := testsearch.Open(t)
	binary := buildEndpointProcess(t)
	mongo := mongoFixtureConfig(t, mongoFixture.URI)
	mongoLocal := &Local{Backend: BackendConfig{MongoDB: mongo}}
	backend := &Search{URL: search.URL}
	// Match the Search fixture's single write thread for deterministic smoke operations.
	searchLocal := &Local{Backend: BackendConfig{Search: backend}}
	mongoStore := StoreConfig{Name: "mongo", Local: mongoLocal}
	searchStore := StoreConfig{Name: "search", Local: searchLocal}
	cfg := emptyConfig(t)
	cfg.Basic.Diagnostics.Address, cfg.Basic.Listeners.Peer = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Basic.Discovery.Group = "data"
	cfg.Routing.Stores = []StoreConfig{mongoStore, searchStore}
	owner := startProcess(t, binary, cfg)
	middleConfig := emptyConfig(t)
	middleConfig.Basic.Diagnostics.Address, middleConfig.Basic.Listeners.Peer = "127.0.0.1:0", "127.0.0.1:0"
	middleConfig.Basic.Discovery.Seeds = []string{owner.addresses[1]}
	middle := startProcess(t, binary, middleConfig)
	firstConfig := emptyConfig(t)
	firstConfig.Basic.Diagnostics.Address, firstConfig.Basic.Listeners.Peer = "127.0.0.1:0", "127.0.0.1:0"
	firstConfig.Basic.Discovery.Seeds = []string{middle.addresses[1]}
	first := startProcess(t, binary, firstConfig)
	discovered := openDiscoveredClient(t, first.address, []string{"mongo", "search"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rawSeed := endpointProcessClient(t, first.address)
	for _, kind := range []string{"mongo", "search"} {
		root := mongoFixture.DB + "/records"
		if kind == "search" {
			root = search.Index
		}
		resolveRequest := &pb.ResolveStoreRequest{StoreName: kind}
		response, err := rawSeed.ResolveStore(ctx, resolveRequest)
		if err != nil || len(response.Endpoints) != 1 || response.Endpoints[0] != owner.address {
			t.Fatal("nonowner did not learn final business target", response, err)
		}
		doc := processDocument(t, kind, "initialized", 7)
		put := &pb.MutateRequest_Put{Put: doc}
		mutation := &pb.MutateRequest{Resource: root + "/s:initialized", Action: put}
		relative := mutation.Resource
		writeRequest := &weirclient.WriteRequest{Resource: relative, Document: doc}
		writeOptions := weirclient.WriteOptions{StoreName: kind, Request: writeRequest}
		applied, err := discovered.Put(ctx, writeOptions)
		if err != nil || applied.GetOutcome() != weirclient.MutationApplied {
			t.Fatal("initialized client direct mutation", applied, err)
		}
		read := &pb.ReadRequest{Resource: mutation.Resource}
		readRequest := &weirclient.ReadRequest{Resource: relative}
		readOptions := weirclient.ReadOneOptions{StoreName: kind, Request: readRequest}
		found, err := discovered.ReadOne(ctx, readOptions)
		if err != nil || processRecordNumber(t, found.Document) != 7 {
			t.Fatal("initialized client persisted read", found, err)
		}
		_, err = testutil.ExecuteRecord(ctx, rawSeed, testutil.RecordRequest(kind, read))
		if status.Code(err) != codes.Unavailable {
			t.Fatal("nonowner business request was not rejected", err)
		}
		direct := endpointProcessClient(t, owner.address)
		options := processSmokeOptions{client: direct, store: kind, root: root, search: search, initialized: true}
		processPublicSmoke(t, options)
	}
	for _, p := range []*process{first, middle, owner} {
		metrics := testmetrics.Scrape(t, p.diagnostic)
		if testmetrics.Sum(metrics, "weir_node_ready") != 1 {
			t.Fatal("process not ready")
		}
		if p != owner {
			if metrics["weir_store_executions_total"] != nil {
				t.Fatal("directory node executed business")
			}
		} else {
			// Each of the two Stores completes 2 SDK records, 4 direct/read-batch
			// records, 2 creates and 2 readbacks. Native and Scan are commands.
			const expectedRecords = 20
			records := testmetrics.Sum(metrics, "weir_store_records_total")
			if records != expectedRecords {
				t.Fatal("owner business record count changed", records)
			}
		}
	}
	t.Logf("independent processes A=%d B=%d C=%d: A learned both Store targets via B; initialized SDK persisted direct writes on C; nonowner rejects Route and has no business runtime", first.command.Process.Pid, middle.command.Process.Pid, owner.command.Process.Pid)
}

func TestDiagnosticProcessSIGTERMReadinessBeforeExit(t *testing.T) {
	testmongo.Open(t)
	binary := filepath.Join(t.TempDir(), "weir")
	buildCtx, stopBuild := context.WithTimeout(context.Background(), 30*time.Second)
	command := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, "./cmd/weir")
	command.Dir = testutil.Root(t)
	output, err := command.CombinedOutput()
	stopBuild()
	if err != nil {
		t.Fatal(err, string(output))
	}
	cfg := emptyConfig(t)
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	cfg.Basic.Transport.Timeouts.Stall = Duration(time.Second)
	p := startProcess(t, binary, cfg)
	conn, err := grpc.NewClient("passthrough:///"+p.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	desc := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
	_, err = conn.NewStream(ctx, desc, pb.StoreService_Execute_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_ingress_sessions") != 1 {
		if time.Now().After(until) {
			t.Fatal("process input slot not occupied")
		}
		time.Sleep(time.Millisecond)
	}
	started := time.Now()
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 200 * time.Millisecond}
	observed := false
	for time.Since(started) < 500*time.Millisecond {
		response, err := client.Get("http://" + p.diagnostic + "/readyz")
		if err != nil {
			t.Fatal("diagnostics closed before drain observation", err)
		}
		_ = response.Body.Close()
		if response.StatusCode == 503 {
			observed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !observed {
		t.Fatal("SIGTERM did not lower readiness")
	}
	families := testmetrics.Scrape(t, p.diagnostic)
	if testmetrics.Sample(families, "weir_node_state", map[string]string{"state": "draining"}).GetGauge().GetValue() != 1 || testmetrics.Sum(families, "weir_node_drains_total") != 1 {
		t.Fatal("drain metrics")
	}
	response, err := client.Get("http://" + p.diagnostic + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("drain liveness")
	}
	p.stop(t)
	if time.Since(started) > 3*time.Second {
		t.Fatal("process exit exceeded bound")
	}
	t.Logf("PID=%d: SIGTERM readiness=503 while livez=200; bounded exit in %s", p.command.Process.Pid, time.Since(started))
}
