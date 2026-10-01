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

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	searchbackend "github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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
	client      pb.WeirClient
	store, root string
	search      *testsearch.Backend
}

func processDocument(t *testing.T, store, id string, n int32) *pb.Document {
	t.Helper()
	document := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if store == "mongo" {
		record := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
		raw, err := bson.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		document.MediaType, document.Data = "application/bson", raw
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
	if document.MediaType == "application/bson" {
		err = bson.Unmarshal(document.Data, &record)
	} else if document.MediaType == "application/json" {
		err = json.Unmarshal(document.Data, &record)
	} else {
		t.Fatal("unexpected record media type", document.MediaType)
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
	result, err := opts.client.Mutate(ctx, mutation)
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil {
		t.Fatal("three-hop mutation", result, err)
	}
	read := &pb.ReadRequest{Resource: resource}
	found, err := opts.client.Read(ctx, read)
	if err != nil || processRecordNumber(t, found.GetDocument()) != 1 {
		t.Fatal("three-hop read", found, err)
	}
	bulk, err := opts.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://" + opts.store}
	opening := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: opening}
	if err := bulk.Send(frame); err != nil {
		t.Fatal(err)
	}
	readVariant := &pb.BulkOperation_Read{Read: read}
	readOperation := &pb.BulkOperation{Index: 0, Operation: readVariant}
	document = processDocument(t, opts.store, "bulk-example", 2)
	create := &pb.MutateRequest_Create{Create: document}
	mutation = &pb.MutateRequest{Resource: opts.root + "/s:bulk-example", Action: create}
	mutationVariant := &pb.BulkOperation_Mutate{Mutate: mutation}
	mutationOperation := &pb.BulkOperation{Index: 1, Operation: mutationVariant}
	for _, operation := range []*pb.BulkOperation{readOperation, mutationOperation} {
		item := &pb.BulkRequestFrame_Operation{Operation: operation}
		frame = &pb.BulkRequestFrame{Frame: item}
		if err := bulk.Send(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := bulk.CloseSend(); err != nil {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for {
		frame, err := bulk.Recv()
		if err != nil {
			t.Fatal("three-hop Bulk response", err)
		}
		if end := frame.GetEnd(); end != nil {
			if end.ReceivedCount != 2 || end.ResultCount != 2 || len(seen) != 2 {
				t.Fatal("Bulk terminal accounting", end, seen)
			}
			break
		}
		result := frame.GetResult()
		if result == nil || seen[result.Index] {
			t.Fatal("duplicate or invalid Bulk result", frame)
		}
		seen[result.Index] = true
		switch result.Index {
		case 0:
			if processRecordNumber(t, result.GetRead().GetDocument()) != 1 {
				t.Fatal("Bulk read changed record", result)
			}
		case 1:
			if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
				t.Fatal("Bulk create", result)
			}
		default:
			t.Fatal("unexpected Bulk result index", result)
		}
	}
	if _, err := bulk.Recv(); err != io.EOF {
		t.Fatal("Bulk final status", err)
	}
	processNativeSmoke(t, ctx, opts)
	if opts.store == "search" {
		status, _ := opts.search.Do(t, "POST", "/"+opts.search.Index+"/_refresh", "")
		if status != http.StatusOK {
			t.Fatal("owned search index refresh", status)
		}
	}
	processScanSmoke(t, ctx, opts)
	t.Logf("three independent Weir processes, %s: Read, Mutate, mixed Bulk, Native and Scan completed", opts.store)
}

func processNativeSmoke(t *testing.T, ctx context.Context, opts processSmokeOptions) {
	t.Helper()
	stream, err := opts.client.Native(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
	open := &pb.NativeOpen{Resource: opts.root, Descriptor_: descriptor, BodyMediaType: "application/bson"}
	var body []byte
	if opts.store == "mongo" {
		query := bson.D{{Key: "_id", Value: "example"}}
		command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
		body, err = bson.Marshal(command)
	} else {
		request := &spb.Request{Method: "GET", Path: "/_doc/example", Query: "realtime=true"}
		descriptor.Data, err = proto.Marshal(request)
		descriptor.MediaType = searchbackend.NativeDescriptor
		open.BodyMediaType = ""
	}
	if err != nil {
		t.Fatal(err)
	}
	opening := &pb.NativeRequestFrame_Open{Open: open}
	frame := &pb.NativeRequestFrame{Frame: opening}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	if len(body) != 0 {
		chunk := &pb.NativeRequestFrame_Chunk{Chunk: body}
		frame = &pb.NativeRequestFrame{Frame: chunk}
		if err := stream.Send(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
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
				metadata := &spb.Response{}
				if proto.Unmarshal(head.GetMetadata().GetData(), metadata) != nil || metadata.StatusCode != http.StatusOK {
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
		if end := frame.GetEnd(); end != nil {
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
	request := &pb.ScanRequest{Resource: opts.root, FetchItemsHint: 1}
	stream, err := opts.client.Scan(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatal("Scan response", err)
		}
		if end := frame.GetEnd(); end != nil {
			if end.Failure != nil || end.DocumentCount != 2 || len(seen) != 2 {
				t.Fatal("Scan terminal evidence", end, seen)
			}
			break
		}
		document := frame.GetDocument()
		if document == nil {
			t.Fatal("unexpected Scan frame", frame)
		}
		var id string
		var n int32
		if opts.store == "mongo" {
			raw := bson.Raw(document.Data)
			id, n = raw.Lookup("_id").StringValue(), raw.Lookup("n").Int32()
		} else {
			var hit struct {
				ID     string            `json:"_id"`
				Source struct{ N int32 } `json:"_source"`
			}
			if json.Unmarshal(document.Data, &hit) != nil {
				t.Fatal("invalid Scan hit", string(document.Data))
			}
			id, n = hit.ID, hit.Source.N
		}
		if seen[id] || id != "example" && id != "bulk-example" || id == "example" && n != 1 || id == "bulk-example" && n != 2 {
			t.Fatal("Scan repeated or changed records", id, n)
		}
		seen[id] = true
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("Scan final status", err)
	}
}

func TestIndependentWeirProcesses(t *testing.T) {
	// testmongo.Open gates the entire smoke before starting child processes.
	mongoFixture := testmongo.Open(t)
	database := mongoFixture.DB
	search := testsearch.Open(t)
	binary := filepath.Join(t.TempDir(), "weir")
	buildCtx, stopBuild := context.WithTimeout(context.Background(), 30*time.Second)
	command := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, "./cmd/weir")
	command.Dir = testutil.Root(t)
	output, err := command.CombinedOutput()
	stopBuild()
	if err != nil {
		t.Fatalf("build weir: %v %s", err, output)
	}
	mongo := mongoFixtureConfig(t, mongoFixture.URI)
	mongoLocal := &Local{MongoDB: mongo}
	backend := &Search{URL: search.URL}
	searchLocal := &Local{Search: backend}
	mongoService := Service{Name: "mongo", Local: mongoLocal}
	searchService := Service{Name: "search", Local: searchLocal}
	mongoRoute := Route{Store: "mongo", Service: "mongo"}
	searchRoute := Route{Store: "search", Service: "search"}
	c := DefaultConfig()
	c.Basic.Diagnostics.Address = "127.0.0.1:0"
	c.Basic.Listeners.Peer = "127.0.0.1:0"
	c.Routing.Services = []Service{mongoService, searchService}
	c.Routing.Routes = []Route{mongoRoute, searchRoute}
	final := startProcess(t, binary, c)
	b := DefaultConfig()
	b.Basic.Diagnostics.Address = "127.0.0.1:0"
	b.Basic.Listeners.Peer = "127.0.0.1:0"
	for _, name := range []string{"mongo", "search"} {
		remote := &Remote{Endpoints: []string{final.address}, MaxConcurrency: 4}
		service := Service{Name: name, Remote: remote}
		route := Route{Store: name, Service: name}
		b.Routing.Services = append(b.Routing.Services, service)
		b.Routing.Routes = append(b.Routing.Routes, route)
	}
	middle := startProcess(t, binary, b)
	a := DefaultConfig()
	a.Basic.Diagnostics.Address = "127.0.0.1:0"
	a.Basic.Listeners.Application = "127.0.0.1:0"
	for _, name := range []string{"mongo", "search"} {
		remote := &Remote{Endpoints: []string{middle.address}, MaxConcurrency: 4}
		service := Service{Name: name, Remote: remote}
		route := Route{Store: name, Service: name}
		a.Routing.Services = append(a.Routing.Services, service)
		a.Routing.Routes = append(a.Routing.Routes, route)
	}
	first := startProcess(t, binary, a)
	conn, err := grpc.NewClient(
		"passthrough:///"+first.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableRetry(),
		grpc.WithDisableServiceConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	for _, kind := range []string{"mongo", "search"} {
		root := "weir://mongo/" + database + "/records"
		if kind == "search" {
			root = "weir://search/" + search.Index
		}
		opts := processSmokeOptions{client: client, store: kind, root: root, search: search}
		processPublicSmoke(t, opts)
	}
	for _, node := range []*process{first, middle, final} {
		families := testmetrics.Scrape(t, node.diagnostic)
		if testmetrics.Sum(families, "weir_node_ready") != 1 {
			t.Fatal("process not ready")
		}
		if node == final {
			if testmetrics.Sum(families, "weir_store_records_total") != 8 {
				t.Fatal("process local records count", testmetrics.Sum(families, "weir_store_records_total"))
			}
			if testmetrics.Sum(families, "weir_store_native_completions_total") != 2 || testmetrics.Sum(families, "weir_store_scan_terminations_total") != 2 {
				t.Fatal("process local stream accounting")
			}
			for _, store := range []string{"mongo", "search"} {
				labels := map[string]string{"store": store, "completion": "response_complete"}
				native := testmetrics.Sample(families, "weir_store_native_completions_total", labels).GetCounter().GetValue()
				labels = map[string]string{"store": store, "result": "exhausted"}
				scan := testmetrics.Sample(families, "weir_store_scan_terminations_total", labels).GetCounter().GetValue()
				if native != 1 || scan != 1 {
					t.Fatal("process stream duplicated or incomplete", store, native, scan)
				}
			}
		} else if families["weir_store_executions_total"] != nil || testmetrics.Sum(families, "weir_relay_terminations_total") != 10 {
			t.Fatal("forward process execution duplication")
		} else {
			for _, store := range []string{"mongo", "search"} {
				for _, method := range []string{"Read", "Mutate", "Bulk", "Native", "Scan"} {
					labels := map[string]string{"service": store, "method": method, "status": "ok"}
					if testmetrics.Sample(families, "weir_relay_terminations_total", labels).GetCounter().GetValue() != 1 {
						t.Fatal("forward process method duplicated or incomplete", store, method)
					}
				}
			}
		}
	}
	t.Log("real process HTTP scrapes: C logical records=8, Native complete=2, Scan exhausted=2; A/B relays=10 each, A/B have no local executions")
	t.Log(fmt.Sprintf(
		"process IDs A=%d B=%d C=%d; profile=%s; plaintext HTTP/2 on isolated loopback sockets",
		first.command.Process.Pid,
		middle.command.Process.Pid,
		final.command.Process.Pid,
		search.Profile,
	))
	c.Basic.Listeners.Peer, b.Basic.Listeners.Peer, a.Basic.Listeners.Application = final.address, middle.address, first.address
	mongoRead := &pb.ReadRequest{Resource: "weir://mongo/" + database + "/records/s:example"}
	searchRead := &pb.ReadRequest{Resource: "weir://search/" + search.Index + "/s:example"}
	for _, node := range []struct {
		name string
		old  *process
		cfg  Config
	}{{"C", final, c}, {"B", middle, b}, {"A", first, a}} {
		node.old.stop(t)
		replacement := startProcess(t, binary, node.cfg)
		for _, request := range []*pb.ReadRequest{mongoRead, searchRead} {
			until := time.Now().Add(5 * time.Second)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				result, err := client.Read(ctx, request)
				cancel()
				if err == nil && result.GetDocument() != nil && processRecordNumber(t, result.GetDocument()) == 1 {
					break
				}
				if time.Now().After(until) {
					t.Fatal("fresh Read did not recover after process replacement", node.name, request.Resource, err)
				}
				// Each probe is a distinct read-only call; no failed write is retried.
				time.Sleep(100 * time.Millisecond)
			}
		}
		t.Logf("%s drained and exited; replacement PID=%d; existing client recovered MongoDB and Search reads", node.name, replacement.command.Process.Pid)
	}
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
	cfg := remoteConfig(t)
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
	_, err = conn.NewStream(ctx, desc, pb.Weir_Read_FullMethodName)
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
