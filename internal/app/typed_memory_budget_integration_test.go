//go:build integration

package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type typedMemoryDocument struct {
	id, resource, media string
	bytes               int
	digest              [32]byte
}

type typedMemoryProcessOptions struct {
	binary   string
	capacity int
	stores   []StoreConfig
}

type typedMemoryOutcome struct {
	index     int
	sent      bool
	validated bool
	err       error
	fail      *pb.Failure
}

// Slow-pressure survival and no-stall correctness are distinct acceptance gates.
// Deliberate pressure may end with complete capacity failure or caller deadline;
// every no-stall large Read must return its entire independently seeded document.
func TestCLITypedMemoryBudget(t *testing.T) {
	if os.Getenv("WEIR_TYPED_MEMORY_BUDGET_INTEGRATION") != "1" {
		t.Skip("requires WEIR_TYPED_MEMORY_BUDGET_INTEGRATION=1, Docker and the local Mongo and Elasticsearch fixtures")
	}
	if os.Getenv("WEIR_INTEGRATION") != "1" || os.Getenv("WEIR_SEARCH_INTEGRATION") != "elasticsearch" || os.Getenv("WEIR_MONGO_PROFILE") == "tls" {
		t.Fatal("requires isolated plain MongoDB and Elasticsearch8.19.22 fixtures")
	}
	mongo := testmongo.Open(t)
	binary := buildLinuxCLI(t)
	documents := seedTypedMemoryMongo(t, mongo)
	mongoBackend := &Mongo{URI: containerMongoURI}
	local := &Local{Backend: BackendConfig{MongoDB: mongoBackend}}
	primary, sibling := StoreConfig{Name: "mongo", Local: local}, StoreConfig{Name: "sibling", Local: local}
	for _, capacity := range []int{512, 1024} {
		t.Run(fmt.Sprintf("original_pressure/%dMiB", capacity), func(t *testing.T) {
			opts := typedMemoryProcessOptions{binary: binary, capacity: capacity, stores: []StoreConfig{primary}}
			process := startTypedMemoryProcess(t, opts)
			pressure := typedMemoryPressureOptions{process: process, documents: documents[:32], health: documents[33], stores: []string{"mongo"}}
			runTypedMemoryPressure(t, pressure)
		})
	}
	for _, capacity := range []int{256, 512, 1024} {
		t.Run(fmt.Sprintf("no_stall/%dMiB", capacity), func(t *testing.T) {
			opts := typedMemoryProcessOptions{binary: binary, capacity: capacity, stores: []StoreConfig{primary}}
			process := startTypedMemoryProcess(t, opts)
			client := openTypedMemoryClient(t, process, []string{"mongo"})
			for _, document := range []typedMemoryDocument{documents[0], documents[32]} {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				request := &weirclient.ReadRequest{Resource: document.resource}
				readOptions := weirclient.ReadOneOptions{StoreName: "mongo", Request: request}
				started := time.Now()
				result, err := client.ReadOne(ctx, readOptions)
				cancel()
				if err != nil || result == nil || result.Failure != nil || result.Document == nil || result.Document.ContentType != document.media {
					t.Fatal("no-stall legal large Read failed", document.id, err)
				}
				if !validateTypedMemoryDocument(t, document, result.Document.Data) {
					t.Fatal("no-stall legal document content differs", document.id)
				}
				t.Logf("no_stall_success id=%s bytes=%d sha256=%x elapsed=%s", document.id, document.bytes, document.digest, time.Since(started))
			}
			business := typedMemoryBusinessOptions{client: client, fixture: mongo, capacity: capacity}
			runTypedMemoryMongoBusiness(t, business)
			cancelledAt := time.Now()
			closeTypedMemoryClient(t, client)
			waitTypedMemoryIdle(t, process, 0, cancelledAt.Add(5*time.Second))
		})
	}
	t.Run("cross_store_typed_native/1024MiB", func(t *testing.T) {
		opts := typedMemoryProcessOptions{binary: binary, capacity: 1024, stores: []StoreConfig{primary, sibling}}
		process := startTypedMemoryProcess(t, opts)
		pressure := typedMemoryPressureOptions{process: process, documents: documents[:32], health: documents[33], stores: []string{"mongo", "sibling"}, nativeEvery: 4}
		runTypedMemoryPressure(t, pressure)
	})
	t.Run("search_no_stall/1024MiB", func(t *testing.T) {
		runTypedMemorySearch(t, binary)
	})
}

func startTypedMemoryProcess(t *testing.T, opts typedMemoryProcessOptions) *memoryRecoveryCLI {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Routing.Stores = opts.stores
	if cfg.Basic.Transport.MaxPendingRecords != 32 || time.Duration(cfg.Basic.Transport.Timeouts.Stall) != 30*time.Second {
		t.Fatal("original stream admission/stall policy changed")
	}
	options := memoryRecoveryOptions{binary: opts.binary, capacityMiB: opts.capacity, automatic: true, config: &cfg}
	process := startMemoryRecoveryCLI(t, options)
	t.Logf("CLI capacity=%dMiB CPUs=2", opts.capacity)
	// Inspect independently before the shared helper stops/removes its owned CLI.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		format := `{{index .Config.Labels "weir.owner"}} {{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}`
		raw, err := exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", format, process.name).CombinedOutput()
		fields := strings.Fields(string(raw))
		if err != nil || len(fields) != 4 || fields[0] != process.owner || fields[1] != "true" || fields[2] != "false" || fields[3] != "0" {
			t.Error("independent Docker survival/OOM check failed", err, string(raw))
		}
		t.Logf("independent_Docker_before_cleanup=%s error=%v", strings.TrimSpace(string(raw)), err)
	})
	return process
}

type typedMemoryPressureOptions struct {
	process     *memoryRecoveryCLI
	documents   []typedMemoryDocument
	health      typedMemoryDocument
	stores      []string
	nativeEvery int
}

func runTypedMemoryPressure(t *testing.T, opts typedMemoryPressureOptions) {
	t.Helper()
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(64 << 10), grpc.WithStaticConnWindowSize(4 << 20), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 << 20))}
	connection, err := grpc.NewClient("passthrough:///"+opts.process.address, dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewStoreServiceClient(connection)
	healthClient := openTypedMemoryClient(t, opts.process, opts.stores)
	initial, err := observeTypedMemory(opts.process)
	if err != nil {
		t.Fatal("initial execution-memory snapshot unavailable", err)
	}
	largestBudget := validateTypedMemorySnapshot(t, opts.process, initial, 0)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	streams := make([]grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse], 32)
	for index := range streams {
		if opts.nativeEvery != 0 && index%opts.nativeEvery == opts.nativeEvery-1 {
			continue
		}
		streamContext := metadata.AppendToOutgoingContext(ctx, "weir-request-id", fmt.Sprintf("typed-memory-%s-%02d", t.Name(), index))
		streams[index], err = client.Execute(streamContext)
		if err != nil {
			t.Fatal("pressure stream open failed before the common barrier", index, err)
		}
	}
	barrier, release := make(chan struct{}), make(chan struct{})
	outcomes := make(chan typedMemoryOutcome, 32)
	var calls sync.WaitGroup
	for index, document := range opts.documents {
		calls.Go(func() {
			<-barrier
			outcome := typedMemoryOutcome{index: index}
			var returned []byte
			if streams[index] == nil {
				request, err := typedMemoryNativeRequest(document)
				if err != nil {
					outcome.err = err
					outcomes <- outcome
					return
				}
				var body []byte
				consume := func(ctx context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					if len(body)+len(chunk) > 17<<20 {
						return fmt.Errorf("pressure Native exceeded the known document bound")
					}
					body = append(body, chunk...)
					return nil
				}
				options := weirclient.NativeOptions{StoreName: opts.stores[index%len(opts.stores)], Request: request, Consume: consume}
				result, err := weirclient.Native(ctx, client, options)
				outcome.err = err
				if result != nil {
					outcome.sent = true
					outcome.fail = result.Failure
					if err == nil && result.Failure == nil && result.Completion == weirclient.NativeResponseComplete {
						returned, outcome.err = typedMemoryNativeDocument(body)
					} else if err == nil && result.Failure == nil {
						outcome.err = fmt.Errorf("Native ended without complete response evidence")
					}
				} else if err == nil {
					outcome.err = fmt.Errorf("Native returned no completion evidence")
				}
			} else {
				read := &pb.ReadRequest{Resource: document.resource}
				operation := &pb.Command_Read{Read: read}
				command := &pb.Command{Operation: operation}
				request := &pb.ExecuteRequest{StoreName: opts.stores[index%len(opts.stores)], Index: 1, Command: command}
				outcome.err = streams[index].Send(request)
				if outcome.err == nil {
					outcome.err = streams[index].CloseSend()
				}
				outcome.sent = outcome.err == nil
				<-release
				if outcome.err == nil {
					response, err := streams[index].Recv()
					outcome.err = err
					if err == nil {
						result := response.GetEvent().GetReadResult()
						outcome.fail = result.GetFailure()
						doc := result.GetDocument()
						if response.GetIndex() != 1 || result == nil || doc == nil && outcome.fail == nil || doc != nil && outcome.fail != nil {
							outcome.err = fmt.Errorf("typed response lost its complete result/failure envelope")
						} else if doc != nil {
							if doc.ContentType != document.media {
								outcome.err = fmt.Errorf("typed result changed its content type")
							}
							// Full payload validation is independent of a later RPC deadline.
							outcome.validated = validateTypedMemoryDocument(t, document, doc.Data)
						}
						if outcome.err == nil {
							next, terminalErr := streams[index].Recv()
							if terminalErr != io.EOF {
								outcome.err = terminalErr
								if terminalErr == nil {
									outcome.err = fmt.Errorf("typed result/failure lacked terminal EOF; extra index=%d", next.GetIndex())
								}
							}
						}
					}
				}
			}
			if streams[index] == nil && outcome.err == nil && outcome.fail == nil {
				outcome.validated = validateTypedMemoryDocument(t, document, returned)
			}
			outcomes <- outcome
		})
	}
	healthResults, healthStart := make(chan error, 2), make(chan struct{})
	started := time.Now()
	holdDeadline := started.Add(3 * time.Second)
	close(barrier)
	for _, native := range []bool{false, true} {
		calls.Go(func() {
			<-healthStart
			if !time.Now().Before(holdDeadline) {
				healthResults <- fmt.Errorf("no held-pressure sample before the original hold ended")
				return
			}
			probeContext, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			probeStarted := time.Now()
			err := typedMemoryHealth(probeContext, healthClient, opts, native)
			finished := time.Now()
			if err == nil && (finished.Sub(probeStarted) > 3*time.Second || finished.After(holdDeadline)) {
				err = fmt.Errorf("health completed after its3s budget or after pressure hold: latency=%s", finished.Sub(probeStarted))
			}
			t.Logf("pressure_health native=%t latency=%s within_original_hold=%t error=%v", native, finished.Sub(probeStarted), !finished.After(holdDeadline), err)
			healthResults <- err
		})
	}
	healthLaunched := false
	for time.Now().Before(holdDeadline) {
		sample, err := observeTypedMemory(opts.process)
		if err != nil {
			t.Logf("pressure_metric_observation_error elapsed=%s error=%v", time.Since(started), err)
		} else {
			largestBudget = validateTypedMemorySnapshot(t, opts.process, sample, largestBudget)
			if !healthLaunched && testmetrics.Sum(sample, "weir_execution_memory_wire_buffers") > 0 && testmetrics.Sum(sample, "weir_execution_memory_resident_bytes") > testmetrics.Sum(sample, "weir_execution_memory_input_reserved_bytes") {
				healthLaunched = true
				close(healthStart)
			}
		}
		if remaining := time.Until(holdDeadline); remaining > 0 {
			time.Sleep(min(remaining, 100*time.Millisecond))
		}
	}
	close(release)
	if !healthLaunched {
		close(healthStart)
	}
	t.Logf("pressure original_noRecv=3s actual_hold=%s original_common_caller=5s count=32 native_every=%d", time.Since(started), opts.nativeEvery)
	joined := make(chan struct{})
	go func() { calls.Wait(); close(joined) }()
	var cancelledAt time.Time
	select {
	case <-joined:
	case <-ctx.Done():
		callerDeadline, _ := ctx.Deadline()
		cancelledAt = callerDeadline
		cancel()
		_ = connection.Close()
		closeTypedMemoryClient(t, healthClient)
		select {
		case <-joined:
		case <-time.After(max(0, time.Until(callerDeadline.Add(time.Second)))):
			t.Error("pressure calls did not join within1s after the original common deadline")
			return
		}
	}
	for range 2 {
		if err := <-healthResults; err != nil {
			t.Error("small query failed during the fixed pressure hold", err)
		}
	}
	close(outcomes)
	validated, completed, capacity, deadline, sent := 0, 0, 0, 0, 0
	for outcome := range outcomes {
		document := opts.documents[outcome.index]
		if outcome.validated {
			validated++
		}
		if outcome.sent {
			sent++
		}
		switch {
		case outcome.err != nil:
			if status.Code(outcome.err) == codes.DeadlineExceeded || outcome.err == context.DeadlineExceeded {
				deadline++
			} else {
				t.Error("unexpected pressure transport/protocol failure", document.id, outcome.err)
			}
		case outcome.fail != nil:
			if outcome.fail.Code == pb.FailureCode_RESOURCE_EXHAUSTED {
				capacity++
			} else {
				t.Error("unexpected pressure command failure", document.id, outcome.fail)
			}
		default:
			if outcome.validated {
				completed++
			}
		}
	}
	if cancelledAt.IsZero() {
		cancelledAt = time.Now()
	}
	cancel()
	_ = connection.Close()
	closeTypedMemoryClient(t, healthClient)
	if opts.nativeEvery == 0 && sent != 32 {
		t.Error("original32 pressure requests were not all sent", sent)
	}
	t.Logf("pressure_survival received=32 sent_terminal_evidence=%d validated_full_documents=%d complete_RPC_OK=%d capacity_denied=%d caller_deadline=%d; caller deadlines are not correctness passes", sent, validated, completed, capacity, deadline)
	waitTypedMemoryIdle(t, opts.process, largestBudget, cancelledAt.Add(5*time.Second))
}

func typedMemoryNativeRequest(document typedMemoryDocument) (*pb.NativeRequest, error) {
	database := strings.SplitN(document.resource, "/", 2)[0]
	filter := bson.D{{Key: "_id", Value: document.id}}
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
	raw, err := bson.Marshal(command)
	if err != nil {
		return nil, err
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: database, Request: body}
	return request, nil
}

func typedMemoryNativeDocument(raw []byte) ([]byte, error) {
	var reply struct {
		OK     float64 `bson:"ok"`
		Cursor struct {
			Batch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	if err := bson.Unmarshal(raw, &reply); err != nil {
		return nil, err
	}
	if reply.OK != 1 || len(reply.Cursor.Batch) != 1 {
		return nil, fmt.Errorf("Native find lost its full single document")
	}
	return reply.Cursor.Batch[0], nil
}

func typedMemoryHealth(ctx context.Context, client *weirclient.Client, opts typedMemoryPressureOptions, native bool) error {
	if !native {
		request := &weirclient.ReadRequest{Resource: opts.health.resource}
		options := weirclient.ReadOneOptions{StoreName: opts.stores[0], Request: request}
		result, err := client.ReadOne(ctx, options)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Document == nil || sha256.Sum256(result.Document.Data) != opts.health.digest {
			return fmt.Errorf("healthy small Read failed full content evidence")
		}
		return nil
	}
	request, err := typedMemoryNativeRequest(opts.health)
	if err != nil {
		return err
	}
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 8<<10 {
			return fmt.Errorf("small Native exceeded8KiB")
		}
		body = append(body, chunk...)
		return nil
	}
	options := weirclient.NativeOptions{StoreName: opts.stores[len(opts.stores)-1], Request: request, Consume: consume}
	result, err := client.Native(ctx, options)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
		return fmt.Errorf("healthy short Native failed: %v", result)
	}
	document, err := typedMemoryNativeDocument(body)
	if err != nil {
		return err
	}
	if sha256.Sum256(document) != opts.health.digest {
		return fmt.Errorf("healthy short Native returned different content")
	}
	return nil
}

func openTypedMemoryClient(t *testing.T, process *memoryRecoveryCLI, stores []string) *weirclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	opts := weirclient.OpenOptions{Seed: process.address, Stores: stores}
	client, err := weirclient.Open(ctx, opts)
	if err != nil {
		t.Fatal("real SDK Resolve initialization failed", err)
	}
	t.Cleanup(func() { closeTypedMemoryClient(t, client) })
	return client
}

func closeTypedMemoryClient(t *testing.T, client *weirclient.Client) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- client.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Error("SDK cleanup failed", err)
		}
	case <-time.After(time.Second):
		t.Error("SDK cleanup did not join within1s")
	}
}

func seedTypedMemoryMongo(t *testing.T, fixture *testmongo.Fixture) []typedMemoryDocument {
	t.Helper()
	pad := strings.Repeat("x", 12<<20)
	documents := make([]typedMemoryDocument, 34)
	for index := range documents {
		id, payload := fmt.Sprintf("typed-large-%02d", index), pad
		if index == 32 {
			id, payload = "typed-near16MiB", ""
		} else if index == 33 {
			id, payload = "typed-health", "healthy"
		}
		var input any = bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(index)}, {Key: "pad", Value: payload}}
		if index == 32 {
			encoded, err := typedMemoryExactBSON(id, int64(index), (16<<20)-64)
			if err != nil {
				t.Fatal(err)
			}
			input = bson.Raw(encoded)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(ctx, input)
		if err != nil {
			cancel()
			t.Fatal("owned Mongo seed failed", id, err)
		}
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
		cancel()
		if err != nil {
			t.Fatal("independent Mongo source oracle failed", id, err)
		}
		documents[index] = typedMemoryDocument{id: id, resource: fixture.DB + "/records/s:" + id, media: "application/bson", bytes: len(raw), digest: sha256.Sum256(raw)}
	}
	return documents
}

type typedMemoryBusinessOptions struct {
	client   *weirclient.Client
	fixture  *testmongo.Fixture
	capacity int
}

func typedMemoryExactBSON(id string, n int64, bytes int) ([]byte, error) {
	base := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}, {Key: "pad", Value: ""}}
	raw, err := bson.Marshal(base)
	if err != nil {
		return nil, err
	}
	if bytes < len(raw) {
		return nil, fmt.Errorf("requested BSON length is smaller than its fixed metadata")
	}
	base[2].Value = strings.Repeat("w", bytes-len(raw))
	raw, err = bson.Marshal(base)
	if err == nil && len(raw) != bytes {
		return nil, fmt.Errorf("exact BSON length construction differs: got%d want%d", len(raw), bytes)
	}
	return raw, err
}

func requireTypedMemoryApplied(t *testing.T, stage string, result *weirclient.MutationResult, err error) {
	t.Helper()
	if result == nil {
		t.Fatal("mutation returned no outcome evidence", stage, err)
	}
	if result.Failure != nil || result.Outcome != weirclient.MutationApplied {
		t.Fatal("mutation did not confirm APPLIED", stage, result.Outcome, result.Failure, err)
	}
	if err != nil {
		// Preserve confirmed effects and still check their independent oracle.
		t.Error("confirmed APPLIED retained despite RPC error", stage, result.Outcome, err)
	}
}

func runTypedMemoryMongoBusiness(t *testing.T, opts typedMemoryBusinessOptions) {
	t.Helper()
	for _, bytes := range []int{12 << 20, (16 << 20) - 64} {
		id := fmt.Sprintf("sdk-write-%d-%d", opts.capacity, bytes)
		raw, err := typedMemoryExactBSON(id, int64(opts.capacity), bytes)
		if err != nil {
			t.Fatal(err)
		}
		resource := opts.fixture.DB + "/records/s:" + id
		expected := typedMemoryDocument{id: id, resource: resource, media: "application/bson", bytes: len(raw), digest: sha256.Sum256(raw)}
		document := &pb.Document{ContentType: expected.media, Data: raw}
		write := &weirclient.WriteRequest{Resource: resource, Document: document}
		writeOptions := weirclient.WriteOptions{StoreName: "mongo", Request: write}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		result, err := opts.client.Put(ctx, writeOptions)
		cancel()
		requireTypedMemoryApplied(t, "largePut", result, err)
		filter := bson.D{{Key: "_id", Value: id}}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		stored, err := opts.fixture.Admin.Database(opts.fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
		cancel()
		if err != nil || !validateTypedMemoryDocument(t, expected, stored) {
			t.Fatal("APPLIED largePut differs from independent Mongo oracle", id, err)
		}
		read := &weirclient.ReadRequest{Resource: resource}
		readOptions := weirclient.ReadOneOptions{StoreName: "mongo", Request: read}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		readback, err := opts.client.ReadOne(ctx, readOptions)
		cancel()
		if err != nil || readback == nil || readback.Failure != nil || readback.Document == nil || readback.Document.ContentType != expected.media {
			t.Fatal("SDK Read after APPLIED largePut failed", id, err)
		}
		if !validateTypedMemoryDocument(t, expected, readback.Document.Data) {
			t.Fatal("SDK Read after APPLIED largePut changed full bytes", id)
		}
		remove := &weirclient.DeleteRequest{Resource: resource}
		removeOptions := weirclient.DeleteOptions{StoreName: "mongo", Request: remove}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		result, err = opts.client.Delete(ctx, removeOptions)
		cancel()
		requireTypedMemoryApplied(t, "largeDelete", result, err)
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		err = opts.fixture.Admin.Database(opts.fixture.DB).Collection("records").FindOne(ctx, filter).Err()
		cancel()
		if !errors.Is(err, mongodriver.ErrNoDocuments) {
			t.Fatal("APPLIED Delete did not remove independent Mongo document", id, err)
		}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		readback, err = opts.client.ReadOne(ctx, readOptions)
		cancel()
		if err != nil || readback == nil || readback.Failure != nil || readback.Document != nil || !readback.Missing {
			t.Fatal("SDK Read after APPLIED Delete did not confirm document absence", id, err)
		}
		t.Logf("business_success id=%s exact_BSON_bytes=%d sha256=%x Put_APPLIED=true independent_hash=true SDK_Read_hash=true Delete_APPLIED=true independent_absent=true SDK_Missing=true caller_per_step=5s", id, len(raw), expected.digest)
	}
}

func validateTypedMemoryDocument(t *testing.T, expected typedMemoryDocument, raw []byte) bool {
	t.Helper()
	var id string
	if expected.media == "application/bson" {
		id, _ = bson.Raw(raw).Lookup("_id").StringValueOK()
	} else {
		var document struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Error("invalid JSON large document", expected.id, err)
		}
		id = document.ID
	}
	if id != expected.id || len(raw) != expected.bytes || sha256.Sum256(raw) != expected.digest {
		t.Error("large document ID/bytes/hash mismatch", expected.id, id, expected.bytes, len(raw))
		return false
	}
	return true
}

func observeTypedMemory(process *memoryRecoveryCLI) (map[string]*dto.MetricFamily, error) {
	client := &http.Client{Timeout: 150 * time.Millisecond}
	response, err := client.Get("http://" + process.diagnostic + "/metrics")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics HTTP status=%d", response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	return parser.TextToMetricFamilies(io.LimitReader(response.Body, 4<<20))
}

func validateTypedMemorySnapshot(t *testing.T, process *memoryRecoveryCLI, sample map[string]*dto.MetricFamily, largestBudget float64) float64 {
	t.Helper()
	values := make(map[string]float64, 26)
	for _, suffix := range []string{"ingress_connection_bytes", "ingress_connections", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting", "budget_bytes", "used_bytes", "resident_bytes", "input_reserved_bytes", "input_used_bytes", "io_reserve_bytes", "budget_known", "leases", "leases_limit", "native_leases", "native_leases_limit", "starting", "settled", "borrowed", "waiting", "terminal", "wire_bytes", "wire_buffers", "budget_rejections_total"} {
		family := sample["weir_execution_memory_"+suffix]
		if family == nil || len(family.Metric) != 1 || len(family.Metric[0].Label) != 0 || suffix != "budget_rejections_total" && family.Metric[0].Gauge == nil || suffix == "budget_rejections_total" && family.Metric[0].Counter == nil {
			t.Fatal("required execution-memory metric missing or mislabeled", suffix)
		}
		value := family.Metric[0].GetGauge().GetValue() + family.Metric[0].GetCounter().GetValue()
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			t.Fatal("invalid execution-memory observation", suffix, value)
		}
		values[suffix] = value
	}
	families := 0
	for name := range sample {
		if strings.HasPrefix(name, "weir_execution_memory_") {
			families++
		}
	}
	if values["budget_known"] != 1 || families != 26 || values["leases_limit"] != 32768 || values["native_leases_limit"] != 1024 || values["budget_bytes"] <= 0 {
		t.Error("fixed shared execution policy unknown or changed", families, values)
	}
	if values["budget_known"] == 1 {
		largestBudget = max(largestBudget, values["budget_bytes"])
	}
	// The absolute bound follows the documented integer arithmetic. Existing
	// owners survive a trusted shrink; sampled observations do not prove that no
	// allocation took place between samples. Mutex/race tests cover that boundary.
	absoluteBudget := float64(uint64(process.capacity)*70/100/3*2 + (uint64(process.capacity)*70/100)%3*2/3)
	states := values["starting"] + values["settled"] + values["borrowed"] + values["waiting"] + values["terminal"]
	if largestBudget > absoluteBudget || values["used_bytes"] > largestBudget || values["resident_bytes"] > values["used_bytes"] || values["input_used_bytes"] > values["input_reserved_bytes"] || values["input_reserved_bytes"] > values["resident_bytes"] || values["wire_bytes"] > values["used_bytes"] || values["wire_buffers"] > values["leases"] || values["leases"] > 32768 || values["native_leases"] > 1024 || values["native_leases"] > values["leases"] || states != values["leases"] {
		t.Error("shared execution ownership exceeded observed trusted capacity", largestBudget, absoluteBudget, values)
	}
	if values["used_bytes"] > values["budget_bytes"] {
		t.Logf("trusted_budget_shrink at=%s Used=%.0f B=%.0f known=%.0f observed_max_B=%.0f", time.Now().UTC().Format(time.RFC3339Nano), values["used_bytes"], values["budget_bytes"], values["budget_known"], largestBudget)
	}
	if testmetrics.Sum(sample, "weir_memory_cgroup_valid") != 1 || testmetrics.Sum(sample, "weir_memory_unknown") != 0 || testmetrics.Sum(sample, "weir_memory_cgroup_limit_bytes") != process.capacity || testmetrics.Sum(sample, "weir_memory_go_runtime_managed") != 1 {
		t.Error("finite CLI cgroup observation unknown or changed")
	}
	if sample["weir_watchdog_expirations_total"] == nil || testmetrics.Sum(sample, "weir_watchdog_expirations_total") != 0 {
		t.Error("short fixed pressure was released by watchdog expiry")
	}
	stores := 0
	for _, store := range []string{"mongo", "sibling", "search"} {
		labels := map[string]string{"store": store}
		if testmetrics.Sample(sample, "weir_store_active_executions_limit", labels) == nil {
			continue
		}
		stores++
		for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_store_batch_max_operations": 128, "weir_store_batch_max_bytes": 4 << 20, "weir_store_batch_max_wait_seconds": .02} {
			metric := testmetrics.Sample(sample, name, labels)
			if metric == nil || metric.Gauge == nil || len(metric.Label) != 1 || metric.GetGauge().GetValue() != want {
				t.Error("default Store policy changed or missing", store, name)
			}
		}
	}
	if stores != process.stores {
		t.Error("expected Store metrics missing", stores, process.stores)
	}
	t.Logf("execution_snapshot at=%s Node=%v RSS=%.0f WS=%.0f latch=%.0f pending=%.0f active=%.0f wire=%.0f sessions=%.0f", time.Now().UTC().Format(time.RFC3339Nano), values, testmetrics.Sum(sample, "process_resident_memory_bytes"), testmetrics.Sum(sample, "weir_memory_cgroup_working_set_bytes"), testmetrics.Sum(sample, "weir_memory_latched"), testmetrics.Sum(sample, "weir_store_pending_entries"), testmetrics.Sum(sample, "weir_store_active_executions"), testmetrics.Sum(sample, "weir_ingress_queued_response_bytes"), testmetrics.Sum(sample, "weir_ingress_sessions"))
	return largestBudget
}

func waitTypedMemoryIdle(t *testing.T, process *memoryRecoveryCLI, largestBudget float64, deadline time.Time) {
	t.Helper()
	for {
		sample, err := observeTypedMemory(process)
		if err == nil {
			largestBudget = validateTypedMemorySnapshot(t, process, sample, largestBudget)
			idle := sample["weir_memory_latched"] != nil && testmetrics.Sum(sample, "weir_memory_latched") == 0 && testmetrics.Sum(sample, "weir_memory_cgroup_working_set_bytes") <= process.capacity*.70 && testmetrics.Sum(sample, "weir_execution_memory_budget_known") == 1
			for _, suffix := range []string{"used_bytes", "resident_bytes", "input_reserved_bytes", "input_used_bytes", "leases", "native_leases", "starting", "settled", "borrowed", "waiting", "terminal", "wire_bytes", "wire_buffers"} {
				idle = idle && testmetrics.Sum(sample, "weir_execution_memory_"+suffix) == 0
			}
			for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
				idle = idle && sample[name] != nil && testmetrics.Sum(sample, name) == 0
			}
			if idle {
				if time.Now().After(deadline) {
					t.Fatal("ownership reached zero only after the fixed5s bound")
				}
				t.Logf("final5s all execution/Store/RPC/wire/input ownership0, latch0; elapsed=%s", time.Since(deadline.Add(-5*time.Second)))
				return
			}
		} else {
			t.Logf("final_metric_observation_error error=%v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("fixed5s cancellation/recovery bound exhausted")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runTypedMemorySearch(t *testing.T, binary string) {
	t.Helper()
	fixture := testsearch.Open(t)
	searchURL := searchContainerURL(t, fixture)
	code, _ := fixture.Do(t, http.MethodPut, "/"+fixture.Index+"/_mapping", `{"dynamic":false}`)
	if code != http.StatusOK {
		t.Fatal("owned many-fields mapping failed", code)
	}
	backend := &Search{URL: searchURL}
	local := &Local{Backend: BackendConfig{Search: backend}}
	store := StoreConfig{Name: "search", Local: local}
	options := typedMemoryProcessOptions{binary: binary, capacity: 1024, stores: []StoreConfig{store}}
	process := startTypedMemoryProcess(t, options)
	client := openTypedMemoryClient(t, process, []string{"search"})
	for index, id := range []string{"large12MiB", "manyfields12MiB"} {
		payload := map[string]any{"id": id, "n": 1, "pad": strings.Repeat("x", 12<<20)}
		if index == 1 {
			payload["pad"] = strings.Repeat("x", 4<<20)
			for field := range 4096 {
				payload[fmt.Sprintf("f%04d", field)] = strings.Repeat("y", 2<<10)
			}
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		document := typedMemoryDocument{id: id, resource: fixture.Index + "/s:" + id, media: "application/json", bytes: len(raw), digest: sha256.Sum256(raw)}
		body := &pb.Document{ContentType: document.media, Data: raw}
		write := &weirclient.WriteRequest{Resource: document.resource, Document: body}
		writeOptions := weirclient.WriteOptions{StoreName: "search", Request: write}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		mutation, err := client.Put(ctx, writeOptions)
		cancel()
		requireTypedMemoryApplied(t, "SearchLargePut", mutation, err)
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, fixture.URL+"/"+fixture.Index+"/_doc/"+id, nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		response, err := fixture.Client.Do(request)
		if err != nil {
			cancel()
			t.Fatal("independent Search source oracle failed", err)
		}
		var reply struct {
			ID     string          `json:"_id"`
			Found  bool            `json:"found"`
			Source json.RawMessage `json:"_source"`
		}
		decoder := json.NewDecoder(io.LimitReader(response.Body, 32<<20))
		err = decoder.Decode(&reply)
		_ = response.Body.Close()
		cancel()
		if err != nil || response.StatusCode != http.StatusOK || !reply.Found || reply.ID != id || !validateTypedMemoryDocument(t, document, reply.Source) {
			t.Fatal("independent Search ID/full-source oracle differs", id, err)
		}
		t.Logf("Search_real_GET_oracle id=%s bytes=%d ContentLength=%d TransferEncoding=%v; typed unknown-length coverage is a separate controlled backend test", id, len(raw), response.ContentLength, response.TransferEncoding)
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		read := &weirclient.ReadRequest{Resource: document.resource}
		opts := weirclient.ReadOneOptions{StoreName: "search", Request: read}
		result, err := client.ReadOne(ctx, opts)
		cancel()
		if err != nil || result == nil || result.Failure != nil || result.Document == nil || result.Document.ContentType != document.media {
			t.Fatal("Search no-stall large document failed", document.id, err)
		}
		if !validateTypedMemoryDocument(t, document, result.Document.Data) {
			t.Fatal("Search no-stall large content differs", document.id)
		}
		t.Logf("Search_business_success id=%s bytes=%d sha256=%x Put_APPLIED=true independent_GET_hash=true SDK_Read_hash=true", document.id, document.bytes, document.digest)
	}
	cancelledAt := time.Now()
	closeTypedMemoryClient(t, client)
	waitTypedMemoryIdle(t, process, 0, cancelledAt.Add(5*time.Second))
}
