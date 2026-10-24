//go:build integration

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type reliabilityProbe struct {
	node     *Node
	resource string
	media    string
	native   *pb.NativeRequest
}

func reliabilityConfig(t *testing.T, definition StoreConfig) Config {
	t.Helper()
	cfg := emptyConfig(t)
	cfg.Stores = []StoreConfig{definition}
	return cfg
}

func TestMongoLowLoadBusinessReliability(t *testing.T) {
	fixture := testmongo.Open(t)
	cfg := reliabilityConfig(t, mongoFixtureStore(t, "records", fixture.URI))
	node := secureNode(t, cfg)
	filter := bson.D{{Key: "_id", Value: "probe"}}
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
	commandBytes, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	input := &pb.Document{ContentType: "application/bson", Data: commandBytes}
	native := &pb.NativeRequest{Resource: fixture.DB, Request: input}
	probe := reliabilityProbe{node: node, resource: fixture.DB + "/records/s:probe", media: "application/bson", native: native}
	runLowLoadReliability(t, probe)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var actual struct {
		N   int64
		Pad string
	}
	if err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Decode(&actual); err != nil || actual.N != 64 || actual.Pad != strings.Repeat("x", 2048) {
		t.Fatal("independent MongoDB observation lost acknowledged transforms", actual.N, err)
	}
}

func TestSearchLowLoadBusinessReliability(t *testing.T) {
	fixture := testsearch.Open(t)
	cfg := reliabilityConfig(t, searchFixtureStore("records", fixture.URL))
	node := secureNode(t, cfg)
	request, err := http.NewRequest(http.MethodGet, "http://weir.invalid/"+fixture.Index+"/_doc/probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	native, err := weirclient.NewHTTPNativeRequest("", request)
	if err != nil {
		t.Fatal(err)
	}
	probe := reliabilityProbe{node: node, resource: fixture.Index + "/s:probe", media: "application/json", native: native}
	runLowLoadReliability(t, probe)
	status, raw := fixture.Do(t, http.MethodGet, "/"+fixture.Index+"/_doc/probe", "")
	var actual struct {
		Source struct {
			N   int64
			Pad string
		} `json:"_source"`
	}
	if err := json.Unmarshal(raw, &actual); err != nil || status != http.StatusOK || actual.Source.N != 64 || actual.Source.Pad != strings.Repeat("x", 2048) {
		t.Fatal("independent Search observation lost acknowledged transforms", status, actual.Source.N, err)
	}
}

// runLowLoadReliability checks latency and recovery under low load; it is not
// a capacity benchmark. Each call has its own budget; latency samples include
// SDK and final RPC completion.
func runLowLoadReliability(t *testing.T, probe reliabilityProbe) {
	t.Helper()
	client, err := weirclient.New(probe.node.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	write := &weirclient.WriteRequest{Resource: probe.resource}
	writeOptions := weirclient.WriteOptions{StoreName: "records", Request: write}
	read := &weirclient.ReadRequest{Resource: probe.resource}
	readOptions := weirclient.ReadOneOptions{StoreName: "records", Request: read}
	lua := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
	transform := &weirclient.AtomicTransformRequest{Resource: probe.resource, Lua: lua}
	transformOptions := weirclient.AtomicTransformOptions{StoreName: "records", Request: transform}
	type phase struct {
		name  string
		count int
	}
	phases := []phase{{name: "put", count: 32}, {name: "read", count: 128}, {name: "native", count: 128}, {name: "lua", count: 32}, {name: "read_after_idle_cancel", count: 128}}
	for _, phase := range phases {
		t.Run(phase.name, func(t *testing.T) {
			if phase.name == "read_after_idle_cancel" {
				cancelReliabilityStreams(t, probe)
			}
			latencies := make([]time.Duration, 0, phase.count)
			for index := range phase.count {
				if phase.name == "put" {
					document, err := reliabilityPutDocument(probe.media, int64(index+1))
					if err != nil {
						t.Fatal(err)
					}
					write.Document = document
				}
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				started := time.Now()
				var err error
				switch phase.name {
				case "put", "lua":
					var result *weirclient.MutationResult
					if phase.name == "put" {
						result, err = client.Put(ctx, writeOptions)
					} else {
						result, err = client.AtomicTransform(ctx, transformOptions)
					}
					if err == nil && (result == nil || result.Failure != nil || result.Outcome != weirclient.MutationApplied) {
						err = fmt.Errorf("mutation lacked successful application evidence: %v", result)
					}
				case "native":
					var body []byte
					consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
						if len(body)+len(chunk) > 64<<10 {
							return errors.New("point query exceeded probe response bound")
						}
						body = append(body, chunk...)
						return nil
					}
					options := weirclient.NativeOptions{StoreName: "records", Request: probe.native, Consume: consume}
					result, callErr := client.Native(ctx, options)
					err = callErr
					if err == nil && (result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete || len(body) == 0) {
						err = fmt.Errorf("native query lacked complete response: %v, bytes=%d", result, len(body))
					}
					if err == nil {
						err = validateReliabilityNative(result, body)
					}
				default:
					result, callErr := client.ReadOne(ctx, readOptions)
					err = callErr
					if err == nil && (result == nil || result.Failure != nil || result.Missing || result.Document == nil) {
						err = fmt.Errorf("read lost acknowledged document: %v", result)
					}
					if err == nil {
						want := int64(32)
						if phase.name == "read_after_idle_cancel" {
							want = 64
						}
						err = validateReliabilityDocument(result.Document, want)
					}
				}
				elapsed := time.Since(started)
				// Verify each distinct acknowledged write outside the Put timer.
				if err == nil && phase.name == "put" {
					result, callErr := client.ReadOne(ctx, readOptions)
					err = callErr
					if err == nil && (result == nil || result.Document == nil || result.Failure != nil) {
						err = fmt.Errorf("write readback failed: %v", result)
					}
					if err == nil {
						err = validateReliabilityDocument(result.Document, int64(index+1))
					}
				}
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				latencies = append(latencies, elapsed)
			}
			slices.Sort(latencies)
			t.Logf("calls=%d p50=%s p95=%s p99=%s max=%s", len(latencies), latencies[(len(latencies)*50+99)/100-1], latencies[(len(latencies)*95+99)/100-1], latencies[(len(latencies)*99+99)/100-1], latencies[len(latencies)-1])
		})
		if t.Failed() {
			return
		}
	}
	budgetWait(t, "low-load SDK resources drained", func() bool {
		stats := probe.node.stores["records"].Stats()
		metrics := testmetrics.Gather(t, probe.node.admission)
		return stats.Queued == 0 && stats.Active == 0 && testmetrics.Sum(metrics, "weir_ingress_sessions") == 0
	})
	metrics := testmetrics.Gather(t, probe.node.admission)
	if rejected := testmetrics.Sum(metrics, "weir_admission_rejections_total"); rejected != 0 {
		t.Fatal("low-load calls experienced admission rejection", rejected)
	}
	memory := probe.node.guard.Snapshot()
	if memory.Overloaded {
		t.Fatal("low-load probe left admission overloaded", memory)
	}
	t.Logf("omitted batching defaults=%+v; memory=%+v; final store=%+v", store.DefaultLimits(), memory, probe.node.stores["records"].Stats())
}

func reliabilityPutDocument(media string, n int64) (*pb.Document, error) {
	var raw []byte
	var err error
	if media == "application/bson" {
		body := bson.D{{Key: "_id", Value: "probe"}, {Key: "n", Value: n}, {Key: "pad", Value: strings.Repeat("x", 2048)}}
		raw, err = bson.Marshal(body)
	} else {
		raw = []byte(fmt.Sprintf(`{"n":%d,"pad":"%s"}`, n, strings.Repeat("x", 2048)))
	}
	if err != nil {
		return nil, err
	}
	document := &pb.Document{ContentType: media, Data: raw}
	return document, nil
}

func validateReliabilityDocument(document *pb.Document, want int64) error {
	var actual struct {
		N   int64  `bson:"n" json:"n"`
		Pad string `bson:"pad" json:"pad"`
	}
	var err error
	if document.ContentType == "application/bson" {
		err = bson.Unmarshal(document.Data, &actual)
	} else {
		err = json.Unmarshal(document.Data, &actual)
	}
	if err != nil {
		return err
	}
	if actual.N != want || actual.Pad != strings.Repeat("x", 2048) {
		return fmt.Errorf("document contents changed: n=%d want=%d pad_bytes=%d", actual.N, want, len(actual.Pad))
	}
	return nil
}

func validateReliabilityNative(result *weirclient.NativeResult, body []byte) error {
	if result.Response.BodyContentType == "application/bson" {
		var reply struct {
			OK     float64 `bson:"ok"`
			Cursor struct {
				FirstBatch []bson.Raw `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		if err := bson.Unmarshal(body, &reply); err != nil {
			return err
		}
		if reply.OK != 1 || len(reply.Cursor.FirstBatch) != 1 {
			return errors.New("native MongoDB query did not return one document successfully")
		}
		document := &pb.Document{ContentType: "application/bson", Data: reply.Cursor.FirstBatch[0]}
		return validateReliabilityDocument(document, 32)
	}
	response, err := weirclient.ParseHTTPNativeResponse(result.Response)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("native Search query status=%d", response.StatusCode)
	}
	var reply struct {
		Found  bool            `json:"found"`
		ID     string          `json:"_id"`
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		return err
	}
	if !reply.Found || reply.ID != "probe" {
		return errors.New("native Search query did not return the expected document")
	}
	document := &pb.Document{ContentType: "application/json", Data: reply.Source}
	return validateReliabilityDocument(document, 32)
}

// Cancel after a confirmed result while the server awaits more input. Backend
// in-flight cancellation is covered separately by the backend integration suites.
func cancelReliabilityStreams(t *testing.T, probe reliabilityProbe) {
	t.Helper()
	client := processClient(t, probe.node.Address())
	for range 32 {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		stream, err := client.Execute(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		read := &pb.ReadRequest{Resource: probe.resource}
		operation := &pb.Command_Read{Read: read}
		command := &pb.Command{Operation: operation}
		request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
		err = stream.Send(request)
		if err == nil {
			response, receiveErr := stream.Recv()
			err = receiveErr
			if err == nil {
				result := response.GetEvent().GetReadResult()
				if result.GetDocument() == nil || result.GetFailure() != nil {
					err = fmt.Errorf("read failed before deliberate cancellation: %v", result)
				} else {
					err = validateReliabilityDocument(result.GetDocument(), 64)
				}
			}
		}
		cancel()
		if err != nil {
			t.Fatal("stream failed before deliberate cancellation", err)
		}
		_, _ = stream.Recv()
	}
}
