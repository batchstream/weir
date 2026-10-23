package search

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/store"
)

// A filtered metadata response from the Elasticsearch 8.19.22 fixture for an
// index with a long n field and an n=1/pad1024 document. The server sent it
// chunked; the body is stored verbatim.
//
//go:embed testdata/read-memory-metadata-elasticsearch-8.19.22.json
var readMemoryMetadata []byte

const readMemoryIndex = "weir_typed_read_metadata_e141aab65f37417c99ee5376b110e94b"

func TestReadNormalMetadataBudgetBeforeDispatch(t *testing.T) {
	for _, spare := range []int{0, 32 << 10} {
		t.Run(fmt.Sprintf("spare=%d", spare), func(t *testing.T) {
			testReadMemoryQualification(t, spare, false, false)
		})
	}
}

func TestReadMetadataRealCapacityPreservesFailureCause(t *testing.T) {
	testReadMemoryQualification(t, 32<<10, true, false)
}

func TestReadNormalChunkedMetadataBudgetBeforeDispatch(t *testing.T) {
	testReadMemoryQualification(t, 0, false, true)
}

func TestReadWorkspaceSubsequentGroupReservesRetainedSourcesBeforeIO(t *testing.T) {
	for _, extra := range []int{0, 4096} {
		t.Run(fmt.Sprintf("extra=%d", extra), func(t *testing.T) {
			const base = 4096
			const reply = 64 << 10
			probe := &recordMemoryProbe{limit: base + reply + extra}
			memory := &recordMemory{owner: probe, base: base, reply: reply}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			source := []byte(`{"n":1,"pad":"` + strings.Repeat("x", 1024) + `"}`)
			atSend := make(chan int, 2)
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				probe.mu.Lock()
				working := probe.working
				probe.mu.Unlock()
				atSend <- working
				var input struct{ IDs []string }
				if err := json.NewDecoder(request.Body).Decode(&input); err != nil || len(input.IDs) != 1 {
					t.Error("expected one record per real read group", input.IDs, err)
					return
				}
				w.Header()["Date"] = nil
				_, _ = fmt.Fprintf(w, `{"docs":[{"_index":%q,"_id":%q,"_version":1,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}]}`, readMemoryIndex, input.IDs[0], source)
			})
			adapter := testHTTPAdapter(t, handler)
			options := backend.DefaultOptions()
			options.ExchangeBytes = 1
			adapter.config.Options = &options
			works := []*execution.Plan{
				batchTestPlan(t, adapter, "read", readMemoryIndex+"/s:first"),
				batchTestPlan(t, adapter, "read", readMemoryIndex+"/s:second"),
			}
			observed := adapter.mgetUnique(ctx, works)
			if observed[0].failure != nil || !bytes.Equal(observed[0].reply.Source, source) || <-atSend != base+reply {
				t.Fatal("first group did not retain its complete source inside the promised workspace", observed[0])
			}
			if memory.reply != reply || probe.working < base+reply || memory.sources < cap(observed[0].reply.Source) {
				t.Fatal("read IO promise or actual retained source was released between groups", memory, probe.working)
			}
			if extra == 0 {
				if calls.Load() != 1 || observed[1].failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || observed[1].reply != nil {
					t.Fatal("next group sent IO before its source-plus-scratch peak fit", calls.Load(), observed[1])
				}
			} else if calls.Load() != 2 || observed[1].failure != nil || !bytes.Equal(observed[1].reply.Source, source) || <-atSend < base+reply+cap(observed[0].reply.Source) {
				t.Fatal("next group lost source accounting or a complete document", calls.Load(), observed[1])
			}
		})
	}
}

func TestReadWorkspaceRejectsOverflowBeforeOwner(t *testing.T) {
	cases := []struct{ base, sources, scratch int }{
		{base: math.MaxInt},
		{sources: math.MaxInt, scratch: 1},
		{base: 1, sources: math.MaxInt},
	}
	for _, current := range cases {
		probe := &recordMemoryProbe{}
		memory := &recordMemory{owner: probe, base: current.base, reply: 64 << 10}
		err := memory.set(t.Context(), current.sources, 0, current.scratch)
		if !errors.Is(err, execution.ErrExecutionMemory) || probe.working != 0 {
			t.Fatal("read workspace overflow reached its owner", current, err, probe.working)
		}
	}
}

func prepareReadMemoryPlan(t *testing.T, runtime *store.Runtime, id uint64, key string) *execution.Plan {
	t.Helper()
	request := &pb.ReadRequest{Resource: readMemoryIndex + "/s:" + key}
	operation := &pb.Command_Read{Read: request}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", id, command)
	if err != nil {
		t.Fatal(err)
	}
	plan, failure := runtime.PrepareRecord(t.Context(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	return plan
}

func testReadMemoryQualification(t *testing.T, spare int, excessiveMetadata, chunked bool) {
	const natives = 4
	const nativeWorking = 24 << 20
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wait, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if got := fmt.Sprintf("%x", sha256.Sum256(readMemoryMetadata)); got != "0277f387485c7d79b1cec1326c6bab5280fc812a0f4d04968f0bb0bee7e0bd64" {
		t.Fatal("actual owned metadata sample changed", got)
	}
	sources := map[string][]byte{
		"staging": []byte(`{"n":0,"pad":"` + strings.Repeat("x", 24<<10) + `"}`),
		"first":   []byte(`{"n":1,"pad":"` + strings.Repeat("a", 1<<10) + `"}`),
		"second":  []byte(`{"n":2,"pad":"` + strings.Repeat("b", 1<<10) + `"}`),
	}
	holds := make(chan struct{}, natives)
	var metadataCalls, readCalls atomic.Int32
	memory := store.NewExecutionMemory()
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		// The sampled ES HTTP response contains no Date header.
		w.Header()["Date"] = nil
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
		case "/hold":
			holds <- struct{}{}
			<-ctx.Done()
		case "/" + readMemoryIndex:
			call := metadataCalls.Add(1)
			if request.URL.Query().Get("flat_settings") != "true" || request.URL.Query().Get("filter_path") != "error,status,*.aliases,*.settings,*.mappings._source,*.mappings._routing,*.data_stream" {
				t.Error("metadata qualification fields changed", request.URL.RawQuery)
				return
			}
			raw := readMemoryMetadata
			if excessiveMetadata && call > 1 {
				var extra strings.Builder
				for i := range 100 {
					fmt.Fprintf(&extra, `"extra_%03d":"v",`, i)
				}
				raw = bytes.Replace(raw, []byte(`"index.uuid":`), []byte(extra.String()+`"index.uuid":`), 1)
			}
			w.(http.Flusher).Flush()
			_, _ = w.Write(raw)
		case "/" + readMemoryIndex + "/_mget":
			readCalls.Add(1)
			var input struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			var reply strings.Builder
			reply.WriteString(`{"docs":[`)
			for i, id := range input.IDs {
				source, known := sources[id]
				if !known {
					t.Error("unexpected mget ID", id)
					return
				}
				if i != 0 {
					reply.WriteByte(',')
				}
				fmt.Fprintf(&reply, `{"_index":%q,"_id":%q,"_version":1,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}`, readMemoryIndex, id, source)
			}
			reply.WriteString(`]}`)
			if chunked {
				w.(http.Flusher).Flush()
			} else {
				w.Header().Set("Content-Length", strconv.Itoa(reply.Len()))
			}
			_, _ = io.WriteString(w, reply.String())
		default:
			t.Error("unexpected owned mock path", request.URL.Path)
			http.NotFound(w, request)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer cancel()
	config := Config{Store: "search", URL: server.URL}
	adapter, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.BatchWait = time.Hour
	limits.MaxInFlight = natives + 1
	runtime, err := store.New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	var sessions []*store.Session
	defer func() {
		cancel()
		for _, session := range sessions {
			session.Close()
		}
		closing, finish := context.WithTimeout(context.Background(), time.Second)
		defer finish()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
		if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
			t.Error("read ownership survived complete Runtime cleanup", snapshot)
		}
	}()
	plans := []*execution.Plan{
		prepareReadMemoryPlan(t, runtime, 1, "first"),
		prepareReadMemoryPlan(t, runtime, 2, "second"),
	}
	known := 0
	for _, plan := range plans {
		// Freeze B to the old read declaration; future forecasts cannot raise it.
		known += 2*plan.Bytes + execution.EntryOverheadBytes
	}
	publicationGrowth := natives * (2*protocol.NativeChunk + execution.ResultOverheadBytes - 2*execution.ResultOverheadBytes - execution.MinimumWireBytes)
	budget := uint64((32 << 20) + natives*nativeWorking + publicationGrowth + known + (64 << 10) + spare)
	sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: (3*budget + 1) / 2}
	memory.ObserveMemory(sample)
	staging := prepareReadMemoryPlan(t, runtime, 3, "staging")
	staged, failure, _ := runtime.Submit(ctx, staging, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := staged.Wait(wait)
	if err != nil || !bytes.Equal(event.GetReadResult().GetDocument().GetData(), sources["staging"]) {
		t.Fatal("real small retained result could not stage bounded scheduler pressure", event.GetReadResult().GetFailure(), err)
	}
	for i := range natives {
		request := nativeRequest(t, "", http.MethodGet, "/hold")
		operation := &pb.Command_Native{Native: request}
		command := &pb.Command{Operation: operation}
		plan, failure := runtime.PrepareCommand(ctx, uint64(4+i), command)
		if failure != nil || plan.WorkingBytes != nativeWorking {
			t.Fatal("unexpected Native initial window", failure)
		}
		session := runtime.NewSession()
		sessions = append(sessions, session)
		ticket, failure, _ := runtime.Submit(ctx, plan, session)
		if failure != nil {
			t.Fatal(failure)
		}
		go func() {
			for {
				select {
				case emission := <-session.Events:
					event := emission.Event
					emission.Release()
					if event == nil {
						ticket.Ack()
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	for range natives {
		select {
		case <-holds:
		case <-wait.Done():
			t.Fatal("Native initial requests did not reach the real HTTP hold", wait.Err(), memory.Snapshot())
		}
	}
	held := memory.Snapshot()
	if held.Starting != natives || held.NativeCount != natives || held.Terminal != 1 || held.ResidentBytes <= 32<<20 || held.WireBytes != 0 || held.BudgetRejections != 0 || runtime.Snapshot().WorkingBytes != natives*nativeWorking {
		t.Fatal("pressure was not initial Native IO plus one real retained result", held, runtime.Snapshot())
	}
	var tickets []*store.Ticket
	for _, plan := range plans {
		ticket, failure, _ := runtime.Submit(ctx, plan, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	if queued := runtime.Snapshot(); queued.Pending != 2 || queued.WorkingBytes != natives*nativeWorking || metadataCalls.Load() != 1 || memory.Snapshot().BudgetRejections != 0 {
		t.Fatal("both reads did not remain behind the actual retained result", queued, memory.Snapshot(), metadataCalls.Load())
	}
	t.Logf("fixedB=%d spare=%d knownInputs=%d oldIO=65536 nativeWorking=%d publicationGrowth=%d metadataWire=%d metadataDecode=%d staging=%+v queued=%+v", budget, spare, known, natives*nativeWorking, publicationGrowth, len(readMemoryMetadata), jsonDecodeWorking(readMemoryMetadata), held, memory.Snapshot())
	staged.Ack()
	good, unavailable, capacity := 0, 0, 0
	for i, ticket := range tickets {
		event, err := ticket.Wait(wait)
		if err != nil {
			t.Fatal("read did not complete after actual staged result release", err, memory.Snapshot())
		}
		result := event.GetReadResult()
		if document := result.GetDocument(); document != nil {
			key := []string{"first", "second"}[i]
			if !bytes.Equal(document.Data, sources[key]) || sha256.Sum256(document.Data) != sha256.Sum256(sources[key]) {
				t.Error("normal read document was incomplete", key, len(document.Data))
			}
			good++
		} else if result.GetFailure().GetCode() == pb.FailureCode_UNAVAILABLE {
			unavailable++
		} else if result.GetFailure().GetCode() == pb.FailureCode_RESOURCE_EXHAUSTED {
			capacity++
		} else {
			t.Error("unexpected read outcome", result)
		}
		ticket.Ack()
	}
	after := memory.Snapshot()
	metadata, gets := metadataCalls.Load()-1, readCalls.Load()-1
	t.Logf("complete reads: goodHash=%d unavailable=%d capacity=%d metadataAttempts=%d mgetAttempts=%d ledgerRejections=%d held=%+v", good, unavailable, capacity, metadata, gets, after.BudgetRejections, after)
	if excessiveMetadata {
		if good != 0 || capacity != 2 || unavailable != 0 || metadata != 1 || gets != 0 || after.BudgetRejections != 1 {
			t.Error("genuine metadata parser capacity lost its cause or sent a read", good, unavailable, capacity, metadata, gets, after)
		}
	} else if good != 2 || unavailable != 0 || capacity != 0 || metadata != 1 || gets != 1 || after.BudgetRejections != 0 {
		t.Error("normal two-document read lost its admitted IO/result budget", good, unavailable, capacity, metadata, gets, after)
	}
}
