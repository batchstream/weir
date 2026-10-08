package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type scanSearchRequest struct {
	PIT struct {
		ID string
	}
	Size  int
	After []int64 `json:"search_after"`
}

func scanBatchHit(position int, size int) json.RawMessage {
	source := fmt.Sprintf(`{"n":%d,"pad":""}`, position)
	if size > len(source) {
		source = strings.Replace(source, `"pad":""`, `"pad":"`+strings.Repeat("x", size-len(source))+`"`, 1)
	}
	raw := fmt.Sprintf(`{"_index":"records","_id":"%d","_score":null,"_source":%s,"sort":[%d]}`, position, source, position)
	return json.RawMessage(raw)
}

func scanBatchReply(rows []json.RawMessage) []byte {
	encodedRows, _ := json.Marshal(rows)
	if rows == nil {
		encodedRows = []byte("[]")
	}
	raw := fmt.Sprintf(`{"pit_id":"latest","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":%s}}`, encodedRows)
	return []byte(raw)
}

func scanBatchAdapter(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := newTransport()
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	adapter := &Adapter{
		dialect: ElasticsearchProduct,
		config:  Config{Store: "search", URL: server.URL},
		ctx:     context.Background(),
		client:  client,
	}
	return adapter
}

func TestScanFetchUsesBatchesAndRemainingPage(t *testing.T) {
	var sizes []int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request scanSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		sizes = append(sizes, request.Size)
		start := 0
		if len(request.After) != 0 {
			start = int(request.After[0]) + 1
		}
		var rows []json.RawMessage
		for position := start; position < min(start+request.Size, 300); position++ {
			rows = append(rows, scanBatchHit(position, 0))
		}
		_, _ = w.Write(scanBatchReply(rows))
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records", PageSize: 250}
	work, failure := adapter.prepareScan(request)
	if failure != nil || work.ResultBytes != execution.ScanResultBytes || work.WorkingBytes != scanWorkingBytes {
		t.Fatal("Scan reservation", failure, work)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit = true, "previous"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var token []byte
	for step := 0; step < 2; step++ {
		page := adapter.fetchScan(ctx, work)
		if page.Failure != nil || page.Exhausted || page.Complete != (step == 1) {
			t.Fatal("batched fetch", step, page)
		}
		for offset, document := range page.Documents {
			var hit struct {
				N *int64 `json:"n"`
			}
			if json.Unmarshal(document.Data, &hit) != nil || hit.N == nil || *hit.N != int64(state.Count)+int64(offset) {
				t.Fatal("lost or duplicate hit", document)
			}
		}
		state.Count += uint64(len(page.Documents))
		token = page.NextContinuationToken
	}
	wantSizes := []int{128, 122}
	if !reflect.DeepEqual(sizes, wantSizes) || state.Count != 250 || state.after != 249 || len(token) == 0 {
		t.Fatal("batch efficiency/page checkpoint", sizes, state.Count, state.after)
	}
	request.ContinuationToken = token
	resumed, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	resumedState := resumed.Backend.(*scanPlan)
	if resumedState.batchSize != 128 || resumedState.after != 249 || !resumedState.resumed {
		t.Fatal("checkpoint lost bounded fetch size", resumedState)
	}
	for step := 0; step < 2; step++ {
		page := adapter.fetchScan(ctx, resumed)
		if page.Failure != nil {
			t.Fatal(page.Failure)
		}
		resumedState.Count += uint64(len(page.Documents))
		if page.Exhausted {
			if resumedState.Count != 50 || step != 1 {
				t.Fatal("resume omissions", resumedState.Count, step)
			}
			return
		}
	}
	t.Fatal("resume did not exhaust")
}

func TestScanRetainsValidatedPrefixAndAdvancesAcceptedCheckpoint(t *testing.T) {
	for _, mode := range []string{"valid_tail", "invalid_tail", "oversized_tail"} {
		t.Run(mode, func(t *testing.T) {
			adapter := &Adapter{dialect: ElasticsearchProduct}
			state := &scanPlan{index: "records", items: 3, batchSize: 3, pit: "previous", after: 7, hasAfter: true}
			rows := []json.RawMessage{
				scanBatchHit(8, protocol.MaxDocument),
				scanBatchHit(9, protocol.MaxDocument),
				scanBatchHit(10, protocol.MaxDocument),
			}
			switch mode {
			case "invalid_tail":
				rows[2] = json.RawMessage(strings.Replace(string(rows[2]), `"_index":"records"`, `"_index":"other"`, 1))
			case "oversized_tail":
				rows[2] = scanBatchHit(10, protocol.MaxDocument+1)
			}
			page := adapter.scanReply(scanBatchReply(rows), state)
			if mode != "valid_tail" {
				if page.Failure == nil || len(page.Documents) != 0 || state.after != 7 || page.Exhausted {
					t.Fatal("invalid discarded tail changed checkpoint", page, state.after)
				}
				return
			}
			if page.Failure != nil || page.Exhausted || len(page.Documents) != 2 || state.after != 9 || state.batchSize != 2 {
				t.Fatal("bounded ordered prefix", page.Failure, len(page.Documents), state.after, state.batchSize)
			}
			retainedBytes := 0
			for _, document := range page.Documents {
				retainedBytes += len(document.Data)
			}
			if retainedBytes != execution.ScanBatchBytes {
				t.Fatal("retained output byte bound", retainedBytes)
			}
			nextRows := []json.RawMessage{scanBatchHit(10, 0)}
			next := adapter.scanReply(scanBatchReply(nextRows), state)
			if next.Failure != nil || len(next.Documents) != 1 || state.after != 10 {
				t.Fatal("discarded tail skipped on resume", next)
			}
		})
	}
}

func TestScanDownsizesExcessiveResponsesWithoutAdvancing(t *testing.T) {
	var sizes []int
	var positions []int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request scanSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		sizes = append(sizes, request.Size)
		positions = append(positions, request.After[0])
		if request.Size > 2 {
			w.Header().Set("Content-Length", fmt.Sprint(responseLimit+1))
			w.WriteHeader(http.StatusOK)
			return
		}
		rows := []json.RawMessage{
			scanBatchHit(int(request.After[0])+1, 0),
			scanBatchHit(int(request.After[0])+2, 0),
		}
		_, _ = w.Write(scanBatchReply(rows))
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records", PageSize: 8}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit, state.after, state.hasAfter = true, "previous", 7, true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for step := 0; step < 2; step++ {
		page := adapter.fetchScan(ctx, work)
		if page.Failure != nil || len(page.Documents) != 2 || page.Exhausted {
			t.Fatal("downsize lost bounded result", page)
		}
		state.Count += uint64(len(page.Documents))
	}
	wantSizes := []int{8, 4, 2, 2}
	wantPositions := []int64{7, 7, 7, 9}
	if !reflect.DeepEqual(sizes, wantSizes) || !reflect.DeepEqual(positions, wantPositions) || state.after != 11 || state.batchSize != 2 {
		t.Fatal("read retry changed checkpoint or forgot size", sizes, positions, state)
	}
}

func TestScanSingleExcessiveResponseFailsWithoutReplay(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Length", fmt.Sprint(responseLimit+1))
		w.WriteHeader(http.StatusOK)
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records", PageSize: 1}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit, state.after, state.hasAfter = true, "previous", 7, true
	page := adapter.fetchScan(context.Background(), work)
	if page.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || len(page.Documents) != 0 || state.after != 7 || calls != 1 {
		t.Fatal("single excessive result was retried or advanced", page, calls, state.after)
	}
}

func TestScanDownsizingRetainsOriginalDeadline(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(responseLimit+1))
			w.WriteHeader(http.StatusOK)
			return
		}
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records", PageSize: 128}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit, state.after, state.hasAfter = true, "previous", 7, true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	page := adapter.fetchScan(ctx, work)
	if page.Failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || len(page.Documents) != 0 || state.after != 7 || calls.Load() != 2 {
		t.Fatal("retry refreshed the deadline or advanced", page, calls.Load(), state.after)
	}
}

func TestScanStructuredSourcesUsePerHitJSONBudget(t *testing.T) {
	values := strings.Repeat("0,", 512) + "0"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []json.RawMessage
		for position := 0; position < execution.ScanBatchDocuments; position++ {
			hit := scanBatchHit(position, 0)
			hit = json.RawMessage(strings.Replace(string(hit), `{"n":0,"pad":""}`, `{"values":[`+values+`]}`, 1))
			rows = append(rows, hit)
		}
		_, _ = w.Write(scanBatchReply(rows))
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records"}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit = true, "previous"
	page := adapter.fetchScan(context.Background(), work)
	if page.Failure != nil || len(page.Documents) != execution.ScanBatchDocuments || !page.Complete {
		t.Fatal("valid structured batch used a singleton JSON limit", page.Failure, len(page.Documents))
	}
	excessiveValues := strings.Repeat("0,", 16384) + "0"
	hit := scanBatchHit(0, 0)
	hit = json.RawMessage(strings.Replace(string(hit), `{"n":0,"pad":""}`, `{"values":[`+excessiveValues+`]}`, 1))
	state.hasAfter = false
	rows := []json.RawMessage{hit}
	page = adapter.scanReply(scanBatchReply(rows), state)
	if page.Failure == nil || len(page.Documents) != 0 {
		t.Fatal("per-hit JSON complexity bound disappeared", page)
	}
}

func TestScanCheckpointContainsTraversalStateOnly(t *testing.T) {
	config := Config{Store: "search"}
	adapter := &Adapter{dialect: ElasticsearchProduct, config: config}
	for _, mode := range []string{"valid", "missing_pit", "negative_after", "internal_capacity", "extra", "duplicate", "wrong_profile"} {
		t.Run(mode, func(t *testing.T) {
			request := &pb.ScanRequest{Resource: "records"}
			profile := adapter.scanProfile()
			fingerprint := protocol.ScanFingerprint(request, "search", profile)
			checkpoint := scanCheckpoint{PIT: "previous", After: 7}
			state, _ := json.Marshal(checkpoint)
			switch mode {
			case "missing_pit":
				state = []byte(`{"pit":"","after":7}`)
			case "negative_after":
				state = []byte(`{"pit":"previous","after":-1}`)
			case "internal_capacity":
				state = []byte(`{"pit":"previous","after":7,"batch_size":3}`)
			case "extra":
				state = []byte(`{"pit":"previous","after":7,"unknown":true}`)
			case "duplicate":
				state = []byte(`{"pit":"previous","after":7,"after":8}`)
			case "wrong_profile":
				profile = "search:" + adapter.dialect + ":v2"
			}
			token, err := protocol.EncodeScanToken(profile, fingerprint, state)
			if err != nil {
				t.Fatal(err)
			}
			request.ContinuationToken = token
			work, failure := adapter.prepareScan(request)
			if mode != "valid" {
				if failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
					t.Fatal("invalid checkpoint reached backend work", mode, failure)
				}
				return
			}
			if failure != nil {
				t.Fatal(failure)
			}
			resumed := work.Backend.(*scanPlan)
			if resumed.batchSize != execution.ScanBatchDocuments || resumed.pit != "previous" || resumed.after != 7 || !resumed.resumed {
				t.Fatal("checkpoint lost traversal state or inherited internal tuning", resumed)
			}
		})
	}
}

func TestScanPublicationFailureEndsWithInternalFailure(t *testing.T) {
	rows := []json.RawMessage{scanBatchHit(0, 0), scanBatchHit(1, 0)}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(scanBatchReply(rows))
	})
	adapter := scanBatchAdapter(t, handler)
	request := &pb.ScanRequest{Resource: "records"}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	state.opened, state.pit = true, "previous"
	var end *pb.ScanEnd
	failed := errors.New("bounded result collector rejected output")
	emit := func(_ *execution.Plan, event *pb.Event) error {
		if event.GetDocument() != nil {
			return failed
		}
		end = event.GetScanEnd()
		return nil
	}
	continuation := adapter.streamScan(context.Background(), work, emit)
	if continuation || end == nil || end.Failure.GetCode() != pb.FailureCode_INTERNAL || end.DocumentCount != 0 || end.Exhausted || len(end.NextContinuationToken) != 0 {
		t.Fatal("collector failure became cancellation or successful terminal", continuation, end)
	}
}

func TestScanOpenAndFetchShareDeadlineAndCleanup(t *testing.T) {
	for _, phase := range []string{"success", "cumulative_deadline", "caller_canceled"} {
		t.Run(phase, func(t *testing.T) {
			var searches, cleanups atomic.Int32
			started := make(chan struct{}, 1)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				switch r.URL.Path {
				case "/records":
					_, _ = io.WriteString(w, testIndexReply)
				case "/records/_pit":
					if phase == "cumulative_deadline" {
						time.Sleep(120 * time.Millisecond)
					}
					_, _ = io.WriteString(w, `{"id":"opened","_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
				case "/_search":
					count := searches.Add(1)
					if phase == "caller_canceled" {
						started <- struct{}{}
						<-r.Context().Done()
						return
					}
					if phase == "cumulative_deadline" {
						timer := time.NewTimer(120 * time.Millisecond)
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-r.Context().Done():
							return
						}
					}
					var rows []json.RawMessage
					if count == 1 {
						rows = append(rows, scanBatchHit(0, 0))
					}
					_, _ = w.Write(scanBatchReply(rows))
				case "/_pit":
					if r.Method != http.MethodDelete {
						t.Error("unexpected PIT cleanup method", r.Method)
					}
					cleanups.Add(1)
					_, _ = io.WriteString(w, `{"succeeded":true,"num_freed":1}`)
				default:
					t.Error("unexpected Scan request", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			adapter := scanBatchAdapter(t, handler)
			request := &pb.ScanRequest{Resource: "records", PageSize: 2}
			variant := &pb.Command_Scan{Scan: request}
			command := &pb.Command{Operation: variant}
			work, failure := adapter.PrepareCommand(1, command)
			if failure != nil {
				t.Fatal(failure)
			}

			deadline := time.Second
			if phase == "cumulative_deadline" {
				deadline = 180 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()
			var end *pb.ScanEnd
			documents := 0
			emit := func(_ *execution.Plan, event *pb.Event) error {
				if event.GetDocument() != nil {
					documents++
				} else {
					end = event.GetScanEnd()
				}
				return nil
			}
			var continuation bool
			if phase == "caller_canceled" {
				done := make(chan bool, 1)
				go func() { done <- adapter.Execute(ctx, []*execution.Plan{work}, emit) }()
				select {
				case <-started:
					cancel()
				case <-ctx.Done():
					t.Fatal("first Scan fetch did not start")
				}
				continuation = <-done
			} else {
				continuation = adapter.Execute(ctx, []*execution.Plan{work}, emit)
			}
			if phase == "success" {
				if !continuation || documents != 1 || end != nil || searches.Load() != 1 {
					t.Fatal("PIT opening became an empty scheduler step", continuation, documents, end, searches.Load())
				}
				continuation = adapter.Execute(ctx, []*execution.Plan{work}, emit)
				if continuation || end.GetFailure() != nil || !end.GetExhausted() || end.GetDocumentCount() != 1 {
					t.Fatal("Scan terminal evidence changed", continuation, end)
				}
			} else {
				code := pb.FailureCode_DEADLINE_EXCEEDED
				if phase == "caller_canceled" {
					code = pb.FailureCode_CANCELLED
				}
				if continuation || documents != 0 || end.GetFailure().GetCode() != code || end.GetExhausted() || len(end.GetNextContinuationToken()) != 0 || searches.Load() != 1 {
					t.Fatal("Scan restarted its deadline or continued after cancellation", continuation, documents, end, searches.Load())
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			failure = adapter.ClosePlan(cleanup, work)
			stop()
			if failure != nil || cleanups.Load() != 1 {
				t.Fatal("allocated PIT was not cleaned up", failure, cleanups.Load())
			}
		})
	}
}
