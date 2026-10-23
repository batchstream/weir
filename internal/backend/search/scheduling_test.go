package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
)

func TestSearchQueuedReadCompletesBeforeUnrelatedSlowWrite(t *testing.T) {
	occupyStarted := make(chan struct{})
	releaseOccupy := make(chan struct{})
	bulkStarted := make(chan struct{})
	releaseBulk := make(chan struct{})
	readResponded := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			var input struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			if len(input.IDs) != 1 {
				t.Error("unexpected read IDs", input.IDs)
				return
			}
			if input.IDs[0] == "occupy" {
				close(occupyStarted)
				select {
				case <-releaseOccupy:
				case <-ctx.Done():
					return
				}
			}
			_, _ = fmt.Fprintf(w, `{"docs":[{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`, input.IDs[0])
			if input.IDs[0] == "read" {
				close(readResponded)
			}
		case "/_bulk":
			close(bulkStarted)
			select {
			case <-releaseBulk:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"write","status":200,"result":"updated","_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		default:
			t.Error("unexpected request", request.URL.Path)
			http.NotFound(w, request)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	// Cancel before closing the HTTP server, including failed assertions with a
	// blocked handler. The explicit gates make queue occupancy deterministic.
	defer cancel()
	config := Config{Store: "search", URL: server.URL}
	adapter, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.MaxInFlight = 1
	limits.BatchWait = 0
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	submit := func(caller context.Context, resource string, read bool) *store.Ticket {
		command := &pb.Command{}
		if read {
			request := &pb.ReadRequest{Resource: resource}
			command.Operation = &pb.Command_Read{Read: request}
		} else {
			document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":2}`)}
			action := &pb.MutateRequest_Put{Put: document}
			request := &pb.MutateRequest{Resource: resource, Action: action}
			command.Operation = &pb.Command_Mutate{Mutate: request}
		}
		record, err := execution.NewRecord("search", 1, command)
		if err != nil {
			t.Fatal(err)
		}
		work, failure := runtime.PrepareRecord(caller, record)
		if failure != nil {
			t.Fatal(failure)
		}
		session := runtime.NewSession()
		ticket, failure := runtime.Submit(caller, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		return ticket
	}
	occupy := submit(ctx, "records/s:occupy", true)
	select {
	case <-occupyStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	readContext, stopRead := context.WithTimeout(ctx, time.Second)
	defer stopRead()
	read := submit(readContext, "records/s:read", true)
	write := submit(ctx, "records/s:write", false)
	close(releaseOccupy)
	if _, err := occupy.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readResponded:
	case <-ctx.Done():
		t.Fatal("read did not receive its backend response", ctx.Err())
	}
	event, err := read.Wait(readContext)
	if err != nil || event.GetReadResult().GetDocument() == nil {
		t.Fatal("successful read was held by an unrelated same-index write", event, err)
	}
	select {
	case <-bulkStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(releaseBulk)
	event, err = write.Wait(ctx)
	if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("independent write lost its confirmed outcome", event, err)
	}
}
