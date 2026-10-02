package search

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func unsentWritePlan() *execution.Plan {
	document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:write", Action: action}
	variant := &pb.Operation_Mutate{Mutate: mutation}
	operation := &pb.Operation{Operation: variant}
	work := &execution.Plan{Operation: operation}
	return work
}

func unsentBulkCall() exchange {
	call := exchange{
		path: "/_bulk", body: []byte("{\"index\":{\"_index\":\"records\",\"_id\":\"write\"}}\n{\"n\":1}\n"),
		limit: responseLimit, mutation: true,
	}
	return call
}

func TestBulkCanceledWhileWaitingForConnectionWasNotApplied(t *testing.T) {
	started := make(chan struct{})
	var writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writes.Add(1) == 1 {
			close(started)
		}
		_, _ = io.ReadAll(r.Body)
		<-r.Context().Done()
	})
	server := httptest.NewServer(handler)
	transport := newTransport(1)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{URL: server.URL}
	adapter := &Adapter{config: config, client: client, ctx: context.Background()}
	first, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	defer func() {
		stopFirst()
		<-firstDone
		transport.CloseIdleConnections()
		server.Close()
	}()
	call := unsentBulkCall()
	go func() {
		_, _, _ = adapter.request(first, call)
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first bulk did not occupy the only connection")
	}
	queued := make(chan struct{})
	trace := &httptrace.ClientTrace{GetConn: func(string) { close(queued) }}
	second, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	second = httptrace.WithClientTrace(second, trace)
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := adapter.request(second, call)
		secondDone <- err
	}()
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("second bulk did not wait for a connection")
	}
	stopSecond()
	var err error
	select {
	case err = <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled pool wait did not terminate")
	}
	if err != errWriteNotSent || writes.Load() != 1 {
		t.Fatal("unstarted bulk was uncertain or reached the server", err, writes.Load())
	}
	work := unsentWritePlan()
	works := []*execution.Plan{work}
	results, feedback := adapter.bulkResults(works, 0, nil, err)
	if results[0].GetMutation().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || feedback != execution.Neutral {
		t.Fatal("connection queue cancellation became a committed write or congestion", results, feedback)
	}
}

type untrustedTraceTransport struct {
	announceConnection bool
}

func (t *untrustedTraceTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	trace := httptrace.ContextClientTrace(request.Context())
	trace.GetConn("example:80")
	if t.announceConnection {
		info := httptrace.GotConnInfo{}
		trace.GotConn(info)
	}
	return nil, errors.New("custom transport request failed")
}

func TestBulkCustomTransportCannotProveAnUnsentWrite(t *testing.T) {
	for _, connected := range []bool{false, true} {
		transport := &untrustedTraceTransport{announceConnection: connected}
		client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
		config := Config{URL: "http://example:80"}
		adapter := &Adapter{config: config, client: client, ctx: context.Background()}
		call := unsentBulkCall()
		_, _, err := adapter.request(context.Background(), call)
		if err == nil || err == errWriteNotSent {
			t.Fatal("untrusted trace became proof that a write was not sent", connected, err)
		}
		work := unsentWritePlan()
		works := []*execution.Plan{work}
		results, feedback := adapter.bulkResults(works, 0, nil, err)
		if results[0].GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
			t.Fatal("custom transport uncertainty became successful or unapplied", results, feedback)
		}
	}
}

func TestBulkConnectionAcquiredBeforeWriteFailureRemainsUnknown(t *testing.T) {
	transport := newTransport(1)
	defer transport.CloseIdleConnections()
	var dials atomic.Int32
	transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		local, remote := net.Pipe()
		_ = remote.Close()
		return local, nil
	}
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{URL: "http://example:80"}
	adapter := &Adapter{config: config, client: client, ctx: context.Background()}
	call := unsentBulkCall()
	_, _, err := adapter.request(context.Background(), call)
	if err == nil || err == errWriteNotSent || dials.Load() != 1 {
		t.Fatal("connection acquisition became proof of no write", err, dials.Load())
	}
	work := unsentWritePlan()
	works := []*execution.Plan{work}
	results, feedback := adapter.bulkResults(works, 0, nil, err)
	if results[0].GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
		t.Fatal("uncertain connected failure became unapplied", results, feedback)
	}
}

func TestBulkBeforeConnectionFailureWasNotApplied(t *testing.T) {
	for _, mode := range []string{"cancel_before_do", "dial_failure"} {
		t.Run(mode, func(t *testing.T) {
			transport := newTransport(1)
			defer transport.CloseIdleConnections()
			var dials atomic.Int32
			transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("test dial refused")
			}
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			config := Config{URL: "http://127.0.0.1:1"}
			adapter := &Adapter{config: config, client: client, ctx: context.Background()}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel_before_do" {
				cancel()
			}
			call := unsentBulkCall()
			_, _, err := adapter.request(ctx, call)
			if err != errWriteNotSent || mode == "cancel_before_do" && dials.Load() != 0 {
				t.Fatal("write before connection establishment was uncertain", err, dials.Load())
			}
			work := unsentWritePlan()
			works := []*execution.Plan{work}
			results, feedback := adapter.bulkResults(works, 0, nil, err)
			if results[0].GetMutation().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || feedback != execution.Neutral {
				t.Fatal("unsent write changed evidence or capacity", results, feedback)
			}
		})
	}
}

func TestBulkCommittedReplyLossRemainsUnknownWithoutReplay(t *testing.T) {
	var committed atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/warm" {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !strings.Contains(string(body), `"_id":"write"`) {
			t.Error("write body unavailable", err, string(body))
			return
		}
		committed.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	transport := newTransport(1)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{URL: server.URL}
	adapter := &Adapter{config: config, client: client, ctx: context.Background()}
	warm := exchange{path: "/warm", limit: 1024}
	_, _, err := adapter.request(context.Background(), warm)
	if err != nil {
		t.Fatal(err)
	}
	call := unsentBulkCall()
	_, _, err = adapter.request(context.Background(), call)
	if err == nil || err == errWriteNotSent || committed.Load() != 1 {
		t.Fatal("a reused connection's committed write was lost or replayed", err, committed.Load())
	}
	work := unsentWritePlan()
	works := []*execution.Plan{work}
	results, feedback := adapter.bulkResults(works, 0, nil, err)
	if results[0].GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
		t.Fatal("lost acknowledgement changed mutation certainty", results, feedback)
	}
}
