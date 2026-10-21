//go:build integration

package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func TestQualificationStreamFailurePublicationJoins33Producers(t *testing.T) {
	failures := make(chan qualificationStreamFailure, 1)
	firstPublished := make(chan struct{})
	var pending sync.WaitGroup
	for producer := range 33 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			if producer != 0 {
				<-firstPublished
			}
			failure := qualificationStreamFailure{index: producer, err: errors.New("sanitized")}
			publishQualificationStreamFailure(failures, failure)
			if producer == 0 {
				close(firstPublished)
			}
		}()
	}
	joined := make(chan struct{})
	go func() {
		pending.Wait()
		close(joined)
	}()
	// Do not drain until every producer has joined, matching the slow-consumer
	// coordinator. The first publication must survive the other 32 producers.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-joined:
	case <-timer.C:
		t.Fatal("33 failure producers did not join before channel drain")
	}
	if len(failures) != 1 || cap(failures) != 1 {
		t.Fatal("failure publication was not bounded to one result")
	}
	if first := <-failures; first.index != 0 {
		t.Fatalf("first failure replaced by producer %d", first.index)
	}
}

func TestQualificationNativeFailureClassification(t *testing.T) {
	const secret = "private-payload-id-backend-error"
	backendFailure := &weirclient.Failure{Code: weirclient.FailureUnavailable, Message: secret}
	partial := &weirclient.NativeResult{Completion: weirclient.NativeResponseIncomplete, Failure: backendFailure}
	complete := &weirclient.NativeResult{Completion: weirclient.NativeResponseComplete}
	unknownFailure := &weirclient.Failure{Code: weirclient.FailureCode(127), Message: secret}
	unknown := &weirclient.NativeResult{Completion: weirclient.NativeCompletion(127), Failure: unknownFailure}
	tests := []struct {
		name           string
		result         *weirclient.NativeResult
		err            error
		code           codes.Code
		completion     weirclient.NativeCompletion
		failurePresent bool
		failureCode    weirclient.FailureCode
	}{
		{name: "nil result nil error", code: codes.OK, completion: weirclient.NativeCompletionUnconfirmed},
		{name: "transport failure", err: status.Error(codes.Unavailable, secret), code: codes.Unavailable, completion: weirclient.NativeCompletionUnconfirmed},
		{name: "opaque error", err: errors.New(secret), code: codes.Unknown, completion: weirclient.NativeCompletionUnconfirmed},
		{name: "business failure with OK transport", result: partial, code: codes.OK, completion: weirclient.NativeResponseIncomplete, failurePresent: true, failureCode: weirclient.FailureUnavailable},
		{name: "complete result transport failure", result: complete, err: status.Error(codes.DataLoss, secret), code: codes.DataLoss, completion: weirclient.NativeResponseComplete},
		{name: "complete result no failure", result: complete, code: codes.OK, completion: weirclient.NativeResponseComplete},
		{name: "unknown enums preserved numerically", result: unknown, code: codes.OK, completion: weirclient.NativeCompletion(127), failurePresent: true, failureCode: weirclient.FailureCode(127)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := qualificationNativeFailure(7, test.result, test.err)
			want := fmt.Sprintf("slow stream 7 failed: grpc_code=%s result_present=%t native_completion=%d failure_present=%t failure_code=%d", test.code, test.result != nil, test.completion, test.failurePresent, test.failureCode)
			if failure.index != 7 || failure.err.Error() != want {
				t.Fatalf("unexpected sanitized classification: %v", failure.err)
			}
			if strings.Contains(failure.err.Error(), secret) || errors.Unwrap(failure.err) != nil {
				t.Fatal("raw error or backend failure message retained")
			}
		})
	}
}

func TestQualificationReceiveStatsAttribution(t *testing.T) {
	start := time.Unix(100, 0)
	observer := &qualificationReceiveStats{start: start}
	ctx := context.WithValue(t.Context(), qualificationStreamKey, 7)
	info := &stats.RPCTagInfo{}
	ctx = observer.TagRPC(ctx, info)
	head := &pb.Event_Head{}
	chunk := &pb.Event_Chunk{}
	end := &pb.Event_NativeEnd{}
	events := []*pb.Event{{Value: head}, {Value: chunk}, {Value: end}}
	for i, event := range events {
		response := &pb.ExecuteResponse{Event: event}
		payload := &stats.InPayload{Payload: response, RecvTime: start.Add(time.Duration(i+1) * time.Second), Length: 101 + i, WireLength: 106 + i}
		observer.HandleRPC(ctx, payload)
	}
	observer.callback(ctx, "callback_enter", start.Add(4*time.Second))
	observer.callback(ctx, "callback_return", start.Add(5*time.Second))
	response := &pb.ExecuteResponse{Event: events[1]}
	payload := &stats.InPayload{Payload: response, RecvTime: start.Add(8 * time.Second), Length: 200, WireLength: 205}
	observer.HandleRPC(ctx, payload)
	observer.callback(ctx, "callback_enter", start.Add(9*time.Second))
	stream := &observer.streams[7]
	if stream.counts != [4]int64{1, 2, 1, 0} || stream.attributed != 1 || stream.maxGap != 4*time.Second || stream.preReceive != 3*time.Second || stream.postReceive != time.Second {
		t.Fatalf("incorrect attribution: %s", observer.summary(7))
	}
	if observer.streams[0].next != 0 {
		t.Fatal("RPC context attributed to wrong worker")
	}
	if !strings.Contains(observer.summary(7), "Length:200 WireLength:205") {
		t.Fatal("missing bounded length record")
	}
}

func TestQualificationReceiveStatsBoundedConcurrent(t *testing.T) {
	observer := &qualificationReceiveStats{start: time.Unix(100, 0)}
	var pending sync.WaitGroup
	for worker := range 64 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			ctx := context.WithValue(t.Context(), qualificationStreamKey, worker%32)
			payload := &stats.InPayload{RecvTime: observer.start}
			for range 100 {
				observer.HandleRPC(ctx, payload)
				observer.summary(worker % 32)
			}
		}()
	}
	pending.Wait()
	ignored := &stats.InPayload{}
	observer.HandleRPC(t.Context(), ignored)
	invalid := context.WithValue(t.Context(), qualificationStreamKey, 32)
	observer.HandleRPC(invalid, ignored)
	for i := range observer.streams {
		stream := &observer.streams[i]
		if stream.next != 200 || stream.counts[3] != 200 || len(stream.records) != 16 {
			t.Fatalf("unbounded or incorrect worker %d", i)
		}
	}
}
