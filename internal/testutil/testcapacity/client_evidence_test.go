package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
)

type evidencePeer struct {
	pb.UnimplementedStoreServiceServer
	outcome pb.MutationOutcome
	end     bool
	calls   atomic.Int64
}

func (p *evidencePeer) Execute(stream pb.StoreService_ExecuteServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	p.calls.Add(1)
	if p.outcome != pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED {
		mutation := &pb.MutationResult{Outcome: p.outcome}
		if p.outcome != pb.MutationOutcome_APPLIED {
			mutation.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE, Message: "fixture mutation evidence"}
		}
		value := &pb.Result_Mutation{Mutation: mutation}
		result := &pb.Result{Index: request.RequestId, Result: value}
		variant := &pb.Event_Result{Result: result}
		event := &pb.Event{Version: 1, Value: variant}
		var encoded bytes.Buffer
		_, err = protodelim.MarshalTo(&encoded, event)
		if err != nil {
			return err
		}
		response := &pb.ExecuteResponse{RequestId: request.RequestId, EventFragment: encoded.Bytes()}
		err = stream.Send(response)
		if err != nil {
			return err
		}
		if p.end {
			response = &pb.ExecuteResponse{RequestId: request.RequestId, RequestComplete: true}
			err = stream.Send(response)
			if err != nil {
				return err
			}
		}
	}
	return status.Error(codes.Unavailable, "fixture terminal unavailable")
}

func evidenceClient(t *testing.T, peer *evidencePeer) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterStoreServiceServer(server, peer)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	client, err := newClient("http://fixture.invalid", listener.Addr().String(), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestRPCFailureRetainsValidatedMutationEvidence(t *testing.T) {
	cases := []struct {
		outcome pb.MutationOutcome
		ledger  byte
	}{
		{outcome: pb.MutationOutcome_APPLIED, ledger: applied},
		{outcome: pb.MutationOutcome_NOT_APPLIED, ledger: notApplied},
		{outcome: pb.MutationOutcome_NOT_STARTED, ledger: notStarted},
		{outcome: pb.MutationOutcome_UNKNOWN, ledger: unknown},
		{outcome: pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED, ledger: unknown},
	}
	for _, tc := range cases {
		for _, end := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/end=%t", tc.outcome, end), func(t *testing.T) {
				peer := &evidencePeer{outcome: tc.outcome, end: end}
				client := evidenceClient(t, peer)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				op := Operation{Write: true, ID: "evidence"}
				result := client.Execute(ctx, op)
				if result.Outcome != tc.ledger || result.Class != "transport_Unavailable" || peer.calls.Load() != 1 {
					t.Fatal("RPC completion replaced write evidence or retried", result, peer.calls.Load())
				}
				metrics := Metrics{}
				metrics.finish(result, op, time.Now(), time.Now())
				wantUnknown := uint64(0)
				if tc.ledger == unknown {
					wantUnknown = 1
				}
				if metrics.Success != 0 || metrics.Failures["transport_Unavailable"] != 1 || metrics.Unknown != wantUnknown {
					t.Fatal("API failure counted as success or changed uncertainty", metrics)
				}
			})
		}
	}
}

func TestTrialLedgerKeepsAppliedAfterLostTerminal(t *testing.T) {
	for _, outcome := range []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED} {
		t.Run(outcome.String(), func(t *testing.T) {
			peer := &evidencePeer{outcome: outcome}
			client := evidenceClient(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			opts := TrialOptions{Rate: 10, Seconds: 1, Prefix: "lost-terminal", Workers: 1, WriteEvery: 1, ClientQueue: 8, ArrivalExpiryMS: 100, MaxCatchup: 512}
			trial, err := runTrial(ctx, client, opts)
			if err != nil {
				t.Fatal(err)
			}
			want := applied
			if outcome == pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED {
				want = unknown
			}
			for _, evidence := range trial.Ledger {
				if evidence != want {
					t.Fatal("trial overwrote irreversible evidence", trial.Ledger)
				}
			}
			if trial.Measure.All.Success != 0 || trial.Measure.All.Failures["transport_Unavailable"] != 10 || peer.calls.Load() != 10 {
				t.Fatal("lost terminal was hidden or replayed", trial.Measure.All, peer.calls.Load())
			}
		})
	}
}

func TestBoundedFailureDiagnosticsDoNotExposeArbitraryMessages(t *testing.T) {
	cases := []struct {
		err error
		tag string
	}{
		{err: status.Error(codes.Internal, "invalid execution emission"), tag: "invalid_execution_emission"},
		{err: status.Error(codes.Internal, "wrapped private-request: invalid execution emission"), tag: "invalid_execution_emission"},
		{err: status.Error(codes.Internal, "stream123: RST_STREAM with error code: INTERNAL_ERROR private-resource"), tag: "http2_internal_reset"},
		{err: status.Error(codes.Internal, "stream456: RST_STREAM with error code: INTERNAL_ERROR"), tag: "http2_internal_reset"},
		{err: context.DeadlineExceeded, tag: "deadline_exceeded"},
		{err: context.Canceled, tag: "canceled"},
	}
	for _, tc := range cases {
		if got := failureMessage(tc.err); got != tc.tag {
			t.Fatal(got, tc.tag)
		}
	}
	metrics := Metrics{}
	for n := 0; n < 100; n++ {
		err := errors.New(fmt.Sprintf("private-payload-secret-%d", n))
		message := failureMessage(err)
		if len(message) != 23 || strings.Contains(message, "private") || strings.Contains(message, "secret") {
			t.Fatal("unbounded or raw diagnostic", message)
		}
		result := Result{Class: "transport_Internal", Message: message}
		op := Operation{}
		metrics.finish(result, op, time.Now(), time.Now())
	}
	if len(metrics.ErrorMessages) != 17 || metrics.ErrorMessages["other"] != 84 || metrics.Failures["transport_Internal"] != 100 || metrics.Success != 0 {
		t.Fatal("unbounded diagnostics or lost failure counts", metrics.ErrorMessages)
	}
}
