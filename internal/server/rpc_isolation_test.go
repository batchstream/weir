package server

import (
	"context"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func TestRPCExpiryKeepsSharedConnectionAndMutation(t *testing.T) {
	for _, mode := range []string{"caller-deadline", "initial-input-stall", "idle-input-stall"} {
		t.Run(mode, func(t *testing.T) {
			gate := make(chan struct{})
			defer func() {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}()
			firstAdapter, firstStore := localStore(t, "records")
			secondAdapter, secondStore := localStore(t, "mutations")
			secondAdapter.block = gate
			limits := DefaultLimits()
			limits.Stall = 2 * time.Second

			expected := codes.DeadlineExceeded
			if mode == "initial-input-stall" || mode == "idle-input-stall" {
				limits.Stall = 150 * time.Millisecond
				expected = codes.Canceled
			} else {
				firstAdapter.block = gate
			}
			opts := testServerOptions{stores: map[string]*store.Runtime{"records": firstStore, "mutations": secondStore}, limits: limits}
			server, address := startTestServer(t, opts)
			conn, client := testClient(t, address)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			firstContext := ctx
			if mode == "caller-deadline" {
				var stopFirst context.CancelFunc
				firstContext, stopFirst = context.WithTimeout(ctx, 250*time.Millisecond)
				defer stopFirst()
			}
			first, err := testutil.Execute(firstContext, client)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "initial-input-stall" {
				request := readFrame(1, testRequest())
				if mode != "idle-input-stall" {
					mutation := testMutation("expired")
					operation := &pb.Command_Mutate{Mutate: mutation}
					request.Command = &pb.Command{Operation: operation}
				}
				if err := first.Send(request); err != nil {
					t.Fatal(err)
				}
				select {
				case <-firstAdapter.seen:
				case <-ctx.Done():
					t.Fatal("first RPC did not reach its backend")
				}
				if mode == "idle-input-stall" {
					response, err := first.Recv()
					if err != nil || response.GetIndex() != 1 {
						t.Fatal("first result was not delivered", response, err)
					}
				}
			} else {
				until := time.Now().Add(time.Second)
				for server.admission.activeRPCs.Load() != 1 {
					if time.Now().After(until) {
						t.Fatal("empty RPC was not admitted")
					}
					time.Sleep(time.Millisecond)
				}
			}
			type expiryResult struct {
				err        error
				returnedAt time.Time
			}
			// Observe expiry immediately, even while the second RPC is opening.
			// Delaying Recv until afterward could hide an early cancellation.
			firstDone := make(chan expiryResult, 1)
			go func() {
				for {
					_, err := first.Recv()
					if err != nil {
						result := expiryResult{err: err, returnedAt: time.Now()}
						firstDone <- result
						return
					}
				}
			}()
			second, err := testutil.Execute(ctx, client)
			if err != nil {
				t.Fatal(err)
			}
			mutation := testMutation("confirmed")
			operation := &pb.Command_Mutate{Mutate: mutation}
			command := &pb.Command{Operation: operation}
			request := &pb.ExecuteRequest{StoreName: "mutations", Index: 1, Command: command}
			if err := second.Send(request); err != nil {
				t.Fatal(err)
			}
			if err := second.CloseSend(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-secondAdapter.seen:
			case <-ctx.Done():
				t.Fatal("surviving mutation did not reach its backend")
			}
			var expired expiryResult
			select {
			case expired = <-firstDone:
			case <-ctx.Done():
				t.Fatal("first RPC did not finish within the shared caller bound", ctx.Err())
			}
			actual := status.Code(expired.err)
			if mode == "caller-deadline" {
				deadline, ok := firstContext.Deadline()
				if !ok || expired.returnedAt.Before(deadline) || expired.returnedAt.After(deadline.Add(time.Second)) {
					t.Fatal("first RPC expired outside its original deadline window", expired.err, expired.returnedAt, deadline)
				}
				timer := time.NewTimer(time.Until(deadline.Add(time.Second)))
				defer timer.Stop()
				select {
				case <-firstContext.Done():
				case <-timer.C:
					t.Fatal("original caller deadline did not fire")
				}
				if firstContext.Err() != context.DeadlineExceeded || actual != expected && actual != codes.Canceled {
					t.Fatal("first RPC did not end with its own expiry", expired.err, firstContext.Err())
				}
				// Native gRPC's deadline timer can cancel its server context before
				// its context timer runs, yielding a Canceled trailer. The local
				// cause and independently recorded return time must still prove expiry.
			} else if actual != expected {
				t.Fatal("first RPC did not end with its own input stall", expired.err)
			}
			close(gate)
			response, err := second.Recv()
			if err != nil || response.GetEvent().GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("expired RPC lost the other mutation's confirmation", response, err)
			}
			if _, err := second.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			waitIdle(t, server)
			// A caller deadline can race an already admitted write. It must never
			// replay that write, and the unrelated RPC keeps its confirmation.
			if conn.GetState() != connectivity.Ready || firstAdapter.commands.Load() > 1 || secondAdapter.commands.Load() != 1 {
				t.Fatal("expiry closed the shared connection or repeated a mutation", conn.GetState(), firstAdapter.commands.Load(), secondAdapter.commands.Load())
			}
		})
	}
}

// The large Read cannot be encoded; the small mutation result still fits.
