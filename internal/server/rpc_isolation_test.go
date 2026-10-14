package server

import (
	"context"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
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
			firstAdapter, firstStore := peerLocal(t, "records")
			secondAdapter, secondStore := peerLocal(t, "mutations")
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
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": firstStore, "mutations": secondStore}, limits: limits}
			server, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			firstContext := ctx
			if mode == "caller-deadline" {
				var stopFirst context.CancelFunc
				firstContext, stopFirst = context.WithTimeout(ctx, 250*time.Millisecond)
				defer stopFirst()
			}
			first, err := client.Execute(firstContext)
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
				for server.Snapshot().ActiveRPCs != 1 {
					if time.Now().After(until) {
						t.Fatal("empty RPC was not admitted")
					}
					time.Sleep(time.Millisecond)
				}
			}
			second, err := client.Execute(ctx)
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
			var connection *limitedConn
			server.connections.Range(func(_, value any) bool { connection = value.(*limitedConn); return false })
			if connection == nil || server.admission.activeConnections.Load() != 1 {
				t.Fatal("RPCs did not share one connection")
			}
			for {
				_, err := first.Recv()
				if err != nil {
					if status.Code(err) != expected {
						t.Fatal("first RPC did not end with its own expiry", err)
					}
					break
				}
			}
			close(gate)
			response, err := second.Recv()
			if err != nil || response.GetEvent().GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("expired RPC lost the other mutation's confirmation", response, err)
			}
			if _, err := second.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			waitPeerIdle(t, server)
			// A caller deadline can race an already admitted write. It must never
			// replay that write, and the unrelated RPC keeps its confirmation.
			if connection.closed.Load() || firstAdapter.commands.Load() > 1 || secondAdapter.commands.Load() != 1 {
				t.Fatal("expiry closed the connection or repeated a mutation", connection.closed.Load(), firstAdapter.commands.Load(), secondAdapter.commands.Load())
			}
		})
	}
}

// The large Read cannot be encoded; the small mutation result still fits.
