package server

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func TestSubmitPlanDefinitiveCapacityRejectionDoesNotWait(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	adapter := &nativeStallAdapter{chunks: 1}
	memory := store.NewExecutionMemory()
	sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: 1}
	memory.ObserveMemory(sample)
	limits := store.DefaultLimits()
	local, err := store.New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close(context.Background())
	session := local.NewSession()
	defer session.Close()
	native := &pb.NativeRequest{Resource: "slow"}
	operation := &pb.Command_Native{Native: native}
	command := &pb.Command{Operation: operation}
	plan := &execution.Plan{ID: 1, Command: command, Bytes: execution.EntryOverheadBytes, ResultBytes: execution.ResultOverheadBytes}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ticket, failure := fixture.server.submitPlan(ctx, local, plan, session)
	if ticket != nil || failure == nil || failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED || ctx.Err() != nil {
		t.Fatal("definitive Native capacity rejection waited for a runtime change", ticket, failure, ctx.Err())
	}
	if memory.Snapshot().BudgetRejections != 1 || adapter.nativeStarted.Load() != 0 {
		t.Fatal("definitive rejection was retried or reached backend execution", memory.Snapshot(), adapter.nativeStarted.Load())
	}
}

type wireLifetimeFixture struct {
	server     *Server
	stats      transportStats
	connection context.Context
	rpc        context.Context
	cancel     context.CancelFunc
}

func newWireLifetimeFixture(t *testing.T) wireLifetimeFixture {
	t.Helper()
	limits := DefaultLimits()
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{limits: limits, admission: admission, metrics: newTransportMetrics()}
	handler := transportStats{}
	info := &stats.ConnTagInfo{}
	connection := handler.TagConn(context.Background(), info)
	rpc, cancel := context.WithCancel(connection)
	state := &rpcState{server: server, method: "execute", cancel: cancel, started: true}
	admission.activeRPCs.Add(1)
	rpc = context.WithValue(rpc, rpcKey, state)
	t.Cleanup(func() {
		cancel()
		state.finish(context.Canceled)
		end := &stats.ConnEnd{}
		handler.HandleConn(connection, end)
	})
	fixture := wireLifetimeFixture{server: server, stats: handler, connection: connection, rpc: rpc, cancel: cancel}
	return fixture
}

func wireLifetimeBuffer(t *testing.T, fixture wireLifetimeFixture) (mem.BufferSlice, *responseBufferOwner, *pb.ExecuteResponse) {
	t.Helper()
	chunk := &pb.Event_Chunk{Chunk: bytes.Repeat([]byte("x"), 64<<10)}
	event := &pb.Event{Value: chunk}
	response := &pb.ExecuteResponse{Index: 1, Event: event}
	codec := &responseCodec{admission: fixture.server.admission}
	encoded, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := fixture.server.admission.responses.Load(response)
	if !ok {
		t.Fatal("missing encoded response account")
	}
	owner := value.(*responseBufferOwner)
	return encoded, owner, response
}

func wireConnectionOwnerCount(connection *responseConnection) int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return len(connection.owners)
}

func TestWireLiveConnectionCancellationKeepsQueuedAccount(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	encoded, owner, response := wireLifetimeBuffer(t, fixture)
	defer encoded.Free()
	before := encoded.Materialize()
	charged := fixture.server.admission.wireBytes.Load()
	payload := &stats.OutPayload{Payload: response}
	fixture.stats.HandleRPC(fixture.rpc, payload)
	fixture.cancel()
	owner.detachMessage()
	end := &stats.End{Error: context.Canceled}
	fixture.stats.HandleRPC(fixture.rpc, end)
	connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
	if fixture.server.admission.wireBytes.Load() != charged || wireConnectionOwnerCount(connection) != 1 {
		t.Fatal("RPC cancellation retired bytes still owned by a live transport")
	}
	select {
	case <-owner.done:
		t.Fatal("RPC End completed queued ownership on a live connection")
	default:
	}
	connEnd := &stats.ConnEnd{}
	fixture.stats.HandleConn(fixture.connection, connEnd)
	if fixture.server.admission.wireBytes.Load() != 0 || wireConnectionOwnerCount(connection) != 0 {
		t.Fatal("final connection retirement retained queued accounts")
	}
	if !bytes.Equal(encoded.Materialize(), before) {
		t.Fatal("connection retirement freed or modified still-referenced encoded storage")
	}
	select {
	case <-owner.done:
	default:
		t.Fatal("connection retirement did not wake the output waiter")
	}
}

func TestWireNativeFreeUnlinksConnectionAccount(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	encoded, _, response := wireLifetimeBuffer(t, fixture)
	payload := &stats.OutPayload{Payload: response}
	fixture.stats.HandleRPC(fixture.rpc, payload)
	connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
	encoded.Ref()
	encoded.Free()
	if fixture.server.admission.wireBytes.Load() == 0 || wireConnectionOwnerCount(connection) != 1 {
		t.Fatal("a remaining native reference lost its queued account")
	}
	encoded.Free()
	if fixture.server.admission.wireBytes.Load() != 0 || wireConnectionOwnerCount(connection) != 0 {
		t.Fatal("final native Free retained a connection account")
	}
	connection.retire()
	if fixture.server.admission.wireBytes.Load() != 0 {
		t.Fatal("connection retirement returned native Free credit twice")
	}
}

func TestWireLatePayloadAfterConnectionRetirement(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "live_rpc_context"
		if canceled {
			name = "canceled_rpc_context"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newWireLifetimeFixture(t)
			encoded, owner, response := wireLifetimeBuffer(t, fixture)
			defer encoded.Free()
			connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
			connection.retire()
			if canceled {
				fixture.cancel()
			}
			payload := &stats.OutPayload{Payload: response}
			fixture.stats.HandleRPC(fixture.rpc, payload)
			owner.mu.Lock()
			watching := owner.timer != nil
			owner.mu.Unlock()
			if fixture.server.admission.wireBytes.Load() != 0 || watching || wireConnectionOwnerCount(connection) != 0 {
				t.Fatal("late OutPayload revived a retired connection account or watchdog")
			}
			if encoded.Len() == 0 {
				t.Fatal("late account retirement changed native buffer ownership")
			}
		})
	}
}

func TestWireDetachedMessageStillAttachesToRetiredConnection(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	encoded, owner, _ := wireLifetimeBuffer(t, fixture)
	defer encoded.Free()
	owner.detachMessage()
	connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
	connection.retire()
	owner.attachConnection(connection)
	owner.watch(fixture.rpc, fixture.server)
	if fixture.server.admission.wireBytes.Load() != 0 {
		t.Fatal("a detached message bypassed final connection retirement")
	}
}

func TestWireRegistrationRetirementNativeFreeAndCleanupRace(t *testing.T) {
	for range 100 {
		fixture := newWireLifetimeFixture(t)
		encoded, owner, response := wireLifetimeBuffer(t, fixture)
		connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
		start := make(chan struct{})
		var pending sync.WaitGroup
		pending.Go(func() { <-start; owner.attachConnection(connection) })
		pending.Go(func() { <-start; connection.retire() })
		pending.Go(func() { <-start; encoded.Free() })
		pending.Go(func() { <-start; owner.finish() }) // Same path as AddCleanup.
		pending.Go(func() { <-start; owner.watch(fixture.rpc, fixture.server); owner.detachMessage() })
		close(start)
		pending.Wait()
		if fixture.server.admission.wireBytes.Load() != 0 || wireConnectionOwnerCount(connection) != 0 {
			t.Fatal("concurrent native Free, retirement and cleanup did not return accounting exactly once")
		}
		if _, retained := fixture.server.admission.responses.Load(response); retained {
			t.Fatal("completed account retained its original response")
		}
	}
}

func TestWireConnectionCollectionDoesNotRetainOrphanedRPCContext(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	owner := orphanWireLifetimeBuffer(t, fixture)
	connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
	deadline := time.Now().Add(2 * time.Second)
	for fixture.server.admission.wireBytes.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a live connection's account collection retained orphaned encoded storage")
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if connection.closed || wireConnectionOwnerCount(connection) != 0 {
		t.Fatal("GC fallback required retiring the connection or failed to unlink its account")
	}
	select {
	case <-owner.done:
	default:
		t.Fatal("unreachable-storage cleanup did not finish the output account")
	}
	runtime.KeepAlive(fixture.connection)
	runtime.KeepAlive(owner)
}

func orphanWireLifetimeBuffer(t *testing.T, fixture wireLifetimeFixture) *responseBufferOwner {
	t.Helper()
	encoded, owner, _ := wireLifetimeBuffer(t, fixture)
	connection := fixture.connection.Value(responseConnectionKey).(*responseConnection)
	owner.attachConnection(connection)
	// Model the RPC context's path to transport storage. The live connection
	// collection must not retain this context after watchdog detachment.
	ctx := context.WithValue(fixture.rpc, rpcKey, encoded)
	owner.watch(ctx, fixture.server)
	fixture.cancel()
	owner.detachMessage()
	if fixture.server.admission.wireBytes.Load() == 0 {
		t.Fatal("cancellation returned a still-live native buffer's account")
	}
	runtime.KeepAlive(encoded)
	return owner
}

type wireLifetimeStream struct {
	grpc.ServerStream
	fixture          wireLifetimeFixture
	retireDuringSend bool
	encoded          mem.BufferSlice
	waiting          chan struct{}
	once             sync.Once
}

func (stream *wireLifetimeStream) Context() context.Context {
	stream.once.Do(func() { close(stream.waiting) })
	return stream.fixture.rpc
}

func (*wireLifetimeStream) Recv() (*pb.ExecuteRequest, error) {
	return nil, io.EOF
}

func (stream *wireLifetimeStream) Send(response *pb.ExecuteResponse) error {
	codec := &responseCodec{admission: stream.fixture.server.admission}
	encoded, err := codec.Marshal(response)
	if err != nil {
		return err
	}
	stream.encoded = encoded
	payload := &stats.OutPayload{Payload: response}
	stream.fixture.stats.HandleRPC(stream.fixture.rpc, payload)
	if stream.retireDuringSend {
		stream.fixture.cancel()
		end := &stats.ConnEnd{}
		stream.fixture.stats.HandleConn(stream.fixture.connection, end)
	}
	return nil
}

func TestSendEventConnectionRetirementReportsCancellation(t *testing.T) {
	for _, duringSend := range []bool{false, true} {
		name := "while_waiting_for_encoded_output"
		if duringSend {
			name = "before_Send_returns"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newWireLifetimeFixture(t)
			stream := &wireLifetimeStream{fixture: fixture, retireDuringSend: duringSend, waiting: make(chan struct{})}
			head := &pb.NativeHead{BodyContentType: "application/bson"}
			value := &pb.Event_Head{Head: head}
			event := &pb.Event{Value: value}
			result := make(chan error, 1)
			go func() { result <- fixture.server.sendEvent(stream, event, 1, nil) }()
			if !duringSend {
				select {
				case <-stream.waiting:
				case <-time.After(time.Second):
					t.Fatal("Send did not begin waiting for encoded output")
				}
				fixture.cancel()
				end := &stats.ConnEnd{}
				fixture.stats.HandleConn(fixture.connection, end)
			}
			select {
			case err := <-result:
				if status.Code(err) != codes.Canceled {
					t.Fatal("connection retirement reported a canceled send as success", err)
				}
			case <-time.After(time.Second):
				t.Fatal("connection retirement did not unblock Send")
			}
			stream.encoded.Free()
			if fixture.server.admission.wireBytes.Load() != 0 {
				t.Fatal("late native Free returned retired credit twice")
			}
		})
	}
}
