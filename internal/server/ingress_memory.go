package server

import (
	"context"
	"sync"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protowire"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

type receiveMemory struct{ admission *Admission }

func (memory receiveMemory) NewConnection() (func(), error) {
	return memory.admission.memory.ReserveIngressConnection()
}

func (memory receiveMemory) NewStream(ctx context.Context, bytes int) (mem.ReceiveStream, error) {
	account, err := memory.admission.memory.NewIngress(ctx, bytes)
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, execution.ErrExecutionMemory.Error())
	}
	owner := &receiveStream{account: account, admission: memory.admission, ctx: ctx, decoded: make(map[any]*store.IngressInput)}
	return owner, nil
}

type receiveStream struct {
	mu        sync.Mutex
	account   *store.IngressMemory
	admission *Admission
	ctx       context.Context
	decoded   map[any]*store.IngressInput
	closed    bool
}

func (owner *receiveStream) ReserveMessage(length int, compressed bool) error {
	ctx := owner.ctx
	if compressed {
		return status.Error(codes.Unimplemented, "compressed requests are not served by the bounded receiver")
	}
	if err := owner.account.ReserveMessage(length); err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.ResourceExhausted, execution.ErrExecutionMemory.Error())
	}
	return nil
}
func (owner *receiveStream) Buffer(bytes int) (func(), error) {
	release, err := owner.account.Buffer(bytes)
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, execution.ErrExecutionMemory.Error())
	}
	return release, nil
}
func (owner *receiveStream) Decoded(value any) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	bytes := owner.account.Decoded()
	if bytes == 0 {
		return
	}
	owner.decoded[value] = owner.account.Input(bytes)
	owner.admission.received.Store(value, owner)
}
func (owner *receiveStream) ReleaseMessage() { owner.account.ReleaseMessage() }
func (owner *receiveStream) release(value any) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	input, ok := owner.decoded[value]
	if !ok {
		return
	}
	delete(owner.decoded, value)
	owner.admission.received.CompareAndDelete(value, owner)
	input.Release()
}
func (owner *receiveStream) Close() {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed {
		return
	}
	owner.closed = true
	for value := range owner.decoded {
		owner.admission.received.CompareAndDelete(value, owner)
		owner.decoded[value].Release()
	}
	owner.decoded = nil
	owner.account.Close()
}

// A successful Submit has accepted the same command/source under its existing
// execution input lease. Only then relinquish the decoded ingress promise.
func (s *Server) releaseReceived(value any) {
	if owner, ok := s.admission.received.Load(value); ok {
		owner.(*receiveStream).release(value)
	}
}

func (s *Server) bindReceived(value any, plan *execution.Plan) {
	if valueOwner, ok := s.admission.received.Load(value); ok {
		owner := valueOwner.(*receiveStream)
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if input := owner.decoded[value]; input != nil {
			input.Bind(plan)
		}
	}
}

func (owner *receiveStream) BeforeDecode(data mem.BufferSlice, value any) error {
	if _, ok := value.(*peerpb.SyncDirectoryRequest); !ok {
		return nil
	}
	bytes, err := protowire.PeerDecodeBytes(data)
	if err != nil {
		return err
	}
	if err := owner.account.ReserveDecode(bytes); err != nil {
		return status.Error(codes.ResourceExhausted, execution.ErrExecutionMemory.Error())
	}
	return nil
}

func (owner *receiveStream) Retire() { owner.account.Retire() }

func (owner *receiveStream) SetContext(ctx context.Context) {
	owner.ctx = ctx
	owner.account.SetContext(ctx)
}
