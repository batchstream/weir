package server

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type deliveryContextKey uint8

const deliveryKey deliveryContextKey = 0

// delivery outlives the gRPC handler/context: net/http owns the HTTP/2 stream's
// write deadline until END_STREAM is written or the stream is reset.
type delivery struct {
	mu           sync.Mutex
	controller   *http.ResponseController
	deadline     time.Time
	readDeadline time.Time
	inputDecoded bool
	stall        time.Duration
	unary        bool
	singleInput  bool
	finished     bool
	rpcStarted   bool
	rpcEnded     bool
	released     bool
	slots        chan struct{}
	ticket       *store.Ticket
	input        *creditedBody
}

func (s *Server) serveHTTP(w http.ResponseWriter, request *http.Request) {
	lifetime := s.limits.UnaryLifetime
	bulk := request.RequestURI == pb.Weir_Bulk_FullMethodName
	scan := request.RequestURI == pb.Weir_Scan_FullMethodName
	unary := !bulk && !scan
	if bulk {
		lifetime = s.limits.BulkLifetime
	} else if scan {
		lifetime = s.limits.ScanLifetime
	}
	ctx, cancel := context.WithTimeout(request.Context(), lifetime)
	defer cancel()
	deadline, _ := ctx.Deadline()
	controller := http.NewResponseController(w)
	// These timers belong to the HTTP/2 stream, not a handler-scoped context.
	if err := controller.SetReadDeadline(deadline); err != nil {
		return
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		return
	}
	// A ServeHTTP transport dispatches exactly one RPC. Keep that lifecycle
	// contract explicit, including malformed/unregistered method paths.
	switch request.RequestURI {
	case pb.Weir_Read_FullMethodName, pb.Weir_Mutate_FullMethodName, pb.Weir_Bulk_FullMethodName, pb.Weir_Scan_FullMethodName:
	default:
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Status", strconv.Itoa(int(codes.Unimplemented)))
		return
	}
	if err := s.enter(); err != nil {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Status", strconv.Itoa(int(status.Code(err))))
		w.Header().Set("Grpc-Message", status.Convert(err).Message())
		return
	}
	state := &delivery{controller: controller, deadline: deadline, readDeadline: deadline, stall: s.limits.Stall, unary: unary, singleInput: !bulk, slots: s.slots}
	input := newCreditedBody(ctx, request.Body, state)
	state.input = input
	defer input.Close()
	// Handler transport flushes response DATA before returning. Keep application
	// result credits through that flush; final trailers retain the native deadline.
	defer state.finish()
	ctx = context.WithValue(ctx, deliveryKey, state)
	request = request.WithContext(ctx)
	request.Body = input
	writer := &deadlineWriter{ResponseWriter: w, delivery: state}
	s.grpc.ServeHTTP(writer, request)
}

func (d *delivery) shorten(deadline time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return
	}
	if deadline.Before(d.deadline) {
		d.deadline = deadline
	}
	if d.readDeadline.IsZero() || d.deadline.Before(d.readDeadline) {
		d.readDeadline = d.deadline
	}
	_ = d.controller.SetReadDeadline(d.readDeadline)
	_ = d.controller.SetWriteDeadline(d.deadline)
}

func (d *delivery) armRead() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return io.ErrClosedPipe
	}
	d.readDeadline = d.deadline
	if (d.unary || d.singleInput) && !d.inputDecoded {
		if stall := time.Now().Add(d.stall); stall.Before(d.readDeadline) {
			d.readDeadline = stall
		}
	}
	return d.controller.SetReadDeadline(d.readDeadline)
}

func (d *delivery) consumedInput(n int) {
	if d.unary || d.singleInput {
		d.mu.Lock()
		d.inputDecoded = true
		if !d.finished {
			// Backend execution is not an input stall after the unary frame
			// has been decoded, including clients that have not half-closed.
			d.readDeadline = d.deadline
			_ = d.controller.SetReadDeadline(d.readDeadline)
		}
		d.mu.Unlock()
	}
	d.input.grant(n)
}

func (d *delivery) beginResponse() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return
	}
	if deadline := time.Now().Add(d.stall); deadline.Before(d.deadline) {
		d.deadline = deadline
	}
	_ = d.controller.SetWriteDeadline(d.deadline)
}

func (d *delivery) armWrite() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return io.ErrClosedPipe
	}
	deadline := d.deadline
	if stall := time.Now().Add(d.stall); stall.Before(deadline) {
		deadline = stall
	}
	if err := d.controller.SetWriteDeadline(deadline); err != nil {
		return err
	}
	// ResponseWriter may accept buffered writes after expiry. Do not enqueue
	// any new response bytes once the absolute deadline has passed.
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (d *delivery) finishWrite(err error) {
	if err != nil || d.unary {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return
	}
	// A live Bulk may legitimately be idle between fully flushed frames. Its
	// existing idle/input watchdog owns that wait, not a stale write-stall timer.
	_ = d.controller.SetWriteDeadline(d.deadline)
}

func (d *delivery) retain(ticket *store.Ticket) {
	d.mu.Lock()
	if d.finished {
		d.mu.Unlock()
		ticket.Ack()
		return
	}
	d.ticket = ticket
	d.mu.Unlock()
}

func (d *delivery) finish() {
	d.mu.Lock()
	d.finished = true
	ticket := d.ticket
	d.ticket = nil
	d.releaseSlotLocked()
	d.mu.Unlock()
	if ticket != nil {
		ticket.Ack()
	}
}

func (d *delivery) endRPC() {
	d.mu.Lock()
	d.rpcEnded = true
	d.releaseSlotLocked()
	d.mu.Unlock()
}

func (d *delivery) releaseSlotLocked() {
	if d.slots != nil && d.finished && (!d.rpcStarted || d.rpcEnded) && !d.released {
		d.released = true
		<-d.slots
	}
}

type deadlineWriter struct {
	http.ResponseWriter
	delivery *delivery
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	if err := w.delivery.armWrite(); err != nil {
		return 0, err
	}
	n, err := w.ResponseWriter.Write(p)
	w.delivery.finishWrite(err)
	return n, err
}
func (w *deadlineWriter) Flush() {
	if err := w.delivery.armWrite(); err != nil {
		return
	}
	err := w.delivery.controller.Flush()
	w.delivery.finishWrite(err)
}

// ServeHTTP's gRPC transport otherwise eagerly drains request.Body into an
// unbounded receive buffer. Only a decoded gRPC message returns its wire credits.
// This limits read-ahead to one maximum frame, independently of application load.
type creditedBody struct {
	source    io.ReadCloser
	delivery  *delivery
	ctx       context.Context
	mu        sync.Mutex
	available int
	wake      chan struct{}
	closed    chan struct{}
	once      sync.Once
	err       error
}

const inputCredits = protocol.MaxFrame + 5

func newCreditedBody(ctx context.Context, source io.ReadCloser, owner *delivery) *creditedBody {
	b := &creditedBody{source: source, delivery: owner, ctx: ctx, available: inputCredits, wake: make(chan struct{}, 1), closed: make(chan struct{})}
	return b
}
func (b *creditedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		select {
		case <-b.closed:
			return 0, io.ErrClosedPipe
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		default:
		}
		b.mu.Lock()
		n := min(len(p), b.available)
		b.available -= n
		b.mu.Unlock()
		if n > 0 {
			if b.delivery != nil {
				if err := b.delivery.armRead(); err != nil {
					b.grant(n)
					return 0, err
				}
			}
			read, err := b.source.Read(p[:n])
			b.grant(n - read)
			return read, err
		}
		select {
		case <-b.wake:
		case <-b.closed:
			return 0, io.ErrClosedPipe
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
}
func (b *creditedBody) grant(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.available += min(n, inputCredits-b.available)
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
func (b *creditedBody) Close() error {
	b.once.Do(func() { close(b.closed); b.err = b.source.Close() })
	return b.err
}

type deliveryStats struct{}

func (deliveryStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	if state, ok := ctx.Value(deliveryKey).(*delivery); ok {
		if deadline, ok := ctx.Deadline(); ok {
			state.shorten(deadline)
		}
	}
	return ctx
}
func (deliveryStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	state, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return
	}
	switch payload := event.(type) {
	case *stats.InPayload:
		state.consumedInput(payload.WireLength)
	case *stats.End:
		// End alone is not delivery completion. It only ends gRPC's ownership
		// of the slot after the HTTP transport has also finished.
		state.endRPC()
	}
}
func (deliveryStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	if state, ok := ctx.Value(deliveryKey).(*delivery); ok {
		// This is synchronous in ServeHTTP, before its RPC goroutine starts.
		state.mu.Lock()
		state.rpcStarted = true
		state.mu.Unlock()
	}
	return ctx
}
func (deliveryStats) HandleConn(context.Context, stats.ConnStats) {}
