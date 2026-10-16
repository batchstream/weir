package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

type nativeStallAdapter struct {
	peerAdapter
	nativeStarted atomic.Int32
}

func (*nativeStallAdapter) PrepareCommand(_ context.Context, id uint64, command *pb.Command) (*execution.Plan, *pb.Failure) {
	plan := &execution.Plan{ID: id, Command: command, Bytes: execution.EntryOverheadBytes, ResultBytes: execution.ResultOverheadBytes}
	return plan, nil
}

func (adapter *nativeStallAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	if plans[0].Command.GetNative() == nil {
		adapter.peerAdapter.Execute(ctx, plans, emit)
		return
	}
	adapter.nativeStarted.Add(1)
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(plans[0], event); err != nil {
		return
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	_ = emit(plans[0], endEvent)
}

type flowControlledRequest struct {
	framer         *http2.Framer
	address        string
	streamID       uint32
	command        *pb.Command
	responseWindow uint32
}

func sendFlowControlledRequest(t *testing.T, opts flowControlledRequest) {
	t.Helper()
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: opts.address}, {Name: ":path", Value: pb.StoreService_Execute_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
	for _, field := range fields {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	headers := http2.HeadersFrameParam{StreamID: opts.streamID, BlockFragment: block.Bytes(), EndHeaders: true}
	if err := opts.framer.WriteHeaders(headers); err != nil {
		t.Fatal(err)
	}
	request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: opts.command}
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 5, 5+len(raw))
	binary.BigEndian.PutUint32(body[1:5], uint32(len(raw)))
	body = append(body, raw...)
	if err := opts.framer.WriteData(opts.streamID, true, body); err != nil {
		t.Fatal(err)
	}
	if opts.responseWindow > 0 {
		if err := opts.framer.WriteWindowUpdate(opts.streamID, opts.responseWindow); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeOutputStallsReleaseSaturatedStoreAndAbortSharedConnection(t *testing.T) {
	adapter := &nativeStallAdapter{}
	adapter.documents = make(map[string]*pb.Document)
	adapter.seen = make(chan context.Context, 64)
	storeLimits := store.DefaultLimits()
	local, err := store.New(adapter, storeLimits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := local.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	limits := DefaultLimits()
	limits.Stall = 500 * time.Millisecond
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: limits}
	server, address := startPeerServer(t, opts)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/octet-stream", Data: []byte("request")}
	native := &pb.NativeRequest{Resource: "data", Request: document}
	nativeValue := &pb.Command_Native{Native: native}
	nativeCommand := &pb.Command{Operation: nativeValue}
	started := time.Now()
	for index := range storeLimits.MaxInFlight {
		requestOpts := flowControlledRequest{framer: framer, address: address, streamID: uint32(index*2 + 1), command: nativeCommand}
		sendFlowControlledRequest(t, requestOpts)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for adapter.nativeStarted.Load() != int32(storeLimits.MaxInFlight) {
		if ctx.Err() != nil {
			t.Fatal("Native requests did not saturate Store execution", adapter.nativeStarted.Load(), local.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if snapshot := local.Snapshot(); snapshot.Active != storeLimits.MaxInFlight || snapshot.Pending != 0 || server.admission.wireBytes.Load() == 0 {
		t.Fatal("zero-window Native output did not hold all execution slots", snapshot, server.admission.wireBytes.Load())
	}
	read := testRequest()
	readValue := &pb.Command_Read{Read: read}
	readCommand := &pb.Command{Operation: readValue}
	sharedReadID := uint32(storeLimits.MaxInFlight*2 + 1)
	requestOpts := flowControlledRequest{framer: framer, address: address, streamID: sharedReadID, command: readCommand, responseWindow: 64 << 10}
	// This sibling can receive responses. It queues only because the Native
	// streams occupy the Store, then shares the connection's watchdog abort.
	sendFlowControlledRequest(t, requestOpts)
	_, independent := peerClient(t, address)
	completed := make(chan error, 1)
	go func() {
		result, err := routeRead(independent, ctx, read)
		if err == nil && result.GetMissing() == nil {
			err = io.ErrUnexpectedEOF
		}
		completed <- err
	}()
	for local.Snapshot().Pending != 2 {
		select {
		case err := <-completed:
			t.Fatal("independent read completed before output watchdog released execution", err, local.Snapshot())
		case <-ctx.Done():
			t.Fatal("ordinary reads did not reach the saturated Store queue", local.Snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("only the client deadline closed stalled Native output", err)
			}
			break
		}
		if data, ok := frame.(*http2.DataFrame); ok {
			t.Fatal("Native output bypassed zero-window flow control or sibling survived its connection abort", data.StreamID)
		}
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal("independent queued read did not recover after Native output abort", err)
		}
	case <-ctx.Done():
		t.Fatal("independent read remained blocked after watchdog", local.Snapshot())
	}
	metric := &dto.Metric{}
	if err := server.metrics.watchdogs.WithLabelValues("output").Write(metric); err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() == 0 || time.Since(started) > 3*time.Second {
		t.Fatal("output watchdog did not bound recovery", metric, time.Since(started))
	}
	// An aborted gRPC connection may orphan DATA buffers. Credits return only
	// when GC proves those native transport buffers are unreachable.
	until := time.Now().Add(2 * time.Second)
	for server.admission.wireBytes.Load() != 0 && time.Now().Before(until) {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	waitPeerIdle(t, server)
	if connections := server.admission.activeConnections.Load(); connections != 1 {
		t.Fatal("watchdog affected the independent connection or retained the stalled connection", connections)
	}
	if snapshot := local.Snapshot(); snapshot.Overloaded {
		t.Fatal("flow control saturation was misreported as process overload", snapshot)
	}
}
