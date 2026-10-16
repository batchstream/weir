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

	"github.com/batchstream/weir-protocol/api/protocol"
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
	chunks        int
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
	if plans[0].Command.GetNative().Resource == "slow" {
		body := bytes.Repeat([]byte("s"), protocol.NativeChunk)
		plans[0].WorkingBytes = cap(body) + execution.EntryOverheadBytes
		for range adapter.chunks {
			value := &pb.Event_Chunk{Chunk: body}
			event := &pb.Event{Value: value}
			if err := emit(plans[0], event); err != nil {
				return
			}
		}
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	_ = emit(plans[0], endEvent)
}

type nativeFlowFrame struct {
	streamID uint32
	bytes    int
	ended    bool
	settings bool
	err      error
}

func TestNativeContinuousSlowProgressPreservesHealthySiblingQueries(t *testing.T) {
	adapter := &nativeStallAdapter{chunks: 64}
	adapter.documents = make(map[string]*pb.Document)
	limits := DefaultLimits()
	limits.Stall = 400 * time.Millisecond
	opts := routeAcceptanceNodeOptions{adapter: adapter, limits: limits}
	node := startRouteAcceptanceNode(t, opts)
	conn, err := net.DialTimeout("tcp", node.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(4 * time.Second)); err != nil {
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
	frames := make(chan nativeFlowFrame, 128)
	stopReader := make(chan struct{})
	defer close(stopReader)
	go func() {
		for {
			frame, err := framer.ReadFrame()
			observed := nativeFlowFrame{err: err}
			switch value := frame.(type) {
			case *http2.DataFrame:
				observed.streamID, observed.bytes = value.StreamID, len(value.Data())
			case *http2.HeadersFrame:
				observed.streamID, observed.ended = value.StreamID, value.StreamEnded()
			case *http2.SettingsFrame:
				observed.settings = !value.IsAck()
			}
			select {
			case frames <- observed:
			case <-stopReader:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	document := &pb.Document{ContentType: "application/octet-stream", Data: []byte("request")}
	native := &pb.NativeRequest{Resource: "slow", Request: document}
	value := &pb.Command_Native{Native: native}
	command := &pb.Command{Operation: value}
	const slowStreams = 32
	for index := range slowStreams {
		request := flowControlledRequest{framer: framer, address: node.address, streamID: uint32(index*2 + 1), command: command, responseWindow: 128}
		sendFlowControlledRequest(t, request)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	readID, nativeID := uint32(slowStreams*2+1), uint32(slowStreams*2+3)
	probeSent, readComplete, nativeComplete := false, false, false
	progress := make(map[uint32]int)
	for time.Since(started) < 900*time.Millisecond {
		select {
		case frame := <-frames:
			if frame.err != nil {
				t.Fatal("slow progress closed the shared connection", frame.err)
			}
			if frame.settings {
				if err := framer.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
			if frame.bytes > 0 {
				progress[frame.streamID] += frame.bytes
				if err := framer.WriteWindowUpdate(0, uint32(frame.bytes)); err != nil {
					t.Fatal(err)
				}
			}
			if frame.ended {
				readComplete = readComplete || frame.streamID == readID
				nativeComplete = nativeComplete || frame.streamID == nativeID
				if frame.streamID < readID {
					t.Fatal("slow Native finished before healthy probes", frame.streamID)
				}
			}
		case <-ticker.C:
			for index := range slowStreams {
				if err := framer.WriteWindowUpdate(uint32(index*2+1), 16<<10); err != nil {
					t.Fatal(err)
				}
			}
			if !probeSent && time.Since(started) > 500*time.Millisecond {
				read := testRequest()
				readValue := &pb.Command_Read{Read: read}
				readCommand := &pb.Command{Operation: readValue}
				request := flowControlledRequest{framer: framer, address: node.address, streamID: readID, command: readCommand, responseWindow: 64 << 10}
				sendFlowControlledRequest(t, request)
				short := &pb.NativeRequest{Resource: "short", Request: document}
				shortValue := &pb.Command_Native{Native: short}
				shortCommand := &pb.Command{Operation: shortValue}
				request.streamID, request.command = nativeID, shortCommand
				sendFlowControlledRequest(t, request)
				probeSent = true
			}
			snapshot := node.runtime.Snapshot()
			if snapshot.Active > store.DefaultLimits().MaxInFlight || snapshot.Paused > store.DefaultLimits().QueueOperations || snapshot.PausedBytes > store.DefaultLimits().QueueBytes {
				t.Fatal("continuous Native progress exceeded bounded execution or output credit", snapshot)
			}
		}
	}
	if !readComplete || !nativeComplete || adapter.nativeStarted.Load() != slowStreams+1 {
		t.Fatal("healthy typed and Native siblings did not finish during slow progress", readComplete, nativeComplete, adapter.nativeStarted.Load())
	}
	for index := range slowStreams {
		if progress[uint32(index*2+1)] < 2*protocol.NativeChunk {
			t.Fatal("Native consumer did not demonstrate continued slow progress", index, progress)
		}
	}
	metric := &dto.Metric{}
	if err := node.server.metrics.watchdogs.WithLabelValues("output").Write(metric); err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 0 {
		t.Fatal("healthy siblings depended on the output watchdog", metric)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
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

func TestNativeOutputStallsPreserveSiblingProgressBeforeWatchdogAbort(t *testing.T) {
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
	for adapter.nativeStarted.Load() != int32(storeLimits.MaxInFlight) || local.Snapshot().Paused != storeLimits.MaxInFlight || server.admission.wireBytes.Load() == 0 {
		if ctx.Err() != nil {
			t.Fatal("Native requests did not saturate Store execution", adapter.nativeStarted.Load(), local.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if snapshot := local.Snapshot(); snapshot.Active != 0 || snapshot.Paused != storeLimits.MaxInFlight || snapshot.Pending != 0 || server.admission.wireBytes.Load() == 0 {
		t.Fatal("zero-window Native output did not return backend execution slots", snapshot, server.admission.wireBytes.Load())
	}
	read := testRequest()
	readValue := &pb.Command_Read{Read: read}
	readCommand := &pb.Command{Operation: readValue}
	sharedReadID := uint32(storeLimits.MaxInFlight*2 + 1)
	requestOpts := flowControlledRequest{framer: framer, address: address, streamID: sharedReadID, command: readCommand, responseWindow: 64 << 10}
	sendFlowControlledRequest(t, requestOpts)
	sharedNativeID := sharedReadID + 2
	requestOpts.streamID, requestOpts.command = sharedNativeID, nativeCommand
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
	readComplete, nativeComplete := false, false
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("only the client deadline closed stalled Native output", err)
			}
			break
		}
		if data, ok := frame.(*http2.DataFrame); ok && data.StreamID != sharedReadID && data.StreamID != sharedNativeID {
			t.Fatal("Native output bypassed zero-window flow control", data.StreamID)
		}
		if trailers, ok := frame.(*http2.HeadersFrame); ok && trailers.StreamEnded() {
			readComplete = readComplete || trailers.StreamID == sharedReadID
			nativeComplete = nativeComplete || trailers.StreamID == sharedNativeID
		}
	}
	if !readComplete || !nativeComplete {
		t.Fatal("healthy siblings did not finish on their shared connection before watchdog abort", readComplete, nativeComplete)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal("independent read failed beside stalled Native output", err)
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
	// The fixture records backend contexts. Release those observations before
	// asking GC to prove the shared transport's orphaned DATA is unreachable.
	for len(adapter.seen) != 0 {
		<-adapter.seen
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
