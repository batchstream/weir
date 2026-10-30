package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	recordAdapter
	nativeStarted atomic.Int32
	chunks        int
}

func (*nativeStallAdapter) PrepareNative(_ context.Context, command *pb.Command) (*execution.Plan, *pb.Failure) {
	plan := &execution.Plan{Command: command}
	return plan, nil
}

// ExecuteNative streams a head and, for the "slow" resource, chunks large
// enough to exhaust the gRPC stream write quota when the client grants no window.
func (adapter *nativeStallAdapter) ExecuteNative(_ context.Context, plan *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	adapter.nativeStarted.Add(1)
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(event); err != nil {
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "output closed")
		return protocol.NativeFailure(false, failure)
	}
	if plan.Command.GetNative().Resource == "slow" {
		body := bytes.Repeat([]byte("s"), protocol.NativeChunk)
		for range adapter.chunks {
			value := &pb.Event_Chunk{Chunk: body}
			event := &pb.Event{Value: value}
			if err := emit(event); err != nil {
				failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "output closed")
				return protocol.NativeFailure(true, failure)
			}
		}
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end
}

// nativeHealthyResponse keeps only the two small probe replies. Slow Native
// bodies remain on the flow-control path and are never retained by this oracle.
type nativeHealthyResponse struct {
	native  bool
	headers bool
	ended   bool
	body    []byte
}

func (response *nativeHealthyResponse) observe(frame http2.Frame) error {
	switch frame := frame.(type) {
	case *http2.MetaHeadersFrame:
		if response.ended || frame.Truncated {
			return fmt.Errorf("healthy reply has extra or truncated headers")
		}
		status, contentType, grpcStatus := "", "", ""
		for _, field := range frame.Fields {
			var target *string
			switch field.Name {
			case ":status":
				target = &status
			case "content-type":
				target = &contentType
			case "grpc-status":
				target = &grpcStatus
			}
			if target != nil {
				if *target != "" || field.Value == "" {
					return fmt.Errorf("healthy reply has duplicate or empty %s", field.Name)
				}
				*target = field.Value
			}
		}
		if !response.headers {
			if status != "200" || contentType != "application/grpc" {
				return fmt.Errorf("healthy reply has HTTP status %q or content type %q", status, contentType)
			}
			response.headers = true
		} else if status != "" || contentType != "" {
			return fmt.Errorf("healthy reply repeats initial headers in its trailers")
		}
		if !frame.StreamEnded() {
			if grpcStatus != "" {
				return fmt.Errorf("healthy reply sends grpc-status before its terminal trailers")
			}
			return nil
		}
		if grpcStatus != "0" {
			return fmt.Errorf("healthy reply grpc-status=%q, want 0", grpcStatus)
		}
		if err := response.validateBody(); err != nil {
			return err
		}
		response.ended = true
	case *http2.DataFrame:
		if !response.headers || response.ended || frame.StreamEnded() {
			return fmt.Errorf("healthy reply DATA is outside its headers and trailers")
		}
		if len(response.body)+len(frame.Data()) > 64<<10 {
			return fmt.Errorf("healthy probe reply exceeds its finite 64 KiB bound")
		}
		response.body = append(response.body, frame.Data()...)
	case *http2.RSTStreamFrame:
		return fmt.Errorf("healthy reply reset with %s", frame.ErrCode)
	case *http2.HeadersFrame, *http2.ContinuationFrame:
		return fmt.Errorf("healthy reply bypassed decoded complete headers")
	}
	return nil
}

func (response *nativeHealthyResponse) validateBody() error {
	missing := protocol.Missing()
	readValue := &pb.Event_ReadResult{ReadResult: missing}
	readEvent := &pb.Event{Value: readValue}
	want := []*pb.Event{readEvent}
	if response.native {
		head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
		headValue := &pb.Event_Head{Head: head}
		headEvent := &pb.Event{Value: headValue}
		end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
		endValue := &pb.Event_NativeEnd{NativeEnd: end}
		endEvent := &pb.Event{Value: endValue}
		want = []*pb.Event{headEvent, endEvent}
	}
	body, count := response.body, 0
	for len(body) > 0 {
		if len(body) < 5 || body[0] != 0 {
			return fmt.Errorf("healthy reply has a truncated or compressed gRPC envelope")
		}
		size := uint64(binary.BigEndian.Uint32(body[1:5]))
		if size > uint64(len(body)-5) {
			return fmt.Errorf("healthy reply has a truncated gRPC message")
		}
		message := &pb.ExecuteResponses{}
		if err := proto.Unmarshal(body[5:5+int(size)], message); err != nil {
			return fmt.Errorf("healthy reply protobuf: %w", err)
		}
		if err := protocol.ValidateExecuteResponses(message); err != nil {
			return fmt.Errorf("healthy reply protocol: %w", err)
		}
		for _, response := range message.Responses {
			if err := protocol.ValidateExecuteResponse(response); err != nil {
				return fmt.Errorf("healthy reply protocol: %w", err)
			}
			if count >= len(want) || response.Index != 1 || !proto.Equal(response.Event, want[count]) {
				return fmt.Errorf("healthy reply has an unexpected event at position %d: %v", count, response)
			}
			count++
		}
		body = body[5+int(size):]
	}
	if count != len(want) {
		return fmt.Errorf("healthy reply has %d events, want %d", count, len(want))
	}
	return nil
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
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	framer.MaxHeaderListSize = 16 << 10
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	const slowStreams = 32
	readID, nativeID := uint32(slowStreams*2+1), uint32(slowStreams*2+3)
	readReply, nativeReply := &nativeHealthyResponse{}, &nativeHealthyResponse{native: true}
	healthy := map[uint32]*nativeHealthyResponse{readID: readReply, nativeID: nativeReply}
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
			case *http2.MetaHeadersFrame:
				observed.streamID, observed.ended = value.StreamID, value.StreamEnded()
			case *http2.SettingsFrame:
				observed.settings = !value.IsAck()
			case *http2.RSTStreamFrame:
				observed.err = fmt.Errorf("stream %d reset during slow progress: %s", value.StreamID, value.ErrCode)
			case *http2.GoAwayFrame:
				observed.err = fmt.Errorf("connection sent GOAWAY during slow progress: %s", value.ErrCode)
			}
			if err == nil && observed.err == nil {
				if reply := healthy[frame.Header().StreamID]; reply != nil {
					observed.err = reply.observe(frame)
				}
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
	for index := range slowStreams {
		request := flowControlledRequest{framer: framer, address: node.address, streamID: uint32(index*2 + 1), command: command, responseWindow: 128}
		sendFlowControlledRequest(t, request)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	probeSent, readComplete, nativeComplete := false, false, false
	progress := make(map[uint32]int)
	for time.Since(started) < 900*time.Millisecond {
		select {
		case frame := <-frames:
			if frame.err != nil {
				t.Fatal("shared connection or healthy probe reply failed during slow progress", frame.err)
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
	message := &pb.ExecuteRequests{Requests: []*pb.ExecuteRequest{request}}
	raw, err := proto.Marshal(message)
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

// Stalled Native output holds Native slots only: a sibling read proceeds at
// once, while a sibling Native waits until the output watchdog frees a slot.
func TestNativeOutputStallsHoldOnlyNativeSlots(t *testing.T) {
	adapter := &nativeStallAdapter{chunks: 4}
	adapter.documents = make(map[string]*pb.Document)
	storeLimits := store.DefaultLimits()
	storeLimits.MaxNative = 4
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
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: limits}
	server, address := startTestServer(t, opts)
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
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	framer.MaxHeaderListSize = 16 << 10
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/octet-stream", Data: []byte("request")}
	slow := &pb.NativeRequest{Resource: "slow", Request: document}
	slowValue := &pb.Command_Native{Native: slow}
	slowCommand := &pb.Command{Operation: slowValue}
	started := time.Now()
	for index := range storeLimits.MaxNative {
		requestOpts := flowControlledRequest{framer: framer, address: address, streamID: uint32(index*2 + 1), command: slowCommand}
		sendFlowControlledRequest(t, requestOpts)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for adapter.nativeStarted.Load() != int32(storeLimits.MaxNative) {
		if ctx.Err() != nil {
			t.Fatal("Native requests did not saturate the Native slots", adapter.nativeStarted.Load())
		}
		time.Sleep(time.Millisecond)
	}
	read := testRequest()
	readValue := &pb.Command_Read{Read: read}
	readCommand := &pb.Command{Operation: readValue}
	sharedReadID := uint32(storeLimits.MaxNative*2 + 1)
	requestOpts := flowControlledRequest{framer: framer, address: address, streamID: sharedReadID, command: readCommand, responseWindow: 64 << 10}
	sendFlowControlledRequest(t, requestOpts)
	short := &pb.NativeRequest{Resource: "short", Request: document}
	shortValue := &pb.Command_Native{Native: short}
	shortCommand := &pb.Command{Operation: shortValue}
	sharedNativeID := sharedReadID + 2
	requestOpts.streamID, requestOpts.command = sharedNativeID, shortCommand
	sendFlowControlledRequest(t, requestOpts)
	_, independent := testClient(t, address)
	completed := make(chan error, 1)
	go func() {
		result, err := routeRead(independent, ctx, read)
		if err == nil && result.GetMissing() == nil {
			err = io.ErrUnexpectedEOF
		}
		completed <- err
	}()
	watchdogs := func() float64 {
		metric := &dto.Metric{}
		if err := server.metrics.watchdogs.WithLabelValues("output").Write(metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetCounter().GetValue()
	}
	readReply, nativeReply := &nativeHealthyResponse{}, &nativeHealthyResponse{native: true}
	healthy := map[uint32]*nativeHealthyResponse{sharedReadID: readReply, sharedNativeID: nativeReply}
	for !readReply.ended || !nativeReply.ended {
		frame, err := framer.ReadFrame()
		if err != nil {
			t.Fatal("shared connection failed before healthy probes completed", err)
		}
		switch value := frame.(type) {
		case *http2.SettingsFrame:
			if !value.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.GoAwayFrame:
			t.Fatal("connection sent GOAWAY before healthy probes completed", value.ErrCode)
		case *http2.DataFrame:
			if value.StreamID != sharedReadID && value.StreamID != sharedNativeID {
				t.Fatal("Native output bypassed zero-window flow control", value.StreamID)
			}
		}
		if reply := healthy[frame.Header().StreamID]; reply != nil {
			if err := reply.observe(frame); err != nil {
				t.Fatal("healthy shared-connection probe failed", err)
			}
			if reply.ended && reply == readReply && watchdogs() != 0 {
				t.Fatal("read waited for the output watchdog beside stalled Natives")
			}
			if reply.ended && reply == nativeReply && watchdogs() == 0 {
				t.Fatal("Native ran while stalled Natives held every Native slot")
			}
		}
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal("independent read failed beside stalled Natives", err)
		}
	case <-ctx.Done():
		t.Fatal("independent read remained blocked beside stalled Natives")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatal("output watchdog did not bound recovery", elapsed)
	}
	for watchdogs() != float64(storeLimits.MaxNative) {
		if ctx.Err() != nil {
			t.Fatal("output watchdog did not end every stalled Native", watchdogs())
		}
		time.Sleep(time.Millisecond)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, server)
}

func nativeHealthyReplyBytes(t *testing.T, messages []*pb.ExecuteResponse) []byte {
	t.Helper()
	var body []byte
	for _, response := range messages {
		message := &pb.ExecuteResponses{Responses: []*pb.ExecuteResponse{response}}
		raw, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		envelope := make([]byte, 5)
		binary.BigEndian.PutUint32(envelope[1:], uint32(len(raw)))
		body = append(body, envelope...)
		body = append(body, raw...)
	}
	return body
}

func TestNativeHealthyResponseRejectsFalseBusinessSuccess(t *testing.T) {
	missing := protocol.Missing()
	readValue := &pb.Event_ReadResult{ReadResult: missing}
	readEvent := &pb.Event{Value: readValue}
	read := &pb.ExecuteResponse{Index: 1, Event: readEvent}
	wrongIndex := &pb.ExecuteResponse{Index: 2, Event: readEvent}
	failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "synthetic refusal")
	failedRead := protocol.ReadFailure(failure)
	failedReadValue := &pb.Event_ReadResult{ReadResult: failedRead}
	failedReadEvent := &pb.Event{Value: failedReadValue}
	readFailure := &pb.ExecuteResponse{Index: 1, Event: failedReadEvent}
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	nativeHead := &pb.ExecuteResponse{Index: 1, Event: headEvent}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	nativeEnd := &pb.ExecuteResponse{Index: 1, Event: endEvent}
	incomplete := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, Failure: failure}
	incompleteValue := &pb.Event_NativeEnd{NativeEnd: incomplete}
	incompleteEvent := &pb.Event{Value: incompleteValue}
	nativeIncomplete := &pb.ExecuteResponse{Index: 1, Event: incompleteEvent}
	tests := []struct {
		name     string
		native   bool
		messages []*pb.ExecuteResponse
		alter    string
	}{
		{name: "read-index", messages: []*pb.ExecuteResponse{wrongIndex}},
		{name: "read-failure", messages: []*pb.ExecuteResponse{readFailure}},
		{name: "read-extra-event", messages: []*pb.ExecuteResponse{read, read}},
		{name: "native-incomplete", native: true, messages: []*pb.ExecuteResponse{nativeHead, nativeIncomplete}},
		{name: "native-missing-end", native: true, messages: []*pb.ExecuteResponse{nativeHead}},
		{name: "native-extra-event", native: true, messages: []*pb.ExecuteResponse{nativeHead, nativeEnd, nativeEnd}},
		{name: "truncated-envelope", messages: []*pb.ExecuteResponse{read}, alter: "envelope"},
		{name: "truncated-message", messages: []*pb.ExecuteResponse{read}, alter: "message"},
		{name: "compressed-envelope", messages: []*pb.ExecuteResponse{read}, alter: "compressed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := nativeHealthyReplyBytes(t, test.messages)
			switch test.alter {
			case "envelope":
				body = body[:4]
			case "message":
				body = body[:len(body)-1]
			case "compressed":
				body[0] = 1
			}
			reply := &nativeHealthyResponse{native: test.native, body: body}
			if err := reply.validateBody(); err == nil {
				t.Fatal("failed or incomplete business reply passed the healthy probe oracle")
			}
		})
	}
}

func TestNativeHealthyResponseAcceptsFragmentedWire(t *testing.T) {
	missing := protocol.Missing()
	readValue := &pb.Event_ReadResult{ReadResult: missing}
	readEvent := &pb.Event{Value: readValue}
	read := &pb.ExecuteResponse{Index: 1, Event: readEvent}
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	nativeHead := &pb.ExecuteResponse{Index: 1, Event: headEvent}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	nativeEnd := &pb.ExecuteResponse{Index: 1, Event: endEvent}
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%t", native), func(t *testing.T) {
			messages := []*pb.ExecuteResponse{read}
			if native {
				messages = []*pb.ExecuteResponse{nativeHead, nativeEnd}
			}
			body := nativeHealthyReplyBytes(t, messages)
			var wire, block bytes.Buffer
			writer := http2.NewFramer(&wire, nil)
			encoder := hpack.NewEncoder(&block)
			fields := []hpack.HeaderField{{Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}}
			for _, field := range fields {
				if err := encoder.WriteField(field); err != nil {
					t.Fatal(err)
				}
			}
			initial := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}
			if err := writer.WriteHeaders(initial); err != nil {
				t.Fatal(err)
			}
			// Split every byte, including all five bytes of each gRPC envelope.
			for index := range body {
				if err := writer.WriteData(1, false, body[index:index+1]); err != nil {
					t.Fatal(err)
				}
			}
			block.Reset()
			status := hpack.HeaderField{Name: "grpc-status", Value: "0"}
			if err := encoder.WriteField(status); err != nil {
				t.Fatal(err)
			}
			trailers := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: true}
			if err := writer.WriteHeaders(trailers); err != nil {
				t.Fatal(err)
			}
			reader := http2.NewFramer(nil, &wire)
			reader.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
			reply := &nativeHealthyResponse{native: native}
			for wire.Len() > 0 {
				frame, err := reader.ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				if err := reply.observe(frame); err != nil {
					t.Fatal("valid fragmented probe was rejected", err)
				}
			}
			if !reply.ended {
				t.Fatal("valid fragmented probe did not reach confirmed terminal trailers")
			}
		})
	}
}
