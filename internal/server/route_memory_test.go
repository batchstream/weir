package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestEncodedResponseCreditsFollowFinalTransportReference(t *testing.T) {
	limits := DefaultLimits()
	limits.Sessions = 1
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: admission}
	for _, size := range []int{0, 1, 1024, 1 << 20} {
		document := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), size)}
		read := protocol.ReadDocument(document)
		response := &pb.ExecuteResponse{Index: 1, Event: &pb.Event{Value: &pb.Event_ReadResult{ReadResult: read}}}
		encoded, err := codec.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		charged := admission.wireBytes.Load()
		if charged < 1025 || charged < int64(encoded.Len()) {
			t.Fatal("small response bypassed pooled lifetime accounting", charged, encoded.Len())
		}
		encoded.Ref()
		runtime.GC()
		runtime.KeepAlive(encoded)
		if admission.wireBytes.Load() != charged {
			t.Fatal("GC returned credit for a live native buffer")
		}
		encoded.Free()
		if admission.wireBytes.Load() != charged {
			t.Fatal("handler ownership ended before the transport's last reference")
		}
		encoded.Free()
		if admission.wireBytes.Load() != 0 {
			t.Fatal("transport final free leaked encoded byte credits")
		}
	}
}

func TestEncodedResponseConcurrentCleanupReturnsCreditOnce(t *testing.T) {
	limits := DefaultLimits()
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: admission}
	response := &pb.ExecuteResponse{Index: 1}
	encoded, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := admission.responses.Load(response)
	if !ok {
		t.Fatal("encoded response did not retain its owner")
	}
	owner := entry.(*responseBufferOwner)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Go(func() { <-start; encoded.Free() })
	for range 8 {
		group.Go(func() { <-start; owner.Put(nil) })
	}
	close(start)
	group.Wait()
	if admission.wireBytes.Load() != 0 {
		t.Fatal("concurrent native and orphan cleanup returned credits more than once")
	}
	if _, retained := admission.responses.Load(response); retained {
		t.Fatal("concurrent cleanup retained the source message")
	}
	select {
	case <-owner.done:
	default:
		t.Fatal("concurrent cleanup did not complete publication ownership")
	}
}

func TestEncodedResponseBudgetDoesNotWaitOnPartialOwners(t *testing.T) {
	limits := DefaultLimits()
	limits.Sessions = 1
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: admission}
	admission.wireLimit = 2048
	response := &pb.ExecuteResponse{Index: 1}
	first, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Free()
	if second, err := codec.Marshal(response); err == nil {
		second.Free()
		t.Fatal("encoded response quota was exceeded")
	}
	if admission.wireBytes.Load() != 1025 {
		t.Fatal("failed encoded admission charged memory")
	}
}

func TestEncodedResponseMarshalErrorReturnsCredit(t *testing.T) {
	limits := DefaultLimits()
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: admission}
	failure := &pb.Failure{Message: string([]byte{255})}
	read := protocol.ReadFailure(failure)
	response := &pb.ExecuteResponse{Index: 1, Event: &pb.Event{Value: &pb.Event_ReadResult{ReadResult: read}}}
	if encoded, err := codec.Marshal(response); err == nil {
		encoded.Free()
		t.Fatal("invalid UTF8 unexpectedly marshaled")
	}
	if admission.wireBytes.Load() != 0 {
		t.Fatal("marshal error leaked its allocation charge")
	}
	owner := &responseBufferOwner{admission: admission, bytes: 100}
	admission.wireBytes.Store(100)
	buffer := owner.Get(1025)
	encoded := mem.NewBuffer(buffer, owner)
	encoded.Free()
	owner.Put(buffer)
	if admission.wireBytes.Load() != 0 {
		t.Fatal("buffer double cleanup returned byte credits twice")
	}
}

func TestZeroWindowReaderCannotKeepQueuedResponseAlive(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	adapter.documents["records/s:value"] = &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), 1<<20)}
	limits := DefaultLimits()
	limits.Stall = 150 * time.Millisecond
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: limits}
	server, address := startPeerServer(t, opts)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: address}, {Name: ":path", Value: pb.StoreService_Execute_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
	for _, field := range fields {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	headers := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}
	if err := framer.WriteHeaders(headers); err != nil {
		t.Fatal(err)
	}
	read := &pb.ReadRequest{Resource: "records/s:value"}
	var body []byte
	for index := uint64(1); index <= 64; index++ {
		command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
		request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
		raw, err := proto.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		header := make([]byte, 5)
		binary.BigEndian.PutUint32(header[1:5], uint32(len(raw)))
		body = append(body, header...)
		body = append(body, raw...)
	}
	for start := 0; start < len(body); {
		end := min(start+(16<<10), len(body))
		if err := framer.WriteData(1, end == len(body), body[start:end]); err != nil {
			t.Fatal(err)
		}
		start = end
	}
	queued := false
	started := time.Now()
	bounded := false
	for {
		if server.admission.wireBytes.Load() > 0 {
			queued = true
		}
		if snapshot := local.Snapshot(); snapshot.ResultBytes > 0 {
			bounded = true
			if snapshot.Retained > RecordStreamItems || snapshot.ResultBytes > store.DefaultLimits().ResultBytes || snapshot.Publishers != 0 {
				t.Fatal("slow reader accumulated result windows", snapshot)
			}
		}
		frame, err := framer.ReadFrame()
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("zero-window reader survived output stall", err)
			}
			break
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.PingFrame:
			if !frame.IsAck() {
				if err := framer.WritePing(true, frame.Data); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.DataFrame:
			t.Fatal("server ignored zero stream window", len(frame.Data()))
		}
	}
	if !queued || !bounded || time.Since(started) > time.Second {
		t.Fatal("response ownership/output watchdog not exercised", queued, bounded, time.Since(started))
	}
	// Native grpc may orphan DATA references on an aborted connection.
	// Collection proves the encoded storage is unreachable before credit release.
	for len(adapter.seen) > 0 {
		<-adapter.seen
	}
	until := time.Now().Add(time.Second)
	for server.admission.wireBytes.Load() != 0 && time.Now().Before(until) {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	waitPeerIdle(t, server)
	if snapshot := local.Snapshot(); snapshot.ResultBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("stalled native output retained Store results", snapshot)
	}
}

func TestExecuteDecodeRejectsRepeatedEnvelopesBeforeAllocation(t *testing.T) {
	limits := DefaultLimits()
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: admission}
	read := &pb.ReadRequest{Resource: "a/b/s:k"}
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for range 4096 {
		raw = append(raw, 0x1a, 0)
	}
	input := mem.BufferSlice{mem.SliceBuffer(raw)}
	decoded := &pb.ExecuteRequest{}
	if err := codec.Unmarshal(input, decoded); err == nil || decoded.Command != nil || decoded.StoreName != "" {
		t.Fatal("repeated envelope reached protobuf object allocation", err, decoded)
	}
}

func TestControlDecodeBudgetRejectsBeforeObjectConstruction(t *testing.T) {
	codec := &responseCodec{}
	for _, count := range []int{directory.MaxAnnouncements, directory.MaxAnnouncements + 1} {
		announcements := make([]*peerpb.NodeAnnouncement, count)
		node := &peerpb.NodeAnnouncement{}
		for i := range announcements {
			announcements[i] = node
		}
		request := &peerpb.SyncDirectoryRequest{Announcements: announcements}
		raw, err := proto.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		input := mem.BufferSlice{mem.SliceBuffer(raw)}
		decoded := &peerpb.SyncDirectoryRequest{}
		err = codec.Unmarshal(input, decoded)
		if count == directory.MaxAnnouncements && (err != nil || len(decoded.Announcements) != count) {
			t.Fatal("legitimate watermark capacity rejected", err)
		}
		if count > directory.MaxAnnouncements && (err == nil || len(decoded.Announcements) != 0) {
			t.Fatal("over-budget peer decode allocated nodes", err, len(decoded.Announcements))
		}
	}
	request := &pb.ResolveStoreRequest{StoreName: string(bytes.Repeat([]byte("x"), directory.MaxSyncBytes))}
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	input := mem.BufferSlice{mem.SliceBuffer(raw)}
	decoded := &pb.ResolveStoreRequest{}
	if err := codec.Unmarshal(input, decoded); err == nil || decoded.StoreName != "" {
		t.Fatal("over-budget discovery scalar decoded", err)
	}
}
