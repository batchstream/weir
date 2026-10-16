package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type receiveParserReader struct {
	length         uint32
	compressed     bool
	rejected       error
	reserved, read int
}

func (r *receiveParserReader) ReadMessageHeader(header []byte) error {
	binary.BigEndian.PutUint32(header[1:], r.length)
	if r.compressed {
		header[0] = 1
	}
	return nil
}
func (r *receiveParserReader) ReserveReceiveMessage(length int, compressed bool) error {
	if length != int(r.length) || compressed != r.compressed {
		panic("incorrect frame reservation")
	}
	r.reserved++
	return r.rejected
}
func (r *receiveParserReader) Read(int) (mem.BufferSlice, error) {
	r.read++
	data := mem.BufferSlice{mem.SliceBuffer(make([]byte, int(r.length)))}
	return data, nil
}
func TestReceiveParserReservesAfterActualHeaderBeforeBody(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		denied := status.Error(codes.ResourceExhausted, "owned capacity")
		reader := &receiveParserReader{length: 12 << 20, compressed: compressed, rejected: denied}
		parser := &parser{r: reader}
		_, _, err := parser.recvMsg(16 << 20)
		if !errors.Is(err, denied) || reader.reserved != 1 || reader.read != 0 {
			t.Fatal("body read preceded exact-length budget", err, reader.reserved, reader.read)
		}
	}
	reader := &receiveParserReader{length: 17 << 20}
	parser := &parser{r: reader}
	_, _, err := parser.recvMsg(16 << 20)
	if status.Code(err) != codes.ResourceExhausted || reader.reserved != 0 || reader.read != 0 {
		t.Fatal("oversized frame entered allocation hook", err, reader.reserved, reader.read)
	}
}

type receiveUnaryMemory struct{ reserved, decoded, closed, retired, connections atomic.Int32 }

func (m *receiveUnaryMemory) NewConnection() (func(), error) {
	m.connections.Add(1)
	var once sync.Once
	release := func() { once.Do(func() { m.connections.Add(-1) }) }
	return release, nil
}
func (m *receiveUnaryMemory) NewStream(context.Context, int) (mem.ReceiveStream, error) {
	owner := &receiveUnaryOwner{memory: m}
	return owner, nil
}

type receiveUnaryOwner struct{ memory *receiveUnaryMemory }

func (o *receiveUnaryOwner) ReserveMessage(length int, compressed bool) error {
	if length <= 0 || compressed {
		return status.Error(codes.InvalidArgument, "bad test frame")
	}
	o.memory.reserved.Add(1)
	return nil
}
func (*receiveUnaryOwner) Buffer(int) (func(), error)              { release := func() {}; return release, nil }
func (*receiveUnaryOwner) BeforeDecode(mem.BufferSlice, any) error { return nil }
func (o *receiveUnaryOwner) Decoded(any)                           { o.memory.decoded.Add(1) }
func (*receiveUnaryOwner) ReleaseMessage()                         {}
func (o *receiveUnaryOwner) Close()                                { o.memory.closed.Add(1) }
func (o *receiveUnaryOwner) Retire()                               { o.memory.retired.Add(1) }

type receiveUnaryService interface{}

func receiveUnaryHandler(_ any, _ context.Context, decode func(any) error, _ UnaryServerInterceptor) (any, error) {
	request := &wrapperspb.BytesValue{}
	if err := decode(request); err != nil {
		return nil, err
	}
	return request, nil
}
func TestReceiveUnaryUsesNativeMessageAndRetirementHooks(t *testing.T) {
	memory := &receiveUnaryMemory{}
	server := NewServer(ReceiveMemory(memory))
	method := MethodDesc{MethodName: "Unary", Handler: receiveUnaryHandler}
	description := ServiceDesc{ServiceName: "receive.Memory", HandlerType: (*receiveUnaryService)(nil), Methods: []MethodDesc{method}}
	implementation := new(struct{})
	server.RegisterService(&description, implementation)
	listener := bufconn.Listen(1 << 20)
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-serving })
	dial := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	client, err := NewClient("passthrough:///memory", WithTransportCredentials(insecure.NewCredentials()), WithContextDialer(dial), WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request := &wrapperspb.BytesValue{Value: make([]byte, 8192)}
	response := &wrapperspb.BytesValue{}
	if err := client.Invoke(ctx, "/receive.Memory/Unary", request, response); err != nil {
		t.Fatal(err)
	}
	if len(response.Value) != len(request.Value) || memory.reserved.Load() != 1 || memory.decoded.Load() != 1 {
		t.Fatal("unary bypassed native accounting", len(response.Value), memory.reserved.Load(), memory.decoded.Load())
	}
	deadline := time.Now().Add(time.Second)
	for (memory.closed.Load() != 1 || memory.retired.Load() != 1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if memory.closed.Load() != 1 || memory.retired.Load() != 1 {
		t.Fatal("unary lifecycle incomplete", memory.closed.Load(), memory.retired.Load())
	}
	client.Close()
	for memory.connections.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if memory.connections.Load() != 0 {
		t.Fatal("native connection did not retire")
	}
}

func (*receiveUnaryOwner) SetContext(context.Context) {}
