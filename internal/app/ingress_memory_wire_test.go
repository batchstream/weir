//go:build integration

package app

import (
	"bytes"
	"errors"
	"net"
	"testing"

	"golang.org/x/net/http2"
)

// Only Write is used. The deliberately short writes exercise successful-byte
// boundaries without any socket, Docker process, or backend dependency.
type ingressMemoryPartialConn struct {
	net.Conn
	limit int
	err   error
}

func (connection *ingressMemoryPartialConn) Write(data []byte) (int, error) {
	return min(connection.limit, len(data)), connection.err
}

func TestIngressMemoryWireFragmentedWrites(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString(http2.ClientPreface)
	framer := http2.NewFramer(&stream, nil)
	if err := framer.WriteSettingsAck(); err != nil {
		t.Fatal(err)
	}
	if err := framer.WriteData(3, false, []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := framer.WriteData(1, false, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	firstPayload := stream.Len() - 3
	var ping [8]byte
	if err := framer.WritePing(true, ping); err != nil {
		t.Fatal(err)
	}
	if err := framer.WriteData(1, true, []byte("defgh")); err != nil {
		t.Fatal(err)
	}
	secondPayload := stream.Len() - 5
	connection := &ingressMemoryPartialConn{limit: 3}
	wire := &ingressMemoryWire{Conn: connection, prefix: 5, total: 8}
	packet := stream.Bytes()
	for written := 0; written < len(packet); {
		offered := packet[written:min(written+11, len(packet))]
		n, err := wire.Write(offered)
		if err != nil || n != min(3, len(offered)) {
			t.Fatal("short successful Write changed", n, err)
		}
		written += n
		want := min(max(written-firstPayload, 0), 3) + min(max(written-secondPayload, 0), 5)
		if wire.written.Load() != int64(want) || wire.invalid.Load() {
			t.Fatal("counter includes unwritten bytes, a control frame, or another stream", written, wire.written.Load(), want)
		}
		if (wire.prefixAt.Load() != 0) != (want >= 5) || (wire.allAt.Load() != 0) != (want == 8) {
			t.Fatal("completion marker crossed before the successful DATA boundary", written, want)
		}
	}
	if wire.headerBytes != 0 || wire.remaining != 0 || wire.prefaceBytes != len(http2.ClientPreface) {
		t.Fatal("fragmented parser did not consume complete framing")
	}
}

func TestIngressMemoryWireWriteErrorCountsOnlySuccessfulBytes(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString(http2.ClientPreface)
	framer := http2.NewFramer(&stream, nil)
	if err := framer.WriteData(1, true, []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("synthetic socket write failure")
	tests := []struct {
		name, description string
		accepted, data    int
	}{
		{name: "partial", description: "header and only four DATA bytes succeeded", accepted: len(http2.ClientPreface) + 9 + 4, data: 4},
		{name: "zero", description: "no bytes succeeded", accepted: 0, data: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &ingressMemoryPartialConn{limit: test.accepted, err: failure}
			wire := &ingressMemoryWire{Conn: connection, prefix: 5, total: 8}
			n, err := wire.Write(stream.Bytes())
			if n != test.accepted || !errors.Is(err, failure) || wire.written.Load() != int64(test.data) || !wire.invalid.Load() {
				t.Fatal("write error lost actual successful-byte evidence", test.description, n, err, wire.written.Load())
			}
			if wire.prefixAt.Load() != 0 || wire.allAt.Load() != 0 {
				t.Fatal("unwritten offered DATA created a completion marker")
			}
		})
	}
}

func TestIngressMemoryWirePaddedDataCannotFormPrefixCondition(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString(http2.ClientPreface)
	framer := http2.NewFramer(&stream, nil)
	// PADDED is valid HTTP/2, but is not emitted by this official-client profile;
	// counting its padding as request bytes would make our observation invalid.
	payload := []byte{1, 'a', 'b', 'c', 0}
	err := framer.WriteRawFrame(http2.FrameData, http2.FlagDataPadded, 1, payload)
	if err != nil {
		t.Fatal(err)
	}
	ready := 0
	for range 32 {
		connection := &ingressMemoryPartialConn{limit: stream.Len()}
		wire := &ingressMemoryWire{Conn: connection, prefix: 5, total: 5}
		if _, err := wire.Write(stream.Bytes()); err != nil {
			t.Fatal(err)
		}
		if !wire.invalid.Load() {
			t.Fatal("PADDED payload was accepted as trustworthy message bytes")
		}
		if wire.prefixAt.Load() != 0 && wire.written.Load() == wire.prefix && !wire.invalid.Load() {
			ready++
		}
	}
	if ready != 0 {
		t.Fatal("invalid DATA observations formed the32-prefix condition", ready)
	}
}
