//go:build integration

package app

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func qualificationTestFrame(kind http2.FrameType, flags byte, id uint32, body []byte) []byte {
	frame := make([]byte, 9+len(body))
	frame[0], frame[1], frame[2] = byte(len(body)>>16), byte(len(body)>>8), byte(len(body))
	frame[3], frame[4] = byte(kind), flags
	binary.BigEndian.PutUint32(frame[5:], id)
	copy(frame[9:], body)
	return frame
}

func qualificationTestHeaders(t *testing.T, worker string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder := hpack.NewEncoder(&buffer)
	field := hpack.HeaderField{Name: qualificationWorkerHeader, Value: worker}
	if err := encoder.WriteField(field); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestQualificationTransportFragmented(t *testing.T) {
	start := time.Now()
	observer := &qualificationTransport{start: start}
	headers := qualificationTestHeaders(t, "7")
	request := append([]byte(http2.ClientPreface), qualificationTestFrame(http2.FrameHeaders, 0, 3, headers[:1])...)
	request = append(request, qualificationTestFrame(http2.FrameContinuation, 4, 3, headers[1:])...)
	for _, b := range request {
		observer.observe([]byte{b}, true, start)
	}
	payload := bytes.Repeat([]byte("private-body"), 10000)
	data := qualificationTestFrame(http2.FrameData, 0, 3, payload)
	for _, b := range data[:len(data)-1] {
		observer.observe([]byte{b}, false, start.Add(time.Second))
	}
	if observer.streams[0].dataFrames != 0 {
		t.Fatal("recorded incomplete DATA")
	}
	observer.observe(data[len(data)-1:], false, start.Add(2*time.Second))
	increment := []byte{0, 0, 4, 0}
	for _, id := range []uint32{0, 3} {
		frame := qualificationTestFrame(http2.FrameWindowUpdate, 0, id, increment)
		for _, b := range frame {
			observer.observe([]byte{b}, true, start.Add(3*time.Second))
		}
	}
	stream := observer.streams[0]
	if observer.incomplete != "" || observer.used != 1 || stream.worker != 7 || stream.id != 3 || stream.dataFrames != 1 || stream.dataBytes != uint64(len(payload)) || stream.updates != 1 || observer.connection.updates != 1 {
		t.Fatalf("bad observation: %s", observer.summary(7))
	}
	if stream.records[0].Offset != 2*time.Second || stream.records[1].Bytes != 1024 {
		t.Fatal("wrong DATA completion or increment")
	}
	if strings.Contains(observer.summary(7), "private-body") || observer.outgoing.blockN != 0 || observer.outgoing.block != [qualificationHeaderLimit]byte{} {
		t.Fatal("retained body or decoded header block")
	}
	for range 100 {
		observer.observe(data, false, start)
	}
	if observer.streams[0].next != 102 || len(observer.streams[0].records) != 32 {
		t.Fatal("ring counters incorrect")
	}
}

func TestQualificationTransportRepeatedWorkerRPCs(t *testing.T) {
	start := time.Now()
	observer := &qualificationTransport{start: start}
	observer.observe([]byte(http2.ClientPreface), true, start)
	nextID := uint32(1)
	startRPC := func(worker int, offset time.Duration) uint32 {
		id := nextID
		nextID += 2
		headers := qualificationTestHeaders(t, strconv.Itoa(worker))
		observer.observe(qualificationTestFrame(http2.FrameHeaders, 4, id, headers), true, start.Add(offset))
		return id
	}
	// Worker31's original stream1 remains active while the other workers
	// complete three successive RPCs. Completed IDs must not exhaust the
	// worker count or evict the still-active failure evidence.
	failedID := startRPC(31, 0)
	observer.observe(qualificationTestFrame(http2.FrameData, 0, failedID, []byte{1}), false, start)
	for round := range 3 {
		for worker := range 31 {
			offset := time.Duration(round*40) * time.Second
			id := startRPC(worker, offset)
			observer.observe(qualificationTestFrame(http2.FrameData, 1, id, []byte{1}), false, start.Add(offset))
		}
	}
	observer.observe(qualificationTestFrame(http2.FrameWindowUpdate, 0, failedID, []byte{0, 0, 4, 0}), true, start.Add(118*time.Second))
	observer.observe(qualificationTestFrame(http2.FrameData, 1, failedID, []byte{1, 2}), false, start.Add(119*time.Second))
	// The delayed worker also performs two successive RPCs after its first.
	for round := range 2 {
		offset := 119*time.Second + time.Duration(round+1)*100*time.Millisecond
		id := startRPC(31, offset)
		observer.observe(qualificationTestFrame(http2.FrameData, 1, id, []byte{1}), false, start.Add(offset))
	}
	if observer.incomplete != "" || observer.used != 96 {
		t.Fatalf("repeated RPC coverage incomplete: %s", observer.summary(31))
	}
	original := observer.streams[0]
	if original.id != 1 || original.worker != 31 || original.dataFrames != 2 || original.dataBytes != 3 || original.updates != 1 || original.records[2].Offset != 119*time.Second {
		t.Fatalf("original failed stream evidence overwritten: %s", observer.summary(31))
	}
	if !strings.Contains(observer.summary(31), `transport_diagnostic_incomplete=""`) {
		t.Fatal("complete repeated-RPC timeline reported incomplete")
	}
}

func TestQualificationTransportCorrelationBounds(t *testing.T) {
	for _, worker := range []string{"0", "31"} {
		observer := &qualificationTransport{}
		headers := qualificationTestHeaders(t, worker)
		request := append([]byte(http2.ClientPreface), qualificationTestFrame(http2.FrameHeaders, 4, 1, headers)...)
		observer.observe(request, true, time.Now())
		if observer.incomplete != "" || observer.used != 1 || observer.streams[0].worker < 0 {
			t.Fatal("valid boundary worker not classified")
		}
		observer.observe(qualificationTestFrame(http2.FrameHeaders, 4, 1, headers), true, time.Now())
		if observer.incomplete != "duplicate_stream_headers" {
			t.Fatal("duplicate stream correlation accepted")
		}
	}
	t.Run("duplicate_worker_header", func(t *testing.T) {
		observer := &qualificationTransport{}
		headers := qualificationTestHeaders(t, "3")
		headers = append(headers, headers...)
		request := append([]byte(http2.ClientPreface), qualificationTestFrame(http2.FrameHeaders, 4, 1, headers)...)
		observer.observe(request, true, time.Now())
		if observer.incomplete != "worker_header" {
			t.Fatal("duplicate worker header accepted")
		}
	})
	for _, worker := range []string{"", "-1", "32", "07", "7,8", "private-secret"} {
		t.Run("invalid_"+worker, func(t *testing.T) {
			observer := &qualificationTransport{}
			request := append([]byte(http2.ClientPreface), qualificationTestFrame(http2.FrameHeaders, 4, 1, qualificationTestHeaders(t, worker))...)
			observer.observe(request, true, time.Now())
			if observer.incomplete != "worker_header" || observer.used != 0 {
				t.Fatal("invalid worker classified")
			}
		})
	}
	t.Run("data_before_write_completion", func(t *testing.T) {
		observer := &qualificationTransport{}
		observer.observe(qualificationTestFrame(http2.FrameData, 0, 1, []byte("body")), false, time.Now())
		request := append([]byte(http2.ClientPreface), qualificationTestFrame(http2.FrameHeaders, 4, 1, qualificationTestHeaders(t, "2"))...)
		observer.observe(request, true, time.Now())
		if observer.incomplete != "" || observer.streams[0].worker != 2 || observer.streams[0].dataBytes != 4 {
			t.Fatal("read/write completion race lost correlation")
		}
	})
	t.Run("stream_limit", func(t *testing.T) {
		observer := &qualificationTransport{}
		for id := uint32(1); id <= 257; id += 2 {
			observer.observe(qualificationTestFrame(http2.FrameData, 0, id, nil), false, time.Now())
		}
		if observer.used != 128 || observer.incomplete != "stream_limit" || observer.streams[0].id != 1 || observer.streams[0].dataFrames != 1 {
			t.Fatal("stream bound not enforced")
		}
	})
	for _, test := range []struct {
		name  string
		frame []byte
		want  string
	}{
		{"oversize", qualificationTestFrame(http2.FrameHeaders, 4, 1, make([]byte, qualificationHeaderLimit+1)), "header_limit"},
		{"bad_hpack", qualificationTestFrame(http2.FrameHeaders, 4, 1, []byte{0xff}), "hpack_decode"},
		{"unexpected_continuation", qualificationTestFrame(http2.FrameContinuation, 4, 1, nil), "unexpected_continuation"},
		{"bad_window", qualificationTestFrame(http2.FrameWindowUpdate, 0, 0, []byte{1}), "bad_window_update"},
		{"zero_window", qualificationTestFrame(http2.FrameWindowUpdate, 0, 0, make([]byte, 4)), "zero_window_update"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observer := &qualificationTransport{}
			observer.observe(append([]byte(http2.ClientPreface), test.frame...), true, time.Now())
			if observer.incomplete != test.want {
				t.Fatalf("got %q want %q", observer.incomplete, test.want)
			}
		})
	}
}

type qualificationPartialConn struct {
	net.Conn
	read    []byte
	written []byte
	limit   int
	err     error
}

func (c *qualificationPartialConn) Read(p []byte) (int, error) {
	n := copy(p, c.read)
	c.read = c.read[n:]
	return n, c.err
}
func (c *qualificationPartialConn) Write(p []byte) (int, error) {
	n := min(c.limit, len(p))
	c.written = append(c.written, p[:n]...)
	return n, c.err
}

func TestQualificationTransportPartialIO(t *testing.T) {
	marker := errors.New("partial")
	raw := &qualificationPartialConn{limit: 5, err: marker}
	observer := &qualificationTransport{}
	conn := &qualificationObservedConn{Conn: raw, observer: observer}
	request := []byte(http2.ClientPreface)
	n, err := conn.Write(request)
	if n != 5 || err != marker || !bytes.Equal(raw.written, request[:5]) || observer.outgoing.preface != 5 {
		t.Fatal("partial write changed")
	}
	raw.err = nil
	raw.limit = len(request)
	n, err = conn.Write(request[5:])
	if n != len(request)-5 || err != nil || !bytes.Equal(raw.written, request) {
		t.Fatal("write remainder changed")
	}
	data := qualificationTestFrame(http2.FrameData, 0, 1, []byte("private"))
	raw.read = data
	raw.err = io.EOF
	buffer := make([]byte, len(data)+10)
	n, err = conn.Read(buffer)
	if n != len(data) || err != io.EOF || !bytes.Equal(buffer[:n], data) || observer.streams[0].dataBytes != 7 {
		t.Fatal("partial read changed")
	}
}

func TestQualificationTransportConcurrentDirections(t *testing.T) {
	observer := &qualificationTransport{}
	observer.observe([]byte(http2.ClientPreface), true, time.Now())
	var pending sync.WaitGroup
	pending.Add(2)
	go func() {
		defer pending.Done()
		for range 100 {
			observer.observe(qualificationTestFrame(http2.FrameWindowUpdate, 0, 0, []byte{0, 0, 0, 1}), true, time.Now())
		}
	}()
	go func() {
		defer pending.Done()
		for range 100 {
			observer.observe(qualificationTestFrame(http2.FrameData, 0, 1, nil), false, time.Now())
			_ = observer.summary(0)
		}
	}()
	pending.Wait()
	if observer.connection.updates != 100 || observer.streams[0].dataFrames != 100 {
		t.Fatal("concurrent evidence lost")
	}
}
