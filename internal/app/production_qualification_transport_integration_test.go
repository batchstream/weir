//go:build integration

package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const qualificationWorkerHeader = "weir-qualification-worker"
const qualificationHeaderLimit = 16 << 10

type qualificationFrameRecord struct {
	Kind   string
	Stream uint32
	Bytes  uint32
	Offset time.Duration
}

type qualificationFrameStream struct {
	id                             uint32
	worker                         int
	records                        [32]qualificationFrameRecord
	next                           uint64
	dataBytes, dataFrames, updates uint64
}

// These are net.Conn operation completion times, not packet arrival times.
// Only outgoing HPACK blocks are buffered; DATA bodies are never retained.
type qualificationTransport struct {
	mu                 sync.Mutex
	start              time.Time
	incomplete         string
	connections        int
	incoming, outgoing qualificationFrameParser
	decoder            *hpack.Decoder
	streams            [32]qualificationFrameStream
	used               int
	connection         qualificationFrameStream
}

type qualificationFrameParser struct {
	header               [9]byte
	headerN              int
	left, length, stream uint32
	kind, flags          byte
	preface              int
	block                [qualificationHeaderLimit]byte
	blockN               int
	headerStream         uint32
	increment            [4]byte
}

type qualificationObservedConn struct {
	net.Conn
	observer *qualificationTransport
}

func (c *qualificationObservedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.observer.observe(p[:n], false, time.Now())
	return n, err
}
func (c *qualificationObservedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.observer.observe(p[:n], true, time.Now())
	return n, err
}

func (o *qualificationTransport) dial(ctx context.Context, target string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.connections++
	if o.connections > 1 {
		o.incomplete = "multiple_connections"
	}
	o.mu.Unlock()
	wrapped := &qualificationObservedConn{Conn: conn, observer: o}
	return wrapped, nil
}

func (s *qualificationFrameStream) add(record qualificationFrameRecord) {
	s.records[s.next%uint64(len(s.records))] = record
	s.next++
	if record.Kind == "data_read" {
		s.dataFrames++
		s.dataBytes += uint64(record.Bytes)
	} else {
		s.updates++
	}
}

func (o *qualificationTransport) observe(data []byte, outgoing bool, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.incomplete != "" {
		return
	}
	p := &o.incoming
	if outgoing {
		p = &o.outgoing
	}
	for len(data) > 0 && o.incomplete == "" {
		if outgoing && p.preface < len(http2.ClientPreface) {
			if data[0] != http2.ClientPreface[p.preface] {
				o.incomplete = "bad_preface"
				return
			}
			p.preface++
			data = data[1:]
			continue
		}
		if p.headerN < 9 {
			n := copy(p.header[p.headerN:], data)
			p.headerN += n
			data = data[n:]
			if p.headerN < 9 {
				continue
			}
			p.length = uint32(p.header[0])<<16 | uint32(p.header[1])<<8 | uint32(p.header[2])
			p.left = p.length
			p.kind, p.flags = p.header[3], p.header[4]
			p.stream = binary.BigEndian.Uint32(p.header[5:]) & 0x7fffffff
			if outgoing {
				if p.headerStream != 0 && (p.kind != byte(http2.FrameContinuation) || p.stream != p.headerStream) {
					o.incomplete = "interleaved_headers"
					return
				}
				if p.kind == byte(http2.FrameHeaders) {
					// gRPC emits unpadded, non-priority request headers. Fail closed for anything else.
					if p.stream == 0 || p.flags&0x28 != 0 {
						o.incomplete = "unsupported_headers"
						return
					}
					p.headerStream = p.stream
				}
				if p.kind == byte(http2.FrameContinuation) && p.headerStream == 0 {
					o.incomplete = "unexpected_continuation"
					return
				}
				if (p.kind == byte(http2.FrameHeaders) || p.kind == byte(http2.FrameContinuation)) && uint64(p.blockN)+uint64(p.length) > qualificationHeaderLimit {
					o.incomplete = "header_limit"
					return
				}
				if p.kind == byte(http2.FrameWindowUpdate) && p.length != 4 {
					o.incomplete = "bad_window_update"
					return
				}
			}
		}
		n := min(len(data), int(p.left))
		if outgoing && (p.kind == byte(http2.FrameHeaders) || p.kind == byte(http2.FrameContinuation)) {
			p.blockN += copy(p.block[p.blockN:], data[:n])
		}
		if outgoing && p.kind == byte(http2.FrameWindowUpdate) {
			copy(p.increment[int(p.length-p.left):], data[:n])
		}
		p.left -= uint32(n)
		data = data[n:]
		if p.left != 0 {
			continue
		}
		o.complete(p, outgoing, at)
		p.headerN = 0
	}
}

func (o *qualificationTransport) complete(p *qualificationFrameParser, outgoing bool, at time.Time) {
	if outgoing && (p.kind == byte(http2.FrameHeaders) || p.kind == byte(http2.FrameContinuation)) && p.flags&byte(http2.FlagHeadersEndHeaders) != 0 {
		o.correlate(p)
		p.blockN, p.headerStream = 0, 0
	}
	kind, size := "data_read", p.length
	if outgoing {
		if p.kind != byte(http2.FrameWindowUpdate) {
			return
		}
		kind, size = "window_update_write", binary.BigEndian.Uint32(p.increment[:])&0x7fffffff
		if size == 0 {
			o.incomplete = "zero_window_update"
			return
		}
	} else if p.kind != byte(http2.FrameData) {
		return
	}
	record := qualificationFrameRecord{Kind: kind, Stream: p.stream, Bytes: size, Offset: at.Sub(o.start)}
	if p.stream == 0 {
		if !outgoing {
			o.incomplete = "data_stream_zero"
			return
		}
		o.connection.add(record)
		return
	}
	stream := o.stream(p.stream)
	if stream != nil {
		stream.add(record)
	}
}

// A read may complete before the request Write returns. Reserve the stream
// without guessing its worker; the outgoing HEADERS must later classify it.
func (o *qualificationTransport) stream(id uint32) *qualificationFrameStream {
	for i := range o.used {
		if o.streams[i].id == id {
			return &o.streams[i]
		}
	}
	if o.used == len(o.streams) {
		o.incomplete = "stream_limit"
		return nil
	}
	stream := &o.streams[o.used]
	stream.id, stream.worker = id, -1
	o.used++
	return stream
}

func (o *qualificationTransport) correlate(p *qualificationFrameParser) {
	worker, fields := -1, 0
	emit := func(f hpack.HeaderField) {
		if f.Name != qualificationWorkerHeader {
			return
		}
		fields++
		parsed, err := strconv.Atoi(f.Value)
		if err == nil && parsed >= 0 && parsed < 32 && strconv.Itoa(parsed) == f.Value {
			worker = parsed
		}
	}
	if o.decoder == nil {
		o.decoder = hpack.NewDecoder(4096, emit)
		o.decoder.SetMaxStringLength(4096)
	} else {
		o.decoder.SetEmitFunc(emit)
	}
	_, err := o.decoder.Write(p.block[:p.blockN])
	if err == nil {
		err = o.decoder.Close()
	}
	// Clear header bytes immediately, including on failure. Never include them in diagnostics.
	clear(p.block[:p.blockN])
	if err != nil {
		o.incomplete = "hpack_decode"
		return
	}
	if worker < 0 || fields != 1 {
		o.incomplete = "worker_header"
		return
	}
	stream := o.stream(p.stream)
	if stream == nil {
		return
	}
	if stream.worker != -1 {
		o.incomplete = "duplicate_stream_headers"
		return
	}
	stream.worker = worker
}

func qualificationFrameSummary(s qualificationFrameStream) string {
	records := make([]qualificationFrameRecord, 0, len(s.records))
	for n := s.next - min(s.next, uint64(len(s.records))); n < s.next; n++ {
		records = append(records, s.records[n%uint64(len(s.records))])
	}
	return fmt.Sprintf("stream=%d worker=%d data_frames=%d data_bytes=%d window_updates=%d recent=%+v", s.id, s.worker, s.dataFrames, s.dataBytes, s.updates, records)
}

func (o *qualificationTransport) summary(worker int) string {
	o.mu.Lock()
	incomplete := o.incomplete
	if o.outgoing.headerN != 0 || o.incoming.headerN != 0 || o.outgoing.headerStream != 0 {
		incomplete += "/partial_frame_or_headers"
	}
	captured, used, connection := o.streams, o.used, o.connection
	o.mu.Unlock()
	streams := ""
	for i := range used {
		if captured[i].worker < 0 {
			incomplete += "/uncorrelated_stream"
		}
		if captured[i].worker == worker {
			streams += " [" + qualificationFrameSummary(captured[i]) + "]"
		}
	}
	if streams == "" {
		incomplete += "/missing_worker"
	}
	return fmt.Sprintf("transport_diagnostic_incomplete=%q transport_worker=%s transport_connection=[%s] transport_note=net_Conn_completion_not_packet_arrival_or_backend_attribution", incomplete, streams, qualificationFrameSummary(connection))
}
