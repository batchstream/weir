//go:build integration

package testmongo

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type WireEvent struct {
	Command      string
	Transaction  int64
	Session      string
	Acknowledged bool
	Dropped      bool
}
type Proxy struct {
	listener       net.Listener
	mu             sync.Mutex
	events         []WireEvent
	conns          map[net.Conn]struct{}
	group          sync.WaitGroup
	closed         bool
	DropCommand    string
	DropGate       <-chan struct{}
	DropRemaining  atomic.Int64
	AlterCommand   string
	AlterMode      string
	AlterRemaining atomic.Int64
}

func StartProxy(t *testing.T) *Proxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{listener: listener, conns: make(map[net.Conn]struct{})}
	p.group.Add(1)
	go func() {
		defer p.group.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.conns[conn] = struct{}{}
			p.mu.Unlock()
			p.group.Add(1)
			go p.relay(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		p.closed = true
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.group.Wait()
	})
	return p
}
func (p *Proxy) URI() string {
	return "mongodb://" + p.listener.Addr().String() + "/?directConnection=true&serverMonitoringMode=poll"
}
func (p *Proxy) Events() []WireEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]WireEvent(nil), p.events...)
}
func (p *Proxy) relay(client net.Conn) {
	defer p.group.Done()
	defer client.Close()
	defer func() { p.mu.Lock(); delete(p.conns, client); p.mu.Unlock() }()
	backend, err := net.DialTimeout("tcp", "127.0.0.1:27028", time.Second)
	if err != nil {
		return
	}
	defer backend.Close()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.conns[backend] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, backend); p.mu.Unlock() }()
	for {
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		_ = backend.SetDeadline(time.Now().Add(5 * time.Second))
		request, err := readMessage(client)
		if err != nil {
			return
		}
		doc := commandDocument(request)
		elements, _ := doc.Elements()
		name := ""
		if len(elements) > 0 {
			name = elements[0].Key()
		}
		if _, err = backend.Write(request); err != nil {
			return
		}
		response, err := readMessage(backend)
		if err != nil {
			return
		}
		e := WireEvent{Command: name}
		if n, ok := doc.Lookup("txnNumber").Int64OK(); ok {
			e.Transaction = n
		}
		if lsid, ok := doc.Lookup("lsid").DocumentOK(); ok {
			e.Session = fmt.Sprintf("%x", []byte(lsid))
		}
		reply := commandDocument(response)
		e.Acknowledged = reply.Lookup("ok").AsInt64() == 1
		if name == p.DropCommand && p.DropRemaining.Load() > 0 {
			e.Dropped = true
			p.DropRemaining.Add(-1)
		}
		p.mu.Lock()
		p.events = append(p.events, e)
		p.mu.Unlock()
		if e.Dropped {
			if p.DropGate != nil {
				select {
				case <-p.DropGate:
				case <-time.After(time.Second):
				}
			}
			return
		}
		if name == p.AlterCommand && p.AlterRemaining.Load() > 0 {
			p.AlterRemaining.Add(-1)
			response = alterScanReply(response, p.AlterMode)
		}
		if _, err = client.Write(response); err != nil {
			return
		}
	}
}

// Deliberate envelope faults after a real backend reply; this is not evidence
// of real sharded MongoDB failure handling.
func alterScanReply(message []byte, mode string) []byte {
	if len(message) < 21 || binary.LittleEndian.Uint32(message[12:16]) != 2013 || message[20] != 0 {
		return message
	}
	var doc bson.D
	if bson.Unmarshal(commandDocument(message), &doc) != nil {
		return message
	}
	partial := bson.E{Key: "partialResultsReturned", Value: true}
	if mode == "partial_top" {
		doc = append(doc, partial)
	} else {
		for i := range doc {
			if doc[i].Key != "cursor" {
				continue
			}
			cursor, ok := doc[i].Value.(bson.D)
			if !ok {
				return message
			}
			if mode == "partial" {
				cursor = append(cursor, partial)
			}
			if mode == "missing_batch" {
				for j := range cursor {
					if cursor[j].Key == "firstBatch" || cursor[j].Key == "nextBatch" {
						cursor[j].Key = "unrecognizedBatch"
					}
				}
			}
			doc[i].Value = cursor
		}
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return message
	}
	result := append([]byte(nil), message[:21]...)
	result = append(result, raw...)
	if mode == "truncate" {
		result = result[:len(result)-1]
	}
	binary.LittleEndian.PutUint32(result, uint32(len(result)))
	return result
}
func readMessage(r io.Reader) ([]byte, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := int(binary.LittleEndian.Uint32(header))
	if length < 16 || length > 48<<20 {
		return nil, fmt.Errorf("wire length")
	}
	data := make([]byte, length)
	copy(data, header)
	_, err := io.ReadFull(r, data[16:])
	return data, err
}
func commandDocument(message []byte) bson.Raw {
	if len(message) < 21 {
		return nil
	}
	switch binary.LittleEndian.Uint32(message[12:16]) {
	case 2013:
		if message[20] == 0 {
			return bson.Raw(message[21:])
		}
	case 2004:
		i := 20
		for i < len(message) && message[i] != 0 {
			i++
		}
		i += 9
		if i < len(message) {
			return bson.Raw(message[i:])
		}
	case 1:
		if len(message) > 36 {
			return bson.Raw(message[36:])
		}
	}
	return nil
}
