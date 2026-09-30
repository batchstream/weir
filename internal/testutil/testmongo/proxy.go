package testmongo

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type WireEvent struct {
	Command      string
	Transaction  int64
	Session      string
	Acknowledged bool
	Dropped      bool
	ReplyDigest  [32]byte
}
type Proxy struct {
	backendAddress string
	uri            string
	clientTLS      *tls.Config
	Monitor        *event.CommandMonitor
	listener       net.Listener
	mu             sync.Mutex
	events         []WireEvent
	conns          map[net.Conn]struct{}
	group          sync.WaitGroup
	closed         bool
	upstream       int
	peakUpstream   int
	DropCommand    string
	DropGate       <-chan struct{}
	DropRemaining  atomic.Int64
	AlterCommand   string
	AlterMode      string
	AlterRemaining atomic.Int64
}

type proxyOptions struct {
	uri                  string
	serverTLS, clientTLS *tls.Config
}

func startProxy(t *testing.T, opts proxyOptions) *Proxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := url.Parse(opts.uri)
	if err != nil {
		t.Fatal("invalid owned fixture URI")
	}
	p := &Proxy{listener: listener, backendAddress: upstream.Host, conns: make(map[net.Conn]struct{})}
	if opts.serverTLS != nil {
		// Test-only TLS termination observes decrypted commands for fault injection.
		// Production direct-to-mongod TLS is qualified separately.
		listener = tls.NewListener(listener, opts.serverTLS)
		p.listener = listener
		p.clientTLS = opts.clientTLS
	}
	upstream.Host = listener.Addr().String()
	p.uri = upstream.String()
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
	return p.uri
}
func (p *Proxy) Events() []WireEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]WireEvent(nil), p.events...)
}

// Sockets counts observer-owned upstream connections. A serial relay can retain
// one after downstream Close while waiting for a callback or backend reply.
// Counters bracket Dial/Close and are not atomic OS or database measurements.
func (p *Proxy) Sockets() (current, peak int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.upstream, p.peakUpstream
}
func (p *Proxy) relay(client net.Conn) {
	defer p.group.Done()
	defer client.Close()
	defer func() { p.mu.Lock(); delete(p.conns, client); p.mu.Unlock() }()
	backend, err := net.DialTimeout("tcp", p.backendAddress, time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	p.upstream++
	p.peakUpstream = max(p.peakUpstream, p.upstream)
	p.mu.Unlock()
	rawBackend := backend
	defer func() {
		// Keep the original proxy's raw Close semantics after backend is wrapped
		// in TLS; waiting for close_notify would manufacture a retirement tail.
		_ = rawBackend.Close()
		p.mu.Lock()
		p.upstream--
		p.mu.Unlock()
	}()
	if p.clientTLS != nil {
		secure := tls.Client(backend, p.clientTLS)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := secure.HandshakeContext(ctx)
		cancel()
		if err != nil {
			return
		}
		backend = secure
	}
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
		if p.Monitor != nil && p.Monitor.Started != nil {
			e := &event.CommandStartedEvent{CommandName: name, Command: doc}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.Monitor.Started(ctx, e)
			cancel()
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
		if p.Monitor != nil && p.Monitor.Succeeded != nil {
			finished := event.CommandFinishedEvent{CommandName: name}
			observed := &event.CommandSucceededEvent{CommandFinishedEvent: finished, Reply: reply}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.Monitor.Succeeded(ctx, observed)
			cancel()
		}
		e.ReplyDigest = sha256.Sum256(reply)
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
	bulk := false
	for _, field := range doc {
		bulk = bulk || field.Key == "nErrors"
	}
	if mode == "write_error_391" && !bulk {
		writeError := bson.D{{Key: "index", Value: 0}, {Key: "code", Value: 391}, {Key: "errmsg", Value: "injected reauth error"}}
		doc = bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: 0}, {Key: "writeErrors", Value: bson.A{writeError}}}
	}
	if mode == "missing_n" {
		for i := range doc {
			if doc[i].Key == "n" {
				doc[i].Key = "missing_n"
			}
		}
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
			if bulk && (mode == "missing_n" || mode == "write_error_391") {
				for j := range cursor {
					if cursor[j].Key != "firstBatch" && cursor[j].Key != "nextBatch" {
						continue
					}
					batch, ok := cursor[j].Value.(bson.A)
					if !ok {
						return message
					}
					for k := range batch {
						item, ok := batch[k].(bson.D)
						if !ok {
							return message
						}
						if mode == "write_error_391" {
							item = bson.D{{Key: "idx", Value: int32(k)}, {Key: "ok", Value: int32(0)}, {Key: "n", Value: int32(0)}, {Key: "code", Value: int32(391)}}
						} else {
							for m := range item {
								if item[m].Key == "n" {
									item[m].Key = "missing_n"
								}
							}
						}
						batch[k] = item
					}
					cursor[j].Value = batch
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
