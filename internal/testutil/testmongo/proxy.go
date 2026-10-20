package testmongo

import (
	"bytes"
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

// ProxyFault targets one command on this observer's connections. Match is an
// exact byte fragment of the owned request, usually its unique document ID.
// Empty Match is useful only for a strictly serial commitTransaction phase.
type ProxyFault struct {
	Command      string
	Match        []byte
	Before       <-chan struct{}
	After        <-chan struct{}
	Started      chan<- struct{}
	Acknowledged chan<- struct{}
	Drop         bool
}

// Keep the observer alive beyond the caller's cancellation and recovery gates.
// Its own timeout must not release a held reply to make those assertions pass.
const proxyFaultTimeout = 20 * time.Second

type Proxy struct {
	backendAddress  string
	uri             string
	clientTLS       *tls.Config
	discovery       *replicaAddressMap
	operations      *operationRegistry
	memberIndex     int
	observations    []ReplicaProxyConnection
	replicaRequests int
	rejections      int
	lastRejection   ReplicaProxyRejection
	closeOnce       sync.Once
	Monitor         *event.CommandMonitor
	listener        net.Listener
	mu              sync.Mutex
	events          []WireEvent
	forwarded       []string
	conns           map[net.Conn]struct{}
	group           sync.WaitGroup
	closed          bool
	unavailable     bool
	upstream        int
	peakUpstream    int
	fault           *ProxyFault
	done            chan struct{}
	DropCommand     string
	DropGate        <-chan struct{}
	DropRemaining   atomic.Int64
	AlterCommand    string
	AlterMode       string
	AlterRemaining  atomic.Int64
}

type proxyOptions struct {
	uri                  string
	listenAddress        string
	serverTLS, clientTLS *tls.Config
	listener             net.Listener
	discovery            *replicaAddressMap
	operations           *operationRegistry
	memberIndex          int
}

func startProxy(t *testing.T, opts proxyOptions) *Proxy {
	t.Helper()
	address := opts.listenAddress
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener := opts.listener
	var err error
	if listener == nil {
		listener, err = net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
	}
	upstream, err := url.Parse(opts.uri)
	if err != nil {
		t.Fatal("invalid owned fixture URI")
	}
	p := &Proxy{listener: listener, backendAddress: upstream.Host, conns: make(map[net.Conn]struct{}), done: make(chan struct{}), discovery: opts.discovery, operations: opts.operations, memberIndex: opts.memberIndex}
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
			if p.closed || p.unavailable {
				p.mu.Unlock()
				_ = conn.Close()
				continue
			}
			p.conns[conn] = struct{}{}
			p.mu.Unlock()
			p.group.Add(1)
			go p.relay(conn)
		}
	}()
	t.Cleanup(p.Close)
	return p
}

// Close interrupts owned sockets and joins every relay before fixture cleanup.
func (p *Proxy) Close() {
	p.closeOnce.Do(func() {
		_ = p.listener.Close()
		p.mu.Lock()
		p.closed = true
		close(p.done)
		for conn := range p.conns {
			p.closeConnection(conn)
		}
		p.mu.Unlock()
		p.group.Wait()
	})
}

func (p *Proxy) closeConnection(conn net.Conn) {
	if secure, ok := conn.(*tls.Conn); ok && p.discovery != nil {
		_ = secure.NetConn().Close()
		return
	}
	_ = conn.Close()
}

// ArmFault replaces a consumed fault only after the caller has observed its
// completion. Control state is synchronized with the active wire relays.
func (p *Proxy) ArmFault(fault ProxyFault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fault.Match = append([]byte(nil), fault.Match...)
	p.fault = &fault
}

func (p *Proxy) takeFault(command string, request []byte) *ProxyFault {
	p.mu.Lock()
	defer p.mu.Unlock()
	fault := p.fault
	if fault == nil || fault.Command != command || !bytes.Contains(request, fault.Match) {
		return nil
	}
	p.fault = nil
	return fault
}

func (p *Proxy) waitGate(gate <-chan struct{}) bool {
	if gate == nil {
		return true
	}
	select {
	case <-gate:
		return true
	case <-p.done:
	case <-time.After(proxyFaultTimeout):
	}
	return false
}

func (p *Proxy) URI() string {
	return p.uri
}

func (p *Proxy) Events() []WireEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]WireEvent(nil), p.events...)
}

// ForwardedCommands records complete request writes independently of whether a
// reply reached this observer. It distinguishes replay attempts from ACK counts.
func (p *Proxy) ForwardedCommands() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.forwarded...)
}

// Sockets counts observer-owned upstream connections. A serial relay can retain
// one after downstream Close while waiting for a callback or backend reply.
// Counters bracket Dial/Close and are not atomic OS or database measurements.
func (p *Proxy) Sockets() (current, peak int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.upstream, p.peakUpstream
}

// SetUnavailable interrupts only this proxy's connections. The real backend and
// the listener stay running so the same Weir process can reconnect after recovery.
func (p *Proxy) SetUnavailable(unavailable bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unavailable = unavailable
	if unavailable {
		for conn := range p.conns {
			_ = conn.Close()
		}
	}
}

func (p *Proxy) relay(client net.Conn) {
	defer p.group.Done()
	defer p.closeConnection(client)
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()
	observation := -1
	if p.discovery != nil {
		observation = p.beginReplicaConnection()
		if observation < 0 {
			return
		}
		if secure, ok := client.(*tls.Conn); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := secure.HandshakeContext(ctx)
			cancel()
			if err != nil {
				return
			}
			p.replicaTLS(observation, false)
		}
	}
	backend, err := net.DialTimeout("tcp", p.backendAddress, time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = backend.Close()
		return
	}
	p.conns[backend] = struct{}{}
	p.upstream++
	p.peakUpstream = max(p.peakUpstream, p.upstream)
	p.mu.Unlock()
	rawBackend := backend
	defer func() {
		// Keep the original proxy's raw Close semantics after backend is wrapped
		// in TLS; waiting for close_notify would manufacture a retirement tail.
		_ = rawBackend.Close()
		p.mu.Lock()
		delete(p.conns, rawBackend)
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
		if p.discovery != nil {
			p.replicaTLS(observation, true)
		}
	}
	p.mu.Lock()
	if p.closed || p.unavailable {
		p.mu.Unlock()
		return
	}
	p.conns[backend] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, backend)
		p.mu.Unlock()
	}()
	for {
		_ = client.SetDeadline(time.Now().Add(proxyFaultTimeout))
		_ = backend.SetDeadline(time.Now().Add(proxyFaultTimeout))
		request, err := readMessage(client)
		if err != nil {
			return
		}
		doc := commandDocument(request)
		if p.discovery != nil {
			doc, _, err = replicaWireDocument(request, false)
			if err != nil {
				p.rejectReplicaMessage(request, false, err)
				return
			}
		}
		elements, _ := doc.Elements()
		name := ""
		if len(elements) > 0 {
			name = elements[0].Key()
		}
		if p.discovery != nil && !p.reserveReplicaRequest(name) {
			return
		}
		var matches []operationMatch
		if p.operations != nil {
			observationOptions := operationBeginOptions{message: request, member: p.memberIndex, connection: observation}
			matches, err = p.operations.begin(observationOptions)
			if err != nil {
				return
			}
		}
		fault := p.takeFault(name, request)
		if fault != nil {
			if fault.Started != nil {
				select {
				case fault.Started <- struct{}{}:
				case <-p.done:
					return
				}
			}
			if !p.waitGate(fault.Before) {
				return
			}
		}
		if p.Monitor != nil && p.Monitor.Started != nil {
			e := &event.CommandStartedEvent{CommandName: name, Command: doc}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.Monitor.Started(ctx, e)
			cancel()
		}
		written, writeErr := backend.Write(request)
		if writeErr != nil || written != len(request) {
			if p.operations != nil && len(matches) != 0 {
				p.operations.fail()
			}
			return
		}
		if p.operations != nil {
			p.operations.forwarded(matches)
		}
		if p.discovery != nil {
			p.recordReplicaCommand(observation, name)
		}
		p.mu.Lock()
		p.forwarded = append(p.forwarded, name)
		p.mu.Unlock()
		response, err := readMessage(backend)
		if err != nil {
			return
		}
		if p.discovery != nil {
			rewritten, rewriteErr := p.discovery.rewriteReply(request, response)
			if rewriteErr != nil {
				p.rejectReplicaMessage(response, true, rewriteErr)
				return
			}
			response = rewritten
			if replicaHello(name) {
				p.mu.Lock()
				p.observations[observation].DiscoveryReplies++
				p.mu.Unlock()
			}
		}
		e := WireEvent{Command: name}
		if n, ok := doc.Lookup("txnNumber").Int64OK(); ok {
			e.Transaction = n
		}
		if lsid, ok := doc.Lookup("lsid").DocumentOK(); ok {
			e.Session = fmt.Sprintf("%x", []byte(lsid))
		}
		reply := commandDocument(response)
		if p.discovery != nil {
			reply, _, err = replicaWireDocument(response, true)
			if err != nil {
				return
			}
		}
		if p.operations != nil {
			held := p.operations.received(matches, reply)
			if held != nil {
				if !p.waitGate(held.release) {
					held.gateFailed()
					return
				}
				p.closeConnection(client)
				p.operations.finish(matches, true)
				return
			}
		}
		if p.Monitor != nil && p.Monitor.Succeeded != nil {
			finished := event.CommandFinishedEvent{CommandName: name}
			observed := &event.CommandSucceededEvent{CommandFinishedEvent: finished, Reply: reply}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.Monitor.Succeeded(ctx, observed)
			cancel()
		}
		e.ReplyDigest = sha256.Sum256(reply)
		e.Acknowledged = reply.Lookup("ok").AsInt64() == 1
		if fault != nil && fault.Drop {
			e.Dropped = true
		}
		if name == p.DropCommand && p.DropRemaining.Load() > 0 {
			e.Dropped = true
			p.DropRemaining.Add(-1)
		}
		p.mu.Lock()
		p.events = append(p.events, e)
		p.mu.Unlock()
		if fault != nil {
			if fault.Acknowledged != nil {
				select {
				case fault.Acknowledged <- struct{}{}:
				case <-p.done:
					return
				}
			}
			if !p.waitGate(fault.After) {
				return
			}
		}
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
			response = alterCommandReply(response, p.AlterMode)
		}
		written, err = client.Write(response)
		if err != nil || written != len(response) {
			return
		}
		if p.operations != nil {
			p.operations.finish(matches, false)
		}
	}
}

// Deliberate envelope faults after a real backend reply; this is not evidence
// of real sharded MongoDB failure handling.
func alterCommandReply(message []byte, mode string) []byte {
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
							item = bson.D{
								{Key: "idx", Value: int32(k)},
								{Key: "ok", Value: int32(0)},
								{Key: "n", Value: int32(0)},
								{Key: "code", Value: int32(391)},
							}
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
