// Package netlimit provides bounded standard Go DNS I/O for clients and servers.
// Callers retain lifecycle, concurrency and address selection.
package netlimit

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MaxDNSAddresses = 8
const maxDNSReadBytes = 4098

// LookupHost joins the pure-Go A/AAAA queries and closes all owned sockets before
// returning. The caller must provide a deadline; there is no cache or worker.
func LookupHost(ctx context.Context, base *net.Resolver, host string) ([]string, error) {
	return LookupHostLimit(ctx, base, host, MaxDNSAddresses)
}

// LookupHostLimit uses the same owned DNS transport with an explicit answer
// bound. Store discovery can accept larger replica sets without changing the
// backend transports' eight-address budget.
func LookupHostLimit(ctx context.Context, base *net.Resolver, host string, limit int) ([]string, error) {
	if limit < 1 || limit > 128 {
		return nil, errors.New("DNS answer bound requires 1-128")
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("DNS requires a deadline")
	}
	ctx, cancel := context.WithCancel(ctx)
	transport := &dnsTransport{ctx: ctx, base: base, conns: make(map[*dnsConn]struct{})}
	stop := context.AfterFunc(ctx, transport.close)
	defer func() { stop(); cancel(); transport.close() }()
	native := &net.Resolver{PreferGo: true, StrictErrors: true, Dial: transport.dial}
	// Match Go's host-file canonicalization: a bare name such as localhost
	// is not stored with a trailing dot. Qualified names stay absolute.
	if strings.Contains(host, ".") && !strings.HasSuffix(host, ".") {
		host += "."
	}
	ips, err := native.LookupHost(ctx, host)
	if err != nil || ctx.Err() != nil || transport.oversized.Load() || len(ips) == 0 || len(ips) > limit {
		return nil, errors.New("DNS failed or answer count outside configured bound")
	}
	return ips, nil
}

// A lookup can issue A and AAAA concurrently. Cancellation closes their I/O,
// and the resolution credit is held until LookupHost and all owned I/O end.
type dnsTransport struct {
	ctx       context.Context
	base      *net.Resolver
	mu        sync.Mutex
	conns     map[*dnsConn]struct{}
	active    int
	io        sync.WaitGroup
	oversized atomic.Bool
}

func (d *dnsTransport) dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.ctx.Err() != nil || d.active == 2 {
		d.mu.Unlock()
		return nil, errors.New("DNS I/O closed or bounded")
	}
	d.active++
	d.io.Add(1)
	d.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.ctx, cancel)
	defer func() { stop(); cancel() }()
	var conn net.Conn
	var err error
	if d.base != nil && d.base.Dial != nil {
		conn, err = d.base.Dial(ctx, network, address)
	} else {
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err = dialer.DialContext(ctx, network, address)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil && d.ctx.Err() != nil {
		_ = conn.Close()
		err = d.ctx.Err()
	}
	if err != nil {
		d.active--
		d.io.Done()
		return nil, err
	}
	wrapped := &dnsConn{Conn: conn, owner: d}
	d.conns[wrapped] = struct{}{}
	// net.Resolver detects PacketConn to choose DNS's UDP framing.
	if packet, ok := conn.(net.PacketConn); ok {
		udp := &dnsPacketConn{dnsConn: wrapped, packet: packet}
		return udp, nil
	}
	return wrapped, nil
}

func (d *dnsTransport) close() {
	d.mu.Lock()
	conns := make([]*dnsConn, 0, len(d.conns))
	for conn := range d.conns {
		conns = append(conns, conn)
	}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	d.io.Wait()
}

type dnsConn struct {
	net.Conn
	owner     *dnsTransport
	once      sync.Once
	err       error
	readBytes int // net.Resolver has one reader per DNS connection.
}

func (c *dnsConn) Read(p []byte) (int, error) {
	remaining := maxDNSReadBytes - c.readBytes
	if remaining < 0 {
		return 0, errors.New("DNS response byte bound")
	}
	n, err := c.Conn.Read(p[:min(len(p), remaining+1)])
	c.readBytes += n
	if c.readBytes > maxDNSReadBytes {
		c.owner.oversized.Store(true)
		return 0, errors.New("DNS response byte bound")
	}
	return n, err
}

func (c *dnsConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.active--
		c.owner.mu.Unlock()
		c.owner.io.Done()
	})
	return c.err
}

type dnsPacketConn struct {
	*dnsConn
	packet net.PacketConn
}

func (c *dnsPacketConn) ReadFrom(p []byte) (int, net.Addr, error) { return c.packet.ReadFrom(p) }
func (c *dnsPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return c.packet.WriteTo(p, addr)
}
