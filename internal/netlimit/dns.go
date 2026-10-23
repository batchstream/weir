// Package netlimit provides owned standard Go DNS I/O for backend dialers.
// Callers retain lifecycle, concurrency and address selection.
package netlimit

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// LookupHost joins the pure-Go A/AAAA queries and closes all owned sockets before
// returning. The caller must provide a deadline; there is no cache or worker.
func LookupHost(ctx context.Context, base *net.Resolver, host string) ([]string, error) {
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
	if err != nil || ctx.Err() != nil || len(ips) == 0 {
		return nil, errors.New("DNS failed or returned no addresses")
	}
	return ips, nil
}

// A lookup can issue A and AAAA concurrently. Cancellation closes their sockets,
// and close waits for all owned I/O, so LookupHost returns only after it ends.
type dnsTransport struct {
	ctx   context.Context
	base  *net.Resolver
	mu    sync.Mutex
	conns map[*dnsConn]struct{}
	io    sync.WaitGroup
}

func (d *dnsTransport) dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.ctx.Err() != nil {
		d.mu.Unlock()
		return nil, errors.New("DNS I/O closed")
	}
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
	owner *dnsTransport
	once  sync.Once
	err   error
}

func (c *dnsConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
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
