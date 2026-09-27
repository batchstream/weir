package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/resolver"
)

const maxDNSAddresses = 8
const maxDNSReadBytes = 4098 // 4 KiB DNS message plus the TCP length prefix.
const dnsRefresh = 30 * time.Second
const dnsMinInterval = time.Second

// The stock resolver has no answer-count cap and retains its last good result
// indefinitely on errors. This adapter bounds those two policies; Go owns DNS
// encoding, system resolver configuration and A/AAAA, gRPC owns pick-first.
type peerDNSBuilder struct {
	endpoint   *remoteEndpoint
	host, port string
	resolver   *net.Resolver
}

func (*peerDNSBuilder) Scheme() string { return "weir-dns" }
func (b *peerDNSBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(b.endpoint.ctx)
	d := &peerDNS{builder: b, cc: cc, ctx: ctx, cancel: cancel, now: make(chan struct{}, 1), done: make(chan struct{})}
	go d.run()
	return d, nil
}

type peerDNS struct {
	builder *peerDNSBuilder
	cc      resolver.ClientConn
	ctx     context.Context
	cancel  context.CancelFunc
	now     chan struct{}
	done    chan struct{}
}

func (d *peerDNS) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case d.now <- struct{}{}:
	default:
	}
}
func (d *peerDNS) Close() { d.cancel(); <-d.done }

func (d *peerDNS) run() {
	defer close(d.done)
	delay := dnsMinInterval
	for {
		addresses, err := d.lookup()
		if d.ctx.Err() != nil {
			return
		}
		e := d.builder.endpoint
		e.mu.Lock()
		e.addresses = make(map[string]bool, len(addresses))
		for _, address := range addresses {
			e.addresses[address.Addr] = true
		}
		e.mu.Unlock()
		// Empty updates close the old pick-first SubConns gracefully and make
		// future calls fail. ReportError alone would silently keep stale IPs.
		state := resolver.State{Addresses: addresses}
		updateErr := d.cc.UpdateState(state)
		if err == nil {
			err = updateErr
		}
		interval := dnsRefresh
		if err != nil {
			interval = delay
			delay = min(2*delay, dnsRefresh)
		} else {
			delay = dnsMinInterval
		}
		finished := time.Now()
		timer := time.NewTimer(interval)
		select {
		case <-d.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-d.now:
			timer.Stop()
			// ResolveNow coalesces requests; it cannot defeat failure backoff.
			minimum := dnsMinInterval
			if err != nil {
				minimum = interval
			}
			timer.Reset(max(0, time.Until(finished.Add(minimum))))
			select {
			case <-d.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (d *peerDNS) lookup() ([]resolver.Address, error) {
	gate := d.builder.endpoint.owner.resolutions
	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case gate <- struct{}{}:
	}
	defer func() { <-gate }()
	ctx, cancel := context.WithTimeout(d.ctx, peerConnectTimeout)
	transport := &dnsTransport{ctx: ctx, base: d.builder.resolver, conns: make(map[*dnsConn]struct{})}
	stop := context.AfterFunc(ctx, transport.close)
	defer func() { stop(); cancel(); transport.close() }()
	native := &net.Resolver{PreferGo: true, StrictErrors: true, Dial: transport.dial}
	// LookupHost's pure-Go path joins both query goroutines synchronously.
	// LookupNetIP's singleflight can return before its work has stopped.
	ips, err := native.LookupHost(ctx, d.builder.host+".")
	if err != nil || ctx.Err() != nil || transport.oversized.Load() || len(ips) == 0 || len(ips) > maxDNSAddresses {
		return nil, errors.New("peer DNS failed or answer count outside 1-8")
	}
	strings := make([]string, 0, len(ips))
	for _, address := range ips {
		ip, err := netip.ParseAddr(address)
		if err != nil || ip.Zone() != "" {
			return nil, errors.New("invalid peer DNS address")
		}
		strings = append(strings, net.JoinHostPort(ip.Unmap().String(), d.builder.port))
	}
	slices.Sort(strings)
	strings = slices.Compact(strings)
	addresses := make([]resolver.Address, 0, len(strings))
	for _, address := range strings {
		entry := resolver.Address{Addr: address}
		addresses = append(addresses, entry)
	}
	return addresses, nil
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
		dialer := net.Dialer{Timeout: peerConnectTimeout}
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
		return 0, errors.New("peer DNS response byte bound")
	}
	n, err := c.Conn.Read(p[:min(len(p), remaining+1)])
	c.readBytes += n
	if c.readBytes > maxDNSReadBytes {
		c.owner.oversized.Store(true)
		return 0, errors.New("peer DNS response byte bound")
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
