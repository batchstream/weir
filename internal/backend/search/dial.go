package search

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/netlimit"
)

// One owner joins raw sockets and HTTP dial attempts during adapter shutdown.
type connectionDialer struct {
	ctx                context.Context
	resolver           *net.Resolver
	tlsConfig          *tls.Config
	owned              int
	mu                 sync.Mutex
	closed             bool
	peak               int
	acquired, released uint64
	conns              map[*searchConn]struct{}
	workers            sync.WaitGroup
}

func (d *connectionDialer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errTransport
	}
	d.workers.Add(1)
	d.mu.Unlock()
	defer d.workers.Done()
	ctx, cancel := context.WithTimeout(ctx, connectionTimeout)
	stop := context.AfterFunc(d.ctx, cancel)
	defer func() { stop(); cancel() }()
	key := requestContextKey{}
	if original, ok := ctx.Value(key).(context.Context); ok {
		if original.Err() != nil {
			cancel()
		}
		stopRequest := context.AfterFunc(original, cancel)
		defer stopRequest()
	}
	d.mu.Lock()
	d.owned++
	d.peak = max(d.peak, d.owned)
	d.acquired++
	d.mu.Unlock()
	owned := false
	defer func() {
		if !owned {
			d.mu.Lock()
			d.owned--
			d.released++
			d.mu.Unlock()
		}
	}()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errTransport
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		ips, err := netlimit.LookupHost(ctx, d.resolver, host)
		if err != nil {
			return nil, err
		}
		// One endpoint, one selected address, one TCP attempt. A fresh connection
		// resolves again; there is no failover loop or stale address cache.
		ip, err = netip.ParseAddr(ips[0])
		if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
			return nil, errTransport
		}
	}
	dialer := net.Dialer{Timeout: connectionTimeout, KeepAlive: 30 * time.Second}
	raw, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
	if err != nil {
		return nil, err
	}
	conn := &searchConn{Conn: raw, raw: raw, owner: d}
	d.mu.Lock()
	if d.closed || ctx.Err() != nil {
		d.mu.Unlock()
		_ = raw.Close()
		return nil, errTransport
	}
	d.conns[conn] = struct{}{}
	owned = true
	d.mu.Unlock()
	if d.tlsConfig != nil {
		config := d.tlsConfig.Clone()
		config.ServerName = host // Preserve original DNS hostname for SAN and SNI.
		secured := tls.Client(raw, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, errors.New("Search TLS verification or handshake failed")
		}
		state := secured.ConnectionState()
		if len(state.PeerCertificates) > 8 {
			_ = conn.Close()
			return nil, errResponse
		}
		for _, cert := range state.PeerCertificates {
			if len(cert.Raw) > 64<<10 {
				_ = conn.Close()
				return nil, errResponse
			}
		}
		conn.Conn = secured
	}
	if ctx.Err() != nil {
		_ = conn.Close()
		return nil, errTransport
	}
	return conn, nil
}

func (d *connectionDialer) close() {
	d.mu.Lock()
	d.closed = true
	for conn := range d.conns {
		_ = conn.raw.Close()
	}
	d.mu.Unlock()
	d.workers.Wait()
	// HTTP read/write loops may still be releasing their wrappers. Closing each
	// wrapper here releases its socket slot once, without waiting for TLS alerts.
	d.mu.Lock()
	conns := make([]*searchConn, 0, len(d.conns))
	for conn := range d.conns {
		conns = append(conns, conn)
	}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

type searchConn struct {
	net.Conn
	raw   net.Conn
	owner *connectionDialer
	once  sync.Once
	err   error
}

func (c *searchConn) Close() error {
	c.once.Do(func() {
		c.err = c.raw.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.owned--
		c.owner.released++
		c.owner.mu.Unlock()
	})
	return c.err
}
