package mongodb

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/netlimit"
)

var errConnections = errors.New("MongoDB connection owner closed")

// connectionOwner tracks raw sockets through shutdown without a capacity gate.
type connectionOwner struct {
	resolver  *net.Resolver
	tlsConfig *tls.Config
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	owned     int
	peak      int
	dialing   int
	closing   int
	acquired  uint64
	released  uint64
	conns     map[*mongoConn]struct{}
	workers   sync.WaitGroup
}

func newConnectionOwner() *connectionOwner {
	ctx, cancel := context.WithCancel(context.Background())
	d := &connectionOwner{ctx: ctx, cancel: cancel, conns: make(map[*mongoConn]struct{})}
	return d
}

func (d *connectionOwner) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.dialConnection(ctx, network, address)
	if err != nil {
		return nil, err
	}
	bounded := &boundedConn{Conn: conn}
	return bounded, nil
}

// OCSP uses the same raw socket ownership without the Mongo wire guard.
func (d *connectionOwner) dialConnection(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errConnections
	}
	d.dialing++
	d.workers.Add(1)
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.dialing--
		d.mu.Unlock()
		d.workers.Done()
	}()
	ctx, cancel := context.WithTimeout(ctx, mongoConnectTimeout)
	stop := context.AfterFunc(d.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := d.acquire(ctx); err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			d.mu.Lock()
			d.releaseLocked()
			d.mu.Unlock()
		}
	}()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errConnections
	}
	ips := []string{host}
	if _, err := netip.ParseAddr(host); err != nil {
		ips, err = netlimit.LookupHost(ctx, d.resolver, strings.TrimSuffix(host, "."))
		if err != nil {
			return nil, err
		}
	}
	// DNS is joined first. Try its finite address list serially under the same
	// deadline: no Happy Eyeballs overlap, discovery, or business replay.
	dialer := net.Dialer{Timeout: mongoConnectTimeout, KeepAlive: 30 * time.Second}
	var raw net.Conn
	for _, address := range ips {
		ip, parseErr := netip.ParseAddr(address)
		if parseErr != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
			return nil, errConnections
		}
		raw, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	conn := &mongoConn{Conn: raw, raw: raw, owner: d}
	d.mu.Lock()
	if d.closed || ctx.Err() != nil {
		d.mu.Unlock()
		_ = raw.Close()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errConnections
	}
	d.conns[conn] = struct{}{}
	transferred = true
	d.mu.Unlock()
	if d.tlsConfig != nil {
		secured, err := mongoTLS(ctx, raw, d.tlsConfig, address)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn.Conn = secured
	}
	if ctx.Err() != nil {
		_ = conn.Close()
		return nil, ctx.Err()
	}
	return conn, nil
}

func (d *connectionOwner) acquire(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return errConnections
	}
	d.owned++
	d.acquired++
	d.peak = max(d.peak, d.owned)
	return nil
}

func (d *connectionOwner) releaseLocked() {
	d.owned--
	d.released++
}

func (d *connectionOwner) stop() {
	d.mu.Lock()
	d.closed = true
	d.cancel()
	d.mu.Unlock()
}

func (d *connectionOwner) close() {
	d.stop()
	d.mu.Lock()
	conns := make([]*mongoConn, 0, len(d.conns))
	for conn := range d.conns {
		conns = append(conns, conn)
	}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	d.workers.Wait()
}

type mongoConn struct {
	net.Conn
	raw   net.Conn
	owner *connectionOwner
	once  sync.Once
	err   error
}

func (c *mongoConn) Close() error {
	c.once.Do(func() {
		c.owner.mu.Lock()
		c.owner.closing++
		c.owner.mu.Unlock()
		// Do not wait for TLS close_notify, or release at a pool removal event.
		c.err = c.raw.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.closing--
		c.owner.releaseLocked()
		c.owner.mu.Unlock()
	})
	return c.err
}
