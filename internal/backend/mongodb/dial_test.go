package mongodb

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type slowCloseConn struct {
	net.Conn
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (c *slowCloseConn) Close() error {
	c.calls.Add(1)
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

func ownerListener(t *testing.T) (net.Listener, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	active := new(atomic.Int32)
	var mu sync.Mutex
	conns := make(map[net.Conn]bool)
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			active.Add(1)
			mu.Lock()
			conns[conn] = true
			mu.Unlock()
			workers.Go(func() {
				defer active.Add(-1)
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
				mu.Lock()
				delete(conns, conn)
				mu.Unlock()
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return listener, active
}
func ownerWait(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("owner barrier timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
func ownerZero(t *testing.T, d *connectionOwner) {
	t.Helper()
	s := d.snapshot()
	if s.Owned != 0 || s.Closing != 0 || s.Dialing != 0 || s.Acquired != s.Released {
		t.Fatal("ownership not closed", s)
	}
}

// External net.Conn seam forces a driver-retired raw Close to remain in progress.

// Cancellation while full must neither release somebody else's credit nor spin/replay.

func TestMongoOwnerCloseDuringTLSAndDial(t *testing.T) {
	for _, pool := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(pool), func(t *testing.T) {
			listener, _ := ownerListener(t)
			d := newConnectionOwner()
			d.tlsConfig = &tls.Config{}
			var workers sync.WaitGroup
			for range 8 {
				workers.Go(func() {
					conn, err := d.DialContext(context.Background(), "tcp", listener.Addr().String())
					if conn != nil {
						conn.Close()
					}
					if err == nil {
						t.Error("stalled TLS accepted")
					}
				})
			}
			ownerWait(t, func() bool {
				return d.snapshot().Owned == 8 && d.snapshot().Dialing == 8
			})
			workers.Go(d.close)
			workers.Go(d.close)
			workers.Wait()
			ownerZero(t, d)
			if conn, err := d.DialContext(context.Background(), "tcp", listener.Addr().String()); conn != nil || err == nil {
				t.Fatal("closed owner admitted dial")
			}
		})
	}
}

func TestMongoOwnerJoinsSlowResolverClose(t *testing.T) {
	d := newConnectionOwner()
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); d.close() })
	var mu sync.Mutex
	var peers []net.Conn
	var dns []*slowCloseConn
	d.resolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		slow := &slowCloseConn{Conn: client, entered: make(chan struct{}), release: release}
		mu.Lock()
		peers = append(peers, server)
		dns = append(dns, slow)
		mu.Unlock()
		return slow, nil
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := d.DialContext(context.Background(), "tcp", "owned.weir.test:27017")
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			t.Error("stalled DNS accepted")
		}
	}()
	ownerWait(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(dns) == 2 })
	closed := make(chan struct{})
	go func() { d.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close did not join DNS")
	case <-time.After(10 * time.Millisecond):
	}
	if d.snapshot().Owned != 1 {
		t.Fatal("DNS credit released early")
	}
	once.Do(func() { close(release) })
	<-done
	<-closed
	ownerZero(t, d)
	mu.Lock()
	defer mu.Unlock()
	for _, peer := range peers {
		peer.Close()
	}
	if len(dns) != 2 {
		t.Fatal("DNS exceeds A/AAAA", len(dns))
	}
}

func TestMongoOwnerOpenFailure(t *testing.T) {
	listener, active := ownerListener(t)
	for range 4 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		cfg := Config{URI: "mongodb://" + listener.Addr().String() + "/?directConnection=true", Store: "records"}
		a, err := Open(ctx, cfg)
		cancel()
		if a != nil || err == nil {
			t.Fatal("silent backend qualified")
		}
		ownerWait(t, func() bool { return active.Load() == 0 })
	}
}
