package server

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testdns"
	"github.com/batchstream/weir/internal/testmetrics"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

// Only the gRPC publication sink is recorded here. Resolution uses real UDP/TCP
// DNS packets through net.Resolver, not manual resolver address injection.
type dnsPublication struct {
	resolver.ClientConn
	mu        sync.Mutex
	updates   int
	addresses []string
	invalid   bool
}

func (p *dnsPublication) UpdateState(state resolver.State) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates++
	p.addresses = nil
	p.invalid = state.ServiceConfig != nil || len(state.Endpoints) != 0 || len(state.Addresses) > maxDNSAddresses
	for _, a := range state.Addresses {
		p.addresses = append(p.addresses, a.Addr)
	}
	return nil
}

func (p *dnsPublication) snapshot() (int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.updates, slices.Clone(p.addresses)
}

func resolveFixture(t *testing.T, fixture *testdns.Server) (*peerDNS, *dnsPublication) {
	t.Helper()
	r := &RemoteWeir{resolutions: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	e := &remoteEndpoint{owner: r, ctx: ctx}
	b := &peerDNSBuilder{endpoint: e, host: "peer.weir.test", port: "7448", resolver: fixture.Resolver()}
	p := &dnsPublication{}
	target := resolver.Target{}
	opts := resolver.BuildOptions{}
	resolved, err := b.Build(target, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	d := resolved.(*peerDNS)
	t.Cleanup(func() {
		cancel()
		d.Close()
		if len(r.resolutions) != 0 {
			t.Error("DNS resolution credit leak")
		}
	})
	return d, p
}

func TestDNSRealAnswersOrderAndTCP(t *testing.T) {
	s := testdns.Start(t)
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.2"), netip.MustParseAddr("::1"), netip.MustParseAddr("127.0.0.1")}, TCP: true}
	s.Set("peer.weir.test", answer)
	d, p := resolveFixture(t, s)
	awaitEndpoint(t, func() bool { n, _ := p.snapshot(); return n == 1 })
	_, got := p.snapshot()
	want := []string{"127.0.0.1:7448", "127.0.0.2:7448", "[::1]:7448"}
	if !slices.Equal(got, want) || s.TCP.Load() != 2 || s.A.Load() == 0 || s.AAAA.Load() == 0 || s.Other.Load() != 0 {
		t.Fatal(got, s.TCP.Load(), s.Other.Load())
	}
	slices.Reverse(answer.Addresses)
	s.Set("peer.weir.test", answer)
	opts := resolver.ResolveNowOptions{}
	for range 1000 {
		d.ResolveNow(opts)
	}
	awaitEndpoint(t, func() bool { n, _ := p.snapshot(); return n >= 2 })
	n, got := p.snapshot()
	if n != 2 || !slices.Equal(got, want) {
		t.Fatal("order or refresh flood", n, got)
	}
	p.mu.Lock()
	invalid := p.invalid
	p.mu.Unlock()
	if invalid {
		t.Fatal("unbounded state/service config")
	}
	if s.Queries.Load() > 8 {
		t.Fatal("ResolveNow query flood", s.Queries.Load())
	}
	t.Log("actual A/AAAA UDP and TCP fallback; sorted identities unchanged; 1000 refresh hints coalesced")
}

func TestDNSRealFailureClearsAndRecovers(t *testing.T) {
	for _, mode := range []string{"nxdomain", "empty", "excess", "oversized-wire", "delay", "drop"} {
		t.Run(mode, func(t *testing.T) {
			s := testdns.Start(t)
			good := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
			s.Set("peer.weir.test", good)
			d, p := resolveFixture(t, s)
			awaitEndpoint(t, func() bool { n, _ := p.snapshot(); return n == 1 })
			bad := testdns.Answer{}
			switch mode {
			case "nxdomain":
				bad.Code = dnsmessage.RCodeNameError
			case "excess":
				for i := 1; i <= 9; i++ {
					bad.Addresses = append(bad.Addresses, netip.MustParseAddr(fmt.Sprintf("127.0.0.%d", i)))
				}
			case "oversized-wire":
				bad.TCP = true
				for range 600 {
					bad.Addresses = append(bad.Addresses, netip.MustParseAddr("127.0.0.1"))
				}
				bad.Addresses = append(bad.Addresses, netip.MustParseAddr("::1"))
			case "delay":
				bad = good
				bad.Delay = 3 * time.Second
			case "drop":
				bad.Drop = true
			}
			s.Set("peer.weir.test", bad)
			opts := resolver.ResolveNowOptions{}
			d.ResolveNow(opts)
			started := time.Now()
			awaitEndpoint(t, func() bool { n, _ := p.snapshot(); return n >= 2 })
			_, addresses := p.snapshot()
			if len(addresses) != 0 || time.Since(started) > 3500*time.Millisecond {
				t.Fatal("stale/excess answer or lookup lifetime", addresses, time.Since(started))
			}
			s.Set("peer.weir.test", good)
			d.ResolveNow(opts)
			awaitEndpoint(t, func() bool { _, addresses := p.snapshot(); return len(addresses) == 1 })
			if s.Other.Load() != 0 {
				t.Fatal("TXT/SRV requested")
			}
		})
	}
}

func TestDNSRealAddressReplacementNewCallsAndClose(t *testing.T) {
	s := testdns.Start(t)
	var addresses []string
	var adapters []*peerAdapter
	var peers []*Server
	for _, ip := range []string{"127.0.0.1", "::1"} {
		port := "0"
		if len(addresses) > 0 {
			_, port, _ = net.SplitHostPort(addresses[0])
		}
		listener, err := net.Listen("tcp", net.JoinHostPort(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		a, rt := peerLocal(t, "records")
		adapters = append(adapters, a)
		local := Service{LocalStore: rt}
		opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true, listener: listener}
		peer, address := startPeerServer(t, opts)
		peers = append(peers, peer)
		addresses = append(addresses, address)
	}
	_, port, _ := net.SplitHostPort(addresses[0])
	good := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	s.Set("peer.weir.test", good)
	cfg := RemoteConfig{Endpoints: []string{"peer.weir.test:" + port}, Relays: 2, Resolver: s.Resolver()}
	r, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	readyRemote(t, r)
	service := Service{RemoteWeir: r}
	opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	entry, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Mutate(ctx, testMutation("first"))
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	good.Addresses = []netip.Addr{netip.MustParseAddr("::1")}
	s.Set("peer.weir.test", good)
	// A failed reconnect triggers gRPC ResolveNow. A formerly READY channel
	// goes IDLE on loss; Connect is the standard hint for a future call.
	if err := peers[0].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	e := r.endpoints[0]
	e.mu.Lock()
	var conns []*peerConn
	for conn := range e.physical {
		conns = append(conns, conn)
	}
	e.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	awaitEndpoint(t, func() bool { return e.conn.GetState() != connectivity.Ready })
	e.conn.Connect()
	awaitEndpoint(t, func() bool { e.mu.Lock(); defer e.mu.Unlock(); return e.addresses[addresses[1]] })
	readyRemote(t, r)
	result, err = client.Mutate(ctx, testMutation("second"))
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	if adapters[0].commands.Load() != 1 || adapters[1].commands.Load() != 1 {
		t.Fatal("old mutation replayed", adapters[0].commands.Load(), adapters[1].commands.Load())
	}
	waitPeerIdle(t, entry)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { testmetrics.Gather(t, r); _ = r.Close() })
	}
	wg.Wait()
	if len(r.sockets) != 0 || len(r.resolutions) != 0 {
		t.Fatal("DNS endpoint cleanup")
	}
	if s.Other.Load() != 0 {
		t.Fatal("external service config lookup")
	}
}

func TestDNSMaximumGraphResolutionStormAndCancellation(t *testing.T) {
	s := testdns.Start(t)
	baseline := runtime.NumGoroutine()
	var remotes []*RemoteWeir
	for service := 0; service < 16; service++ {
		var addresses []string
		for endpoint := 0; endpoint < 8; endpoint++ {
			name := fmt.Sprintf("s%d-e%d.weir.test", service, endpoint)
			answer := testdns.Answer{Drop: true}
			s.Set(name, answer)
			addresses = append(addresses, name+":7448")
		}
		cfg := RemoteConfig{Endpoints: addresses, Relays: 16, Resolver: s.Resolver()}
		r, err := NewRemote(cfg)
		if err != nil {
			t.Fatal(err)
		}
		remotes = append(remotes, r)
		t.Cleanup(func() { _ = r.Close() })
	}
	awaitEndpoint(t, func() bool { return s.Queries.Load() >= 64 })
	var active, channels int
	for _, r := range remotes {
		active += len(r.resolutions)
		channels += len(r.endpoints)
		if cap(r.slots) != 16 || cap(r.sockets) != 16 {
			t.Fatal("multiplied relay credit")
		}
	}
	if active > 32 || channels != 128 || s.Queries.Load() > 64 {
		t.Fatal("DNS storm bound", active, channels, s.Queries.Load())
	}
	peak := runtime.NumGoroutine()
	started := time.Now()
	var wg sync.WaitGroup
	for _, r := range remotes {
		wg.Go(func() { _ = r.Close() })
	}
	wg.Wait()
	if time.Since(started) > 3*time.Second {
		t.Fatal("DNS shutdown unbounded")
	}
	for _, r := range remotes {
		if len(r.resolutions) != 0 || len(r.sockets) != 0 {
			t.Fatal("leaked DNS credits")
		}
		for _, e := range r.endpoints {
			if e.conn.GetState() != connectivity.Shutdown {
				t.Fatal("channel live")
			}
		}
	}
	awaitEndpoint(t, func() bool { return runtime.NumGoroutine() <= baseline+8 })
	t.Logf("graph: services=16 endpoints=128 DNS resolutions=%d (max32) A/AAAA queries=%d (max64), goroutines baseline=%d peak=%d after=%d, close=%v", active, s.Queries.Load(), baseline, peak, runtime.NumGoroutine(), time.Since(started))
}

func TestDNSPickFirstMultipleAddressesAndOriginalDeadline(t *testing.T) {
	dns := testdns.Start(t)
	// IPv4 accepts TCP but never completes HTTP/2; IPv6 is a real healthy peer.
	stalled, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	accepted := make(chan net.Conn, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := stalled.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	t.Cleanup(func() {
		_ = stalled.Close()
		<-done
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
	})
	_, port, _ := net.SplitHostPort(stalled.Addr().String())
	listener, err := net.Listen("tcp", net.JoinHostPort("::1", port))
	if err != nil {
		t.Fatal(err)
	}
	_, rt := peerLocal(t, "records")
	local := Service{LocalStore: rt}
	opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true, listener: listener}
	_, _ = startPeerServer(t, opts)
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1")}, Delay: 100 * time.Millisecond}
	dns.Set("peer.weir.test", answer)
	cfg := RemoteConfig{Endpoints: []string{"peer.weir.test:" + port}, Relays: 1, Resolver: dns.Resolver()}
	r, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	started := time.Now()
	_, err = r.selectClient(ctx, "key")
	cancel()
	if err == nil || time.Since(started) > 100*time.Millisecond {
		t.Fatal("DNS/connection wait reset original deadline", err, time.Since(started))
	}
	readyRemote(t, r)
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		t.Fatal("IPv4 connection attempt missing")
	}
	awaitEndpoint(t, func() bool { return len(r.sockets) == 1 })
	if cap(r.endpoints[0].sockets) != 2 || len(r.endpoints) != 1 {
		t.Fatal("DNS answers became endpoint identities")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(r.sockets) != 0 {
		t.Fatal("pending/connected sockets leaked")
	}
}

func TestDNSPeriodicRefreshPinsBulkAndClosesDrainingSocket(t *testing.T) {
	dns := testdns.Start(t)
	var addresses []string
	var adapters []*peerAdapter
	for _, ip := range []string{"127.0.0.1", "::1"} {
		port := "0"
		if len(addresses) > 0 {
			_, port, _ = net.SplitHostPort(addresses[0])
		}
		listener, err := net.Listen("tcp", net.JoinHostPort(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		a, rt := peerLocal(t, "records")
		adapters = append(adapters, a)
		local := Service{LocalStore: rt}
		opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true, listener: listener, limits: DefaultLimits()}
		_, address := startPeerServer(t, opts)
		addresses = append(addresses, address)
	}
	_, port, _ := net.SplitHostPort(addresses[0])
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("periodic.weir.test", answer)
	cfg := RemoteConfig{Endpoints: []string{"periodic.weir.test:" + port}, Relays: 2, Resolver: dns.Resolver()}
	r, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	readyRemote(t, r)
	service := Service{RemoteWeir: r}
	opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4, limits: DefaultLimits()}
	entry, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	bulk, err := client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	first := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: first}
	if err := bulk.Send(frame); err != nil {
		t.Fatal(err)
	}
	answer.Addresses = []netip.Addr{netip.MustParseAddr("::1")}
	dns.Set("periodic.weir.test", answer)
	var count uint64
	started := time.Now()
	for {
		mv := &pb.BulkOperation_Mutate{Mutate: testMutation("old-stream")}
		op := &pb.BulkOperation{Index: count, Operation: mv}
		ov := &pb.BulkRequestFrame_Operation{Operation: op}
		frame := &pb.BulkRequestFrame{Frame: ov}
		if err := bulk.Send(frame); err != nil {
			t.Fatal(err)
		}
		result, err := bulk.Recv()
		if err != nil || result.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal(result, err)
		}
		count++
		e := r.endpoints[0]
		e.mu.Lock()
		replaced := e.addresses[addresses[1]]
		e.mu.Unlock()
		if replaced {
			break
		}
		if time.Since(started) > 35*time.Second {
			t.Fatal("periodic DNS refresh missing")
		}
		time.Sleep(250 * time.Millisecond)
	}
	readyRemote(t, r)
	awaitEndpoint(t, func() bool { return len(r.sockets) == 2 })
	result, err := client.Mutate(ctx, testMutation("new-call"))
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	if adapters[0].commands.Load() != int32(count) || adapters[1].commands.Load() != 1 {
		t.Fatal("DNS refresh moved/replayed old Bulk or failed to move new call", count, adapters[0].commands.Load(), adapters[1].commands.Load())
	}
	closeStarted := time.Now()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(closeStarted) > time.Second || len(r.sockets) != 0 {
		t.Fatal("draining socket escaped ClientConn close")
	}
	_ = bulk.CloseSend()
	if frame, err := bulk.Recv(); err == nil {
		t.Fatal("closed active Bulk reported completion", frame)
	}
	waitPeerIdle(t, entry)
	t.Logf("real periodic DNS refresh=%v; old Bulk acknowledgements=%d stayed on IPv4, new call=1 on IPv6, old+new sockets=2, Close reclaimed both", time.Since(started), count)
}
