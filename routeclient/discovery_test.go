package routeclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testdns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type discoveryPeer struct {
	pb.UnimplementedWeirServer
	business *clientTestPeer
	routes   atomic.Int64
	resolves atomic.Int64
	mu       sync.Mutex
	records  map[string]*pb.ResolveResponse
	err      error
	delay    time.Duration
	delays   map[string]time.Duration
}

func (p *discoveryPeer) Resolve(ctx context.Context, request *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	p.resolves.Add(1)
	p.mu.Lock()
	err, delay := p.err, p.delay
	if override, exists := p.delays[request.Store]; exists {
		delay = override
	}
	var response *pb.ResolveResponse
	if record := p.records[request.Store]; record != nil {
		response = proto.Clone(record).(*pb.ResolveResponse)
	}
	p.mu.Unlock()
	if delay != 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, status.Error(codes.Unavailable, "directory not learned")
	}
	return response, nil
}

func (p *discoveryPeer) Route(stream pb.Weir_RouteServer) error {
	p.routes.Add(1)
	if p.business == nil {
		return status.Error(codes.FailedPrecondition, "initialization node received business traffic")
	}
	return p.business.Route(stream)
}

func (p *discoveryPeer) set(store string, response *pb.ResolveResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.records == nil {
		p.records = make(map[string]*pb.ResolveResponse)
	}
	p.records[store] = response
}

type discoveryListener struct {
	address string
	stop    func()
}

func listenDiscovery(t *testing.T, peer *discoveryPeer, address string) *discoveryListener {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(10 << 20))
	pb.RegisterWeirServer(server, peer)
	done := make(chan struct{})
	go func() { _ = server.Serve(listener); close(done) }()
	var once sync.Once
	fixture := &discoveryListener{address: listener.Addr().String()}
	fixture.stop = func() {
		once.Do(func() { server.Stop(); _ = listener.Close(); <-done })
	}
	t.Cleanup(fixture.stop)
	return fixture
}

func discoveryRecord(store string, targets ...string) *pb.ResolveResponse {
	response := &pb.ResolveResponse{Store: store, Group: "group-" + store, Targets: targets, CacheTtlMs: 30000}
	return response
}

func openDiscovery(t *testing.T, options OpenOptions) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	client, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func discoveryRead(t *testing.T, client *Client, store string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	options := RecordOptions{Destination: store, Call: clientTestRead()}
	result, err := client.Record(ctx, options)
	if err != nil || result == nil || result.GetRead() == nil {
		t.Fatalf("direct Store read: result=%v error=%v", result, err)
	}
}

func TestOpenResolvesMultipleStoresAndBalancesDirectStreams(t *testing.T) {
	firstBusiness := &clientTestPeer{mode: "normal"}
	first := &discoveryPeer{business: firstBusiness}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", firstListener.address, secondListener.address))
	seed.set("other", discoveryRecord("other", secondListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records", "other"}}
	client := openDiscovery(t, options)
	for range 20 {
		discoveryRead(t, client, "records")
	}
	discoveryRead(t, client, "other")
	if first.routes.Load() == 0 || second.routes.Load() == 0 || seed.routes.Load() != 0 {
		t.Fatalf("business must balance direct replicas: first=%d second=%d seed=%d", first.routes.Load(), second.routes.Load(), seed.routes.Load())
	}
	if first.resolves.Load() != 0 || second.resolves.Load() != 0 {
		t.Fatal("business replicas received initialization traffic")
	}
	unknown := RecordOptions{Destination: "uninitialized", Call: clientTestRead()}
	if _, err := client.Record(t.Context(), unknown); err == nil {
		t.Fatal("uninitialized Store accepted")
	}
}

func TestClientDNSDiscoversScaleAndDrainsRetiredReplica(t *testing.T) {
	dns := testdns.Start(t)
	firstBusiness := &clientTestPeer{mode: "normal"}
	first := &discoveryPeer{business: firstBusiness}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(firstListener.address)
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, net.JoinHostPort("::1", port))
	_ = secondListener
	name := "store.replicas.test"
	ipv4 := netip.MustParseAddr("127.0.0.1")
	ipv6 := netip.MustParseAddr("::1")
	answer := testdns.Answer{Addresses: []netip.Addr{ipv4}}
	dns.Set(name, answer)
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", net.JoinHostPort(name, port)))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, Resolver: dns.Resolver(), RefreshInterval: 50 * time.Millisecond, ResolveTimeout: time.Second}
	client := openDiscovery(t, options)
	discoveryRead(t, client, "records")
	answer.Addresses = []netip.Addr{ipv4, ipv6}
	dns.Set(name, answer)
	deadline := time.Now().Add(3 * time.Second)
	for second.routes.Load() == 0 && time.Now().Before(deadline) {
		discoveryRead(t, client, "records")
		time.Sleep(10 * time.Millisecond)
	}
	if second.routes.Load() == 0 {
		t.Fatal("healthy connection prevented discovery of scaled replica")
	}
	// Existing Store replicas remain discoverable while their directory lease
	// is valid, even when the initialization Service loses its only endpoint.
	seedListener.stop()
	answer.Addresses = []netip.Addr{ipv6}
	dns.Set(name, answer)
	time.Sleep(200 * time.Millisecond)
	before := first.routes.Load()
	for range 12 {
		discoveryRead(t, client, "records")
	}
	if first.routes.Load() != before || seed.routes.Load() != 0 {
		t.Fatal("retired DNS replica or seed still receives business RPCs")
	}
	if dns.Queries.Load() < 4 {
		t.Fatal("business DNS was not proactively refreshed")
	}
}

func TestClientRefreshChangesGroupWithoutReplayingActiveStream(t *testing.T) {
	blocked := &clientTestPeer{mode: "blocked_receive", canceled: make(chan struct{})}
	first := &discoveryPeer{business: blocked}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", firstListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, RefreshInterval: 50 * time.Millisecond}
	client := openDiscovery(t, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		options := RecordOptions{Destination: "records", Call: clientTestRead()}
		_, err := client.Record(ctx, options)
		finished <- err
	}()
	deadline := time.Now().Add(time.Second)
	for first.routes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if first.routes.Load() != 1 {
		t.Fatal("initial finite stream did not start")
	}
	response := discoveryRecord("records", secondListener.address)
	response.Group = "replacement-group"
	seed.set("records", response)
	time.Sleep(200 * time.Millisecond)
	discoveryRead(t, client, "records")
	select {
	case err := <-finished:
		t.Fatalf("route migration interrupted active stream: %v", err)
	case <-blocked.canceled:
		t.Fatal("removed endpoint canceled an active stream")
	default:
	}
	if first.routes.Load() != 1 || second.routes.Load() != 1 {
		t.Fatal("active stream was replayed after directory migration")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("active stream failed to cancel")
	}
}

func TestClientLeaseExpiresAndConflictInvalidates(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			business := &clientTestPeer{mode: "normal"}
			target := &discoveryPeer{business: business}
			targetListener := listenDiscovery(t, target, "127.0.0.1:0")
			seed := &discoveryPeer{}
			response := discoveryRecord("records", targetListener.address)
			response.CacheTtlMs = 300
			seed.set("records", response)
			seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
			options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 100 * time.Millisecond}
			client := openDiscovery(t, options)
			seed.mu.Lock()
			if conflict {
				seed.err = status.Error(codes.FailedPrecondition, "two owning groups")
			} else {
				seed.err = status.Error(codes.Unavailable, "temporarily unavailable")
			}
			seed.mu.Unlock()
			if !conflict {
				discoveryRead(t, client, "records")
			}
			wait := 150 * time.Millisecond
			if !conflict {
				wait = 400 * time.Millisecond
			}
			time.Sleep(wait)
			before := target.routes.Load()
			optionsRecord := RecordOptions{Destination: "records", Call: clientTestRead()}
			if _, err := client.Record(t.Context(), optionsRecord); err == nil {
				t.Fatal("expired or conflicting mapping accepted business request")
			}
			if target.routes.Load() != before {
				t.Fatal("rejected request reached previous Store owner")
			}
		})
	}
}

func TestOpenValidatesWholeMappingAndBoundsDeadTargets(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddress := dead.Addr().String()
	_ = dead.Close()
	seed := &discoveryPeer{}
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	for _, invalid := range []string{"wrong-store", "bad-target", "duplicate-target", "empty-ttl", "overflow-ttl", "invalid-group", "dead-only"} {
		t.Run(invalid, func(t *testing.T) {
			response := discoveryRecord("records", targetListener.address)
			switch invalid {
			case "wrong-store":
				response.Store = "other"
			case "bad-target":
				response.Targets = []string{"http://arbitrary.example:7447"}
			case "duplicate-target":
				response.Targets = []string{targetListener.address, targetListener.address}
			case "empty-ttl":
				response.CacheTtlMs = 0
			case "overflow-ttl":
				response.CacheTtlMs = ^uint64(0)
			case "invalid-group":
				response.Group = "invalid group"
			case "dead-only":
				response.Targets = []string{deadAddress}
			}
			seed.set("records", response)
			options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, ResolveTimeout: 100 * time.Millisecond}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			started := time.Now()
			client, err := Open(ctx, options)
			if err == nil {
				_ = client.Close()
				t.Fatal("invalid or dead-only directory accepted")
			}
			if time.Since(started) > time.Second {
				t.Fatal("initialization exceeded caller deadline")
			}
		})
	}
	seed.set("records", discoveryRecord("records", deadAddress, targetListener.address))
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, ResolveTimeout: 100 * time.Millisecond}
	client := openDiscovery(t, options)
	discoveryRead(t, client, "records")
	if seed.routes.Load() != 0 {
		t.Fatal("initialization sent a business request")
	}
}

func TestOpenCancellationAndCloseJoinDiscovery(t *testing.T) {
	dns := testdns.Start(t)
	answer := testdns.Answer{Drop: true}
	dns.Set("unreachable.seed.test", answer)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	options := OpenOptions{Seed: "unreachable.seed.test:7447", Stores: []string{"records"}, Resolver: dns.Resolver()}
	started := time.Now()
	if client, err := Open(ctx, options); err == nil {
		_ = client.Close()
		t.Fatal("dropped DNS initialized")
	}
	if time.Since(started) > time.Second {
		t.Fatal("initial DNS ignored caller cancellation")
	}
	business := &clientTestPeer{mode: "normal"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(targetListener.address)
	answer = testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("live.store.test", answer)
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", "live.store.test:"+port))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options = OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, Resolver: dns.Resolver(), RefreshInterval: 50 * time.Millisecond, ResolveTimeout: time.Second}
	client := openDiscovery(t, options)
	answer.Drop = true
	dns.Set("live.store.test", answer)
	queries := dns.Queries.Load()
	deadline := time.Now().Add(time.Second)
	for dns.Queries.Load() == queries && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	started = time.Now()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("Close did not cancel and join in-flight DNS")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	record := RecordOptions{Destination: "records", Call: clientTestRead()}
	if _, err := client.Record(t.Context(), record); !errors.Is(err, ErrClosed) {
		t.Fatal("closed client admitted request", err)
	}
	queries = dns.Queries.Load()
	time.Sleep(100 * time.Millisecond)
	if dns.Queries.Load() != queries {
		t.Fatal("discovery queried DNS after Close returned")
	}
}

func TestDiscoveredWriteLossIsNeverReplayed(t *testing.T) {
	business := &clientTestPeer{mode: "write_reply_loss"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", targetListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}}
	client := openDiscovery(t, options)
	document := &pb.Document{MediaType: "application/octet-stream", Data: []byte("one write")}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "records/s:key", Action: action}
	variant := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: variant}
	record := RecordOptions{Destination: "records", Call: call}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := client.Record(ctx, record)
	if err == nil || result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || target.routes.Load() != 1 || seed.routes.Load() != 0 {
		t.Fatalf("write evidence/replay: result=%v error=%v direct=%d seed=%d", result, err, target.routes.Load(), seed.routes.Load())
	}
}
