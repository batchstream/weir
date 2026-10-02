package directory

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func newTestDirectory(t *testing.T, cfg Config) *Directory {
	t.Helper()
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = d.Close(ctx)
	})
	return d
}

func advertisement(id int, group, target string) *pb.NodeAdvertisement {
	ad := &pb.NodeAdvertisement{NodeId: fmt.Sprintf("%032x", id), Sequence: 1, Group: group, Stores: []string{"data"}, Targets: []string{target}, RemainingLeaseMs: 10000}
	return ad
}

func TestPortableAddressValidation(t *testing.T) {
	input := []string{"EXAMPLE.com.:07447", "[::ffff:127.0.0.1]:7447", "[0:0::1]:7447"}
	targets, err := CanonicalTargets(input)
	want := []string{"127.0.0.1:7447", "[::1]:7447", "example.com:7447"}
	if err != nil || !slices.Equal(targets, want) {
		t.Fatal(targets, err)
	}
	for _, bad := range []string{"0.0.0.0:1", "[::]:1", "[fe80::1%en0]:1", "bad:0", "dns:///bad:1", "bad:65536", "bad:1/path", "bad..name:1", "[bad]:1", "bad:-1", "_name:1", "中文:1", "peer :1"} {
		if _, err := CanonicalAddress(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	duplicate := []string{"example.com:1", "EXAMPLE.COM.:01"}
	if _, err := CanonicalTargets(duplicate); err == nil {
		t.Fatal("canonical duplicate accepted")
	}
}

func TestRelayLeaseCannotRenewOrResurrect(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	start := time.Now()
	ad := advertisement(1, "owner", "127.0.0.1:7447")
	if err := d.merge([]*pb.NodeAdvertisement{ad}, start); err != nil {
		t.Fatal(err)
	}
	original := d.records[ad.NodeId].expires
	if err := d.merge([]*pb.NodeAdvertisement{ad}, start.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.NodeId].expires.Equal(original) {
		t.Fatal("same sequence renewed lease")
	}
	relayed := d.snapshot(start.Add(5 * time.Second))
	var owner *pb.NodeAdvertisement
	for _, node := range relayed {
		if node.NodeId == ad.NodeId {
			owner = node
		}
	}
	if owner == nil || owner.RemainingLeaseMs != 3000 {
		t.Fatal("relay did not debit age and full transport allowance", owner)
	}
	other := newTestDirectory(t, cfg)
	if err := other.merge([]*pb.NodeAdvertisement{owner}, start.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if other.records[ad.NodeId].expires.After(original) {
		t.Fatal("transport delay extended owner lease")
	}
	if err := d.merge([]*pb.NodeAdvertisement{ad}, start.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.NodeId].expires.Equal(original) {
		t.Fatal("expired owner resurrected from same sequence")
	}
	for _, node := range d.snapshot(start.Add(20 * time.Second)) {
		if node.NodeId == ad.NodeId {
			t.Fatal("expired record relayed")
		}
	}
	ad.Sequence = 2
	if err := d.merge([]*pb.NodeAdvertisement{ad}, start.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.NodeId].expires.After(original) {
		t.Fatal("new owner heartbeat did not renew")
	}
}

func TestSameGroupUnionConflictAndWithdrawal(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	first := advertisement(1, "same", "shared.example:7447")
	second := advertisement(2, "same", "127.0.0.1:7447")
	third := advertisement(3, "same", "shared.example:7447")
	if err := d.merge([]*pb.NodeAdvertisement{first, second, third}, time.Now()); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveRequest{Store: "data"}
	response, err := d.Resolve(context.Background(), request)
	if err != nil || len(response.Targets) != 2 || response.Group != "same" || response.CacheTtlMs == 0 || response.CacheTtlMs > uint64(CacheTTL/time.Millisecond) {
		t.Fatal(response, err)
	}
	conflict := advertisement(4, "other", "127.0.0.1:7449")
	if err := d.merge([]*pb.NodeAdvertisement{conflict}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resolve(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("group conflict not rejected", err)
	}
	conflict.Sequence++
	conflict.Withdrawn = true
	conflict.Stores = nil
	if err := d.merge([]*pb.NodeAdvertisement{conflict}, time.Now()); err != nil {
		t.Fatal(err)
	}
	conflict.Sequence--
	conflict.Withdrawn = false
	conflict.Stores = []string{"data"}
	if err := d.merge([]*pb.NodeAdvertisement{conflict}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resolve(context.Background(), request); err != nil {
		t.Fatal("old advertisement resurrected withdrawn owner", err)
	}
	if len(first.Stores) != 1 {
		t.Fatal("merge modified caller advertisement")
	}
}

func TestRestartAndExpiryRemoveOldEndpoint(t *testing.T) {
	cfg := Config{Group: "same", Targets: []string{"127.0.0.1:7447"}, Stores: []string{"data"}}
	first := newTestDirectory(t, cfg)
	cfg.Targets = []string{"127.0.0.1:7448"}
	restarted := newTestDirectory(t, cfg)
	if first.self == restarted.self {
		t.Fatal("restart reused process incarnation")
	}
	observerCfg := Config{Group: "observer"}
	observer := newTestDirectory(t, observerCfg)
	now := time.Now()
	if err := observer.merge(first.snapshot(now), now); err != nil {
		t.Fatal(err)
	}
	if err := observer.merge(restarted.snapshot(now), now); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	old := observer.records[first.self]
	old.expires = now.Add(-time.Second)
	observer.records[first.self] = old
	observer.mu.Unlock()
	request := &pb.ResolveRequest{Store: "data"}
	response, err := observer.Resolve(context.Background(), request)
	if err != nil || !slices.Equal(response.Targets, cfg.Targets) {
		t.Fatal(response, err)
	}
}

func TestDirectoryBoundsAndAtomicValidation(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	nodes := make([]*pb.NodeAdvertisement, 0, MaxNodes-1)
	for i := 1; i < MaxNodes; i++ {
		nodes = append(nodes, advertisement(i, "same", "127.0.0.1:7447"))
	}
	if err := d.merge(nodes, time.Now()); err != nil {
		t.Fatal(err)
	}
	extra := advertisement(MaxNodes, "same", "127.0.0.1:7447")
	if err := d.merge([]*pb.NodeAdvertisement{extra}, time.Now()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("live node capacity not bounded", err)
	}
	withdraw := advertisement(1, "same", "127.0.0.1:7447")
	withdraw.Sequence = 2
	withdraw.Withdrawn = true
	withdraw.Stores = nil
	if err := d.merge([]*pb.NodeAdvertisement{withdraw, extra}, time.Now()); err != nil {
		t.Fatal("replacement after withdrawal blocked", err)
	}
	malformed := advertisement(MaxNodes+1, "same", "0.0.0.0:7447")
	newer := advertisement(2, "same", "127.0.0.1:7447")
	newer.Sequence = 2
	if err := d.merge([]*pb.NodeAdvertisement{newer, malformed}, time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if d.records[newer.NodeId].advertisement.Sequence != 1 {
		t.Fatal("invalid snapshot partially merged")
	}
	oversized := advertisement(MaxNodes+2, "same", "127.0.0.1:7447")
	oversized.RemainingLeaseMs = uint64(Lease / time.Millisecond)
	if err := d.merge([]*pb.NodeAdvertisement{oversized}, time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatal("uncapped lease accepted", err)
	}
}

func TestExpiredOwnersAndCancelledResolve(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	ad := advertisement(1, "same", "127.0.0.1:7447")
	if err := d.merge([]*pb.NodeAdvertisement{ad}, time.Now().Add(-20*time.Second)); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveRequest{Store: "data"}
	if _, err := d.Resolve(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("expired ownership usable", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Resolve(ctx, request); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}

func startDirectoryTransport(t *testing.T, group, store string, seeds []string) *Directory {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Group: group, PeerAddress: listener.Addr().String(), Targets: []string{group + ".example:7447"}, Stores: []string{store}, Seeds: seeds}
	d := newTestDirectory(t, cfg)
	server := grpc.NewServer()
	pb.RegisterDirectoryServer(server, d)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	d.Start(context.Background())
	return d
}

func TestPeriodicSyncConvergesAcrossPeersAndStops(t *testing.T) {
	a := startDirectoryTransport(t, "a", "alpha", nil)
	a.mu.Lock()
	seedA := a.records[a.self].advertisement.PeerAddress
	a.mu.Unlock()
	b := startDirectoryTransport(t, "b", "beta", []string{seedA})
	b.mu.Lock()
	seedB := b.records[b.self].advertisement.PeerAddress
	b.mu.Unlock()
	c := startDirectoryTransport(t, "c", "gamma", []string{seedB})
	until := time.Now().Add(15 * time.Second)
	for {
		ready := true
		for _, d := range []*Directory{a, b, c} {
			for _, name := range []string{"alpha", "beta", "gamma"} {
				request := &pb.ResolveRequest{Store: name}
				if _, err := d.Resolve(context.Background(), request); err != nil {
					ready = false
				}
			}
		}
		if ready {
			break
		}
		if time.Now().After(until) {
			t.Fatal("multi-hop directory did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveRequest{Store: "gamma"}
	if _, err := b.Resolve(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("graceful withdrawal did not propagate", err)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("sync worker not joined")
	}
}

func TestReplayWatermarksAreBoundedAndEventuallyCollected(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	now := time.Now()
	nodes := make([]*pb.NodeAdvertisement, 0, maxWatermarks-1)
	for i := 1; i < maxWatermarks; i++ {
		ad := advertisement(i, "same", "127.0.0.1:7447")
		ad.Withdrawn = true
		ad.Stores = nil
		ad.Targets = nil
		nodes = append(nodes, ad)
	}
	if err := d.merge(nodes, now); err != nil {
		t.Fatal(err)
	}
	extra := advertisement(maxWatermarks, "same", "127.0.0.1:7447")
	if err := d.merge([]*pb.NodeAdvertisement{extra}, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("watermark capacity unbounded", err)
	}
	if len(d.records) != maxWatermarks {
		t.Fatal("bounded watermark store changed", len(d.records))
	}
	if err := d.merge([]*pb.NodeAdvertisement{extra}, now.Add(3*Lease)); err != nil {
		t.Fatal("expired watermarks never collected", err)
	}
	if len(d.records) != 2 {
		t.Fatal("expired watermarks retained indefinitely", len(d.records))
	}
}

type seedSelector struct {
	pb.UnimplementedDirectoryServer
	self, other        *Directory
	calls, connections atomic.Int32
}

func (s *seedSelector) Exchange(ctx context.Context, request *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	if s.calls.Add(1) == 1 {
		return s.self.Exchange(ctx, request)
	}
	return s.other.Exchange(ctx, request)
}

func (s *seedSelector) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (s *seedSelector) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnBegin); ok {
		s.connections.Add(1)
	}
}
func (s *seedSelector) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (s *seedSelector) HandleRPC(context.Context, stats.RPCStats)                       {}

func TestSeedSelfSelectionRetriesWithFreshConnection(t *testing.T) {
	selfCfg := Config{Group: "a", Targets: []string{"a.example:7447"}, Stores: []string{"alpha"}}
	self := newTestDirectory(t, selfCfg)
	otherCfg := Config{Group: "b", Targets: []string{"b.example:7447"}, Stores: []string{"beta"}}
	other := newTestDirectory(t, otherCfg)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	selector := &seedSelector{self: self, other: other}
	server := grpc.NewServer(grpc.StatsHandler(selector))
	pb.RegisterDirectoryServer(server, selector)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := self.exchange(ctx, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveRequest{Store: "beta"}
	if _, err := self.Resolve(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatal("self selection fabricated another Store", err)
	}
	if err := self.exchange(ctx, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := self.Resolve(ctx, request); err != nil {
		t.Fatal("retry did not discover another peer", err)
	}
	if selector.connections.Load() != 2 {
		t.Fatal("seed remained pinned to one connection", selector.connections.Load())
	}
}

type malformedDirectoryResponse struct {
	pb.UnimplementedDirectoryServer
}

func (*malformedDirectoryResponse) Exchange(context.Context, *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	ad := advertisement(1, "bad", "127.0.0.1:7447")
	response := &pb.ExchangeResponse{Nodes: []*pb.NodeAdvertisement{ad}}
	response.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	return response, nil
}

func TestExchangeRejectsUnknownResponseBeforeMerge(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	peer := &malformedDirectoryResponse{}
	pb.RegisterDirectoryServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	if err := d.exchange(context.Background(), listener.Addr().String()); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unknown peer response accepted", err)
	}
	request := &pb.ResolveRequest{Store: "data"}
	if _, err := d.Resolve(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("invalid response partially merged", err)
	}
}
