package directory

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
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

func announcement(id int, group, target string) *peerpb.NodeAnnouncement {
	ad := &peerpb.NodeAnnouncement{IncarnationId: fmt.Sprintf("%032x", id), Revision: 1, ReplicaGroup: group, StoreNames: []string{"data"}, StoreEndpoints: []string{target}, LeaseRemainingMs: 10000, OriginLeaseMs: 60000}
	return ad
}

func TestRelayLeaseCannotRenewOrResurrect(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	start := time.Now()
	ad := announcement(1, "owner", "127.0.0.1:7447")
	if err := d.merge([]*peerpb.NodeAnnouncement{ad}, start, start); err != nil {
		t.Fatal(err)
	}
	original := d.records[ad.IncarnationId].expires
	if err := d.merge([]*peerpb.NodeAnnouncement{ad}, start.Add(5*time.Second), start.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.IncarnationId].expires.Equal(original) {
		t.Fatal("same sequence renewed lease")
	}
	relayed := d.snapshot()
	var owner *peerpb.NodeAnnouncement
	for _, node := range relayed {
		if node.IncarnationId == ad.IncarnationId {
			owner = node
		}
	}
	if owner == nil || owner.LeaseRemainingMs == 0 || owner.LeaseRemainingMs > ad.LeaseRemainingMs {
		t.Fatal("relay did not preserve remaining lifetime", owner)
	}
	other := newTestDirectory(t, cfg)
	if err := other.merge([]*peerpb.NodeAnnouncement{owner}, start, start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if other.records[ad.IncarnationId].expires.After(original) {
		t.Fatal("transport delay extended owner lease")
	}
	if err := d.merge([]*peerpb.NodeAnnouncement{ad}, start.Add(20*time.Second), start.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.IncarnationId].expires.Equal(original) {
		t.Fatal("expired owner resurrected from same sequence")
	}
	ad.Revision = 2
	if err := d.merge([]*peerpb.NodeAnnouncement{ad}, start.Add(20*time.Second), start.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !d.records[ad.IncarnationId].expires.After(original) {
		t.Fatal("new owner heartbeat did not renew")
	}
}

func TestSameGroupUnionConflictAndWithdrawal(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	first := announcement(1, "same", "shared.example:7447")
	second := announcement(2, "same", "127.0.0.1:7447")
	third := announcement(3, "same", "shared.example:7447")
	if err := d.merge([]*peerpb.NodeAnnouncement{first, second, third}, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveStoreRequest{StoreName: "data"}
	response, err := d.ResolveStore(context.Background(), request)
	if err != nil || len(response.Endpoints) != 2 || response.StoreName != "data" || response.CacheTtlMs == 0 || response.CacheTtlMs > uint64(protocol.MaxDiscoveryCacheTTL/time.Millisecond) {
		t.Fatal(response, err)
	}
	conflict := announcement(4, "other", "127.0.0.1:7449")
	if err := d.merge([]*peerpb.NodeAnnouncement{conflict}, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ResolveStore(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("group conflict not rejected", err)
	}
	conflict.Revision++
	conflict.Withdrawn = true
	conflict.StoreNames = nil
	if err := d.merge([]*peerpb.NodeAnnouncement{conflict}, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	conflict.Revision--
	conflict.Withdrawn = false
	conflict.StoreNames = []string{"data"}
	if err := d.merge([]*peerpb.NodeAnnouncement{conflict}, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ResolveStore(context.Background(), request); err != nil {
		t.Fatal("old announcement resurrected withdrawn owner", err)
	}
	if len(first.StoreNames) != 1 {
		t.Fatal("merge modified caller announcement")
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
	if err := observer.merge(first.snapshot(), now, now); err != nil {
		t.Fatal(err)
	}
	if err := observer.merge(restarted.snapshot(), now, now); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	old := observer.records[first.self]
	old.expires = now.Add(-time.Second)
	observer.records[first.self] = old
	observer.mu.Unlock()
	request := &pb.ResolveStoreRequest{StoreName: "data"}
	response, err := observer.ResolveStore(context.Background(), request)
	if err != nil || !slices.Equal(response.Endpoints, cfg.Targets) {
		t.Fatal(response, err)
	}
}

func TestDirectoryLargeMembershipAndAtomicValidation(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	nodes := make([]*peerpb.NodeAnnouncement, 0, 640-1)
	for i := 1; i < 640; i++ {
		nodes = append(nodes, announcement(i, "same", "127.0.0.1:7447"))
	}
	if err := d.merge(nodes, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	extra := announcement(640, "same", "127.0.0.1:7447")
	if err := d.merge([]*peerpb.NodeAnnouncement{extra}, time.Now(), time.Now()); err != nil {
		t.Fatal("large live membership rejected", err)
	}
	withdraw := announcement(1, "same", "127.0.0.1:7447")
	withdraw.Revision = 2
	withdraw.Withdrawn = true
	withdraw.StoreNames = nil
	if err := d.merge([]*peerpb.NodeAnnouncement{withdraw, extra}, time.Now(), time.Now()); err != nil {
		t.Fatal("replacement after withdrawal blocked", err)
	}
	malformed := announcement(640+1, "same", "0.0.0.0:7447")
	newer := announcement(2, "same", "127.0.0.1:7447")
	newer.Revision = 2
	if err := d.merge([]*peerpb.NodeAnnouncement{newer, malformed}, time.Now(), time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if d.records[newer.IncarnationId].announcement.Revision != 1 {
		t.Fatal("invalid snapshot partially merged")
	}
	oversized := announcement(640+2, "same", "127.0.0.1:7447")
	oversized.LeaseRemainingMs = ^uint64(0)
	if err := d.merge([]*peerpb.NodeAnnouncement{oversized}, time.Now(), time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unrepresentable lease accepted", err)
	}
}

func TestExpiredOwnersAndCancelledResolve(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	ad := announcement(1, "same", "127.0.0.1:7447")
	if err := d.merge([]*peerpb.NodeAnnouncement{ad}, time.Now().Add(-20*time.Second), time.Now().Add(-20*time.Second)); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveStoreRequest{StoreName: "data"}
	if _, err := d.ResolveStore(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("expired ownership usable", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.ResolveStore(ctx, request); status.Code(err) != codes.Canceled {
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
	peerpb.RegisterPeerDiscoveryServiceServer(server, d)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	d.Start(context.Background())
	return d
}

func TestPeriodicSyncConvergesAcrossPeersAndStops(t *testing.T) {
	a := startDirectoryTransport(t, "a", "alpha", nil)
	a.mu.Lock()
	seedA := a.records[a.self].announcement.PeerEndpoint
	a.mu.Unlock()
	b := startDirectoryTransport(t, "b", "beta", []string{seedA})
	b.mu.Lock()
	seedB := b.records[b.self].announcement.PeerEndpoint
	b.mu.Unlock()
	c := startDirectoryTransport(t, "c", "gamma", []string{seedB})
	until := time.Now().Add(15 * time.Second)
	for {
		ready := true
		for _, d := range []*Directory{a, b, c} {
			for _, name := range []string{"alpha", "beta", "gamma"} {
				request := &pb.ResolveStoreRequest{StoreName: name}
				if _, err := d.ResolveStore(context.Background(), request); err != nil {
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
	request := &pb.ResolveStoreRequest{StoreName: "gamma"}
	if _, err := b.ResolveStore(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("graceful withdrawal did not propagate", err)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("sync worker not joined")
	}
}

func TestLargeReplayWatermarksAreEventuallyCollected(t *testing.T) {
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	now := time.Now()
	nodes := make([]*peerpb.NodeAnnouncement, 0, 768-1)
	for i := 1; i < 768; i++ {
		ad := announcement(i, "same", "127.0.0.1:7447")
		ad.Withdrawn = true
		ad.StoreNames = nil
		ad.StoreEndpoints = nil
		nodes = append(nodes, ad)
	}
	if err := d.merge(nodes, now, now); err != nil {
		t.Fatal(err)
	}
	extra := announcement(768, "same", "127.0.0.1:7447")
	if err := d.merge([]*peerpb.NodeAnnouncement{extra}, now, now); err != nil {
		t.Fatal("large watermark snapshot rejected", err)
	}
	if len(d.records) != 769 {
		t.Fatal("large watermark store changed", len(d.records))
	}
	if err := d.merge([]*peerpb.NodeAnnouncement{extra}, now, now.Add(3*time.Minute)); err != nil {
		t.Fatal("expired watermarks never collected", err)
	}
	if len(d.records) != 1 {
		t.Fatal("expired watermarks retained or delayed response resurrected", len(d.records))
	}
}

type seedSelector struct {
	self, other        *Directory
	calls, connections atomic.Int32
}

func (s *seedSelector) SyncDirectory(stream grpc.BidiStreamingServer[peerpb.SyncDirectoryRequest, peerpb.SyncDirectoryResponse]) error {
	if s.calls.Add(1) == 1 {
		return s.self.SyncDirectory(stream)
	}
	return s.other.SyncDirectory(stream)
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
	peerpb.RegisterPeerDiscoveryServiceServer(server, selector)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := self.syncPeer(ctx, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	request := &pb.ResolveStoreRequest{StoreName: "beta"}
	if _, err := self.ResolveStore(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatal("self selection fabricated another Store", err)
	}
	if err := self.syncPeer(ctx, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := self.ResolveStore(ctx, request); err != nil {
		t.Fatal("retry did not discover another peer", err)
	}
	if selector.connections.Load() != 2 {
		t.Fatal("seed remained pinned to one connection", selector.connections.Load())
	}
}

type malformedDirectoryResponse struct {
}

func (*malformedDirectoryResponse) SyncDirectory(stream grpc.BidiStreamingServer[peerpb.SyncDirectoryRequest, peerpb.SyncDirectoryResponse]) error {
	ad := announcement(1, "bad", "127.0.0.1:7447")
	response := &peerpb.SyncDirectoryResponse{Announcements: []*peerpb.NodeAnnouncement{ad}}
	response.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	return stream.Send(response)
}

func TestSyncDirectoryRejectsUnknownResponseBeforeMerge(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	peer := &malformedDirectoryResponse{}
	peerpb.RegisterPeerDiscoveryServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	cfg := Config{Group: "observer"}
	d := newTestDirectory(t, cfg)
	if err := d.syncPeer(context.Background(), listener.Addr().String()); status.Code(err) != codes.Internal {
		t.Fatal("unknown peer response accepted", err)
	}
	request := &pb.ResolveStoreRequest{StoreName: "data"}
	if _, err := d.ResolveStore(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatal("invalid response partially merged", err)
	}
}

func TestSnapshotRetainsAllWithdrawalWatermarks(t *testing.T) {
	cfg := Config{Group: "observer"}
	source := newTestDirectory(t, cfg)
	nodes := make([]*peerpb.NodeAnnouncement, 768-1)
	for i := range nodes {
		node := announcement(i+1, "same", "127.0.0.1:7447")
		node.Withdrawn = true
		node.StoreNames = nil
		nodes[i] = node
	}
	now := time.Now()
	if err := source.merge(nodes, now, now); err != nil {
		t.Fatal(err)
	}
	snapshot := source.snapshot()
	if len(snapshot) != 768 {
		t.Fatal("withdrawal watermarks lost during sync", len(snapshot))
	}
	withdrawn := 0
	for _, node := range snapshot {
		if node.Withdrawn {
			withdrawn++
		}
	}
	if withdrawn != 768-1 {
		t.Fatal("withdrawals changed live node semantics", withdrawn)
	}
}

func TestLocalDirectoryAcceptsLargeStoreTargetAndSeedSets(t *testing.T) {
	stores, targets, seeds := make([]string, 32), make([]string, 32), make([]string, 32)
	for i := range stores {
		stores[i] = fmt.Sprintf("store-%03d", i)
		targets[i] = fmt.Sprintf("127.0.0.1:%d", 7400+i)
		seeds[i] = fmt.Sprintf("127.0.0.1:%d", 8400+i)
	}
	config := Config{Group: strings.Repeat("group", 1024), PeerAddress: "127.0.0.1:8447", Stores: stores, Targets: targets, Seeds: seeds}
	directory, err := New(config)
	if err != nil {
		t.Fatal("valid local directory rejected by former scale limits", err)
	}
	defer func() {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_ = directory.Close(ctx)
	}()
	local := directory.records[directory.self].announcement
	if len(local.StoreNames) != 32 || len(local.StoreEndpoints) != 32 || len(directory.seeds) != 32 || local.ReplicaGroup != config.Group {
		t.Fatal("local directory metadata was truncated", local)
	}
}
