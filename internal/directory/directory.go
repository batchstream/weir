package directory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Policy struct {
	Lease        time.Duration
	SyncInterval time.Duration
	SyncTimeout  time.Duration
}

func DefaultPolicy() Policy {
	policy := Policy{Lease: time.Minute, SyncInterval: time.Second, SyncTimeout: 2 * time.Second}
	return policy
}

func (p Policy) Validate() error {
	for _, duration := range []time.Duration{p.Lease, p.SyncInterval, p.SyncTimeout} {
		if duration <= 0 {
			return errors.New("discovery lease, sync interval and timeout must be positive")
		}
	}
	if p.Lease%time.Millisecond != 0 {
		return errors.New("discovery lease must be whole milliseconds for peer encoding")
	}
	remaining := p.Lease - p.SyncInterval
	if remaining <= p.SyncTimeout || remaining-p.SyncTimeout <= p.SyncTimeout {
		return errors.New("discovery lease must exceed sync interval plus two sync timeouts")
	}
	return nil
}

type Config struct {
	Group, PeerAddress     string
	Targets, Stores, Seeds []string
	Policy                 Policy
}

type record struct {
	announcement *peerpb.NodeAnnouncement
	expires      time.Time
	forget       time.Time
}

type Directory struct {
	mu      sync.Mutex
	self    string
	records map[string]record
	seeds   []string
	closed  bool
	cancel  context.CancelFunc
	done    chan struct{}
	policy  Policy
}

func New(cfg Config) (*Directory, error) {
	if cfg.Policy == (Policy{}) {
		cfg.Policy = DefaultPolicy()
	}
	if err := cfg.Policy.Validate(); err != nil {
		return nil, err
	}
	stores := slices.Clone(cfg.Stores)
	slices.Sort(stores)
	for i, name := range stores {
		if !protocol.ValidStoreName(name) || i > 0 && stores[i-1] == name {
			return nil, errors.New("invalid or duplicate advertised Store")
		}
	}
	var targets, seeds []string
	var err error
	if len(cfg.Targets) > 0 {
		targets, err = protocol.CanonicalEndpoints(cfg.Targets)
		if err != nil {
			return nil, err
		}
	} else if len(stores) > 0 {
		return nil, errors.New("local Stores require advertised business targets")
	}
	if len(cfg.Seeds) > 0 {
		seeds, err = protocol.CanonicalEndpoints(cfg.Seeds)
		if err != nil {
			return nil, err
		}
		if cfg.PeerAddress == "" {
			return nil, errors.New("discovery seeds require advertised peer address")
		}
	}
	if cfg.PeerAddress != "" {
		cfg.PeerAddress, err = protocol.CanonicalEndpoint(cfg.PeerAddress)
		if err != nil {
			return nil, err
		}
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	self := hex.EncodeToString(raw)
	if cfg.Group == "" {
		cfg.Group = self
	}
	if !ValidReplicaGroup(cfg.Group) {
		return nil, errors.New("invalid discovery group")
	}
	now := time.Now()
	announcement := &peerpb.NodeAnnouncement{IncarnationId: self, Revision: 1, PeerEndpoint: cfg.PeerAddress, ReplicaGroup: cfg.Group, StoreNames: stores, StoreEndpoints: targets, OriginLeaseMs: uint64(cfg.Policy.Lease / time.Millisecond)}
	owned := record{announcement: announcement, expires: now.Add(cfg.Policy.Lease)}
	d := &Directory{self: self, records: map[string]record{self: owned}, seeds: seeds, done: make(chan struct{}), policy: cfg.Policy}
	return d, nil
}

func ValidReplicaGroup(value string) bool {
	if len(value) < 1 {
		return false
	}
	for _, c := range value {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func (d *Directory) ResolveStore(ctx context.Context, request *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || !protocol.ValidStoreName(request.StoreName) || len(request.ProtoReflect().GetUnknown()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid Store resolution request")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, status.Error(codes.Unavailable, "directory closed")
	}
	now := time.Now()
	group := ""
	targetExpiry := make(map[string]time.Time)
	for _, owned := range d.records {
		ad := owned.announcement
		if ad.Withdrawn || !now.Before(owned.expires) || !slices.Contains(ad.StoreNames, request.StoreName) {
			continue
		}
		if group != "" && group != ad.ReplicaGroup {
			return nil, status.Error(codes.FailedPrecondition, "Store advertised by conflicting groups")
		}
		group = ad.ReplicaGroup
		for _, target := range ad.StoreEndpoints {
			if targetExpiry[target].Before(owned.expires) {
				targetExpiry[target] = owned.expires
			}
		}
	}
	if len(targetExpiry) == 0 {
		return nil, status.Error(codes.Unavailable, "Store has no live announcement; retry discovery")
	}

	targets := make([]string, 0, len(targetExpiry))
	ttl := protocol.MaxDiscoveryCacheTTL
	for target, expires := range targetExpiry {
		targets = append(targets, target)
		ttl = min(ttl, expires.Sub(now))
	}
	if ttl < time.Millisecond {
		return nil, status.Error(codes.Unavailable, "Store announcement expiring")
	}
	slices.Sort(targets)
	response := &pb.ResolveStoreResponse{StoreName: request.StoreName, Endpoints: targets, CacheTtlMs: uint64(ttl / time.Millisecond)}
	return response, nil
}

func validateAnnouncement(ad *peerpb.NodeAnnouncement) error {
	const maximumDurationMilliseconds = uint64((1<<63 - 1) / int64(time.Millisecond))
	if ad == nil || len(ad.ProtoReflect().GetUnknown()) > 0 || len(ad.IncarnationId) != 32 || strings.Trim(ad.IncarnationId, "0123456789abcdef") != "" || ad.Revision == 0 || !ValidReplicaGroup(ad.ReplicaGroup) || ad.LeaseRemainingMs == 0 || ad.LeaseRemainingMs > ad.OriginLeaseMs || ad.OriginLeaseMs > maximumDurationMilliseconds {
		return status.Error(codes.InvalidArgument, "invalid node announcement")
	}
	if ad.PeerEndpoint != "" {
		address, err := protocol.CanonicalEndpoint(ad.PeerEndpoint)
		if err != nil || address != ad.PeerEndpoint {
			return status.Error(codes.InvalidArgument, "invalid advertised peer address")
		}
	}
	if ad.Withdrawn && len(ad.StoreNames) > 0 || len(ad.StoreNames) > 0 && len(ad.StoreEndpoints) == 0 {
		return status.Error(codes.InvalidArgument, "invalid advertised Store targets")
	}
	if len(ad.StoreEndpoints) > 0 {
		targets, err := protocol.CanonicalEndpoints(ad.StoreEndpoints)
		if err != nil || !slices.Equal(targets, ad.StoreEndpoints) {
			return status.Error(codes.InvalidArgument, "noncanonical advertised targets")
		}
	}
	for i, name := range ad.StoreNames {
		if !protocol.ValidStoreName(name) || i > 0 && ad.StoreNames[i-1] >= name {
			return status.Error(codes.InvalidArgument, "invalid advertised Stores")
		}
	}
	return nil
}

func (d *Directory) merge(nodes []*peerpb.NodeAnnouncement, anchor, now time.Time) error {
	seen := make(map[string]bool, len(nodes))
	for _, ad := range nodes {
		if err := validateAnnouncement(ad); err != nil {
			return err
		}
		if seen[ad.IncarnationId] {
			return status.Error(codes.InvalidArgument, "duplicate node announcement")
		}
		seen[ad.IncarnationId] = true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return status.Error(codes.Unavailable, "directory closed")
	}
	// Check immutable incarnation policy before mutating any records, including GC.
	for _, ad := range nodes {
		if owned, known := d.records[ad.IncarnationId]; known && ad.OriginLeaseMs != owned.announcement.OriginLeaseMs {
			return status.Error(codes.InvalidArgument, "origin lease changed within an incarnation")
		}
	}
	for id, owned := range d.records {
		if id != d.self && !now.Before(owned.forget) {
			delete(d.records, id)
		}
	}
	for _, ad := range nodes {
		if ad.IncarnationId == d.self {
			continue
		}
		owned, known := d.records[ad.IncarnationId]
		if known && (ad.Revision <= owned.announcement.Revision || owned.announcement.Withdrawn) {
			continue
		}
		expires := anchor.Add(time.Duration(ad.LeaseRemainingMs) * time.Millisecond)
		if !now.Before(expires) {
			continue
		}
		copy := proto.Clone(ad).(*peerpb.NodeAnnouncement)
		copy.LeaseRemainingMs = 0
		updated := record{announcement: copy, expires: expires, forget: now.Add(time.Duration(ad.OriginLeaseMs) * time.Millisecond)}
		d.records[ad.IncarnationId] = updated
	}
	return nil
}

// The receiver anchors each remaining lease before this snapshot was formed.
// Its complete exchange delay is therefore deducted without synchronized clocks.
func (d *Directory) snapshot() []*peerpb.NodeAnnouncement {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	nodes := make([]*peerpb.NodeAnnouncement, 0, len(d.records))
	for _, owned := range d.records {
		remaining := owned.expires.Sub(now)
		if remaining < time.Millisecond {
			continue
		}
		copy := proto.Clone(owned.announcement).(*peerpb.NodeAnnouncement)
		copy.LeaseRemainingMs = uint64(remaining / time.Millisecond)
		nodes = append(nodes, copy)
	}
	slices.SortFunc(nodes, func(a, b *peerpb.NodeAnnouncement) int { return strings.Compare(a.IncarnationId, b.IncarnationId) })
	return nodes
}

func (d *Directory) ExchangeTimeout() time.Duration { return d.policy.SyncTimeout }

func (d *Directory) SyncDirectory(stream grpc.BidiStreamingServer[peerpb.SyncDirectoryRequest, peerpb.SyncDirectoryResponse]) error {
	opening, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(opening.ProtoReflect().GetUnknown()) != 0 || len(opening.Announcements) != 0 {
		return status.Error(codes.InvalidArgument, "directory exchange must open with an empty request")
	}
	anchor := time.Now()
	response := &peerpb.SyncDirectoryResponse{Announcements: d.snapshot()}
	if err := stream.Send(response); err != nil {
		return err
	}
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(request.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "unknown directory request fields")
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return err
		}
		return status.Error(codes.InvalidArgument, "directory exchange contains extra requests")
	}
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return d.merge(request.Announcements, anchor, time.Now())
}
