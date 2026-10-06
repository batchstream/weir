package directory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxNodes          = 128
	MaxSyncBytes      = 2 << 20
	SyncTimeout       = 2 * time.Second
	Lease             = 60 * time.Second
	MaxAnnouncements  = 512
	heartbeatInterval = 5 * time.Second
	syncInterval      = time.Second
)

type Config struct {
	Group, PeerAddress     string
	Targets, Stores, Seeds []string
}

type record struct {
	announcement *peerpb.NodeAnnouncement
	expires      time.Time
}

type Directory struct {
	peerpb.UnimplementedPeerDiscoveryServiceServer
	mu              sync.Mutex
	self            string
	records         map[string]record
	seeds           []string
	started, closed bool
	cancel          context.CancelFunc
	done            chan struct{}
}

func New(cfg Config) (*Directory, error) {
	if len(cfg.Stores) > 16 || len(cfg.Targets) > 16 || len(cfg.Seeds) > 16 {
		return nil, errors.New("local discovery bounds exceeded")
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
	announcement := &peerpb.NodeAnnouncement{IncarnationId: self, Revision: 1, PeerEndpoint: cfg.PeerAddress, ReplicaGroup: cfg.Group, StoreNames: stores, StoreEndpoints: targets}
	owned := record{announcement: announcement, expires: now.Add(Lease)}
	d := &Directory{self: self, records: map[string]record{self: owned}, seeds: seeds, done: make(chan struct{})}
	return d, nil
}

func ValidReplicaGroup(value string) bool {
	if len(value) < 1 || len(value) > 128 {
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
	if len(targetExpiry) > protocol.MaxDiscoveryEndpoints {
		return nil, status.Error(codes.ResourceExhausted, "Store target bound exceeded")
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
	if ad == nil || len(ad.ProtoReflect().GetUnknown()) > 0 || len(ad.IncarnationId) != 32 || strings.Trim(ad.IncarnationId, "0123456789abcdef") != "" || ad.Revision == 0 || !ValidReplicaGroup(ad.ReplicaGroup) || len(ad.StoreNames) > 16 || len(ad.StoreEndpoints) > 16 || ad.LeaseRemainingMs == 0 || ad.LeaseRemainingMs > uint64((Lease-SyncTimeout)/time.Millisecond) {
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

func (d *Directory) merge(nodes []*peerpb.NodeAnnouncement, now time.Time) error {
	if len(nodes) > MaxAnnouncements {
		return status.Error(codes.ResourceExhausted, "directory node bound exceeded")
	}
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
	for id, owned := range d.records {
		if id != d.self && !now.Before(owned.expires.Add(2*Lease)) {
			delete(d.records, id)
		}
	}
	newRecords, live := 0, 0
	for _, owned := range d.records {
		if now.Before(owned.expires) && !owned.announcement.Withdrawn {
			live++
		}
	}
	for _, ad := range nodes {
		if ad.IncarnationId == d.self {
			continue
		}
		owned, known := d.records[ad.IncarnationId]
		if !known {
			newRecords++
			if !ad.Withdrawn {
				live++
			}
		} else if ad.Revision > owned.announcement.Revision {
			if now.Before(owned.expires) && !owned.announcement.Withdrawn {
				live--
			}
			if !ad.Withdrawn {
				live++
			}
		}
	}
	if len(d.records)+newRecords > MaxAnnouncements || live > MaxNodes {
		return status.Error(codes.ResourceExhausted, "directory capacity exhausted")
	}
	for _, ad := range nodes {
		if ad.IncarnationId == d.self {
			continue
		}
		owned, known := d.records[ad.IncarnationId]
		if known && ad.Revision <= owned.announcement.Revision {
			continue
		}
		copy := proto.Clone(ad).(*peerpb.NodeAnnouncement)
		copy.LeaseRemainingMs = 0
		expires := now.Add(time.Duration(ad.LeaseRemainingMs) * time.Millisecond)
		updated := record{announcement: copy, expires: expires}
		d.records[ad.IncarnationId] = updated
	}
	return nil
}

// snapshot charges the full RPC timeout before sending. The receiver's bounded
// timeout therefore cannot turn network delay into additional lease lifetime.
func (d *Directory) snapshot(now time.Time) []*peerpb.NodeAnnouncement {
	d.mu.Lock()
	defer d.mu.Unlock()
	nodes := make([]*peerpb.NodeAnnouncement, 0, MaxNodes)
	for _, owned := range d.records {
		remaining := owned.expires.Sub(now) - SyncTimeout
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

func (d *Directory) SyncDirectory(ctx context.Context, request *peerpb.SyncDirectoryRequest) (*peerpb.SyncDirectoryResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || len(request.ProtoReflect().GetUnknown()) > 0 || proto.Size(request) > MaxSyncBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid directory sync")
	}
	if err := d.merge(request.Announcements, time.Now()); err != nil {
		return nil, err
	}
	response := &peerpb.SyncDirectoryResponse{Announcements: d.snapshot(time.Now())}
	if proto.Size(response) > MaxSyncBytes {
		return nil, status.Error(codes.ResourceExhausted, "directory sync byte bound exceeded")
	}
	return response, nil
}
