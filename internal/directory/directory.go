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

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxNodes          = 128
	MaxExchangeBytes  = 2 << 20
	ExchangeTimeout   = 2 * time.Second
	Lease             = 60 * time.Second
	CacheTTL          = 30 * time.Second
	maxWatermarks     = 512
	heartbeatInterval = 5 * time.Second
	syncInterval      = time.Second
)

type Config struct {
	Group, PeerAddress     string
	Targets, Stores, Seeds []string
}

type record struct {
	advertisement   *pb.NodeAdvertisement
	expires, forget time.Time
}

type Directory struct {
	pb.UnimplementedDirectoryServer
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
		if !ValidStore(name) || i > 0 && stores[i-1] == name {
			return nil, errors.New("invalid or duplicate advertised Store")
		}
	}
	var targets, seeds []string
	var err error
	if len(cfg.Targets) > 0 {
		targets, err = CanonicalTargets(cfg.Targets)
		if err != nil {
			return nil, err
		}
	} else if len(stores) > 0 {
		return nil, errors.New("local Stores require advertised business targets")
	}
	if len(cfg.Seeds) > 0 {
		seeds, err = CanonicalTargets(cfg.Seeds)
		if err != nil {
			return nil, err
		}
		if cfg.PeerAddress == "" {
			return nil, errors.New("discovery seeds require advertised peer address")
		}
	}
	if cfg.PeerAddress != "" {
		cfg.PeerAddress, err = CanonicalAddress(cfg.PeerAddress)
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
	if !ValidGroup(cfg.Group) {
		return nil, errors.New("invalid discovery group")
	}
	now := time.Now()
	advertisement := &pb.NodeAdvertisement{NodeId: self, Sequence: 1, PeerAddress: cfg.PeerAddress, Group: cfg.Group, Stores: stores, Targets: targets}
	owned := record{advertisement: advertisement, expires: now.Add(Lease), forget: now.Add(3 * Lease)}
	d := &Directory{self: self, records: map[string]record{self: owned}, seeds: seeds, done: make(chan struct{})}
	return d, nil
}

func ValidGroup(value string) bool {
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

func (d *Directory) Resolve(ctx context.Context, request *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || !ValidStore(request.Store) || len(request.ProtoReflect().GetUnknown()) > 0 {
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
		ad := owned.advertisement
		if ad.Withdrawn || !now.Before(owned.expires) || !slices.Contains(ad.Stores, request.Store) {
			continue
		}
		if group != "" && group != ad.Group {
			return nil, status.Error(codes.FailedPrecondition, "Store advertised by conflicting groups")
		}
		group = ad.Group
		for _, target := range ad.Targets {
			if targetExpiry[target].Before(owned.expires) {
				targetExpiry[target] = owned.expires
			}
		}
	}
	if len(targetExpiry) == 0 {
		return nil, status.Error(codes.Unavailable, "Store has no live advertisement; retry discovery")
	}
	if len(targetExpiry) > MaxTargets {
		return nil, status.Error(codes.ResourceExhausted, "Store target bound exceeded")
	}
	targets := make([]string, 0, len(targetExpiry))
	ttl := CacheTTL
	for target, expires := range targetExpiry {
		targets = append(targets, target)
		ttl = min(ttl, expires.Sub(now))
	}
	if ttl < time.Millisecond {
		return nil, status.Error(codes.Unavailable, "Store advertisement expiring")
	}
	slices.Sort(targets)
	response := &pb.ResolveResponse{Store: request.Store, Group: group, Targets: targets, CacheTtlMs: uint64(ttl / time.Millisecond)}
	return response, nil
}

func validateAdvertisement(ad *pb.NodeAdvertisement) error {
	if ad == nil || len(ad.ProtoReflect().GetUnknown()) > 0 || len(ad.NodeId) != 32 || strings.Trim(ad.NodeId, "0123456789abcdef") != "" || ad.Sequence == 0 || !ValidGroup(ad.Group) || len(ad.Stores) > 16 || len(ad.Targets) > 16 || ad.RemainingLeaseMs == 0 || ad.RemainingLeaseMs > uint64((Lease-ExchangeTimeout)/time.Millisecond) {
		return status.Error(codes.InvalidArgument, "invalid node advertisement")
	}
	if ad.PeerAddress != "" {
		address, err := CanonicalAddress(ad.PeerAddress)
		if err != nil || address != ad.PeerAddress {
			return status.Error(codes.InvalidArgument, "invalid advertised peer address")
		}
	}
	if ad.Withdrawn && len(ad.Stores) > 0 || len(ad.Stores) > 0 && len(ad.Targets) == 0 {
		return status.Error(codes.InvalidArgument, "invalid advertised Store targets")
	}
	if len(ad.Targets) > 0 {
		targets, err := CanonicalTargets(ad.Targets)
		if err != nil || !slices.Equal(targets, ad.Targets) {
			return status.Error(codes.InvalidArgument, "noncanonical advertised targets")
		}
	}
	for i, name := range ad.Stores {
		if !ValidStore(name) || i > 0 && ad.Stores[i-1] >= name {
			return status.Error(codes.InvalidArgument, "invalid advertised Stores")
		}
	}
	return nil
}

func (d *Directory) merge(nodes []*pb.NodeAdvertisement, now time.Time) error {
	if len(nodes) > maxWatermarks {
		return status.Error(codes.ResourceExhausted, "directory node bound exceeded")
	}
	seen := make(map[string]bool, len(nodes))
	for _, ad := range nodes {
		if err := validateAdvertisement(ad); err != nil {
			return err
		}
		if seen[ad.NodeId] {
			return status.Error(codes.InvalidArgument, "duplicate node advertisement")
		}
		seen[ad.NodeId] = true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return status.Error(codes.Unavailable, "directory closed")
	}
	for id, owned := range d.records {
		if id != d.self && !now.Before(owned.forget) {
			delete(d.records, id)
		}
	}
	newRecords, live := 0, 0
	for _, owned := range d.records {
		if now.Before(owned.expires) && !owned.advertisement.Withdrawn {
			live++
		}
	}
	for _, ad := range nodes {
		if ad.NodeId == d.self {
			continue
		}
		owned, known := d.records[ad.NodeId]
		if !known {
			newRecords++
			if !ad.Withdrawn {
				live++
			}
		} else if ad.Sequence > owned.advertisement.Sequence {
			if now.Before(owned.expires) && !owned.advertisement.Withdrawn {
				live--
			}
			if !ad.Withdrawn {
				live++
			}
		}
	}
	if len(d.records)+newRecords > maxWatermarks || live > MaxNodes {
		return status.Error(codes.ResourceExhausted, "directory capacity exhausted")
	}
	for _, ad := range nodes {
		if ad.NodeId == d.self {
			continue
		}
		owned, known := d.records[ad.NodeId]
		if known && ad.Sequence <= owned.advertisement.Sequence {
			continue
		}
		copy := proto.Clone(ad).(*pb.NodeAdvertisement)
		copy.RemainingLeaseMs = 0
		expires := now.Add(time.Duration(ad.RemainingLeaseMs) * time.Millisecond)
		updated := record{advertisement: copy, expires: expires, forget: expires.Add(2 * Lease)}
		d.records[ad.NodeId] = updated
	}
	return nil
}

// snapshot charges the full RPC timeout before sending. The receiver's bounded
// timeout therefore cannot turn network delay into additional lease lifetime.
func (d *Directory) snapshot(now time.Time) []*pb.NodeAdvertisement {
	d.mu.Lock()
	defer d.mu.Unlock()
	nodes := make([]*pb.NodeAdvertisement, 0, MaxNodes)
	for _, owned := range d.records {
		remaining := owned.expires.Sub(now) - ExchangeTimeout
		if remaining < time.Millisecond {
			continue
		}
		copy := proto.Clone(owned.advertisement).(*pb.NodeAdvertisement)
		copy.RemainingLeaseMs = uint64(remaining / time.Millisecond)
		nodes = append(nodes, copy)
	}
	slices.SortFunc(nodes, func(a, b *pb.NodeAdvertisement) int { return strings.Compare(a.NodeId, b.NodeId) })
	return nodes
}

func (d *Directory) Exchange(ctx context.Context, request *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || len(request.ProtoReflect().GetUnknown()) > 0 || proto.Size(request) > MaxExchangeBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid directory exchange")
	}
	if err := d.merge(request.Nodes, time.Now()); err != nil {
		return nil, err
	}
	response := &pb.ExchangeResponse{Nodes: d.snapshot(time.Now())}
	if proto.Size(response) > MaxExchangeBytes {
		return nil, status.Error(codes.ResourceExhausted, "directory exchange byte bound exceeded")
	}
	return response, nil
}
