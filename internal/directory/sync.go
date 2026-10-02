package directory

import (
	"context"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/netlimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (d *Directory) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started || d.closed {
		return
	}
	d.started = true
	ctx, d.cancel = context.WithCancel(ctx)
	go d.run(ctx)
}

func (d *Directory) renew(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	owned := d.records[d.self]
	if d.closed || owned.advertisement.Withdrawn {
		return
	}
	owned.advertisement.Sequence++
	owned.expires = now.Add(Lease)
	owned.forget = now.Add(3 * Lease)
	d.records[d.self] = owned
	for id, remote := range d.records {
		if id != d.self && !now.Before(remote.forget) {
			delete(d.records, id)
		}
	}
}

func (d *Directory) peers(now time.Time) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	peers := make([]string, 0, MaxNodes)
	for id, owned := range d.records {
		ad := owned.advertisement
		if id != d.self && !ad.Withdrawn && ad.PeerAddress != "" && now.Before(owned.expires) {
			peers = append(peers, ad.PeerAddress)
		}
	}
	slices.Sort(peers)
	return slices.Compact(peers)
}

func (d *Directory) run(ctx context.Context) {
	defer close(d.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	nextHeartbeat := time.Time{}
	nextSeed := time.Time{}
	seedIndex := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			if !now.Before(nextHeartbeat) {
				d.renew(now)
				nextHeartbeat = now.Add(heartbeatInterval)
			}
			peers := d.peers(now)
			if len(peers) > 0 {
				_ = d.exchange(ctx, peers[rand.IntN(len(peers))])
			}
			if len(d.seeds) > 0 && !now.Before(nextSeed) {
				_ = d.exchange(ctx, d.seeds[seedIndex%len(d.seeds)])
				seedIndex++
				nextSeed = now.Add(heartbeatInterval)
			}
			timer.Reset(syncInterval)
		}
	}
}

// A short-lived connection is intentional: an ordinary seed Service can select
// this process. Reconnecting later gives that Service another chance to select
// a different peer and reconnect independently formed membership sets.
func (d *Directory) exchange(parent context.Context, address string) error {
	ctx, cancel := context.WithTimeout(parent, ExchangeTimeout)
	defer cancel()
	host, port, _ := net.SplitHostPort(address)
	if _, err := netip.ParseAddr(host); err != nil {
		ips, err := netlimit.LookupHostLimit(ctx, nil, host, MaxTargets)
		if err != nil {
			return err
		}
		address = net.JoinHostPort(ips[rand.IntN(len(ips))], port)
	}
	dialer := net.Dialer{Timeout: ExchangeTimeout}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}),
		grpc.WithReadBufferSize(16 << 10), grpc.WithWriteBufferSize(16 << 10), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(MaxExchangeBytes), grpc.MaxCallRecvMsgSize(MaxExchangeBytes)),
	}
	connection, err := grpc.NewClient("passthrough:///"+address, options...)
	if err != nil {
		return err
	}
	defer connection.Close()
	request := &pb.ExchangeRequest{Nodes: d.snapshot(time.Now())}
	client := pb.NewDirectoryClient(connection)
	response, err := client.Exchange(ctx, request)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(response.ProtoReflect().GetUnknown()) > 0 {
		return status.Error(codes.InvalidArgument, "unknown directory response fields")
	}
	return d.merge(response.Nodes, time.Now())
}

func (d *Directory) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	started, cancel := d.started, d.cancel
	owned := d.records[d.self]
	owned.advertisement.Sequence++
	owned.advertisement.Withdrawn = true
	owned.advertisement.Stores = nil
	owned.advertisement.Targets = nil
	owned.expires = time.Now().Add(Lease)
	d.records[d.self] = owned
	d.mu.Unlock()
	if started {
		cancel()
		<-d.done
	}
	// Withdrawal is best effort. Abrupt death is covered by the same owner lease.
	peers := d.peers(time.Now())
	peers = append(peers, d.seeds...)
	for _, peer := range peers[:min(len(peers), 3)] {
		if ctx.Err() != nil {
			break
		}
		_ = d.exchange(ctx, peer)
	}
	return nil
}
