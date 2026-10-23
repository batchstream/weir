package directory

import (
	"context"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/batchstream/weir-protocol/api/netlimit"
	"github.com/batchstream/weir-protocol/api/protocol"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (d *Directory) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil || d.closed {
		return
	}
	ctx, d.cancel = context.WithCancel(ctx)
	go d.run(ctx)
}

func (d *Directory) renew(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	owned := d.records[d.self]
	if d.closed || owned.announcement.Withdrawn {
		return
	}
	owned.announcement.Revision++
	owned.expires = now.Add(d.policy.Lease)
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
	peers := make([]string, 0, len(d.records))
	for id, owned := range d.records {
		ad := owned.announcement
		if id != d.self && !ad.Withdrawn && ad.PeerEndpoint != "" && now.Before(owned.expires) {
			peers = append(peers, ad.PeerEndpoint)
		}
	}
	slices.Sort(peers)
	return slices.Compact(peers)
}

func (d *Directory) run(ctx context.Context) {
	defer close(d.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	seedIndex := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			d.renew(now)
			peers := d.peers(now)
			if len(peers) > 0 {
				_ = d.syncPeer(ctx, peers[rand.IntN(len(peers))])
			}
			if len(d.seeds) > 0 {
				_ = d.syncPeer(ctx, d.seeds[seedIndex%len(d.seeds)])
				seedIndex++
			}
			timer.Reset(max(0, d.policy.SyncInterval-time.Since(now)))
		}
	}
}

// A short-lived connection is intentional: an ordinary seed Service can select
// this process. Reconnecting later gives that Service another chance to select
// a different peer and reconnect independently formed membership sets.
func (d *Directory) syncPeer(parent context.Context, address string) error {
	ctx, cancel := context.WithTimeout(parent, d.policy.SyncTimeout)
	defer cancel()
	host, port, _ := net.SplitHostPort(address)
	if _, err := netip.ParseAddr(host); err != nil {
		ips, err := netlimit.LookupHost(ctx, nil, host)
		if err != nil {
			return err
		}
		address = net.JoinHostPort(ips[rand.IntN(len(ips))], port)
	}
	dialer := net.Dialer{Timeout: d.policy.SyncTimeout}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}),
		grpc.WithReadBufferSize(16 << 10), grpc.WithWriteBufferSize(16 << 10), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithMaxHeaderListSize(^uint32(0)),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes)),
	}
	connection, err := grpc.NewClient("passthrough:///"+address, options...)
	if err != nil {
		return err
	}
	defer connection.Close()
	client := peerpb.NewPeerDiscoveryServiceClient(connection)
	anchor := time.Now()
	stream, err := client.SyncDirectory(ctx)
	if err != nil {
		return err
	}
	opening := &peerpb.SyncDirectoryRequest{}
	if err := stream.Send(opening); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(response.ProtoReflect().GetUnknown()) > 0 {
		return status.Error(codes.InvalidArgument, "unknown directory response fields")
	}
	request := &peerpb.SyncDirectoryRequest{Announcements: d.snapshot()}
	if err := stream.Send(request); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return err
		}
		return status.Error(codes.InvalidArgument, "directory exchange contains extra responses")
	}
	// Install only a completely validated finite exchange. A closed directory
	// still publishes its withdrawal but no longer installs remote state.
	if err := d.merge(response.Announcements, anchor, time.Now()); err != nil && status.Code(err) != codes.Unavailable {
		return err
	}
	return nil
}

func (d *Directory) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.cancel
	owned := d.records[d.self]
	owned.announcement.Revision++
	owned.announcement.Withdrawn = true
	owned.announcement.StoreNames = nil
	owned.announcement.StoreEndpoints = nil
	owned.expires = time.Now().Add(d.policy.Lease)
	d.records[d.self] = owned
	d.mu.Unlock()
	if cancel != nil {
		cancel()
		<-d.done
	}
	// Withdrawal is best effort. Abrupt death is covered by the same owner lease.
	peers := d.peers(time.Now())
	peers = append(peers, d.seeds...)
	for _, peer := range peers {
		if ctx.Err() != nil {
			break
		}
		_ = d.syncPeer(ctx, peer)
	}
	return nil
}
