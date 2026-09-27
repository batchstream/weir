package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/batchstream/weir/internal/netlimit"
	"google.golang.org/grpc/resolver"
)

const maxDNSAddresses = netlimit.MaxDNSAddresses
const dnsRefresh = 30 * time.Second
const dnsMinInterval = time.Second

// The stock resolver has no answer-count cap and retains its last good result
// indefinitely on errors. This adapter bounds those two policies; Go owns DNS
// encoding, system resolver configuration and A/AAAA, gRPC owns pick-first.
type peerDNSBuilder struct {
	endpoint   *remoteEndpoint
	host, port string
	resolver   *net.Resolver
}

func (*peerDNSBuilder) Scheme() string { return "weir-dns" }
func (b *peerDNSBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(b.endpoint.ctx)
	d := &peerDNS{builder: b, cc: cc, ctx: ctx, cancel: cancel, now: make(chan struct{}, 1), done: make(chan struct{})}
	go d.run()
	return d, nil
}

type peerDNS struct {
	builder *peerDNSBuilder
	cc      resolver.ClientConn
	ctx     context.Context
	cancel  context.CancelFunc
	now     chan struct{}
	done    chan struct{}
}

func (d *peerDNS) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case d.now <- struct{}{}:
	default:
	}
}
func (d *peerDNS) Close() { d.cancel(); <-d.done }

func (d *peerDNS) run() {
	defer close(d.done)
	delay := dnsMinInterval
	for {
		addresses, err := d.lookup()
		if d.ctx.Err() != nil {
			return
		}
		e := d.builder.endpoint
		e.mu.Lock()
		e.addresses = make(map[string]bool, len(addresses))
		for _, address := range addresses {
			e.addresses[address.Addr] = true
		}
		e.mu.Unlock()
		// Empty updates close the old pick-first SubConns gracefully and make
		// future calls fail. ReportError alone would silently keep stale IPs.
		state := resolver.State{Addresses: addresses}
		updateErr := d.cc.UpdateState(state)
		if err == nil {
			err = updateErr
		}
		interval := dnsRefresh
		if err != nil {
			interval = delay
			delay = min(2*delay, dnsRefresh)
		} else {
			delay = dnsMinInterval
		}
		finished := time.Now()
		timer := time.NewTimer(interval)
		select {
		case <-d.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-d.now:
			timer.Stop()
			// ResolveNow coalesces requests; it cannot defeat failure backoff.
			minimum := dnsMinInterval
			if err != nil {
				minimum = interval
			}
			timer.Reset(max(0, time.Until(finished.Add(minimum))))
			select {
			case <-d.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (d *peerDNS) lookup() ([]resolver.Address, error) {
	gate := d.builder.endpoint.owner.resolutions
	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case gate <- struct{}{}:
	}
	defer func() { <-gate }()
	ctx, cancel := context.WithTimeout(d.ctx, peerConnectTimeout)
	defer cancel()
	ips, err := netlimit.LookupHost(ctx, d.builder.resolver, d.builder.host)
	if err != nil {
		return nil, err
	}
	strings := make([]string, 0, len(ips))
	for _, address := range ips {
		ip, err := netip.ParseAddr(address)
		if err != nil || ip.Zone() != "" {
			return nil, errors.New("invalid peer DNS address")
		}
		strings = append(strings, net.JoinHostPort(ip.Unmap().String(), d.builder.port))
	}
	slices.Sort(strings)
	strings = slices.Compact(strings)
	addresses := make([]resolver.Address, 0, len(strings))
	for _, address := range strings {
		entry := resolver.Address{Addr: address}
		addresses = append(addresses, entry)
	}
	return addresses, nil
}
