package netlimit

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testdns"
)

func TestStoreDNSBoundPreservesBackendBound(t *testing.T) {
	dns := testdns.Start(t)
	addresses := make([]netip.Addr, 16)
	for i := range addresses {
		value := [4]byte{127, 0, 0, byte(i + 1)}
		addresses[i] = netip.AddrFrom4(value)
	}
	answer := testdns.Answer{Addresses: addresses}
	dns.Set("replicas.test", answer)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := LookupHost(ctx, dns.Resolver(), "replicas.test"); err == nil {
		t.Fatal("backend eight-address budget changed")
	}
	ips, err := LookupHostLimit(ctx, dns.Resolver(), "replicas.test", 64)
	if err != nil || len(ips) != len(addresses) {
		t.Fatal("Store discovery rejected bounded replica set", len(ips), err)
	}
	for _, limit := range []int{0, 15, 129} {
		_, err := LookupHostLimit(ctx, dns.Resolver(), "replicas.test", limit)
		if err == nil {
			t.Fatal("DNS bound accepted", limit)
		}
	}
}
