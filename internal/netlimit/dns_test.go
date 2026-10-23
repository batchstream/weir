package netlimit

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testdns"
)

func TestLookupHostReturnsLargeTCPAnswer(t *testing.T) {
	dns := testdns.Start(t)
	addresses := make([]netip.Addr, 100)
	for i := range addresses {
		value := [4]byte{127, 0, 0, byte(i + 1)}
		addresses[i] = netip.AddrFrom4(value)
	}
	answer := testdns.Answer{Addresses: addresses, TCP: true}
	dns.Set("replicas.test", answer)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ips, err := LookupHost(ctx, dns.Resolver(), "replicas.test")
	if err != nil || len(ips) != len(addresses) {
		t.Fatal("large DNS answer rejected", len(ips), err)
	}
}
