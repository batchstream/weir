package protocol

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestPortableEndpointValidation(t *testing.T) {
	input := []string{"EXAMPLE.com.:07447", "[::ffff:127.0.0.1]:7447", "[0:0::1]:7447"}
	targets, err := CanonicalEndpoints(input)
	want := []string{"127.0.0.1:7447", "[::1]:7447", "example.com:7447"}
	if err != nil || !slices.Equal(targets, want) {
		t.Fatal(targets, err)
	}
	for _, bad := range []string{"0.0.0.0:1", "[::ffff:0.0.0.0]:1", "[::ffff:224.0.0.1]:1", "224.0.0.1:1", "[ff02::1]:1", "[::]:1", "[fe80::1%en0]:1", "bad:0", "dns:///bad:1", "bad:65536", "bad:1/path", "bad..name:1", "[bad]:1", "bad:-1", "_name:1", "中文:1", "peer :1"} {
		if _, err := CanonicalEndpoint(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	duplicate := []string{"example.com:1", "EXAMPLE.COM.:01"}
	if _, err := CanonicalEndpoints(duplicate); err == nil {
		t.Fatal("canonical duplicate accepted")
	}
}

func TestDiscoveryEndpointBounds(t *testing.T) {
	endpoints := make([]string, MaxDiscoveryEndpoints)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("replica-%03d.example:7447", i)
	}
	canonical, err := CanonicalEndpoints(endpoints)
	if err != nil || len(canonical) != MaxDiscoveryEndpoints {
		t.Fatal("maximum endpoint set rejected", len(canonical), err)
	}
	for _, invalid := range [][]string{nil, append(slices.Clone(endpoints), "extra.example:7447")} {
		if _, err := CanonicalEndpoints(invalid); err == nil {
			t.Fatal("endpoint count bound not enforced", len(invalid))
		}
	}
}

func TestDiscoveryStoreNames(t *testing.T) {
	for _, name := range []string{"data", "mongo-1", strings.Repeat("a", 63)} {
		if !ValidStoreName(name) {
			t.Errorf("valid Store name rejected: %q", name)
		}
	}
	for _, name := range []string{"", "Data", "data/path", "data?filter", "data:7447", "a-", "weir://data", strings.Repeat("a", 64)} {
		if ValidStoreName(name) {
			t.Errorf("invalid Store name accepted: %q", name)
		}
	}
}
