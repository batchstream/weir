package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPeerAddressEnvironmentSource(t *testing.T) {
	input := "listeners:\n  application: 127.0.0.1:0\n  peer: 0.0.0.0:7448\ndiscovery:\n  peer_address_env: WEIR_TEST_PUBLIC_PEER\n"
	basic, err := DecodeBasic(strings.NewReader(input))
	if err != nil || basic.Discovery.PeerAddressEnv != "WEIR_TEST_PUBLIC_PEER" {
		t.Fatal("pure decoding accessed the environment or rejected the source", err)
	}
	filename := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(filename, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WEIR_TEST_PUBLIC_PEER", "peer.example:7448")
	cfg, err := Load(filename, "")
	if err != nil || cfg.Basic.Discovery.PeerAddress != "peer.example:7448" || cfg.Basic.Discovery.PeerAddressEnv != "" {
		t.Fatal("explicit public-address source was not resolved", err)
	}
	unresolved := Config{Basic: basic}
	if node, err := Open(context.Background(), unresolved); node != nil || err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatal("programmatic Open accepted an unresolved public address", node, err)
	}
	for _, value := range []string{"", "dns:///private-sentinel:7448", "0.0.0.0:7448"} {
		t.Setenv("WEIR_TEST_PUBLIC_PEER", value)
		_, err := Load(filename, "")
		if err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("invalid public-address source accepted or exposed", err)
		}
	}
	for _, source := range []string{"9INVALID", "INVALID-NAME", "$(private-sentinel)"} {
		invalid := strings.Replace(input, "WEIR_TEST_PUBLIC_PEER", source, 1)
		if _, err := DecodeBasic(strings.NewReader(invalid)); err == nil {
			t.Fatal("invalid public-address environment name accepted")
		}
	}
	both := input + "  peer_address: peer.example:7448\n"
	if _, err := DecodeBasic(strings.NewReader(both)); err == nil {
		t.Fatal("ambiguous peer address sources accepted")
	}
}

func TestDiscoveryRequiresReachableAdvertisements(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	cfg.Basic.Listeners.Application = "0.0.0.0:7447"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unspecified business address would escape through Resolve")
	}
	cfg.Basic.Discovery.Advertise = []string{"stores.example:7447"}
	if err := cfg.Validate(); err != nil {
		t.Fatal("generic advertised DNS address rejected", err)
	}
	cfg.Basic.Listeners.Peer = "0.0.0.0:7448"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unspecified peer address accepted")
	}
	cfg.Basic.Discovery.PeerAddress = "192.0.2.1:7448"
	cfg.Basic.Discovery.Seeds = []string{"bootstrap.example:7448"}
	if err := cfg.Validate(); err != nil {
		t.Fatal("generic IP and DNS discovery rejected", err)
	}
	cfg.Basic.Listeners.Peer = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("peer membership without peer listener accepted")
	}
	cfg.Basic.Discovery = DiscoveryConfig{}
	cfg.Basic.Listeners.Application = ""
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	if err := cfg.Validate(); err == nil {
		t.Fatal("local Store without business listener accepted")
	}
}
