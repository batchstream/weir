package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoverySynchronizationPolicyConfiguration(t *testing.T) {
	prefix := "listeners:\n  application: 127.0.0.1:0\n"
	input := prefix + "discovery:\n  lease: 3m\n  sync_interval: 3s\n  sync_timeout: 7s\n"
	cfg, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	policy := cfg.Discovery.policy()
	if policy.Lease != 3*time.Minute || policy.SyncInterval != 3*time.Second || policy.SyncTimeout != 7*time.Second {
		t.Fatal("discovery synchronization policy did not reach directory assembly", policy)
	}
	for _, fragment := range []string{
		"sync_interval: 0s", "sync_interval: -1s", "sync_interval: 58s",
		"sync_timeout: 0s", "sync_timeout: -1s", "sync_timeout: 30s",
		"lease: 0s", "lease: 5s", "lease: 1500us",
	} {
		input := prefix + "discovery:\n  " + fragment + "\n"
		if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
			t.Fatal("invalid discovery synchronization policy accepted", fragment)
		}
	}
	input = prefix + "discovery:\n  sync_interval: 500us\n  sync_timeout: 1500us\n"
	cfg, err = DecodeBasic(strings.NewReader(input))
	if err != nil || cfg.Discovery.SyncInterval != Duration(500*time.Microsecond) || cfg.Discovery.SyncTimeout != Duration(1500*time.Microsecond) {
		t.Fatal("local synchronization timers unnecessarily rounded to wire lease units", err)
	}
}

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
	cfg.Basic.Discovery = DefaultConfig().Basic.Discovery
	cfg.Basic.Listeners.Application = ""
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	if err := cfg.Validate(); err == nil {
		t.Fatal("local Store without business listener accepted")
	}
}

func TestLargeDiscoverySourcesAndRoutingConfiguration(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	base := cfg.Routing.Stores[0]
	cfg.Routing.Stores = nil
	for i := range 32 {
		service := base
		service.Name = fmt.Sprintf("store-%03d", i)
		cfg.Routing.Stores = append(cfg.Routing.Stores, service)
		cfg.Basic.Discovery.Advertise = append(cfg.Basic.Discovery.Advertise, fmt.Sprintf("127.0.0.1:%d", 7400+i))
		cfg.Basic.Discovery.Seeds = append(cfg.Basic.Discovery.Seeds, fmt.Sprintf("127.0.0.1:%d", 8400+i))
	}
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	key := "WEIR_TEST_PEER_" + strings.Repeat("X", 512)
	cfg.Basic.Discovery.PeerAddressEnv = key
	t.Setenv(key, "peer.example:7448")
	base.Local.Backend.Authentication.Username = strings.Repeat("u", 8192)
	base.Local.Backend.Authentication.Password = strings.Repeat("p", 16384)
	basicFile, routingFile := writeCredentialTestDocuments(t, cfg)
	loaded, err := Load(basicFile, routingFile)
	if err != nil {
		t.Fatal("valid configuration retained an artificial scale threshold", err)
	}
	if len(loaded.Routing.Stores) != 32 || len(loaded.Basic.Discovery.Advertise) != 32 || len(loaded.Basic.Discovery.Seeds) != 32 || loaded.Basic.Discovery.PeerAddress != "peer.example:7448" || loaded.Basic.Discovery.PeerAddressEnv != "" {
		t.Fatal("large configuration was truncated")
	}
}
