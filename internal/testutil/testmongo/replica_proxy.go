//go:build integration

package testmongo

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ReplicaProxy observes only application connections to an owned Replica. Real
// member configuration and inter-member connections never traverse these relays.
type ReplicaProxy struct {
	fixture    *Replica
	mapping    *replicaAddressMap
	members    [3]*Proxy
	operations *operationRegistry
	reads      *readRegistry
}

func StartReplicaProxy(t *testing.T, fixture *Replica) *ReplicaProxy {
	t.Helper()
	if fixture == nil {
		t.Fatal("owned replica required")
	}
	var listeners [3]net.Listener
	t.Cleanup(func() {
		for _, listener := range listeners {
			if listener != nil {
				_ = listener.Close()
			}
		}
	})
	var actual, frontend [3]string
	for i, member := range fixture.members {
		if member.process == nil || member.stopped {
			t.Fatal("all three owned members must be running")
		}
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		host, _, _ := net.SplitHostPort(member.address)
		_, port, _ := net.SplitHostPort(listener.Addr().String())
		actual[i], frontend[i] = member.address, net.JoinHostPort(host, port)
	}
	mapping, err := newReplicaAddressMap(actual, frontend)
	if err != nil {
		t.Fatal("invalid owned replica frontend mapping")
	}
	registry := &operationRegistry{}
	reads := &readRegistry{namespace: fixture.DB + ".records"}
	proxy := &ReplicaProxy{fixture: fixture, mapping: mapping, operations: registry, reads: reads}
	for i, listener := range listeners {
		opts := proxyOptions{uri: "mongodb://" + actual[i], listener: listener, discovery: mapping, operations: registry, reads: reads, memberIndex: i}
		if fixture.secure {
			host, _, _ := net.SplitHostPort(actual[i])
			opts.serverTLS = fixture.proxyTLS[i]
			opts.clientTLS = &tls.Config{RootCAs: fixture.roots, ServerName: host, MinVersion: tls.VersionTLS12}
		}
		proxy.members[i] = startProxy(t, opts)
	}
	t.Cleanup(proxy.Close)
	return proxy
}

func (p *ReplicaProxy) Addresses() []string {
	return append([]string(nil), p.mapping.frontend[:]...)
}

// SeedURI uses an index, never an arbitrary target or a direct fallback.
func (p *ReplicaProxy) SeedURI(seed int) string {
	return p.fixture.SeedURI([]string{p.mapping.frontend[seed]})
}

func (p *ReplicaProxy) Snapshots() [3]ReplicaProxySnapshot {
	var snapshots [3]ReplicaProxySnapshot
	for i, member := range p.members {
		snapshots[i] = member.replicaSnapshot(p.mapping.frontend[i])
	}
	return snapshots
}

func (p *ReplicaProxy) CheckCoverage(discovered []string, primary int) error {
	opts := replicaCoverageOptions{mapping: p.mapping, discovered: discovered, snapshots: p.Snapshots(), primary: primary, secure: p.fixture.secure}
	return checkReplicaCoverage(opts)
}

func (p *ReplicaProxy) Close() {
	for _, member := range p.members {
		if member != nil {
			member.Close()
		}
	}
}

// CheckOriginalAddresses independently reads every surviving member and the real
// replica configuration. It never asks the client-facing observer for topology.
func (f *Replica) CheckOriginalAddresses(ctx context.Context) error {
	for i, member := range f.members {
		if member.stopped {
			continue
		}
		opts := f.directOptions(i, f.secure)
		client, err := mongo.Connect(opts)
		if err != nil {
			return err
		}
		err = f.checkOriginalMember(ctx, client, i)
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		closeErr := client.Disconnect(cleanup)
		cancel()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (f *Replica) checkOriginalMember(ctx context.Context, client *mongo.Client, index int) error {
	command := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		Me      string   `bson:"me"`
		SetName string   `bson:"setName"`
		Hosts   []string `bson:"hosts"`
	}
	if err := client.Database("admin").RunCommand(ctx, command).Decode(&hello); err != nil {
		return err
	}
	if hello.Me != f.members[index].address || hello.SetName != f.SetName || !f.originalAddresses(hello.Hosts) {
		return fmt.Errorf("original owned member hello changed")
	}
	command = bson.D{{Key: "replSetGetConfig", Value: 1}}
	var configuration struct {
		Config struct {
			ID      string `bson:"_id"`
			Members []struct {
				ID   int `bson:"_id"`
				Host string
			}
		}
	}
	if err := client.Database("admin").RunCommand(ctx, command).Decode(&configuration); err != nil {
		return err
	}
	if configuration.Config.ID != f.SetName || len(configuration.Config.Members) != 3 {
		return fmt.Errorf("original owned replica configuration changed")
	}
	seen := make(map[int]bool, 3)
	for _, member := range configuration.Config.Members {
		if member.ID < 0 || member.ID >= 3 || seen[member.ID] || member.Host != f.members[member.ID].address {
			return fmt.Errorf("original owned replica member changed")
		}
		seen[member.ID] = true
	}
	return nil
}

func (f *Replica) originalAddresses(addresses []string) bool {
	if len(addresses) != 3 {
		return false
	}
	seen := make(map[string]bool, 3)
	for _, address := range addresses {
		found := false
		for _, member := range f.members {
			found = found || address == member.address
		}
		if !found || seen[address] {
			return false
		}
		seen[address] = true
	}
	return true
}

// WatchOperation persists across all three frontends until the relays are joined.
func (p *ReplicaProxy) WatchOperation(operation OwnedOperation) (*OperationWatch, error) {
	return p.operations.watch(operation)
}
func (p *ReplicaProxy) ArmReplyLoss(member int, watch *OperationWatch) error {
	return p.operations.arm(member, watch)
}

// CheckWireCoverage proves observable frontend closure and forwarding, not the
// external CLI driver's private topology or session pool. CheckCoverage retains
// the stricter healthy typed/Native/Lua command list for backend-package tests.
func (p *ReplicaProxy) CheckWireCoverage(primary int, command string) error {
	if primary < 0 || primary >= 3 {
		return errReplicaProxyWire
	}
	for i, snapshot := range p.Snapshots() {
		if snapshot.Rejections != 0 || snapshot.Frontend != p.mapping.frontend[i] || snapshot.Upstream != p.mapping.actual[i] {
			return errReplicaProxyWire
		}
		monitor, data := false, false
		for _, connection := range snapshot.Connections {
			if len(connection.Commands) == 0 {
				continue
			}
			if connection.FrontendTLS != p.fixture.secure || connection.UpstreamTLS != p.fixture.secure {
				return errReplicaProxyWire
			}
			onlyHello := true
			for name := range connection.Commands {
				onlyHello = onlyHello && replicaHello(name)
			}
			monitor = monitor || onlyHello && connection.DiscoveryReplies >= 2
			data = data || connection.Commands[command] > 0 && connection.Commands["find"] > 0
		}
		if !monitor || i == primary && !data {
			return errReplicaProxyWire
		}
	}
	return nil
}

// WatchRead persists across every frontend for one bounded fixture record.
func (p *ReplicaProxy) WatchRead(id string) (*ReadWatch, error) { return p.reads.watch(id) }
func (p *ReplicaProxy) ArmReadHold(member int, watch *ReadWatch) error {
	return p.reads.arm(member, watch)
}
