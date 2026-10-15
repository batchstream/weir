package directory

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestDirectoryPolicyValidationAndPropagation(t *testing.T) {
	defaults := DefaultPolicy()
	for _, test := range []struct {
		name   string
		policy Policy
	}{
		{name: "negative interval", policy: Policy{Lease: defaults.Lease, SyncInterval: -time.Millisecond, SyncTimeout: defaults.SyncTimeout}},
		{name: "expired lease", policy: Policy{Lease: 5 * time.Second, SyncInterval: time.Second, SyncTimeout: 2 * time.Second}},
		{name: "negative timeout", policy: Policy{Lease: defaults.Lease, SyncInterval: defaults.SyncInterval, SyncTimeout: -time.Millisecond}},
		{name: "fractional lease", policy: Policy{Lease: defaults.Lease + time.Nanosecond, SyncInterval: defaults.SyncInterval, SyncTimeout: defaults.SyncTimeout}},
		{name: "overflowing relation", policy: Policy{Lease: time.Duration((1<<63-1)/int64(time.Millisecond)) * time.Millisecond, SyncInterval: time.Second, SyncTimeout: time.Duration((1<<63-1)/int64(time.Millisecond)) * time.Millisecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := Config{Group: "policy", Policy: test.policy}
			if _, err := New(config); err == nil {
				t.Fatal("invalid directory policy accepted")
			}
		})
	}
	precise := Policy{Lease: time.Millisecond, SyncInterval: time.Nanosecond, SyncTimeout: time.Nanosecond}
	if err := precise.Validate(); err != nil {
		t.Fatal("local duration precision was constrained by lease wire encoding", err)
	}
	policy := Policy{Lease: 100 * time.Millisecond, SyncInterval: 20 * time.Millisecond, SyncTimeout: 30 * time.Millisecond}
	config := Config{Group: "policy", Policy: policy}
	directory := newTestDirectory(t, config)
	if directory.policy != policy {
		t.Fatal("directory policy was not retained", directory.policy)
	}
	directory.Start(t.Context())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		directory.mu.Lock()
		revision := directory.records[directory.self].announcement.Revision
		directory.mu.Unlock()
		if revision >= 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("configured synchronization cadence did not renew announcements")
}

func TestDirectoryConfiguredSyncTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stalled, stop := context.WithCancel(t.Context())
	finished := make(chan struct{})
	defer func() {
		stop()
		_ = listener.Close()
		<-finished
	}()
	go func() {
		defer close(finished)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		<-stalled.Done()
	}()
	policy := Policy{Lease: time.Minute, SyncInterval: time.Second, SyncTimeout: 30 * time.Millisecond}
	config := Config{Group: "timeout", Policy: policy}
	directory := newTestDirectory(t, config)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err = directory.syncPeer(ctx, listener.Addr().String())
	if err == nil || ctx.Err() != nil {
		t.Fatal("configured sync timeout did not end the exchange before the caller", err, ctx.Err())
	}
}
