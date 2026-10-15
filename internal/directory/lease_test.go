package directory

import (
	"testing"
	"time"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSnapshotSamplesTimeAfterConcurrentRenewal(t *testing.T) {
	config := Config{Group: "owner"}
	directory := newTestDirectory(t, config)
	directory.mu.Lock()
	started := make(chan struct{})
	result := make(chan []*peerpb.NodeAnnouncement, 1)
	go func() {
		close(started)
		result <- directory.snapshot()
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	owned := directory.records[directory.self]
	owned.announcement.Revision++
	owned.expires = time.Now().Add(directory.policy.Lease)
	directory.records[directory.self] = owned
	directory.mu.Unlock()
	select {
	case nodes := <-result:
		if len(nodes) != 1 {
			t.Fatal("renewed owner missing from snapshot", nodes)
		}
		if err := validateAnnouncement(nodes[0]); err != nil {
			t.Fatal("concurrent renewal produced an invalid remaining lease", err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot did not complete after directory unlocked")
	}
}

func TestSnapshotSkipsExpiredRecords(t *testing.T) {
	config := Config{Group: "observer"}
	directory := newTestDirectory(t, config)
	start := time.Now().Add(-time.Minute)
	ad := announcement(1, "owner", "127.0.0.1:7447")
	if err := directory.merge([]*peerpb.NodeAnnouncement{ad}, start, start); err != nil {
		t.Fatal(err)
	}
	for _, node := range directory.snapshot() {
		if node.IncarnationId == ad.IncarnationId {
			t.Fatal("expired record relayed")
		}
	}
}

func TestOriginLeaseAndExchangeAnchorAreIndependentOfReceiverPolicy(t *testing.T) {
	policy := Policy{Lease: 3 * time.Minute, SyncInterval: 3 * time.Second, SyncTimeout: 7 * time.Second}
	ownerConfig := Config{Group: "owner", Stores: []string{"data"}, Targets: []string{"127.0.0.1:7447"}, Policy: policy}
	owner := newTestDirectory(t, ownerConfig)
	observerConfig := Config{Group: "observer"}
	observer := newTestDirectory(t, observerConfig)
	start := time.Now()
	owner.renew(start)
	nodes := owner.snapshot()
	if len(nodes) != 1 || (nodes[0].LeaseRemainingMs <= 60000 || nodes[0].LeaseRemainingMs > 180000) || nodes[0].OriginLeaseMs != 180000 {
		t.Fatal("long owner lease was capped by historical constants", nodes)
	}
	nodes[0].LeaseRemainingMs = 170000
	anchor := start.Add(5 * time.Second)
	received := start.Add(20 * time.Second)
	if err := observer.merge(nodes, anchor, received); err != nil {
		t.Fatal(err)
	}
	installed := observer.records[owner.self]
	if !installed.expires.Equal(start.Add(175*time.Second)) || !installed.forget.Equal(start.Add(200*time.Second)) {
		t.Fatal("receiver renewed transit time or used its own lease for GC", installed.expires, installed.forget)
	}
	if err := observer.merge(nodes, start.Add(30*time.Second), start.Add(40*time.Second)); err != nil {
		t.Fatal(err)
	}
	unchanged := observer.records[owner.self]
	if !unchanged.expires.Equal(installed.expires) || !unchanged.forget.Equal(installed.forget) {
		t.Fatal("same revision refreshed expiry or watermark retention")
	}
	if err := observer.merge(nil, start.Add(190*time.Second), start.Add(190*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, exists := observer.records[owner.self]; !exists {
		t.Fatal("receiver's shorter local lease collected the origin watermark")
	}
	if err := observer.merge(nodes, anchor, start.Add(201*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, exists := observer.records[owner.self]; exists {
		t.Fatal("delayed response resurrected an origin after watermark collection")
	}
}

func TestOriginLeaseIsImmutableAndValidationIsAtomic(t *testing.T) {
	config := Config{Group: "observer"}
	directory := newTestDirectory(t, config)
	start := time.Now()
	first := announcement(1, "owner", "127.0.0.1:7447")
	if err := directory.merge([]*peerpb.NodeAnnouncement{first}, start, start); err != nil {
		t.Fatal(err)
	}
	first.Revision++
	first.OriginLeaseMs++
	second := announcement(2, "owner", "127.0.0.1:7448")
	err := directory.merge([]*peerpb.NodeAnnouncement{second, first}, start, start)
	if status.Code(err) != codes.InvalidArgument || len(directory.records) != 2 || directory.records[first.IncarnationId].announcement.Revision != 1 {
		t.Fatal("changed incarnation lease partially mutated membership", err, directory.records)
	}
}

func TestWithdrawalIsTerminalAndOldResponsesCannotReviveAfterGC(t *testing.T) {
	config := Config{Group: "observer"}
	directory := newTestDirectory(t, config)
	start := time.Now()
	withdrawal := announcement(1, "owner", "127.0.0.1:7447")
	withdrawal.Revision = 2
	withdrawal.Withdrawn = true
	withdrawal.StoreNames = nil
	withdrawal.StoreEndpoints = nil
	if err := directory.merge([]*peerpb.NodeAnnouncement{withdrawal}, start, start); err != nil {
		t.Fatal(err)
	}
	retained := directory.records[withdrawal.IncarnationId]
	revival := announcement(1, "owner", "127.0.0.1:7447")
	revival.Revision = 3
	if err := directory.merge([]*peerpb.NodeAnnouncement{revival}, start, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	after := directory.records[withdrawal.IncarnationId]
	if !after.announcement.Withdrawn || after.announcement.Revision != 2 || !after.forget.Equal(retained.forget) {
		t.Fatal("withdrawn incarnation revived or extended its watermark", after)
	}
	if err := directory.merge([]*peerpb.NodeAnnouncement{revival}, start, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, exists := directory.records[withdrawal.IncarnationId]; exists {
		t.Fatal("expired delayed response revived a collected withdrawal")
	}
}
