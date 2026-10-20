package testmongo

import (
	"context"
	"net"
	"testing"
	"time"
)

type delayedLuaEOFConn struct {
	net.Conn
	readCompleted, publish chan struct{}
}

func (c *delayedLuaEOFConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	close(c.readCompleted)
	<-c.publish
	return n, err
}

func TestReplicaProxyLuaLifecycleNaturalRetirement(t *testing.T) {
	r, w := commitTestReady(t, false)
	w.lifecycle = &luaLifecycle{phase: LuaRetireCommit, close: make(chan struct{}), observed: make(chan struct{})}
	m, _ := commitTestReply(t, r)
	server, product := net.Pipe()
	defer server.Close()
	tail := &readTail{client: server, done: make(chan struct{})}
	readCompleted := make(chan struct{})
	publish := make(chan struct{})
	observer := &delayedLuaEOFConn{Conn: server, readCompleted: readCompleted, publish: publish}
	entered := make(chan struct{})
	go func() {
		close(entered)
		r.passiveLifecycle(m, observer, tail)
		r.lifecycleJoined(tail)
		close(tail.done)
	}()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := w.CloseLifecycleTail(ctx); err == nil {
		t.Fatal("observer allowed manufactured product retirement")
	}
	if err := product.Close(); err != nil {
		t.Fatal(err)
	}
	// Product completion precedes observer EOF publication deterministically.
	<-readCompleted
	expired, stopExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stopExpired()
	if err := w.WaitLifecycleEOF(expired); err != context.DeadlineExceeded {
		t.Fatal("unpublished EOF ignored expired original deadline", err)
	}
	if err := w.ValidateLifecycle(false); err == nil {
		t.Fatal("product completion manufactured observer EOF publication")
	}
	close(publish)
	if err := w.WaitLifecycleEOF(ctx); err != nil {
		t.Fatal("natural EOF barrier", err)
	}
	if err := w.WaitLifecycleEOF(expired); err != context.DeadlineExceeded {
		t.Fatal("ready EOF won over expired original deadline", err)
	}
	if err := w.ValidateLifecycle(false); err != nil {
		t.Fatal(err, w.Snapshot())
	}
	if s := w.Snapshot(); s.Attempt[0].CommitDropped != 0 || s.Attempt[0].CommitForwarded != 0 {
		t.Fatal("natural EOF invented delivery/drop", s)
	}
	if err := w.CloseLifecycleTail(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.ValidateLifecycle(true); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	w.snapshot.Attempt[0].CommitRequests++
	r.mu.Unlock()
	if err := w.ValidateLifecycle(true); err == nil {
		t.Fatal("retirement accepted original commit replay")
	}
}
func TestReplicaProxyLuaLifecycleUnexpectedDataFailsClosed(t *testing.T) {
	r, w := commitTestReady(t, false)
	w.lifecycle = &luaLifecycle{phase: LuaRetireCommit, close: make(chan struct{}), observed: make(chan struct{})}
	m, _ := commitTestReply(t, r)
	server, product := net.Pipe()
	defer server.Close()
	defer product.Close()
	tail := &readTail{client: server, done: make(chan struct{})}
	go func() { r.passiveLifecycle(m, server, tail); close(tail.done) }()
	if _, err := product.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	close(w.lifecycle.close)
	r.mu.Unlock()
	<-tail.done
	if err := w.WaitLifecycleEOF(t.Context()); err == nil {
		t.Fatal("observer failure did not surface at EOF barrier")
	}
	if err := w.ValidateLifecycle(false); err == nil {
		t.Fatal("unexpected post-hold request accepted as EOF")
	}
}
func TestReplicaProxyLuaLifecycleOriginalLedgerContext(t *testing.T) {
	canceled := &LuaWatch{lifecycle: &luaLifecycle{phase: LuaCancelSnapshot}}
	peer := &LuaWatch{lifecycle: &luaLifecycle{phase: LuaHealthyPeer}}
	watches := []*LuaWatch{canceled, peer}
	for _, expire := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		calls := 0
		read := func(original context.Context, w *LuaWatch, expected LuaLedger) error {
			if original != ctx {
				t.Fatal("ledger helper replaced original cancellation context")
			}
			if w == canceled && expected != (LuaLedger{}) || w == peer && (expected.N != 1 || expected.External != 0 || expected.Candidate != 1) {
				t.Fatal("wrong independent lifecycle ledger", expected)
			}
			calls++
			if expire {
				cancel()
			}
			return nil
		}
		err := checkLuaLifecycleLedgers(ctx, watches, read)
		cancel()
		if expire && (err != context.Canceled || calls != 1) || !expire && (err != nil || calls != 2) {
			t.Fatal("original ledger gate expiry", expire, calls, err)
		}
		if canceled.snapshot.Completed || peer.snapshot.Completed {
			t.Fatal("ledger proof marked completion")
		}
	}
}

func TestReplicaProxyLuaLifecycleWatchLimit(t *testing.T) {
	r := &luaRegistry{}
	for i := 0; i < replicaLuaLimit; i++ {
		op := LuaOperation{Namespace: "owned.records", ID: string(rune('a' + i))}
		if _, err := r.watch(op); err != nil {
			t.Fatal(err)
		}
	}
	op := LuaOperation{Namespace: "owned.records", ID: "overflow"}
	if _, err := r.watch(op); err == nil {
		t.Fatal("unbounded lifecycle watches")
	}
}
