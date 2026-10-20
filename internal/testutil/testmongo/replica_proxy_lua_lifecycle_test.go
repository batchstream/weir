package testmongo

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestReplicaProxyLuaLifecycleNaturalRetirement(t *testing.T) {
	r, w := commitTestReady(t, false)
	w.lifecycle = &luaLifecycle{phase: LuaRetireCommit, close: make(chan struct{})}
	m, _ := commitTestReply(t, r)
	server, product := net.Pipe()
	defer server.Close()
	tail := &readTail{client: server, done: make(chan struct{})}
	entered := make(chan struct{})
	go func() {
		close(entered)
		r.passiveLifecycle(m, server, tail)
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
	for {
		r.mu.Lock()
		natural := w.lifecycle.natural
		r.mu.Unlock()
		if natural {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("natural EOF unobserved")
		}
		time.Sleep(time.Millisecond)
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
	w.lifecycle = &luaLifecycle{phase: LuaRetireCommit, close: make(chan struct{})}
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
	if err := w.ValidateLifecycle(false); err == nil {
		t.Fatal("unexpected post-hold request accepted as EOF")
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
