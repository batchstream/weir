package testmongo

import (
	"context"
	"io"
	"net"
	"time"
)

// These are three finite identities, not a fault scheduler. Peer is a separate
// execution/transaction; it does not establish coalesced-batch cancellation.
type LuaLifecyclePhase uint8

const (
	LuaCancelSnapshot LuaLifecyclePhase = iota
	LuaHealthyPeer
	LuaRetireCommit
)

type luaLifecycle struct {
	phase                   LuaLifecyclePhase
	tail                    *readTail
	close                   chan struct{}
	natural, joined, closed bool
}

// passiveLifecycle consumes no reply release intent and never closes either
// socket. Only product-side EOF is classified as natural retirement. The tail
// remains owned until the test proves execution/SDK retirement and joins it.
func (r *luaRegistry) passiveLifecycle(m *luaMatch, client net.Conn, tail *readTail) bool {
	w := m.watch
	r.mu.Lock()
	l := w.lifecycle
	passive := l != nil && (l.phase == LuaCancelSnapshot && m.command == "find" || l.phase == LuaRetireCommit && m.command == "commitTransaction")
	if passive {
		if l.tail != nil {
			r.failure = "duplicate Lua lifecycle tail"
			r.mu.Unlock()
			return true
		}
		l.tail = tail
	}
	r.mu.Unlock()
	if !passive {
		return false
	}
	_ = client.SetReadDeadline(w.deadline)
	var b [1]byte
	n, err := client.Read(b[:])
	r.mu.Lock()
	if n != 0 || err != io.EOF {
		r.failure = "Lua lifecycle expected natural product EOF"
	} else {
		l.natural = true
	}
	r.mu.Unlock()
	timer := time.NewTimer(max(time.Until(w.deadline), 0))
	defer timer.Stop()
	select {
	case <-l.close:
	case <-timer.C:
		r.mu.Lock()
		w.snapshot.Expired = true
		r.failure = "Lua lifecycle tail watchdog expired"
		r.mu.Unlock()
	}
	return true
}
func (r *luaRegistry) lifecycleJoined(tail *readTail) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.watches {
		if w.lifecycle != nil && w.lifecycle.tail == tail {
			w.lifecycle.joined = true
		}
	}
}
func (w *LuaWatch) CloseLifecycleTail(ctx context.Context) error {
	r := w.registry
	r.mu.Lock()
	l := w.lifecycle
	if l == nil || l.phase == LuaHealthyPeer || !l.natural || l.closed || l.tail == nil {
		r.mu.Unlock()
		return errReplicaProxyWire
	}
	l.closed = true
	close(l.close)
	tail := l.tail
	r.mu.Unlock()
	select {
	case <-tail.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (w *LuaWatch) ValidateLifecycle(joined bool) error {
	r := w.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	l := w.lifecycle
	s := w.snapshot
	a := s.Attempt[0]
	if l == nil || r.failure != "" || s.Expired || s.Attempts != 1 || a.FindRequests != 1 || a.FindWrites != 1 || a.FindReplies != 1 || a.CommitDropped != 0 || a.AbortDropped != 0 {
		return errReplicaProxyWire
	}
	if l.phase == LuaCancelSnapshot {
		if a.FindForwarded != 0 || a.WriteRequests != 0 || a.CommitRequests != 0 || a.AbortRequests != 1 || a.AbortWrites != 1 || a.AbortReplies != 1 || a.AbortForwarded != 1 {
			return errReplicaProxyWire
		}
	} else {
		if !w.commitBaseLocked() || a.CommitRequests != 1 || a.CommitWrites != 1 || a.CommitReplies != 1 {
			return errReplicaProxyWire
		}
		if l.phase == LuaHealthyPeer {
			if a.CommitForwarded != 1 {
				return errReplicaProxyWire
			}
			return nil
		}
		if a.CommitForwarded != 0 {
			return errReplicaProxyWire
		}
	}
	if !l.natural || l.closed != joined || l.joined != joined {
		return errReplicaProxyWire
	}
	return nil
}
