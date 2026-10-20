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
	observed                chan struct{}
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
	close(l.observed)
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

// CheckLuaLifecycleDeadline rejects an expired absolute deadline even when a
// ready observation wins a select before context cancellation is published.
func CheckLuaLifecycleDeadline(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// WaitLifecycleEOF observes only the relay's completed real socket read. It
// never releases a gate or closes a socket to synchronize product retirement.
func (w *LuaWatch) WaitLifecycleEOF(ctx context.Context) error {
	if err := CheckLuaLifecycleDeadline(ctx); err != nil {
		return err
	}
	r := w.registry
	r.mu.Lock()
	l := w.lifecycle
	if l == nil || l.phase == LuaHealthyPeer || r.failure != "" {
		r.mu.Unlock()
		return errReplicaProxyWire
	}
	observed := l.observed
	r.mu.Unlock()
	select {
	case <-observed:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := CheckLuaLifecycleDeadline(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != "" || w.snapshot.Expired || !l.natural {
		return errReplicaProxyWire
	}
	return nil
}

func checkLuaLifecycleLedgers(ctx context.Context, watches []*LuaWatch, read func(context.Context, *LuaWatch, LuaLedger) error) error {
	for _, w := range watches {
		if err := CheckLuaLifecycleDeadline(ctx); err != nil {
			return err
		}
		expected := LuaLedger{N: 1, Candidate: 1}
		if w.lifecycle.phase == LuaCancelSnapshot {
			expected.N = 0
			expected.Candidate = 0
		}
		if err := read(ctx, w, expected); err != nil {
			return err
		}
		if err := CheckLuaLifecycleDeadline(ctx); err != nil {
			return err
		}
	}
	return CheckLuaLifecycleDeadline(ctx)
}

func (w *LuaWatch) CloseLifecycleTail(ctx context.Context) error {
	if err := CheckLuaLifecycleDeadline(ctx); err != nil {
		return err
	}
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
		return CheckLuaLifecycleDeadline(ctx)
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
