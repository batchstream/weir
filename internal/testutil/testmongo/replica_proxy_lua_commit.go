package testmongo

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Commit observation shares the strict singleton Lua framing, identity,
// replacement witness and actual relay accounting; only the terminal gate differs.
type luaCommitState struct {
	gate  luaGate
	all   bool
	limit int
}

func (r *luaRegistry) watchCommit(op LuaOperation, all bool, attempts int) (*LuaWatch, error) {
	if attempts < 1 || attempts > replicaLuaAttempts {
		return nil, errReplicaProxyWire
	}
	w, err := r.watch(op)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	w.commit = &luaCommitState{gate: newLuaGate(), all: all, limit: 2 * attempts}
	r.mu.Unlock()
	return w, nil
}
func (w *LuaWatch) CommitHeld() <-chan struct{} { return w.commit.gate.held }

func (w *LuaWatch) beginCommitLocked(doc bson.Raw) bool {
	s := w.snapshot.Attempt[0]
	concern, ok := doc.Lookup("writeConcern").DocumentOK()
	majority, majorityOK := concern.Lookup("w").StringValueOK()
	// Driver retries may strengthen the timeout; require the real majority
	// acknowledgement semantics, not byte-identical writeConcern documents.
	valid := !w.snapshot.Completed && s.CommitRequests <= w.commit.limit && s.WriteReplies == 1 && s.WriteForwarded == 1 && !s.Conflict && s.CommitRequests == s.CommitReplies+1 && s.CommitReplies == s.CommitDropped+s.CommitForwarded && s.CommitForwarded == 0 && ok && uniqueOperationDocument(concern) == nil && majorityOK && majority == "majority"
	if !valid {
		w.registry.failure = "unexpected original Lua commit confirmation"
	}
	return valid
}

func (w *LuaWatch) ValidateCommitHeld() error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	s := w.snapshot.Attempt[0]
	if w.commit == nil || !w.commitBaseLocked() || s.CommitRequests != 1 || s.CommitWrites != 1 || s.CommitReplies != 1 || s.CommitDropped != 0 || s.CommitForwarded != 0 || w.commit.gate.requested {
		return errReplicaProxyWire
	}
	return nil
}
func (w *LuaWatch) DropFirstCommitReply() error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	s := w.snapshot.Attempt[0]
	if w.commit == nil || !w.commitBaseLocked() || s.CommitRequests != 1 || s.CommitReplies != 1 || s.CommitDropped != 0 || s.CommitForwarded != 0 || w.commit.gate.requested {
		return errReplicaProxyWire
	}
	w.commit.gate.requested = true
	close(w.commit.gate.release)
	return nil
}
func (w *LuaWatch) holdCommit(done <-chan struct{}, timeout time.Duration) (bool, bool) {
	r := w.registry
	r.mu.Lock()
	first := w.snapshot.Attempt[0].CommitReplies == 1
	timeout = min(timeout, time.Until(w.deadline))
	r.mu.Unlock()
	timer := time.NewTimer(max(timeout, 0))
	defer timer.Stop()
	if first {
		select {
		case <-w.commit.gate.release:
		case <-done:
			r.fail()
			return false, false
		case <-timer.C:
			r.mu.Lock()
			w.snapshot.Expired = true
			r.failure = "Lua commit watchdog expired"
			r.mu.Unlock()
			return false, false
		}
	}
	select {
	case <-done:
		r.fail()
		return false, false
	default:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Now().After(w.deadline) {
		w.snapshot.Expired = true
		r.failure = "Lua commit lifetime expired"
		return false, false
	}
	return true, first || w.commit.all
}
func (w *LuaWatch) commitBaseLocked() bool {
	s := w.snapshot
	a := s.Attempt[0]
	return w.registry.failure == "" && !s.Expired && s.Attempts == 1 && a.FindRequests == 1 && a.FindWrites == 1 && a.FindReplies == 1 && a.FindForwarded == 1 && a.WriteRequests == 1 && a.WriteItems == 1 && a.WriteReplies == 1 && a.WriteForwarded == 1 && !a.Conflict && a.AbortRequests == 0 && a.AbortWrites == 0 && a.AbortReplies == 0 && a.AbortForwarded == 0 && a.AbortDropped == 0 && a.CandidateN == 1 && a.CandidateExternal == 0 && a.CandidateCount == 1 && a.GlobalFresh == 1 && a.LibraryFresh == 1 && a.InputAttempt == 1 && a.Observed != ""
}
func (w *LuaWatch) validateCommitFinishedLocked() error {
	a := w.snapshot.Attempt[0]
	if w.commit == nil || !w.commitBaseLocked() || !w.commit.gate.requested || a.CommitRequests < 2 || a.CommitRequests > w.commit.limit || a.CommitRequests != a.CommitWrites || a.CommitWrites != a.CommitReplies || a.CommitReplies != a.CommitDropped+a.CommitForwarded {
		return errReplicaProxyWire
	}
	if w.commit.all {
		if a.CommitDropped != a.CommitReplies || a.CommitForwarded != 0 {
			return errReplicaProxyWire
		}
	} else if a.CommitDropped != 1 || a.CommitForwarded != 1 {
		return errReplicaProxyWire
	}
	return nil
}
func (w *LuaWatch) ValidateCommitFinished() error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	return w.validateCommitFinishedLocked()
}

// CompleteCommit follows the real receipt and joined execution/SDK plus idle
// ownership, never merely a successful wire reply or gate release intention.
func (w *LuaWatch) CompleteCommit() error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	if w.snapshot.Completed {
		return errReplicaProxyWire
	}
	if err := w.validateCommitFinishedLocked(); err != nil {
		return err
	}
	w.snapshot.Completed = true
	return nil
}
