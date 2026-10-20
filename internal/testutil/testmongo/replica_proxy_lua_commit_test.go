package testmongo

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestReplicaProxyLuaTopologyUsabilityFirstTimestamp(t *testing.T) {
	first := time.Now().Add(-6 * time.Second)
	observed := make(chan int, 1)
	calls := 0
	observe := func(ctx context.Context, exclude int) (int, time.Time, error) {
		calls++
		observed <- exclude
		return 2, first, nil
	}
	await := ObserveLuaTopologyUsability(t, observe, 1)
	// The independent observation completes before the old-work phase joins.
	select {
	case exclude := <-observed:
		if exclude != 1 {
			t.Fatal("old primary exclusion changed", exclude)
		}
	case <-time.After(time.Second):
		t.Fatal("independent observer did not start")
	}
	primary, usable, err := await()
	if err != nil || primary != 2 || usable != first || calls != 1 {
		t.Fatal("first observation reset or repeated", primary, usable, calls, err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
	defer cancel()
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatal("old work granted hidden new-work head-start")
	}
}

func TestReplicaProxyLuaTopologyUsabilityCleanupJoins(t *testing.T) {
	entered := make(chan struct{})
	returned := make(chan struct{})
	t.Run("unconsumed_observation", func(t *testing.T) {
		observe := func(ctx context.Context, exclude int) (int, time.Time, error) {
			defer close(returned)
			close(entered)
			<-ctx.Done()
			var zero time.Time
			return -1, zero, ctx.Err()
		}
		_ = ObserveLuaTopologyUsability(t, observe, 1)
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("observer did not start")
		}
		// No await: the same cleanup used on a fatal path must cancel/join it.
	})
	select {
	case <-returned:
	default:
		t.Fatal("cleanup returned without observer join")
	}
}

func commitTestReady(t *testing.T, all bool) (*luaRegistry, *LuaWatch) {
	t.Helper()
	r := &luaRegistry{}
	op := LuaOperation{Namespace: "owned.records", ID: "lua"}
	w, err := r.watchCommit(op, all, replicaLuaAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.arm(1, w); err != nil {
		t.Fatal(err)
	}
	luaTestSnapshot(t, r, w, false)
	command := luaTestWrite(10, false)
	m := luaTestBegin(t, r, command)
	reply := ownedReply()
	response := luaTestReceive(t, r, m, reply)
	luaTestForward(t, r, m, response)
	return r, w
}
func commitTestCommand(txn int64) bson.D {
	command := luaTestControl("commitTransaction", txn)
	concern := bson.D{{Key: "w", Value: "majority"}}
	field := bson.E{Key: "writeConcern", Value: concern}
	command = append(command, field)
	return command
}
func commitTestReply(t *testing.T, r *luaRegistry) (*luaMatch, []byte) {
	t.Helper()
	command := commitTestCommand(10)
	m := luaTestBegin(t, r, command)
	reply := bson.D{{Key: "ok", Value: 1}}
	response := luaTestReceive(t, r, m, reply)
	return m, response
}
func commitTestFirstDrop(t *testing.T, r *luaRegistry, w *LuaWatch) {
	t.Helper()
	m, _ := commitTestReply(t, r)
	if err := w.ValidateCommitHeld(); err != nil {
		t.Fatal(w.Snapshot())
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := WaitLuaPhase(ctx, w.CommitHeld()); err != nil {
		t.Fatal(err)
	}
	if err := w.CompleteCommit(); err == nil {
		t.Fatal("wire reply completed before receipt/join")
	}
	if err := w.DropFirstCommitReply(); err != nil {
		t.Fatal(err)
	}
	if w.Snapshot().Attempt[0].CommitDropped != 0 {
		t.Fatal("release intention counted as actual socket drop")
	}
	if err := w.ValidateCommitFinished(); err == nil {
		t.Fatal("release intention satisfied completion")
	}
	ok, drop := r.hold(m, make(chan struct{}), time.Second)
	if !ok || !drop {
		t.Fatal("first commit drop gate")
	}
	// Offline accounting control only. Live relay closes its actual socket under
	// this same registry lock before recording the drop.
	r.finish(m, true)
}
func TestReplicaProxyLuaCommitOnceAndAll(t *testing.T) {
	for _, all := range []bool{false, true} {
		name := "once"
		if all {
			name = "all"
		}
		t.Run(name, func(t *testing.T) {
			r, w := commitTestReady(t, all)
			commitTestFirstDrop(t, r, w)
			count := 2
			if all {
				count = 2 * replicaLuaAttempts
			}
			for i := 1; i < count; i++ {
				// Genuine driver retry writeConcern may add/strengthen its timeout.
				command := commitTestCommand(10)
				concern := bson.D{{Key: "w", Value: "majority"}, {Key: "wtimeout", Value: int64(10000 + i)}}
				command[len(command)-1].Value = concern
				m := luaTestBegin(t, r, command)
				reply := bson.D{{Key: "ok", Value: 1}}
				response := luaTestReceive(t, r, m, reply)
				ok, drop := r.hold(m, make(chan struct{}), time.Second)
				if !ok || drop != all {
					t.Fatal("confirmation gate")
				}
				if drop {
					r.finish(m, true)
				} else {
					if err := w.ValidateCommitFinished(); err == nil {
						t.Fatal("receipt alone became full forward")
					}
					luaTestForward(t, r, m, response)
				}
			}
			if err := w.ValidateCommitFinished(); err != nil {
				t.Fatal(w.Snapshot())
			}
			if w.Snapshot().Completed {
				t.Fatal("wire success became explicit completion")
			}
			if err := w.CompleteCommit(); err != nil {
				t.Fatal(err)
			}
			if err := w.CompleteCommit(); err == nil {
				t.Fatal("duplicate explicit completion")
			}
			// Completed sessions may be reused for unrelated IDs/new transactions.
			command := commitTestCommand(11)
			opts := luaBeginOptions{message: ownedMessage(t, command), member: 2}
			if m, err := r.begin(opts); err != nil || m != nil {
				t.Fatal("unrelated completed pooled reuse", err)
			}
			command = commitTestCommand(10)
			opts.message = ownedMessage(t, command)
			opts.member = 1
			if _, err := r.begin(opts); err == nil {
				t.Fatal("old exact commit identity escaped persistent watch")
			}
		})
	}
}
func TestReplicaProxyLuaCommitConfirmationNewPrimary(t *testing.T) {
	for _, name := range []string{"confirmation", "new_txn", "write", "abort"} {
		t.Run(name, func(t *testing.T) {
			r, w := commitTestReady(t, false)
			commitTestFirstDrop(t, r, w)
			command := commitTestCommand(10)
			switch name {
			case "new_txn":
				command = commitTestCommand(11)
			case "write":
				command = luaTestWrite(10, false)
			case "abort":
				command = luaTestControl("abortTransaction", 10)
			}
			opts := luaBeginOptions{message: ownedMessage(t, command), member: 2}
			m, err := r.begin(opts)
			if name != "confirmation" {
				if err == nil {
					t.Fatal("topology allowed other than original commit confirmation", name)
				}
				return
			}
			if err != nil || m == nil {
				t.Fatal("original confirmation on new primary", err)
			}
			if !r.forwarded(m, len(opts.message), len(opts.message)) {
				t.Fatal("actual upstream confirmation write")
			}
			reply := bson.D{{Key: "ok", Value: 1}}
			response := luaTestReceive(t, r, m, reply)
			luaTestForward(t, r, m, response)
			if err := w.CompleteCommit(); err != nil {
				t.Fatal(err, w.Snapshot())
			}
		})
	}
}

func TestReplicaProxyLuaCommitRejectsCommands(t *testing.T) {
	for _, name := range []string{"new_txn", "malformed_txn", "abort", "second_find", "second_write", "weak_concern", "wrong_member", "unsupported_selector", "unsettled_write"} {
		t.Run(name, func(t *testing.T) {
			r, w := commitTestReady(t, false)
			command := commitTestCommand(10)
			member := 1
			switch name {
			case "new_txn":
				command = commitTestCommand(11)
			case "malformed_txn":
				for i := range command {
					if command[i].Key == "txnNumber" {
						command[i].Value = "bad"
					}
				}
			case "abort":
				command = luaTestControl("abortTransaction", 10)
			case "second_find":
				command = luaTestFind(11)
			case "second_write":
				command = luaTestWrite(10, false)
			case "weak_concern":
				concern := bson.D{{Key: "w", Value: 1}}
				command[len(command)-1].Value = concern
			case "wrong_member":
				member = 2
			case "unsupported_selector":
				command = luaTestFind(11)
				filter := bson.D{{Key: "_id", Value: bson.Regex{Pattern: ".*"}}}
				command[1].Value = filter
			case "unsettled_write":
				w.snapshot.Attempt[0].WriteForwarded = 0
			}
			opts := luaBeginOptions{message: ownedMessage(t, command), member: member}
			if _, err := r.begin(opts); err == nil || w.Snapshot().Error == "" {
				t.Fatal("invalid commit command escaped", name, w.Snapshot())
			}
		})
	}
}
func TestReplicaProxyLuaCommitRejectsReplyAndWrite(t *testing.T) {
	for _, name := range []string{"WCE", "correlation", "framing", "duplicate_ok", "not_success", "partial_upstream", "reply_before_write", "duplicate_reply"} {
		t.Run(name, func(t *testing.T) {
			r, w := commitTestReady(t, false)
			command := commitTestCommand(10)
			opts := luaBeginOptions{message: ownedMessage(t, command), member: 1}
			m, err := r.begin(opts)
			if err != nil {
				t.Fatal(err)
			}
			if name == "partial_upstream" {
				if r.forwarded(m, len(opts.message)-1, len(opts.message)) {
					t.Fatal("partial upstream counted")
				}
				return
			}
			if name != "reply_before_write" && !r.forwarded(m, len(opts.message), len(opts.message)) {
				t.Fatal("upstream write")
			}
			reply := bson.D{{Key: "ok", Value: 1}}
			switch name {
			case "WCE":
				detail := bson.D{{Key: "code", Value: 64}}
				field := bson.E{Key: "writeConcernError", Value: detail}
				reply = append(reply, field)
			case "duplicate_ok":
				field := bson.E{Key: "ok", Value: 1}
				reply = append(reply, field)
			case "not_success":
				reply[0].Value = 0
			}
			response := readTestResponse(t, reply)
			if name == "correlation" {
				binary.LittleEndian.PutUint32(response[8:12], 99)
			}
			if name == "framing" {
				response = response[:len(response)-1]
			}
			if name == "duplicate_reply" && !r.received(m, response) {
				t.Fatal("first reply")
			}
			if r.received(m, response) || w.Snapshot().Error == "" {
				t.Fatal("invalid reply accepted", name)
			}
		})
	}
}
func TestReplicaProxyLuaCommitGateFailsClosed(t *testing.T) {
	for _, name := range []string{"watchdog", "cleanup", "release_then_cleanup", "expired_release"} {
		t.Run(name, func(t *testing.T) {
			r, w := commitTestReady(t, false)
			m, _ := commitTestReply(t, r)
			done := make(chan struct{})
			if name == "release_then_cleanup" || name == "expired_release" {
				if err := w.DropFirstCommitReply(); err != nil {
					t.Fatal(err)
				}
			}
			if name == "cleanup" || name == "release_then_cleanup" {
				close(done)
			}
			if name == "expired_release" {
				w.deadline = time.Now().Add(-time.Second)
			}
			ok, drop := r.hold(m, done, time.Millisecond)
			if ok || drop || w.Snapshot().Error == "" || w.Snapshot().Attempt[0].CommitDropped != 0 {
				t.Fatal("interruption became successful drop", w.Snapshot())
			}
			if err := w.ValidateCommitFinished(); err == nil {
				t.Fatal("interruption completed")
			}
		})
	}
}
func TestReplicaProxyLuaCommitCapAndExactAccounting(t *testing.T) {
	r, w := commitTestReady(t, true)
	commitTestFirstDrop(t, r, w)
	// Exhaustion is bounded, not an assertion that the driver always emits two
	// physical requests per adapter call. Both valid physical counts are accepted.
	m, _ := commitTestReply(t, r)
	r.finish(m, true)
	if err := w.ValidateCommitFinished(); err != nil {
		t.Fatal("two physical requests is valid", err)
	}
	for i := 2; i < 2*replicaLuaAttempts; i++ {
		m, _ = commitTestReply(t, r)
		r.finish(m, true)
	}
	if err := w.ValidateCommitFinished(); err != nil {
		t.Fatal(w.Snapshot())
	}
	command := commitTestCommand(10)
	opts := luaBeginOptions{message: ownedMessage(t, command), member: 1}
	if _, err := r.begin(opts); err == nil {
		t.Fatal("physical commit cap escaped")
	}
	for _, field := range []string{"request", "write", "reply", "drop", "forward", "abort", "find", "item"} {
		t.Run(field, func(t *testing.T) {
			r, w := commitTestReady(t, true)
			commitTestFirstDrop(t, r, w)
			m, _ := commitTestReply(t, r)
			r.finish(m, true)
			a := &w.snapshot.Attempt[0]
			switch field {
			case "request":
				a.CommitRequests++
			case "write":
				a.CommitWrites++
			case "reply":
				a.CommitReplies++
			case "drop":
				a.CommitDropped--
			case "forward":
				a.CommitForwarded++
			case "abort":
				a.AbortRequests++
			case "find":
				a.FindRequests++
			case "item":
				a.WriteItems++
			}
			if err := w.ValidateCommitFinished(); err == nil {
				t.Fatal("incorrect accounting accepted", field)
			}
		})
	}
}
