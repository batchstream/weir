package testmongo

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Synthetic envelopes below are offline parser/gate controls, not live MongoDB
// evidence. Live tests must obtain every reply unchanged from the real server.
func luaTestRegistry(t *testing.T) (*luaRegistry, *LuaWatch) {
	t.Helper()
	r := &luaRegistry{}
	op := LuaOperation{Namespace: "owned.records", ID: "lua"}
	w, err := r.watch(op)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.arm(1, w); err != nil {
		t.Fatal(err)
	}
	return r, w
}
func luaTestFind(txn int64) bson.D {
	ids := bson.D{{Key: "$in", Value: bson.A{"lua"}}}
	filter := bson.D{{Key: "_id", Value: ids}}
	concern := bson.D{{Key: "level", Value: "snapshot"}}
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "readConcern", Value: concern}, {Key: "startTransaction", Value: true}, {Key: "autocommit", Value: false}, {Key: "lsid", Value: readTestSession(1)}, {Key: "txnNumber", Value: txn}, {Key: "$db", Value: "owned"}}
	return command
}
func luaTestControl(command string, txn int64) bson.D {
	doc := bson.D{{Key: command, Value: 1}, {Key: "autocommit", Value: false}, {Key: "lsid", Value: readTestSession(1)}, {Key: "txnNumber", Value: txn}, {Key: "$db", Value: "admin"}}
	return doc
}
func luaTestWrite(txn int64, second bool) bson.D {
	n, external := int32(1), int32(0)
	if second {
		n = 21
		external = 1
	}
	replacement := bson.D{{Key: "_id", Value: "lua"}, {Key: "n", Value: n}, {Key: "external", Value: external}, {Key: "candidate", Value: int32(1)}, {Key: "global_fresh", Value: 1}, {Key: "library_fresh", Value: 1}, {Key: "input_attempt", Value: 1}, {Key: "observed", Value: "2026-10-20T00:00:00Z"}}
	filter := bson.D{{Key: "_id", Value: "lua"}}
	item := bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "updateMods", Value: replacement}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}}
	ns := bson.D{{Key: "ns", Value: "owned.records"}}
	command := bson.D{{Key: "bulkWrite", Value: 1}, {Key: "ops", Value: bson.A{item}}, {Key: "nsInfo", Value: bson.A{ns}}, {Key: "autocommit", Value: false}, {Key: "lsid", Value: readTestSession(1)}, {Key: "txnNumber", Value: txn}, {Key: "$db", Value: "admin"}}
	return command
}
func luaTestBegin(t *testing.T, r *luaRegistry, command bson.D) *luaMatch {
	t.Helper()
	opts := luaBeginOptions{message: ownedMessage(t, command), member: 1}
	m, err := r.begin(opts)
	if err != nil || m == nil {
		t.Fatal("Lua request match", err)
	}
	if !r.forwarded(m, len(opts.message), len(opts.message)) {
		t.Fatal("Lua write", m.watch.Snapshot())
	}
	return m
}
func luaTestFindReply(second bool) bson.D {
	n, external := int32(0), int32(0)
	if second {
		n = 20
		external = 1
	}
	doc := bson.D{{Key: "_id", Value: "lua"}, {Key: "n", Value: n}, {Key: "external", Value: external}, {Key: "candidate", Value: 0}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "owned.records"}, {Key: "firstBatch", Value: bson.A{doc}}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
	return reply
}
func luaTestReceive(t *testing.T, r *luaRegistry, m *luaMatch, doc bson.D) []byte {
	t.Helper()
	response := readTestResponse(t, doc)
	if !r.received(m, response) {
		t.Fatal("Lua response", m.watch.Snapshot())
	}
	return response
}
func luaTestForward(t *testing.T, r *luaRegistry, m *luaMatch, response []byte) {
	t.Helper()
	writer, reader := net.Pipe()
	defer writer.Close()
	defer reader.Close()
	_ = writer.SetDeadline(time.Now().Add(time.Second))
	_ = reader.SetDeadline(time.Now().Add(time.Second))
	result := make(chan bool, 1)
	go func() { result <- r.forwardReply(m, writer, response) }()
	actual := make([]byte, len(response))
	_, err := io.ReadFull(reader, actual)
	forwarded := <-result
	if err != nil || !forwarded || !bytes.Equal(actual, response) {
		t.Fatal("unchanged actual downstream write", err)
	}
}
func luaTestSnapshot(t *testing.T, r *luaRegistry, w *LuaWatch, second bool) {
	t.Helper()
	txn := int64(10)
	attempt := 0
	if second {
		txn = 11
		attempt = 1
	}
	command := luaTestFind(txn)
	m := luaTestBegin(t, r, command)
	reply := luaTestFindReply(second)
	response := luaTestReceive(t, r, m, reply)
	if err := w.ForwardSnapshot(attempt); err != nil {
		t.Fatal(err)
	}
	ok, drop := r.hold(m, make(chan struct{}), time.Second)
	if !ok || drop {
		t.Fatal("snapshot release")
	}
	luaTestForward(t, r, m, response)
}
func luaTestAbort(t *testing.T, r *luaRegistry, w *LuaWatch, code int) (*luaMatch, []byte) {
	t.Helper()
	luaTestSnapshot(t, r, w, false)
	command := luaTestWrite(10, false)
	m := luaTestBegin(t, r, command)
	labels := bson.A{"TransientTransactionError"}
	conflict := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 112}, {Key: "errorLabels", Value: labels}}
	response := luaTestReceive(t, r, m, conflict)
	luaTestForward(t, r, m, response)
	command = luaTestControl("abortTransaction", 10)
	m = luaTestBegin(t, r, command)
	reply := bson.D{{Key: "ok", Value: 1}}
	if code != 0 {
		reply = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: code}}
	}
	response = luaTestReceive(t, r, m, reply)
	if err := w.ValidateAbortHeld(); err != nil {
		t.Fatal("abort held counts", w.Snapshot())
	}
	return m, response
}
func TestReplicaProxyLuaConfirmedAndLostAbort(t *testing.T) {
	for _, confirmed := range []bool{true, false} {
		name := "lost"
		if confirmed {
			name = "confirmed"
		}
		t.Run(name, func(t *testing.T) {
			r, w := luaTestRegistry(t)
			m, response := luaTestAbort(t, r, w, 251)
			if confirmed {
				if err := w.ForwardAbortReply(); err != nil {
					t.Fatal(err)
				}
				if s := w.Snapshot(); s.Attempt[0].AbortForwarded != 0 {
					t.Fatal("intent became forwarding proof")
				}
				ok, drop := r.hold(m, make(chan struct{}), time.Second)
				if !ok || drop {
					t.Fatal("forward gate")
				}
				luaTestForward(t, r, m, response)
				luaTestSnapshot(t, r, w, true)
				command := luaTestWrite(11, true)
				m = luaTestBegin(t, r, command)
				reply := ownedReply()
				response = luaTestReceive(t, r, m, reply)
				luaTestForward(t, r, m, response)
				command = luaTestControl("commitTransaction", 11)
				m = luaTestBegin(t, r, command)
				reply = bson.D{{Key: "ok", Value: 1}}
				response = luaTestReceive(t, r, m, reply)
				luaTestForward(t, r, m, response)
			} else {
				if err := w.DropAbortReply(); err != nil {
					t.Fatal(err)
				}
				if s := w.Snapshot(); s.Attempt[0].AbortDropped != 0 {
					t.Fatal("intent became drop proof")
				}
				ok, drop := r.hold(m, make(chan struct{}), time.Second)
				if !ok || !drop {
					t.Fatal("drop gate")
				}
				r.finish(m, true)
			}
			if err := w.ValidateFinished(confirmed); err != nil {
				t.Fatal(w.Snapshot())
			}
		})
	}
}
func TestReplicaProxyLuaIdentityAndBounds(t *testing.T) {
	tests := []string{"wrong_member", "missing_session", "ordinary_find", "early_retry", "changed_session", "replayed_find", "event_cap", "attempt_cap", "poisoned_input", "unsettled_abort", "unexpected_commit", "partial_upstream", "wrong_reply_correlation", "write_concern", "watchdog", "interrupted", "partial_downstream"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			r, w := luaTestRegistry(t)
			command := luaTestFind(10)
			if name == "event_cap" {
				w.snapshot.Events = replicaLuaEvents
			}
			if name == "attempt_cap" {
				w.snapshot.Attempts = replicaLuaAttempts
			}
			if name == "missing_session" {
				command = append(command[:5], command[6:]...)
			}
			if name == "ordinary_find" {
				command = append(command[:2], command[3:]...)
			}
			member := 1
			if name == "wrong_member" {
				member = 2
			}
			opts := luaBeginOptions{message: ownedMessage(t, command), member: member}
			m, err := r.begin(opts)
			if name == "wrong_member" || name == "missing_session" || name == "ordinary_find" || name == "event_cap" || name == "attempt_cap" {
				if err == nil || w.Snapshot().Error == "" {
					t.Fatal("invalid request accepted", w.Snapshot())
				}
				return
			}
			if err != nil || m == nil {
				t.Fatal(err)
			}
			if name == "partial_upstream" {
				if r.forwarded(m, 1, 2) {
					t.Fatal("partial write accepted")
				}
				return
			}
			if !r.forwarded(m, len(opts.message), len(opts.message)) {
				t.Fatal("full upstream")
			}
			if name == "early_retry" || name == "changed_session" || name == "replayed_find" {
				command = luaTestFind(11)
				if name == "replayed_find" {
					command = luaTestFind(10)
				}
				if name == "changed_session" {
					command[5].Value = readTestSession(2)
				}
				opts.message = ownedMessage(t, command)
				_, err = r.begin(opts)
				if err == nil {
					t.Fatal("early/replayed/rebound attempt accepted")
				}
				return
			}
			if name == "poisoned_input" {
				r, w = luaTestRegistry(t)
				luaTestSnapshot(t, r, w, false)
				command = luaTestWrite(10, false)
				item := command[1].Value.(bson.A)[0].(bson.D)
				replacement := item[2].Value.(bson.D)
				replacement[7].Value = 2
				opts.message = ownedMessage(t, command)
				_, err = r.begin(opts)
				if err == nil {
					t.Fatal("stale input witness accepted")
				}
				return
			}
			if name == "unsettled_abort" || name == "unexpected_commit" {
				control := "abortTransaction"
				if name == "unexpected_commit" {
					control = "commitTransaction"
				}
				command = luaTestControl(control, 10)
				opts.message = ownedMessage(t, command)
				_, err = r.begin(opts)
				if err == nil {
					t.Fatal("unsettled control accepted")
				}
				return
			}
			reply := luaTestFindReply(false)
			if name == "write_concern" {
				concern := bson.D{{Key: "code", Value: 64}}
				field := bson.E{Key: "writeConcernError", Value: concern}
				reply = append(reply, field)
			}
			response := readTestResponse(t, reply)
			if name == "wrong_reply_correlation" {
				binary.LittleEndian.PutUint32(response[8:12], 99)
			}
			received := r.received(m, response)
			if name == "write_concern" || name == "wrong_reply_correlation" {
				if received {
					t.Fatal("invalid reply accepted")
				}
				return
			}
			if !received {
				t.Fatal(w.Snapshot())
			}
			if name == "partial_downstream" {
				writer, reader := net.Pipe()
				_ = reader.Close()
				defer writer.Close()
				if r.forwardReply(m, writer, response) || w.Snapshot().Attempt[0].FindForwarded != 0 {
					t.Fatal("partial downstream credited")
				}
				return
			}
			done := make(chan struct{})
			if name == "interrupted" {
				close(done)
			}
			released, drop := r.hold(m, done, time.Millisecond)
			if released || drop || w.Snapshot().Error == "" || w.Snapshot().Attempt[0].FindForwarded != 0 {
				t.Fatal("cleanup or expiry manufactured evidence", w.Snapshot())
			}
			if name == "watchdog" && !w.Snapshot().Expired {
				t.Fatal("missing expiry failure")
			}
			if err := w.ForwardSnapshot(0); err == nil {
				t.Fatal("released failed watch")
			}
		})
	}
	r, w := luaTestRegistry(t)
	for i := 1; i < replicaLuaLimit; i++ {
		op := LuaOperation{Namespace: "owned.records", ID: string(rune('a' + i))}
		if _, err := r.watch(op); err != nil {
			t.Fatal(err)
		}
	}
	op := LuaOperation{Namespace: "owned.records", ID: "excess"}
	if _, err := r.watch(op); err == nil {
		t.Fatal("unbounded registration")
	}
	if err := w.DropAbortReply(); err == nil {
		t.Fatal("drop before hold")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := WaitLuaPhase(ctx, make(chan struct{})); err == nil {
		t.Fatal("phase outlived original caller")
	}
}
func TestReplicaProxyLuaSequencesAndPooledSession(t *testing.T) {
	r, w := luaTestRegistry(t)
	luaTestSnapshot(t, r, w, false)
	command := luaTestWrite(10, false)
	opts := luaBeginOptions{message: ownedSequence(t, command), member: 1}
	m, err := r.begin(opts)
	if err != nil || m == nil {
		t.Fatal("kind1 replacement", err)
	}
	// An unrelated pooled session using another transaction cannot contaminate
	// this operation, including after the watch has completed.
	command = luaTestControl("commitTransaction", 99)
	opts.message = ownedMessage(t, command)
	m, err = r.begin(opts)
	if err != nil || m != nil {
		t.Fatal("unrelated pooled txn matched", err)
	}
}
func TestReplicaProxyLuaGenuineConflictShapes(t *testing.T) {
	item := bson.D{{Key: "ok", Value: 0}, {Key: "idx", Value: 0}, {Key: "code", Value: 112}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "nErrors", Value: 1}, {Key: "nInserted", Value: 0}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}, {Key: "cursor", Value: cursor}}
	raw := ownedRaw(t, reply)
	conflict, command, labels, transient := luaConflict(raw)
	if !conflict || command || labels != 0 || transient {
		t.Fatal("item conflict was relabelled")
	}
	item[2].Value = 11000
	raw = ownedRaw(t, reply)
	conflict, _, _, _ = luaConflict(raw)
	if conflict {
		t.Fatal("nonconflict converted to112")
	}
}
