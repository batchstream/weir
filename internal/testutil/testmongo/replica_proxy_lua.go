package testmongo

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Four stable cases plus one commit/abort pair at each existing topology action.
const replicaLuaLimit = 8
const replicaLuaAttempts = 5
const replicaLuaEvents = 32
const replicaLuaWatchdog = 20 * time.Second

type luaTopologyUsability struct {
	primary int
	usable  time.Time
	err     error
}

// ObserveLuaTopologyUsability starts independently of held-reply release and old
// receipts. Its single WaitUsable result retains the first successful T0 even if
// the caller joins old work later. Cleanup cancels and joins the actual observer.
func ObserveLuaTopologyUsability(t *testing.T, observe func(context.Context, int) (int, time.Time, error), exclude int) func() (int, time.Time, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	result := make(chan luaTopologyUsability, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		primary, usable, err := observe(ctx, exclude)
		observation := luaTopologyUsability{primary: primary, usable: usable, err: err}
		result <- observation
	}()
	join := func() {
		cancel()
		<-joined
	}
	t.Cleanup(join)
	await := func() (int, time.Time, error) {
		observation := <-result
		join()
		return observation.primary, observation.usable, observation.err
	}
	return await
}

// ReplicaLuaSource deliberately mutates three separate VM-owned surfaces. Both
// attempted replacements must carry fresh sentinels and one observation time.
const ReplicaLuaSource = `return function(current, incoming)
 if lua_attempt_poison ~= nil or math.lua_attempt_poison ~= nil then error('stale VM') end
 lua_attempt_poison = true; math.lua_attempt_poison = true
 incoming.attempt = incoming.attempt + 1
 if incoming.attempt ~= 1 then error('stale input') end
 current.n = current.n + 1; current.candidate = current.candidate + 1
 current.global_fresh = 1; current.library_fresh = 1; current.input_attempt = incoming.attempt
 current.observed = weir.time.now()
 return current
end`

type LuaOperation struct{ Namespace, ID string }
type LuaLedger struct{ N, External, Candidate int64 }
type LuaAttemptSnapshot struct {
	Transaction                                                            int64
	FindRequests, FindWrites, FindReplies, FindForwarded                   int
	WriteRequests, WriteItems, WriteReplies, WriteForwarded                int
	AbortRequests, AbortWrites, AbortReplies, AbortForwarded, AbortDropped int
	CommitRequests, CommitWrites, CommitReplies, CommitForwarded           int
	CommitDropped                                                          int
	SnapshotHeld, AbortHeld, Conflict, ConflictCommand, TransientLabel     bool
	ConflictCode, AbortCode, LabelCount                                    int
	CandidateN, CandidateExternal, CandidateCount                          int64
	GlobalFresh, LibraryFresh, InputAttempt                                int64
	Observed                                                               string
	FindDigest, WriteDigest, AbortDigest                                   [32]byte
}
type LuaSnapshot struct {
	Attempts, Events   int
	SessionDigest      [32]byte
	Attempt            [replicaLuaAttempts]LuaAttemptSnapshot
	Expired, Completed bool
	Error              string
}
type luaGate struct {
	held, release chan struct{}
	requested     bool
	drop          bool
}
type LuaWatch struct {
	registry  *luaRegistry
	operation LuaOperation
	member    int
	armed     bool
	deadline  time.Time
	snapshot  LuaSnapshot
	commit    *luaCommitState
	snapshots [replicaLuaAttempts]luaGate
	abort     luaGate
}
type luaRegistry struct {
	mu      sync.Mutex
	watches []*LuaWatch
	failure string
}
type luaMatch struct {
	watch     *LuaWatch
	attempt   int
	command   string
	requestID uint32
}
type luaBeginOptions struct {
	message []byte
	member  int
}

func newLuaGate() luaGate {
	gate := luaGate{held: make(chan struct{}), release: make(chan struct{})}
	return gate
}
func (r *luaRegistry) watch(op LuaOperation) (*LuaWatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.watches) >= replicaLuaLimit || len(op.Namespace) > 256 || !strings.HasSuffix(op.Namespace, ".records") || len(op.ID) == 0 || len(op.ID) > 128 {
		return nil, errReplicaProxyWire
	}
	for _, w := range r.watches {
		if w.operation == op {
			return nil, errReplicaProxyWire
		}
	}
	w := &LuaWatch{registry: r, operation: op, member: -1, abort: newLuaGate()}
	for i := range w.snapshots {
		w.snapshots[i] = newLuaGate()
	}
	r.watches = append(r.watches, w)
	return w, nil
}
func (r *luaRegistry) arm(member int, w *LuaWatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w == nil || w.registry != r || w.armed || w.snapshot.Events != 0 || member < 0 || member >= 3 {
		return errReplicaProxyWire
	}
	w.armed = true
	w.member = member
	return nil
}
func (w *LuaWatch) SnapshotHeld(attempt int) <-chan struct{} {
	if attempt < 0 || attempt >= replicaLuaAttempts {
		return nil
	}
	return w.snapshots[attempt].held
}
func (w *LuaWatch) AbortHeld() <-chan struct{} { return w.abort.held }
func (w *LuaWatch) Snapshot() LuaSnapshot {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	s := w.snapshot
	if w.registry.failure != "" {
		s.Error = w.registry.failure
	}
	return s
}
func (w *LuaWatch) ForwardSnapshot(attempt int) error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	if attempt < 0 || attempt >= replicaLuaAttempts || !w.snapshot.Attempt[attempt].SnapshotHeld || w.snapshots[attempt].requested || w.snapshot.Expired || w.registry.failure != "" {
		return errReplicaProxyWire
	}
	w.snapshots[attempt].requested = true
	close(w.snapshots[attempt].release)
	return nil
}
func (w *LuaWatch) releaseAbort(drop bool) error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	if !w.snapshot.Attempt[0].AbortHeld || w.abort.requested || w.snapshot.Expired || w.registry.failure != "" {
		return errReplicaProxyWire
	}
	w.abort.requested = true
	w.abort.drop = drop
	close(w.abort.release)
	return nil
}
func (w *LuaWatch) ForwardAbortReply() error { return w.releaseAbort(false) }
func (w *LuaWatch) DropAbortReply() error    { return w.releaseAbort(true) }
func (r *luaRegistry) fail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failure = "owned Lua observation failed closed"
}

// Parse only the existing singleton replacement shape, independently of the old
// mutation increment observer. The framing/sequence helpers remain shared.
func luaReplacement(message []byte, doc bson.Raw) (LuaOperation, bson.Raw, error) {
	var op LuaOperation
	seq, err := operationSequences(message, doc)
	if err != nil || len(seq["ops"]) != 1 || len(seq["nsInfo"]) != 1 {
		return op, nil, errReplicaProxyWire
	}
	item := seq["ops"][0]
	ns, ok := seq["nsInfo"][0].Lookup("ns").StringValueOK()
	filter, filterOK := item.Lookup("filter").DocumentOK()
	id, idErr := operationID(filter)
	if !ok || !filterOK || idErr != nil {
		return op, nil, errReplicaProxyWire
	}
	op = LuaOperation{Namespace: ns, ID: id}
	replacement, _ := item.Lookup("updateMods").DocumentOK()
	return op, replacement, nil
}
func (r *luaRegistry) begin(opts luaBeginOptions) (*luaMatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.watches) == 0 {
		return nil, nil
	}
	doc, _, err := replicaWireDocument(opts.message, false)
	if err != nil || uniqueOperationDocument(doc) != nil {
		r.failure = "invalid Lua request framing"
		return nil, errReplicaProxyWire
	}
	fields, _ := doc.Elements()
	name := fields[0].Key()
	if name != "find" && name != "bulkWrite" && name != "abortTransaction" && name != "commitTransaction" {
		return nil, nil
	}
	session, present, sessionErr := readSession(doc.Lookup("lsid"))
	txn, txnOK := doc.Lookup("txnNumber").Int64OK()
	var identity LuaOperation
	var replacement bson.Raw
	var ids []string
	if name == "find" {
		db, _ := doc.Lookup("$db").StringValueOK()
		coll, _ := doc.Lookup("find").StringValueOK()
		identity.Namespace = db + "." + coll
		filter, ok := doc.Lookup("filter").DocumentOK()
		selectorErr := errReplicaProxyWire
		if ok {
			ids, _, selectorErr = readFindIDs(filter)
		}
		for _, w := range r.watches {
			if identity.Namespace == w.operation.Namespace && selectorErr != nil {
				r.failure = "unsupported Lua watched-namespace selector"
				return nil, errReplicaProxyWire
			}
		}
	}
	if name == "bulkWrite" {
		identity, replacement, err = luaReplacement(opts.message, doc)
	}
	var matched *LuaWatch
	attempt := -1
	for _, w := range r.watches {
		owned := false
		if name == "find" && identity.Namespace == w.operation.Namespace {
			for _, id := range ids {
				owned = owned || id == w.operation.ID
			}
		}
		if name == "bulkWrite" {
			owned = identity == w.operation
		}
		bound := -1
		if present && txnOK && session == w.snapshot.SessionDigest {
			for i := 0; i < w.snapshot.Attempts; i++ {
				if w.snapshot.Attempt[i].Transaction == txn {
					bound = i
				}
			}
		}
		control := name == "commitTransaction" || name == "abortTransaction"
		if control && present && sessionErr == nil && w.snapshot.Attempts > 0 && session == w.snapshot.SessionDigest && !w.snapshot.Completed && bound < 0 {
			r.failure = "active Lua control used unexpected transaction"
			return nil, errReplicaProxyWire
		}
		if !owned && bound < 0 {
			continue
		}
		if matched != nil {
			r.failure = "ambiguous Lua identity"
			return nil, errReplicaProxyWire
		}
		matched = w
		attempt = bound
		if name == "find" {
			if w.snapshot.Completed || !owned || len(ids) != 1 || !w.armed || opts.member != w.member || !present || sessionErr != nil || !txnOK || w.snapshot.Attempts >= replicaLuaAttempts || w.commit != nil && w.snapshot.Attempts != 0 || bound >= 0 || w.snapshot.Attempts > 0 && session != w.snapshot.SessionDigest {
				r.failure = "Lua snapshot identity or retry mismatch"
				return nil, errReplicaProxyWire
			}
			attempt = w.snapshot.Attempts
			if attempt == 0 {
				w.deadline = time.Now().Add(replicaLuaWatchdog)
			}
			if time.Now().After(w.deadline) {
				w.snapshot.Expired = true
				r.failure = "Lua observer lifetime expired"
				return nil, errReplicaProxyWire
			}
			if attempt > 0 && (w.snapshot.Attempt[attempt-1].AbortForwarded != 1 || w.snapshot.Attempt[attempt-1].AbortDropped != 0) {
				r.failure = "Lua rebuilt before confirmed abort"
				return nil, errReplicaProxyWire
			}
			concern, ok := doc.Lookup("readConcern").DocumentOK()
			level, _ := concern.Lookup("level").StringValueOK()
			start, startOK := doc.Lookup("startTransaction").BooleanOK()
			auto, autoOK := doc.Lookup("autocommit").BooleanOK()
			if !ok || level != "snapshot" || !startOK || !start || !autoOK || auto {
				r.failure = "Lua first find is not a snapshot transaction"
				return nil, errReplicaProxyWire
			}
			w.snapshot.SessionDigest = session
			w.snapshot.Attempts++
			w.snapshot.Attempt[attempt].Transaction = txn
		} else if bound < 0 || !present || sessionErr != nil || !txnOK || opts.member != w.member && !(name == "commitTransaction" && w.commit != nil && w.snapshot.Attempt[0].CommitDropped > 0) {
			r.failure = "Lua command identity mismatch"
			return nil, errReplicaProxyWire
		}
	}
	if matched == nil {
		return nil, nil
	}
	w := matched
	s := &w.snapshot.Attempt[attempt]
	w.snapshot.Events++
	if w.snapshot.Events > replicaLuaEvents {
		r.failure = "Lua event cap exceeded"
		return nil, errReplicaProxyWire
	}
	auto, autoOK := doc.Lookup("autocommit").BooleanOK()
	if !autoOK || auto {
		r.failure = "Lua command escaped transaction"
		return nil, errReplicaProxyWire
	}
	switch name {
	case "find":
		s.FindRequests++
	case "bulkWrite":
		s.WriteRequests++
		seq, seqErr := operationSequences(opts.message, doc)
		if s.FindForwarded != 1 || s.FindReplies != 1 || err != nil || seqErr != nil || identity != w.operation || uniqueOperationDocument(replacement) != nil {
			r.failure = "Lua replacement identity invalid"
			return nil, errReplicaProxyWire
		}
		item := seq["ops"][0]
		multi, multiOK := item.Lookup("multi").BooleanOK()
		upsert, upsertErr := operationOptionalBool(item.Lookup("upsert"))
		id, idOK := replacement.Lookup("_id").StringValueOK()
		observed, observedOK := replacement.Lookup("observed").StringValueOK()
		_, observedErr := time.Parse(time.RFC3339Nano, observed)
		if observedErr != nil || !operationNumber(item.Lookup("update"), 0) || !multiOK || multi || upsertErr != nil || upsert || !idOK || id != w.operation.ID || !observedOK || len(observed) > 40 {
			r.failure = "Lua replacement shape invalid"
			return nil, errReplicaProxyWire
		}
		s.CandidateN = luaNumber(replacement.Lookup("n"))
		s.CandidateExternal = luaNumber(replacement.Lookup("external"))
		s.CandidateCount = luaNumber(replacement.Lookup("candidate"))
		s.GlobalFresh = luaNumber(replacement.Lookup("global_fresh"))
		s.LibraryFresh = luaNumber(replacement.Lookup("library_fresh"))
		s.InputAttempt = luaNumber(replacement.Lookup("input_attempt"))
		s.Observed = observed
		if s.WriteRequests != 1 || s.GlobalFresh != 1 || s.LibraryFresh != 1 || s.InputAttempt != 1 || s.CandidateCount != 1 || attempt == 0 && (s.CandidateN != 1 || s.CandidateExternal != 0) || attempt > 0 && (s.CandidateN != 21 || s.CandidateExternal != 1 || s.Observed != w.snapshot.Attempt[0].Observed) {
			r.failure = "Lua fresh attempt witness invalid"
			return nil, errReplicaProxyWire
		}
	case "abortTransaction":
		s.AbortRequests++
		if w.commit != nil || attempt != 0 || s.AbortRequests != 1 || !s.Conflict || s.WriteItems != 1 || s.WriteReplies != 1 || s.WriteForwarded != 1 {
			r.failure = "Lua abort before settled conflict"
			return nil, errReplicaProxyWire
		}
	case "commitTransaction":
		s.CommitRequests++
		if w.commit != nil {
			if !w.beginCommitLocked(doc) {
				return nil, errReplicaProxyWire
			}
		} else if attempt != 1 || s.CommitRequests != 1 || s.WriteReplies != 1 || s.WriteForwarded != 1 || s.Conflict {
			r.failure = "unexpected Lua commit"
			return nil, errReplicaProxyWire
		}
	}
	m := &luaMatch{watch: w, attempt: attempt, command: name, requestID: binary.LittleEndian.Uint32(opts.message[4:8])}
	return m, nil
}
func (r *luaRegistry) forwarded(m *luaMatch, n, total int) bool {
	if m == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n != total {
		r.failure = "partial Lua upstream write"
		return false
	}
	s := &m.watch.snapshot.Attempt[m.attempt]
	switch m.command {
	case "find":
		s.FindWrites++
	case "bulkWrite":
		s.WriteItems++
	case "abortTransaction":
		s.AbortWrites++
	case "commitTransaction":
		s.CommitWrites++
	}
	return true
}
func luaConflict(reply bson.Raw) (bool, bool, int, bool) {
	command := operationNumber(reply.Lookup("ok"), 0) && operationNumber(reply.Lookup("code"), 112)
	labels := 0
	transient := false
	if value := reply.Lookup("errorLabels"); value.Type != 0 {
		array, ok := value.ArrayOK()
		if !ok {
			return false, false, 0, false
		}
		fields, err := bson.Raw(array).Values()
		if err != nil || len(fields) > 8 {
			return false, false, 0, false
		}
		labels = len(fields)
		for _, field := range fields {
			label, ok := field.StringValueOK()
			if !ok {
				return false, false, 0, false
			}
			transient = transient || label == "TransientTransactionError"
		}
	}
	if command {
		return true, true, labels, transient
	}
	if !operationNumber(reply.Lookup("ok"), 1) || !operationNumber(reply.Lookup("nErrors"), 1) {
		return false, false, labels, transient
	}
	for _, key := range []string{"nInserted", "nDeleted", "nMatched", "nModified", "nUpserted"} {
		if !operationNumber(reply.Lookup(key), 0) {
			return false, false, labels, transient
		}
	}
	cursor, ok := reply.Lookup("cursor").DocumentOK()
	ns, _ := cursor.Lookup("ns").StringValueOK()
	items, err := operationDocuments(cursor.Lookup("firstBatch"))
	valid := ok && uniqueOperationDocument(cursor) == nil && operationNumber(cursor.Lookup("id"), 0) && ns == "admin.$cmd.bulkWrite" && err == nil && len(items) == 1
	if !valid {
		return false, false, labels, transient
	}
	item := items[0]
	return operationNumber(item.Lookup("ok"), 0) && operationNumber(item.Lookup("idx"), 0) && operationNumber(item.Lookup("code"), 112), false, labels, transient
}
func (r *luaRegistry) received(m *luaMatch, response []byte) bool {
	if m == nil {
		return true
	}
	doc, _, err := replicaWireDocument(response, true)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || uniqueOperationDocument(doc) != nil || binary.LittleEndian.Uint32(response[8:12]) != m.requestID || doc.Lookup("writeConcernError").Type != 0 {
		r.failure = "Lua reply correlation or WCE invalid"
		return false
	}
	w := m.watch
	s := &w.snapshot.Attempt[m.attempt]
	expected := false
	switch m.command {
	case "find":
		expected = s.FindWrites == 1 && s.FindReplies == 0
	case "bulkWrite":
		expected = s.WriteItems == 1 && s.WriteReplies == 0
	case "abortTransaction":
		expected = s.AbortWrites == 1 && s.AbortReplies == 0
	case "commitTransaction":
		expected = s.CommitWrites == s.CommitRequests && s.CommitReplies == s.CommitRequests-1
	}
	if !expected {
		r.failure = "Lua reply without unique full upstream write"
		return false
	}
	valid := false
	switch m.command {
	case "find":
		cursor, ok := doc.Lookup("cursor").DocumentOK()
		ns, _ := cursor.Lookup("ns").StringValueOK()
		docs, batchErr := operationDocuments(cursor.Lookup("firstBatch"))
		valid = operationNumber(doc.Lookup("ok"), 1) && ok && uniqueOperationDocument(cursor) == nil && ns == w.operation.Namespace && operationNumber(cursor.Lookup("id"), 0) && batchErr == nil && len(docs) == 1
		if valid {
			id, _ := docs[0].Lookup("_id").StringValueOK()
			n := int64(0)
			external := int64(0)
			if m.attempt > 0 {
				n = 20
				external = 1
			}
			valid = id == w.operation.ID && operationNumber(docs[0].Lookup("n"), n) && operationNumber(docs[0].Lookup("external"), external) && operationNumber(docs[0].Lookup("candidate"), 0)
		}
		if valid {
			s.FindReplies++
			s.FindDigest = sha256.Sum256(doc)
			s.SnapshotHeld = true
			close(w.snapshots[m.attempt].held)
		}
	case "bulkWrite":
		if m.attempt == 0 && w.commit == nil {
			s.Conflict, s.ConflictCommand, s.LabelCount, s.TransientLabel = luaConflict(doc)
			valid = s.Conflict
			if valid {
				s.ConflictCode = 112
			}
		} else {
			op := OwnedOperation{Namespace: w.operation.Namespace, ID: w.operation.ID, Kind: "bulkWrite"}
			valid = operationSuccessfulReply(op, doc)
		}
		if valid {
			s.WriteReplies++
			s.WriteDigest = sha256.Sum256(doc)
		}
	case "abortTransaction":
		valid = operationNumber(doc.Lookup("ok"), 1) || operationNumber(doc.Lookup("ok"), 0) && operationNumber(doc.Lookup("code"), 251) && s.Conflict && s.WriteReplies == 1
		if valid {
			if operationNumber(doc.Lookup("code"), 251) {
				s.AbortCode = 251
			}
			s.AbortReplies++
			s.AbortDigest = sha256.Sum256(doc)
			s.AbortHeld = true
			close(w.abort.held)
		}
	case "commitTransaction":
		valid = operationNumber(doc.Lookup("ok"), 1)
		if valid {
			s.CommitReplies++
			if w.commit != nil && s.CommitReplies == 1 {
				close(w.commit.gate.held)
			}
		}
	}
	if !valid {
		r.failure = "unexpected real Lua reply shape"
	}
	return valid
}

// hold never treats observer timeout or cleanup interruption as a successful
// release/drop. The original bytes are written by the relay only after release.
func (r *luaRegistry) hold(m *luaMatch, done <-chan struct{}, timeout time.Duration) (bool, bool) {
	if m != nil && m.watch.commit != nil && m.command == "commitTransaction" {
		return m.watch.holdCommit(done, timeout)
	}
	if m == nil || m.command != "find" && m.command != "abortTransaction" {
		return true, false
	}
	w := m.watch
	gate := &w.snapshots[m.attempt]
	if m.command == "abortTransaction" {
		gate = &w.abort
	}
	r.mu.Lock()
	if !w.deadline.IsZero() {
		timeout = min(timeout, time.Until(w.deadline))
	}
	r.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-gate.release:
		select {
		case <-done:
			r.fail()
			return false, false
		default:
		}
		r.mu.Lock()
		if !w.deadline.IsZero() && time.Now().After(w.deadline) {
			w.snapshot.Expired = true
			r.failure = "Lua observer lifetime expired"
			r.mu.Unlock()
			return false, false
		}
		drop := gate.drop
		r.mu.Unlock()
		return true, drop
	case <-done:
		r.fail()
	case <-timer.C:
		r.mu.Lock()
		w.snapshot.Expired = true
		r.failure = "Lua observer watchdog expired"
		r.mu.Unlock()
	}
	return false, false
}
func (r *luaRegistry) finish(m *luaMatch, drop bool) {
	if m == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishLocked(m, drop)
}
func (r *luaRegistry) finishLocked(m *luaMatch, drop bool) {
	s := &m.watch.snapshot.Attempt[m.attempt]
	switch m.command {
	case "find":
		s.FindForwarded++
	case "bulkWrite":
		s.WriteForwarded++
	case "abortTransaction":
		if drop {
			s.AbortDropped++
		} else {
			s.AbortForwarded++
		}
	case "commitTransaction":
		if drop {
			s.CommitDropped++
		} else {
			s.CommitForwarded++
		}
	}
}

// WaitLuaPhase keeps every stable-case phase inside its original caller budget.
func WaitLuaPhase(ctx context.Context, phase <-chan struct{}) error {
	select {
	case <-phase:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ValidateAbortHeld is the no-rebuild barrier, not merely release intent.
func (w *LuaWatch) ValidateAbortHeld() error {
	s := w.Snapshot()
	a := s.Attempt[0]
	if s.Error != "" || s.Expired || s.Attempts != 1 || a.FindRequests != 1 || a.FindWrites != 1 || a.FindReplies != 1 || a.FindForwarded != 1 || a.WriteRequests != 1 || a.WriteItems != 1 || a.WriteReplies != 1 || a.WriteForwarded != 1 || !a.Conflict || a.ConflictCode != 112 || a.AbortRequests != 1 || a.AbortWrites != 1 || a.AbortReplies != 1 || !a.AbortHeld || a.AbortForwarded != 0 || a.AbortDropped != 0 || a.CommitRequests != 0 {
		return errReplicaProxyWire
	}
	return nil
}
func (w *LuaWatch) ValidateFinished(confirmed bool) error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	return w.validateFinishedLocked(confirmed)
}

// Complete is called only after the integration caller has observed the actual
// terminal receipt and execution/publisher join. Wire success or release intent
// alone never completes observation. Old identities/counts remain watched.
func (w *LuaWatch) Complete(confirmed bool) error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	if w.snapshot.Completed {
		return errReplicaProxyWire
	}
	if err := w.validateFinishedLocked(confirmed); err != nil {
		return err
	}
	w.snapshot.Completed = true
	return nil
}
func (w *LuaWatch) validateFinishedLocked(confirmed bool) error {
	s := w.snapshot
	if w.registry.failure != "" {
		s.Error = w.registry.failure
	}
	a := s.Attempt[0]
	if s.Error != "" || s.Expired || a.FindRequests != 1 || a.FindWrites != 1 || a.FindReplies != 1 || a.FindForwarded != 1 || a.WriteRequests != 1 || a.WriteItems != 1 || a.WriteReplies != 1 || a.WriteForwarded != 1 || !a.Conflict || a.ConflictCode != 112 || a.AbortRequests != 1 || a.AbortWrites != 1 || a.AbortReplies != 1 || a.CommitRequests != 0 {
		return errReplicaProxyWire
	}
	if !confirmed {
		if s.Attempts != 1 || a.AbortDropped != 1 || a.AbortForwarded != 0 {
			return errReplicaProxyWire
		}
		return nil
	}
	b := s.Attempt[1]
	if s.Attempts != 2 || a.AbortForwarded != 1 || a.AbortDropped != 0 || b.Transaction == a.Transaction || b.FindRequests != 1 || b.FindWrites != 1 || b.FindReplies != 1 || b.FindForwarded != 1 || b.WriteRequests != 1 || b.WriteItems != 1 || b.WriteReplies != 1 || b.WriteForwarded != 1 || b.CommitRequests != 1 || b.CommitWrites != 1 || b.CommitReplies != 1 || b.CommitForwarded != 1 || b.AbortRequests != 0 || b.CandidateN != 21 || b.CandidateExternal != 1 || b.CandidateCount != 1 || b.GlobalFresh != 1 || b.LibraryFresh != 1 || b.InputAttempt != 1 || b.Observed == "" || b.Observed != a.Observed {
		return errReplicaProxyWire
	}
	return nil
}

// Serialize the real full downstream write with its accounting so a fast driver
// cannot begin a fresh attempt before the preceding full-forward is recorded.
func (r *luaRegistry) forwardReply(m *luaMatch, client net.Conn, response []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := client.Write(response)
	if err != nil || n != len(response) {
		r.failure = "Lua downstream reply write failed"
		return false
	}
	r.finishLocked(m, false)
	return true
}

func luaNumber(v bson.RawValue) int64 {
	if v.Type == bson.TypeInt32 || v.Type == bson.TypeInt64 {
		return v.AsInt64()
	}
	if v.Type == bson.TypeDouble && v.Double() == float64(int64(v.Double())) {
		return int64(v.Double())
	}
	return -1
}

// All three ledger fields must actually exist as exact numeric BSON values.
// Struct decoding would silently turn missing/null candidate into zero.
func validateLuaLedger(raw bson.Raw, expected LuaLedger) error {
	if len(raw) < 5 || int(binary.LittleEndian.Uint32(raw[:4])) != len(raw) || raw.Validate() != nil || uniqueOperationDocument(raw) != nil {
		return errReplicaProxyWire
	}
	if !operationNumber(raw.Lookup("n"), expected.N) || !operationNumber(raw.Lookup("external"), expected.External) || !operationNumber(raw.Lookup("candidate"), expected.Candidate) {
		return errReplicaProxyWire
	}
	return nil
}
