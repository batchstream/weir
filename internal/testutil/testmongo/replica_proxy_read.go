package testmongo

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// This watchdog bounds test observer ownership only. Expiry is a failed
// observation, never a reply release or evidence of product retirement.
const replicaReadWatchdog = 40 * time.Second
const replicaReadLimit = 4

type ReadSnapshot struct {
	FindRequests, FindWrites, FindReplies, FindForwarded             int
	CleanupRequests, CleanupWrites, CleanupReplies, CleanupForwarded int
	GetMore, KillCursors, Tails, Joined                              int
	Member, Connection                                               int
	RequestID                                                        uint32
	FindPeer, CleanupPeer                                            string
	SessionPresent                                                   bool
	SessionDigest, FindReplyDigest, CleanupReplyDigest               [32]byte
	Held, CleanupHeld, Closed, Expired                               bool
	Error                                                            string
}

type ReadWatch struct {
	registry                 *readRegistry
	id                       string
	member                   int
	armed                    bool
	snapshot                 ReadSnapshot
	held, cleanupHeld, close chan struct{}
	tails                    []*readTail
}
type readRegistry struct {
	mu        sync.Mutex
	namespace string
	watches   []*ReadWatch
	failure   string
}
type readTail struct {
	client, upstream net.Conn
	done             chan struct{}
}
type readMatch struct {
	watch     *ReadWatch
	cleanup   bool
	requestID uint32
}
type readBeginOptions struct {
	message            []byte
	member, connection int
	tail               *readTail
}

func (r *readRegistry) watch(id string) (*ReadWatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.watches) >= replicaReadLimit || len(id) == 0 || len(id) > 128 || !strings.HasSuffix(r.namespace, ".records") || len(r.namespace) > 256 {
		return nil, errReplicaProxyWire
	}
	for _, w := range r.watches {
		if w.id == id {
			return nil, errReplicaProxyWire
		}
	}
	w := &ReadWatch{registry: r, id: id, member: -1, held: make(chan struct{}), cleanupHeld: make(chan struct{}), close: make(chan struct{})}
	r.watches = append(r.watches, w)
	return w, nil
}
func (r *readRegistry) arm(member int, w *ReadWatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w == nil || w.registry != r || w.armed || member < 0 || member >= 3 || w.snapshot.FindRequests != 0 {
		return errReplicaProxyWire
	}
	w.member = member
	w.armed = true
	return nil
}
func (w *ReadWatch) Held() <-chan struct{}        { return w.held }
func (w *ReadWatch) CleanupHeld() <-chan struct{} { return w.cleanupHeld }
func (w *ReadWatch) Snapshot() ReadSnapshot {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	s := w.snapshot
	if w.registry.failure != "" {
		s.Error = w.registry.failure
	}
	return s
}

// CloseHeldRelays joins only this read's observer workers, after the caller has
// independently proved product-owner retirement. It preserves all observations.
func (w *ReadWatch) CloseHeldRelays(ctx context.Context) error {
	r := w.registry
	r.mu.Lock()
	if !w.snapshot.Closed {
		w.snapshot.Closed = true
		close(w.close)
	}
	tails := append([]*readTail(nil), w.tails...)
	r.mu.Unlock()
	for _, tail := range tails {
		if tail.client != nil {
			_ = tail.client.Close()
		}
		if tail.upstream != nil {
			_ = tail.upstream.Close()
		}
	}
	for _, tail := range tails {
		select {
		case <-tail.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (r *readRegistry) fail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failure = "owned read observation failed closed"
}
func readSession(value bson.RawValue) ([32]byte, bool, error) {
	var digest [32]byte
	if value.Type == 0 {
		return digest, false, nil
	}
	doc, ok := value.DocumentOK()
	if !ok || uniqueOperationDocument(doc) != nil {
		return digest, false, errReplicaProxyWire
	}
	fields, _ := doc.Elements()
	if len(fields) != 1 || fields[0].Key() != "id" {
		return digest, false, errReplicaProxyWire
	}
	subtype, data, ok := fields[0].Value().BinaryOK()
	if !ok || subtype != 4 || len(data) != 16 {
		return digest, false, errReplicaProxyWire
	}
	digest = sha256.Sum256(doc)
	return digest, true, nil
}

// readFindIDs recognizes the two existing fixture read selectors. A watched ID
// in a batch selector is rejected rather than silently treated as an exact read.
func readFindIDs(filter bson.Raw) ([]string, bool, error) {
	ids := make([]string, 0, 1)
	if id, err := operationID(filter); err == nil {
		ids = append(ids, id)
		return ids, true, nil
	}
	if uniqueOperationDocument(filter) != nil {
		return nil, false, errReplicaProxyWire
	}
	fields, _ := filter.Elements()
	if len(fields) != 1 || fields[0].Key() != "_id" {
		return nil, false, errReplicaProxyWire
	}
	selector, ok := fields[0].Value().DocumentOK()
	if !ok || uniqueOperationDocument(selector) != nil {
		return nil, false, errReplicaProxyWire
	}
	fields, _ = selector.Elements()
	if len(fields) != 1 || fields[0].Key() != "$in" {
		return nil, false, errReplicaProxyWire
	}
	array, ok := fields[0].Value().ArrayOK()
	if !ok {
		return nil, false, errReplicaProxyWire
	}
	values, err := bson.Raw(array).Elements()
	if err != nil || len(values) == 0 || len(values) > ownedItemLimit {
		return nil, false, errReplicaProxyWire
	}
	for i, value := range values {
		id, ok := value.Value().StringValueOK()
		if value.Key() != strconv.Itoa(i) || !ok || len(id) == 0 || len(id) > 128 {
			return nil, false, errReplicaProxyWire
		}
		ids = append(ids, id)
	}
	return ids, false, nil
}
func (r *readRegistry) begin(opts readBeginOptions) (*readMatch, error) {
	r.mu.Lock()
	empty := len(r.watches) == 0
	r.mu.Unlock()
	if empty {
		return nil, nil
	}
	doc, _, err := replicaWireDocument(opts.message, false)
	if err != nil || uniqueOperationDocument(doc) != nil {
		r.fail()
		return nil, errReplicaProxyWire
	}
	fields, _ := doc.Elements()
	name := fields[0].Key()
	if name != "find" && name != "killSessions" && name != "getMore" && name != "killCursors" {
		return nil, nil
	}
	session, present, err := readSession(doc.Lookup("lsid"))
	if err != nil {
		r.fail()
		return nil, err
	}
	var ids []string
	exactFind := true
	var targets [][32]byte
	if name == "find" {
		database, dbOK := doc.Lookup("$db").StringValueOK()
		collection, collOK := doc.Lookup("find").StringValueOK()
		if !dbOK || !collOK {
			r.fail()
			return nil, errReplicaProxyWire
		}
		if database+"."+collection != r.namespace {
			return nil, nil
		}
		filter, ok := doc.Lookup("filter").DocumentOK()
		if !ok {
			r.fail()
			return nil, errReplicaProxyWire
		}
		ids, exactFind, err = readFindIDs(filter)
		if err != nil {
			r.fail()
			return nil, err
		}
	}
	if name == "killSessions" {
		docs, parseErr := operationDocuments(doc.Lookup("killSessions"))
		if parseErr != nil || len(docs) == 0 {
			r.fail()
			return nil, errReplicaProxyWire
		}
		seen := make(map[[32]byte]bool)
		for _, target := range docs {
			value := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: target}
			digest, has, parseErr := readSession(value)
			if parseErr != nil || !has || seen[digest] {
				r.fail()
				return nil, errReplicaProxyWire
			}
			seen[digest] = true
			targets = append(targets, digest)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched *ReadWatch
	for _, w := range r.watches {
		s := &w.snapshot
		match := false
		if name == "find" {
			for _, id := range ids {
				match = match || id == w.id
			}
		}
		if match && !exactFind {
			r.failure = "owned read used ambiguous batch selector"
			return nil, errReplicaProxyWire
		}
		if name == "killSessions" && s.SessionPresent {
			for _, target := range targets {
				match = match || target == s.SessionDigest
			}
		}
		if (name == "getMore" || name == "killCursors") && present && s.SessionPresent && session == s.SessionDigest {
			if name == "getMore" {
				s.GetMore++
			} else {
				s.KillCursors++
			}
			r.failure = "owned read used unexpected cursor command"
			return nil, errReplicaProxyWire
		}
		if !match {
			continue
		}
		if matched != nil {
			r.failure = "ambiguous owned read cleanup"
			return nil, errReplicaProxyWire
		}
		matched = w
	}
	if matched == nil {
		return nil, nil
	}
	w := matched
	s := &w.snapshot
	cleanup := name == "killSessions"
	if cleanup {
		s.CleanupRequests++
	} else {
		s.FindRequests++
		if s.FindRequests == 1 {
			s.SessionPresent = present
			s.SessionDigest = session
			s.Member = opts.member
			s.Connection = opts.connection
			s.RequestID = binary.LittleEndian.Uint32(opts.message[4:8])
		}
	}
	if !w.armed || opts.member != w.member || s.Closed || !s.SessionPresent || (!cleanup && s.FindRequests != 1) || (cleanup && (s.CleanupRequests != 1 || len(targets) != 1)) {
		r.failure = "owned read identity, member or replay mismatch"
		return nil, errReplicaProxyWire
	}
	if opts.tail != nil {
		if opts.tail.client != nil {
			if cleanup {
				s.CleanupPeer = opts.tail.client.RemoteAddr().String()
			} else {
				s.FindPeer = opts.tail.client.RemoteAddr().String()
			}
		}
		for _, tail := range w.tails {
			if tail == opts.tail {
				r.failure = "owned read reused blocked relay"
				return nil, errReplicaProxyWire
			}
		}
		w.tails = append(w.tails, opts.tail)
		s.Tails++
	}
	match := &readMatch{watch: w, cleanup: cleanup, requestID: binary.LittleEndian.Uint32(opts.message[4:8])}
	return match, nil
}
func (r *readRegistry) forwarded(m *readMatch, written, total int) bool {
	if m == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if written != total {
		r.failure = "owned read partial upstream write"
		return false
	}
	if m.cleanup {
		m.watch.snapshot.CleanupWrites++
	} else {
		m.watch.snapshot.FindWrites++
	}
	return true
}
func (r *readRegistry) received(m *readMatch, response []byte) bool {
	if m == nil {
		return false
	}
	doc, _, err := replicaWireDocument(response, true)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || uniqueOperationDocument(doc) != nil || binary.LittleEndian.Uint32(response[8:12]) != m.requestID || !operationNumber(doc.Lookup("ok"), 1) || doc.Lookup("writeConcernError").Type != 0 {
		r.failure = "owned read reply correlation or success invalid"
		return false
	}
	s := &m.watch.snapshot
	if m.cleanup {
		s.CleanupReplies++
		s.CleanupReplyDigest = sha256.Sum256(doc)
		if s.CleanupHeld {
			r.failure = "duplicate cleanup reply"
			return false
		}
		s.CleanupHeld = true
		close(m.watch.cleanupHeld)
	} else {
		cursor, ok := doc.Lookup("cursor").DocumentOK()
		if !ok || uniqueOperationDocument(cursor) != nil {
			r.failure = "invalid owned find cursor"
			return false
		}
		_, idOK := cursor.Lookup("id").Int64OK()
		namespace, nsOK := cursor.Lookup("ns").StringValueOK()
		_, batchErr := operationDocuments(cursor.Lookup("firstBatch"))
		if !idOK || !nsOK || namespace != r.namespace || batchErr != nil {
			r.failure = "invalid owned find reply"
			return false
		}
		s.FindReplies++
		s.FindReplyDigest = sha256.Sum256(doc)
		if s.Held {
			r.failure = "duplicate find reply"
			return false
		}
		s.Held = true
		close(m.watch.held)
	}
	return true
}
func (r *readRegistry) wait(m *readMatch, done <-chan struct{}, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-m.watch.close:
	case <-done:
		r.mu.Lock()
		if !m.watch.snapshot.Closed {
			r.failure = "owned read observer interrupted before targeted join"
		}
		r.mu.Unlock()
	case <-timer.C:
		r.mu.Lock()
		m.watch.snapshot.Expired = true
		r.failure = "owned read observer watchdog expired"
		r.mu.Unlock()
	}
}
func (r *readRegistry) joined(tail *readTail) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.watches {
		for _, owned := range w.tails {
			if owned == tail {
				w.snapshot.Joined++
			}
		}
	}
	close(tail.done)
}
