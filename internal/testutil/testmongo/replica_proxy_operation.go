package testmongo

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const ownedOperationLimit = 8
const ownedItemLimit = 64

// OwnedOperation identifies business identity, not a connection, session or wire ID.
// Kind is bulkWrite or findAndModify. Controls may observe a real business error
// without arming reply loss; armed operations require the exact increment below.
type OwnedOperation struct{ Namespace, ID, Kind string }

type OperationSnapshot struct {
	MatchedRequests, ForwardedItems                                              int
	Member, Connection                                                           int
	RequestID                                                                    uint32
	SessionPresent, TransactionPresent                                           bool
	SessionDigest                                                                [32]byte
	Transaction                                                                  int64
	RequestValid, ReplyValid, ReplyReceived, Held, DropPerformed, ReplyForwarded bool
	ReplyDigest                                                                  [32]byte
	Error                                                                        string
}

type OperationWatch struct {
	registry               *operationRegistry
	operation              OwnedOperation
	snapshot               OperationSnapshot
	armedMember            int
	armed                  bool
	held, release, dropped chan struct{}
	dropRequested          bool
}

type operationRegistry struct {
	mu      sync.Mutex
	watches []*OperationWatch
	failure string
}
type operationItem struct {
	namespace, id string
	increment     bool
}
type operationRequest struct {
	command            string
	items              []operationItem
	majority           bool
	session            bson.Raw
	transaction        int64
	transactionPresent bool
}
type operationMatch struct {
	watch *OperationWatch
	items int
	armed bool
}

func (r *operationRegistry) watch(operation OwnedOperation) (*OperationWatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.watches) >= ownedOperationLimit || len(operation.Namespace) > 256 || len(operation.ID) == 0 || len(operation.ID) > 128 || !strings.Contains(operation.Namespace, ".") || operation.Kind != "bulkWrite" && operation.Kind != "findAndModify" {
		return nil, errReplicaProxyWire
	}
	for _, w := range r.watches {
		if w.operation.Namespace == operation.Namespace && w.operation.ID == operation.ID {
			return nil, errReplicaProxyWire
		}
	}
	w := &OperationWatch{registry: r, operation: operation, armedMember: -1, held: make(chan struct{}), release: make(chan struct{}), dropped: make(chan struct{})}
	r.watches = append(r.watches, w)
	return w, nil
}

func (w *OperationWatch) Snapshot() OperationSnapshot {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	s := w.snapshot
	if w.registry.failure != "" {
		s.Error = w.registry.failure
	}
	return s
}
func (w *OperationWatch) Held() <-chan struct{}    { return w.held }
func (w *OperationWatch) Dropped() <-chan struct{} { return w.dropped }

// DropReply requests deliberate loss only after a real upstream reply is held.
// Cleanup uses Proxy.Close instead and cannot set DropPerformed.
func (w *OperationWatch) DropReply() error {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	if !w.snapshot.Held || w.dropRequested || w.snapshot.Error != "" || w.registry.failure != "" {
		return errReplicaProxyWire
	}
	w.dropRequested = true
	close(w.release)
	return nil
}
func (r *operationRegistry) arm(member int, w *OperationWatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if member < 0 || member >= 3 || w == nil || w.registry != r || w.armed || w.snapshot.MatchedRequests != 0 {
		return errReplicaProxyWire
	}
	w.armed = true
	w.armedMember = member
	return nil
}
func (r *operationRegistry) fail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failure = "owned operation observation failed closed"
}

func uniqueOperationDocument(doc bson.Raw) error {
	fields, err := doc.Elements()
	if err != nil {
		return errReplicaProxyWire
	}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		if seen[field.Key()] {
			return errReplicaProxyWire
		}
		seen[field.Key()] = true
	}
	return nil
}
func operationDocuments(value bson.RawValue) ([]bson.Raw, error) {
	array, ok := value.ArrayOK()
	if !ok {
		return nil, errReplicaProxyWire
	}
	fields, err := bson.Raw(array).Elements()
	if err != nil || len(fields) > ownedItemLimit {
		return nil, errReplicaProxyWire
	}
	docs := make([]bson.Raw, 0, len(fields))
	for i, field := range fields {
		doc, ok := field.Value().DocumentOK()
		if !ok || field.Key() != strconv.Itoa(i) || uniqueOperationDocument(doc) != nil {
			return nil, errReplicaProxyWire
		}
		docs = append(docs, doc)
	}
	return docs, nil
}
func operationSequences(message []byte, doc bson.Raw) (map[string][]bson.Raw, error) {
	sequences := make(map[string][]bson.Raw, 2)
	for _, key := range []string{"ops", "nsInfo"} {
		if value := doc.Lookup(key); value.Type != 0 {
			docs, err := operationDocuments(value)
			if err != nil {
				return nil, err
			}
			sequences[key] = docs
		}
	}
	if binary.LittleEndian.Uint32(message[12:16]) != 2013 {
		return sequences, nil
	}
	end := 21 + len(doc)
	for end < len(message) {
		start := end + 1
		size := int(binary.LittleEndian.Uint32(message[start:]))
		limit := start + size
		z := bytes.IndexByte(message[start+4:limit], 0) + start + 4
		key := string(message[start+4 : z])
		if _, exists := sequences[key]; exists {
			return nil, errReplicaProxyWire
		}
		docs := make([]bson.Raw, 0)
		position := z + 1
		for position < limit {
			d, next, err := replicaBSONAt(message[:limit], position)
			if err != nil || len(docs) >= ownedItemLimit || uniqueOperationDocument(d) != nil {
				return nil, errReplicaProxyWire
			}
			docs = append(docs, d)
			position = next
		}
		sequences[key] = docs
		end = limit
	}
	return sequences, nil
}
func operationID(doc bson.Raw) (string, error) {
	if uniqueOperationDocument(doc) != nil {
		return "", errReplicaProxyWire
	}
	fields, _ := doc.Elements()
	if len(fields) != 1 || fields[0].Key() != "_id" {
		return "", errReplicaProxyWire
	}
	id, ok := fields[0].Value().StringValueOK()
	if !ok || len(id) == 0 || len(id) > 128 {
		return "", errReplicaProxyWire
	}
	return id, nil
}
func operationIncrement(update bson.Raw) bool {
	if uniqueOperationDocument(update) != nil {
		return false
	}
	fields, _ := update.Elements()
	if len(fields) != 1 || fields[0].Key() != "$inc" {
		return false
	}
	amount, ok := fields[0].Value().DocumentOK()
	if !ok || uniqueOperationDocument(amount) != nil {
		return false
	}
	fields, _ = amount.Elements()
	return len(fields) == 1 && fields[0].Key() == "n" && operationNumber(fields[0].Value(), 1)
}
func operationOptionalBool(value bson.RawValue) (bool, error) {
	if value.Type == 0 {
		return false, nil
	}
	flag, ok := value.BooleanOK()
	if !ok {
		return false, errReplicaProxyWire
	}
	return flag, nil
}
func operationNumber(v bson.RawValue, n int64) bool {
	switch v.Type {
	case bson.TypeInt32:
		return int64(v.Int32()) == n
	case bson.TypeInt64:
		return v.Int64() == n
	case bson.TypeDouble:
		return v.Double() == float64(n)
	}
	return false
}

func parseOperationRequest(message []byte) (operationRequest, error) {
	result := operationRequest{}
	doc, _, err := replicaWireDocument(message, false)
	if err != nil || uniqueOperationDocument(doc) != nil {
		return result, errReplicaProxyWire
	}
	fields, _ := doc.Elements()
	result.command = fields[0].Key()
	if result.command != "bulkWrite" && result.command != "findAndModify" {
		return result, nil
	}
	database, ok := doc.Lookup("$db").StringValueOK()
	if !ok || len(database) > 128 {
		return result, errReplicaProxyWire
	}
	if concern, ok := doc.Lookup("writeConcern").DocumentOK(); ok {
		if uniqueOperationDocument(concern) != nil {
			return result, errReplicaProxyWire
		}
		w, ok := concern.Lookup("w").StringValueOK()
		result.majority = ok && w == "majority"
	}
	if value := doc.Lookup("lsid"); value.Type != 0 {
		session, ok := value.DocumentOK()
		if !ok || uniqueOperationDocument(session) != nil {
			return result, errReplicaProxyWire
		}
		result.session = session
	}
	if value := doc.Lookup("txnNumber"); value.Type != 0 {
		n, ok := value.Int64OK()
		if !ok {
			return result, errReplicaProxyWire
		}
		result.transaction = n
		result.transactionPresent = true
	}
	if result.command == "findAndModify" {
		collection, ok := doc.Lookup("findAndModify").StringValueOK()
		if !ok {
			return result, errReplicaProxyWire
		}
		query, ok := doc.Lookup("query").DocumentOK()
		if !ok {
			return result, errReplicaProxyWire
		}
		id, err := operationID(query)
		if err != nil {
			return result, err
		}
		update, ok := doc.Lookup("update").DocumentOK()
		if !ok {
			return result, errReplicaProxyWire
		}
		newer, newOK := doc.Lookup("new").BooleanOK()
		if !newOK {
			return result, errReplicaProxyWire
		}
		upsert, err := operationOptionalBool(doc.Lookup("upsert"))
		if err != nil {
			return result, err
		}
		item := operationItem{namespace: database + "." + collection, id: id, increment: newer && !upsert && operationIncrement(update)}
		result.items = append(result.items, item)
		return result, nil
	}
	sequences, err := operationSequences(message, doc)
	if err != nil {
		return result, err
	}
	spaces, ops := sequences["nsInfo"], sequences["ops"]
	if len(spaces) == 0 || len(ops) == 0 {
		return result, errReplicaProxyWire
	}
	for _, op := range ops {
		var key string
		count := 0
		for _, candidate := range []string{"update", "insert", "delete"} {
			if op.Lookup(candidate).Type != 0 {
				key = candidate
				count++
			}
		}
		index := op.Lookup(key)
		if count != 1 || index.Type != bson.TypeInt32 && index.Type != bson.TypeInt64 {
			return result, errReplicaProxyWire
		}
		n := index.AsInt64()
		if n < 0 || n >= int64(len(spaces)) {
			return result, errReplicaProxyWire
		}
		namespace, ok := spaces[n].Lookup("ns").StringValueOK()
		if !ok || len(namespace) > 256 {
			return result, errReplicaProxyWire
		}
		var id string
		increment := false
		if key == "insert" {
			document, ok := op.Lookup("document").DocumentOK()
			if !ok || uniqueOperationDocument(document) != nil {
				return result, errReplicaProxyWire
			}
			id, ok = document.Lookup("_id").StringValueOK()
			if !ok {
				return result, errReplicaProxyWire
			}
		} else {
			filter, ok := op.Lookup("filter").DocumentOK()
			if !ok {
				return result, errReplicaProxyWire
			}
			id, err = operationID(filter)
			if err != nil {
				return result, err
			}
			if key == "update" {
				update, ok := op.Lookup("updateMods").DocumentOK()
				if !ok {
					return result, errReplicaProxyWire
				}
				multi, multiOK := op.Lookup("multi").BooleanOK()
				upsert, err := operationOptionalBool(op.Lookup("upsert"))
				if err != nil {
					return result, err
				}
				increment = multiOK && !multi && !upsert && operationIncrement(update)
			}
		}
		item := operationItem{namespace: namespace, id: id, increment: increment}
		result.items = append(result.items, item)
	}
	return result, nil
}

type operationBeginOptions struct {
	message            []byte
	member, connection int
}

func (r *operationRegistry) begin(opts operationBeginOptions) ([]operationMatch, error) {
	request, err := parseOperationRequest(opts.message)
	if err != nil {
		r.fail()
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	matches := make([]operationMatch, 0, len(r.watches))
	for _, w := range r.watches {
		count := 0
		valid := request.majority && request.command == w.operation.Kind
		for _, item := range request.items {
			if item.namespace == w.operation.Namespace && item.id == w.operation.ID {
				count++
				valid = valid && item.increment
			}
		}
		if count == 0 {
			continue
		}
		s := &w.snapshot
		s.MatchedRequests++
		s.Member = opts.member
		s.Connection = opts.connection + 1
		s.RequestID = binary.LittleEndian.Uint32(opts.message[4:8])
		s.SessionPresent = request.session != nil
		s.TransactionPresent = request.transactionPresent
		s.Transaction = request.transaction
		if request.session != nil {
			s.SessionDigest = sha256.Sum256(request.session)
		}
		s.RequestValid = valid && count == 1 && len(request.items) == 1
		armed := w.armed && s.MatchedRequests == 1
		if armed && (opts.member != w.armedMember || !s.RequestValid) {
			s.Error = "armed operation request did not prove exact majority increment"
			return nil, errReplicaProxyWire
		}
		match := operationMatch{watch: w, items: count, armed: armed}
		matches = append(matches, match)
	}
	return matches, nil
}
func (r *operationRegistry) forwarded(matches []operationMatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range matches {
		m.watch.snapshot.ForwardedItems += m.items
	}
}

// Successful majority proof is command-specific; ok:1 alone is insufficient.
func operationSuccessfulReply(operation OwnedOperation, reply bson.Raw) bool {
	if uniqueOperationDocument(reply) != nil || !operationNumber(reply.Lookup("ok"), 1) {
		return false
	}
	for _, key := range []string{"writeConcernError", "writeConcernErrors", "writeErrors", "error", "code"} {
		if reply.Lookup(key).Type != 0 {
			return false
		}
	}
	if operation.Kind == "findAndModify" {
		last, ok := reply.Lookup("lastErrorObject").DocumentOK()
		if !ok || uniqueOperationDocument(last) != nil || !operationNumber(last.Lookup("n"), 1) || last.Lookup("upserted").Type != 0 {
			return false
		}
		existing, ok := last.Lookup("updatedExisting").BooleanOK()
		if !ok || !existing {
			return false
		}
		value, ok := reply.Lookup("value").DocumentOK()
		if !ok || uniqueOperationDocument(value) != nil {
			return false
		}
		id, ok := value.Lookup("_id").StringValueOK()
		return ok && id == operation.ID && operationNumber(value.Lookup("n"), 1)
	}
	for _, key := range []string{"nErrors", "nInserted", "nDeleted", "nUpserted"} {
		if !operationNumber(reply.Lookup(key), 0) {
			return false
		}
	}
	if !operationNumber(reply.Lookup("nMatched"), 1) || !operationNumber(reply.Lookup("nModified"), 1) {
		return false
	}
	cursor, ok := reply.Lookup("cursor").DocumentOK()
	if !ok || uniqueOperationDocument(cursor) != nil || !operationNumber(cursor.Lookup("id"), 0) {
		return false
	}
	namespace, ok := cursor.Lookup("ns").StringValueOK()
	if !ok || namespace != "admin.$cmd.bulkWrite" {
		return false
	}
	items, err := operationDocuments(cursor.Lookup("firstBatch"))
	if err != nil || len(items) != 1 {
		return false
	}
	item := items[0]
	if item.Lookup("code").Type != 0 || item.Lookup("errmsg").Type != 0 || item.Lookup("upserted").Type != 0 || item.Lookup("writeConcernError").Type != 0 {
		return false
	}
	return operationNumber(item.Lookup("ok"), 1) && operationNumber(item.Lookup("idx"), 0) && operationNumber(item.Lookup("n"), 1) && operationNumber(item.Lookup("nModified"), 1)
}
func (r *operationRegistry) received(matches []operationMatch, reply bson.Raw) *OperationWatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	var held *OperationWatch
	for _, m := range matches {
		w := m.watch
		s := &w.snapshot
		s.ReplyReceived = true
		s.ReplyDigest = sha256.Sum256(reply)
		s.ReplyValid = operationSuccessfulReply(w.operation, reply)
		if m.armed {
			if held != nil || !s.ReplyValid {
				s.Error = "held upstream reply failed successful majority proof"
				continue
			}
			held = w
			s.Held = true
			close(w.held)
		}
	}
	return held
}
func (r *operationRegistry) finish(matches []operationMatch, drop bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range matches {
		s := &m.watch.snapshot
		if drop && m.armed {
			s.DropPerformed = true
			close(m.watch.dropped)
		} else if !drop {
			s.ReplyForwarded = true
		}
	}
}
func (w *OperationWatch) gateFailed() {
	w.registry.mu.Lock()
	defer w.registry.mu.Unlock()
	w.snapshot.Error = "held reply gate interrupted or timed out without deliberate drop"
}
