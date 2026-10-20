package testmongo

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func ownedRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func ownedMessage(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw := ownedRaw(t, doc)
	message := make([]byte, 21)
	binary.LittleEndian.PutUint32(message[4:8], 1)
	binary.LittleEndian.PutUint32(message[12:16], 2013)
	message = append(message, raw...)
	binary.LittleEndian.PutUint32(message, uint32(len(message)))
	return message
}
func ownedBulk(t *testing.T) bson.D {
	t.Helper()
	filter := bson.D{{Key: "_id", Value: "old"}}
	amount := bson.D{{Key: "n", Value: 1}}
	update := bson.D{{Key: "$inc", Value: amount}}
	item := bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "updateMods", Value: update}, {Key: "multi", Value: false}}
	namespace := bson.D{{Key: "ns", Value: "owned.records"}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "bulkWrite", Value: 1}, {Key: "ops", Value: bson.A{item}}, {Key: "nsInfo", Value: bson.A{namespace}}, {Key: "writeConcern", Value: concern}, {Key: "$db", Value: "admin"}}
	return command
}
func ownedReply() bson.D {
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0}, {Key: "nDeleted", Value: 0}, {Key: "nUpserted", Value: 0}, {Key: "nMatched", Value: 1}, {Key: "nModified", Value: 1}, {Key: "cursor", Value: cursor}}
	return reply
}
func ownedSequence(t *testing.T, command bson.D) []byte {
	t.Helper()
	body := bson.D{}
	sequences := make([]bson.E, 0, 2)
	for _, field := range command {
		if field.Key == "ops" || field.Key == "nsInfo" {
			sequences = append(sequences, field)
		} else {
			body = append(body, field)
		}
	}
	message := ownedMessage(t, body)
	for _, field := range sequences {
		start := len(message)
		message = append(message, 1, 0, 0, 0, 0)
		message = append(message, field.Key...)
		message = append(message, 0)
		for _, item := range field.Value.(bson.A) {
			message = append(message, ownedRaw(t, item.(bson.D))...)
		}
		binary.LittleEndian.PutUint32(message[start+1:], uint32(len(message)-start-1))
	}
	binary.LittleEndian.PutUint32(message, uint32(len(message)))
	return message
}
func TestReplicaProxyOwnedOperation(t *testing.T) {
	operation := OwnedOperation{Namespace: "owned.records", ID: "old", Kind: "bulkWrite"}
	for _, sequence := range []bool{false, true} {
		name := "inline"
		if sequence {
			name = "sequences"
		}
		t.Run(name, func(t *testing.T) {
			command := ownedBulk(t)
			message := ownedMessage(t, command)
			if sequence {
				message = ownedSequence(t, command)
			}
			registry := &operationRegistry{}
			watch, err := registry.watch(operation)
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.arm(1, watch); err != nil {
				t.Fatal(err)
			}
			opts := operationBeginOptions{message: message, member: 1, connection: 4}
			matches, err := registry.begin(opts)
			if err != nil {
				t.Fatal(err)
			}
			registry.forwarded(matches)
			reply := ownedReply()
			held := registry.received(matches, ownedRaw(t, reply))
			if held != watch {
				t.Fatal("successful majority reply not held")
			}
			snapshot := watch.Snapshot()
			if snapshot.DropPerformed || snapshot.ReplyForwarded || snapshot.SessionPresent || !snapshot.RequestValid || !snapshot.ReplyValid {
				t.Fatal(snapshot)
			}
			if err := watch.DropReply(); err != nil {
				t.Fatal(err)
			}
			if watch.Snapshot().DropPerformed {
				t.Fatal("drop intent was treated as performed")
			}
			registry.finish(matches, true)
			if !watch.Snapshot().DropPerformed {
				t.Fatal("performed drop missing")
			}
			healthy := ownedBulk(t)
			healthyItem := healthy[1].Value.(bson.A)[0].(bson.D)
			healthyFilter := bson.D{{Key: "_id", Value: "new-healthy-id"}}
			healthyItem[1].Value = healthyFilter
			healthyOptions := operationBeginOptions{message: ownedMessage(t, healthy), member: 0, connection: 6}
			healthyMatches, err := registry.begin(healthyOptions)
			if err != nil || len(healthyMatches) != 0 {
				t.Fatal("healthy identity contaminated persistent old watch", err)
			}
			registry.forwarded(healthyMatches)
			if watch.Snapshot().MatchedRequests != 1 || watch.Snapshot().ForwardedItems != 1 {
				t.Fatal("healthy work reset old watch")
			}
			// A different member, wire ID and session is still the same old operation.
			session := bson.D{{Key: "id", Value: "different"}}
			sessionField := bson.E{Key: "lsid", Value: session}
			command = append(command, sessionField)
			transactionField := bson.E{Key: "txnNumber", Value: int64(99)}
			command = append(command, transactionField)
			message = ownedMessage(t, command)
			binary.LittleEndian.PutUint32(message[4:8], 900)
			opts = operationBeginOptions{message: message, member: 2, connection: 8}
			matches, err = registry.begin(opts)
			if err != nil {
				t.Fatal(err)
			}
			registry.forwarded(matches)
			snapshot = watch.Snapshot()
			if snapshot.MatchedRequests != 2 || snapshot.ForwardedItems != 2 || !snapshot.SessionPresent || !snapshot.TransactionPresent || snapshot.Member != 2 {
				t.Fatal(snapshot)
			}
		})
	}
	t.Run("duplicate_items", func(t *testing.T) {
		registry := &operationRegistry{}
		watch, err := registry.watch(operation)
		if err != nil {
			t.Fatal(err)
		}
		command := ownedBulk(t)
		items := command[1].Value.(bson.A)
		command[1].Value = append(items, items[0])
		opts := operationBeginOptions{message: ownedMessage(t, command)}
		matches, err := registry.begin(opts)
		if err != nil {
			t.Fatal(err)
		}
		registry.forwarded(matches)
		snapshot := watch.Snapshot()
		if snapshot.MatchedRequests != 1 || snapshot.ForwardedItems != 2 || snapshot.RequestValid {
			t.Fatal(snapshot)
		}
	})
	t.Run("bounded_persistent", func(t *testing.T) {
		registry := &operationRegistry{}
		for i := 0; i < ownedOperationLimit; i++ {
			spec := operation
			spec.ID = string(rune('a' + i))
			if _, err := registry.watch(spec); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := registry.watch(operation); err == nil {
			t.Fatal("watch overflow accepted")
		}
	})
	t.Run("cleanup_not_injection", func(t *testing.T) {
		registry := &operationRegistry{}
		watch, err := registry.watch(operation)
		if err != nil {
			t.Fatal(err)
		}
		if watch.DropReply() == nil {
			t.Fatal("unheld drop accepted")
		}
		watch.gateFailed()
		if watch.Snapshot().DropPerformed || watch.Snapshot().Error == "" {
			t.Fatal("timeout counted as injection")
		}
	})
}
func TestReplicaProxyOwnedOperationNegative(t *testing.T) {
	tests := []string{"conflicting_sequence", "bad_index", "negative_index", "duplicate_field", "ambiguous_filter", "item_limit", "malformed_sequence", "non_majority", "multi", "upsert", "wrong_increment"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			command := ownedBulk(t)
			item := command[1].Value.(bson.A)[0].(bson.D)
			switch name {
			case "bad_index":
				item[0].Value = int32(1)
			case "negative_index":
				item[0].Value = int32(-1)
			case "duplicate_field":
				item = append(item, item[0])
			case "ambiguous_filter":
				filter := item[1].Value.(bson.D)
				extraFilter := bson.E{Key: "n", Value: 0}
				filter = append(filter, extraFilter)
				item[1].Value = filter
			case "item_limit":
				items := bson.A{}
				for i := 0; i <= ownedItemLimit; i++ {
					items = append(items, item)
				}
				command[1].Value = items
			case "non_majority":
				concern := bson.D{{Key: "w", Value: 1}}
				command[3].Value = concern
			case "multi":
				item[3].Value = true
			case "upsert":
				upsertField := bson.E{Key: "upsert", Value: true}
				item = append(item, upsertField)
			case "wrong_increment":
				amount := bson.D{{Key: "n", Value: 2}}
				update := bson.D{{Key: "$inc", Value: amount}}
				item[2].Value = update
			}
			if name != "item_limit" {
				command[1].Value = bson.A{item}
			}
			message := ownedMessage(t, command)
			if name == "conflicting_sequence" {
				message = ownedSequence(t, command)
				body := ownedMessage(t, command)
				sequenceBody := bson.D{command[0], command[3], command[4]}
				body = append(body, message[21+len(ownedRaw(t, sequenceBody)):]...)
				binary.LittleEndian.PutUint32(body, uint32(len(body)))
				message = body
			}
			if name == "malformed_sequence" {
				message = ownedSequence(t, command)
				message[len(message)-1] = 1
			}
			registry := &operationRegistry{}
			spec := OwnedOperation{Namespace: "owned.records", ID: "old", Kind: "bulkWrite"}
			watch, err := registry.watch(spec)
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.arm(0, watch); err != nil {
				t.Fatal(err)
			}
			opts := operationBeginOptions{message: message}
			if _, err := registry.begin(opts); err == nil {
				t.Fatal("invalid matching majority request accepted")
			}
		})
	}
}
func TestReplicaProxyOwnedOperationMajorityReply(t *testing.T) {
	spec := OwnedOperation{Namespace: "owned.records", ID: "old", Kind: "bulkWrite"}
	for _, name := range []string{"valid", "item_error", "concern_error", "nonterminal", "wrong_index", "extra_effect", "command_error", "missing_modified"} {
		t.Run(name, func(t *testing.T) {
			reply := ownedReply()
			cursor := reply[7].Value.(bson.D)
			item := cursor[2].Value.(bson.A)[0].(bson.D)
			switch name {
			case "item_error":
				item[0].Value = 0
				errorCode := bson.E{Key: "code", Value: 14}
				item = append(item, errorCode)
			case "concern_error":
				concern := bson.D{{Key: "code", Value: 64}}
				concernField := bson.E{Key: "writeConcernError", Value: concern}
				reply = append(reply, concernField)
			case "nonterminal":
				cursor[0].Value = int64(1)
			case "wrong_index":
				item[1].Value = 1
			case "extra_effect":
				reply[2].Value = 1
			case "command_error":
				reply[0].Value = 0
			case "missing_modified":
				item = item[:3]
			}
			cursor[2].Value = bson.A{item}
			reply[7].Value = cursor
			if operationSuccessfulReply(spec, ownedRaw(t, reply)) != (name == "valid") {
				t.Fatal("successful-majority oracle", name)
			}
		})
	}
	spec.Kind = "findAndModify"
	last := bson.D{{Key: "n", Value: 1}, {Key: "updatedExisting", Value: true}}
	value := bson.D{{Key: "_id", Value: "old"}, {Key: "n", Value: 1}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "lastErrorObject", Value: last}, {Key: "value", Value: value}}
	if !operationSuccessfulReply(spec, ownedRaw(t, reply)) {
		t.Fatal("Native positive")
	}
	value[1].Value = 2
	reply[2].Value = value
	if operationSuccessfulReply(spec, ownedRaw(t, reply)) {
		t.Fatal("Native wrong effect accepted")
	}
}

// Exercise the actual relay's held/drop/join boundary without a database. A
// cleanup interruption closes sockets but must never produce DropPerformed.
func TestReplicaProxyOwnedOperationRelayDrop(t *testing.T) {
	for _, deliberate := range []bool{false, true} {
		name := "cleanup"
		if deliberate {
			name = "deliberate"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			upstreamDone := make(chan error, 1)
			reply := ownedReply()
			response := ownedMessage(t, reply)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					upstreamDone <- err
					return
				}
				defer conn.Close()
				request, err := readMessage(conn)
				if err != nil {
					upstreamDone <- err
					return
				}
				copy(response[8:12], request[4:8])
				_, err = conn.Write(response)
				upstreamDone <- err
			}()
			registry := &operationRegistry{}
			spec := OwnedOperation{Namespace: "owned.records", ID: "old", Kind: "bulkWrite"}
			watch, err := registry.watch(spec)
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.arm(0, watch); err != nil {
				t.Fatal(err)
			}
			opts := proxyOptions{uri: "mongodb://" + listener.Addr().String(), operations: registry}
			proxy := startProxy(t, opts)
			address := strings.TrimPrefix(proxy.URI(), "mongodb://")
			client, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			command := ownedBulk(t)
			message := ownedMessage(t, command)
			if _, err := client.Write(message); err != nil {
				t.Fatal(err)
			}
			select {
			case <-watch.Held():
			case <-time.After(time.Second):
				t.Fatal("actual relay did not hold", watch.Snapshot())
			}
			snapshot := watch.Snapshot()
			if snapshot.DropPerformed || snapshot.ReplyForwarded || !snapshot.ReplyValid {
				t.Fatal(snapshot)
			}
			if deliberate {
				if err := watch.DropReply(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-watch.Dropped():
				case <-time.After(time.Second):
					t.Fatal("actual deliberate drop missing")
				}
			} else {
				proxy.Close()
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			var data [1]byte
			n, err := client.Read(data[:])
			if n != 0 || err == nil {
				t.Fatal("held reply leaked")
			}
			proxy.Close()
			snapshot = watch.Snapshot()
			if snapshot.DropPerformed != deliberate || !deliberate && snapshot.Error == "" || snapshot.ReplyForwarded || snapshot.MatchedRequests != 1 || snapshot.ForwardedItems != 1 {
				t.Fatal(snapshot)
			}
			select {
			case err := <-upstreamDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("upstream not joined")
			}
		})
	}
}

func TestReplicaProxyOwnedOperationNativeRequest(t *testing.T) {
	for _, name := range []string{"valid", "ambiguous_query", "bad_upsert_type", "wrong_new", "wrong_amount"} {
		t.Run(name, func(t *testing.T) {
			filter := bson.D{{Key: "_id", Value: "old"}}
			amount := bson.D{{Key: "n", Value: 1}}
			if name == "wrong_amount" {
				amount[0].Value = 2
			}
			update := bson.D{{Key: "$inc", Value: amount}}
			concern := bson.D{{Key: "w", Value: "majority"}}
			if name == "ambiguous_query" {
				extraFilter := bson.E{Key: "n", Value: 0}
				filter = append(filter, extraFilter)
			}
			command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}, {Key: "$db", Value: "owned"}}
			if name == "wrong_new" {
				command[3].Value = false
			}
			if name == "bad_upsert_type" {
				upsertField := bson.E{Key: "upsert", Value: "false"}
				command = append(command, upsertField)
			}
			registry := &operationRegistry{}
			spec := OwnedOperation{Namespace: "owned.records", ID: "old", Kind: "findAndModify"}
			watch, err := registry.watch(spec)
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.arm(0, watch); err != nil {
				t.Fatal(err)
			}
			opts := operationBeginOptions{message: ownedMessage(t, command)}
			_, err = registry.begin(opts)
			if (err == nil) != (name == "valid") {
				t.Fatal("Native exact request validation", name, err)
			}
			if name == "valid" {
				snapshot := watch.Snapshot()
				if !snapshot.RequestValid || snapshot.SessionPresent || snapshot.TransactionPresent {
					t.Fatal("Native absent identity was invented", snapshot)
				}
			}
		})
	}
}
