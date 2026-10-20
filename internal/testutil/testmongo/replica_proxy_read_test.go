package testmongo

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func readTestSession(n byte) bson.D {
	data := make([]byte, 16)
	data[0] = n
	uuid := bson.Binary{Subtype: 4, Data: data}
	session := bson.D{{Key: "id", Value: uuid}}
	return session
}
func readTestFind() bson.D {
	filter := bson.D{{Key: "_id", Value: "held"}}
	session := readTestSession(1)
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "lsid", Value: session}, {Key: "$db", Value: "owned"}}
	return command
}
func readTestCleanup(target bson.D) bson.D {
	envelope := readTestSession(2)
	command := bson.D{{Key: "killSessions", Value: bson.A{target}}, {Key: "lsid", Value: envelope}, {Key: "$db", Value: "admin"}}
	return command
}
func readTestReply() bson.D {
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "owned.records"}, {Key: "firstBatch", Value: bson.A{}}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
	return reply
}
func readTestRegistry(t *testing.T) (*readRegistry, *ReadWatch) {
	t.Helper()
	r := &readRegistry{namespace: "owned.records"}
	w, err := r.watch("held")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.arm(1, w); err != nil {
		t.Fatal(err)
	}
	return r, w
}
func readTestBegin(t *testing.T, r *readRegistry, command bson.D) *readMatch {
	t.Helper()
	opts := readBeginOptions{message: ownedMessage(t, command), member: 1, connection: 7}
	m, err := r.begin(opts)
	if err != nil || m == nil {
		t.Fatal("read begin", err)
	}
	if !r.forwarded(m, len(opts.message), len(opts.message)) {
		t.Fatal("read forward")
	}
	return m
}
func readTestResponse(t *testing.T, doc bson.D) []byte {
	t.Helper()
	response := ownedMessage(t, doc)
	binary.LittleEndian.PutUint32(response[8:12], 1)
	return response
}
func TestReplicaProxyReadIdentityAndCleanupTarget(t *testing.T) {
	r, w := readTestRegistry(t)
	find := readTestFind()
	m := readTestBegin(t, r, find)
	reply := readTestReply()
	if !r.received(m, readTestResponse(t, reply)) {
		t.Fatal(w.Snapshot())
	}
	first := w.Snapshot()
	// The cleanup envelope has a different implicit session. Only its target
	// array identifies the original explicit find session.
	wrong := readTestSession(3)
	command := readTestCleanup(wrong)
	opts := readBeginOptions{message: ownedMessage(t, command), member: 1}
	other, err := r.begin(opts)
	if err != nil || other != nil {
		t.Fatal("wrong target matched", err)
	}
	target := readTestSession(1)
	command = readTestCleanup(target)
	cleanup := readTestBegin(t, r, command)
	ok := bson.D{{Key: "ok", Value: 1}}
	if !r.received(cleanup, readTestResponse(t, ok)) {
		t.Fatal(w.Snapshot())
	}
	s := w.Snapshot()
	if s.FindRequests != 1 || s.FindWrites != 1 || s.FindReplies != 1 || s.FindForwarded != 0 || s.CleanupRequests != 1 || s.CleanupWrites != 1 || s.CleanupReplies != 1 || s.CleanupForwarded != 0 || !s.Held || !s.CleanupHeld || !s.SessionPresent || s.SessionDigest != first.SessionDigest || s.Error != "" {
		t.Fatal(s)
	}
	// A replay with a new session must not overwrite first-find identity.
	find[2].Value = readTestSession(4)
	opts.message = ownedMessage(t, find)
	_, err = r.begin(opts)
	if err == nil || w.Snapshot().SessionDigest != first.SessionDigest || w.Snapshot().FindRequests != 2 {
		t.Fatal("replay/immutable identity", w.Snapshot())
	}
}
func TestReplicaProxyReadRejectsInvalidBoundaries(t *testing.T) {
	for _, name := range []string{"missing_session", "wrong_member", "duplicate_field", "partial_write", "wrong_correlation", "failed_reply", "missing_target", "duplicate_target", "owned_getMore", "owned_killCursors"} {
		t.Run(name, func(t *testing.T) {
			r, w := readTestRegistry(t)
			command := readTestFind()
			opts := readBeginOptions{member: 1}
			switch name {
			case "missing_session":
				command = append(command[:2], command[3:]...)
			case "wrong_member":
				opts.member = 2
			case "duplicate_field":
				command = append(command, command[1])
			}
			opts.message = ownedMessage(t, command)
			m, err := r.begin(opts)
			if name == "missing_session" || name == "wrong_member" || name == "duplicate_field" {
				if err == nil || w.Snapshot().Error == "" {
					t.Fatal("invalid request accepted")
				}
				return
			}
			if err != nil || m == nil {
				t.Fatal(err)
			}
			if name == "partial_write" {
				if r.forwarded(m, len(opts.message)-1, len(opts.message)) || w.Snapshot().FindWrites != 0 {
					t.Fatal("partial write accepted")
				}
				return
			}
			if !r.forwarded(m, len(opts.message), len(opts.message)) {
				t.Fatal("forward")
			}
			reply := readTestReply()
			response := readTestResponse(t, reply)
			switch name {
			case "wrong_correlation":
				binary.LittleEndian.PutUint32(response[8:12], 99)
			case "failed_reply":
				reply[0].Value = 0
				response = readTestResponse(t, reply)
			default:
				switch name {
				case "missing_target":
					command = bson.D{{Key: "killSessions", Value: bson.A{}}, {Key: "$db", Value: "admin"}}
				case "duplicate_target":
					target := readTestSession(1)
					command = readTestCleanup(target)
					command[0].Value = bson.A{target, target}
				case "owned_getMore", "owned_killCursors":
					name := "getMore"
					if strings.HasSuffix(t.Name(), "owned_killCursors") {
						name = "killCursors"
					}
					session := readTestSession(1)
					command = bson.D{{Key: name, Value: int64(1)}, {Key: "lsid", Value: session}, {Key: "$db", Value: "owned"}}
				}
				opts.message = ownedMessage(t, command)
				_, err = r.begin(opts)
				if err == nil || w.Snapshot().Error == "" {
					t.Fatal("invalid cleanup accepted", w.Snapshot())
				}
				return
			}
			if r.received(m, response) || w.Snapshot().Error == "" {
				t.Fatal("invalid reply accepted", w.Snapshot())
			}
		})
	}
}
func TestReplicaProxyReadWatchdogCannotProveRetirement(t *testing.T) {
	r, w := readTestRegistry(t)
	command := readTestFind()
	m := readTestBegin(t, r, command)
	reply := readTestReply()
	if !r.received(m, readTestResponse(t, reply)) {
		t.Fatal("reply")
	}
	r.wait(m, make(chan struct{}), time.Millisecond)
	if s := w.Snapshot(); !s.Expired || s.Error == "" || s.FindForwarded != 0 || s.Joined != 0 {
		t.Fatal(s)
	}
}
func TestReplicaProxyReadTargetTailJoinPreservesHealthyRelay(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	replyDoc := readTestReply()
	response := readTestResponse(t, replyDoc)
	upstreamDone := make(chan error, 2)
	go func() {
		for range 2 {
			conn, err := listener.Accept()
			if err != nil {
				upstreamDone <- err
				return
			}
			go func() {
				defer conn.Close()
				for {
					request, err := readMessage(conn)
					if err != nil {
						upstreamDone <- nil
						return
					}
					reply := append([]byte(nil), response...)
					copy(reply[8:12], request[4:8])
					if _, err = conn.Write(reply); err != nil {
						upstreamDone <- err
						return
					}
				}
			}()
		}
	}()
	r, w := readTestRegistry(t)
	opts := proxyOptions{uri: "mongodb://" + listener.Addr().String(), reads: r, memberIndex: 1}
	proxy := startProxy(t, opts)
	address := strings.TrimPrefix(proxy.URI(), "mongodb://")
	held, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	healthy, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	command := readTestFind()
	message := ownedMessage(t, command)
	if _, err = held.Write(message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.Held():
	case <-time.After(time.Second):
		t.Fatal("hold", w.Snapshot())
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := w.CloseHeldRelays(ctx); err != nil {
		t.Fatal(err)
	}
	_ = held.SetReadDeadline(time.Now().Add(time.Second))
	var data [1]byte
	if n, err := held.Read(data[:]); n != 0 || err == nil {
		t.Fatal("held reply forwarded")
	}
	command[1].Value = bson.D{{Key: "_id", Value: "healthy"}}
	message = ownedMessage(t, command)
	if _, err = healthy.Write(message); err != nil {
		t.Fatal(err)
	}
	_ = healthy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = readMessage(healthy); err != nil {
		t.Fatal("unrelated relay closed", err)
	}
	s := w.Snapshot()
	if s.FindRequests != 1 || s.FindWrites != 1 || s.FindReplies != 1 || s.Tails != 1 || s.Joined != 1 || !s.Closed || s.Expired || s.Error != "" {
		t.Fatal(s)
	}
	proxy.Close()
	for range 2 {
		select {
		case err := <-upstreamDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("upstream not joined")
		}
	}
}

func TestReplicaProxyReadUnrelatedProgramSelectorIsolation(t *testing.T) {
	r, w := readTestRegistry(t)
	command := readTestFind()
	selector := bson.D{{Key: "$in", Value: bson.A{"healthy"}}}
	filter := bson.D{{Key: "_id", Value: selector}}
	command[1].Value = filter
	opts := readBeginOptions{message: ownedMessage(t, command), member: 1}
	m, err := r.begin(opts)
	if err != nil || m != nil || w.Snapshot().FindRequests != 0 || w.Snapshot().Error != "" {
		t.Fatal("unrelated healthy Lua read intercepted", err, w.Snapshot())
	}
	selector[0].Value = bson.A{"held"}
	filter[0].Value = selector
	command[1].Value = filter
	opts.message = ownedMessage(t, command)
	_, err = r.begin(opts)
	if err == nil || w.Snapshot().Error == "" {
		t.Fatal("ambiguous watched read accepted", w.Snapshot())
	}
}

func TestReplicaProxyReadNamespaceAndAmbiguousSelectors(t *testing.T) {
	for _, name := range []string{"other_namespace", "duplicate_filter", "watched_batch_replay", "duplicate_watched_batch"} {
		t.Run(name, func(t *testing.T) {
			r, w := readTestRegistry(t)
			command := readTestFind()
			if name == "other_namespace" {
				command[3].Value = "unrelated"
			}
			if name == "duplicate_filter" {
				command = append(command, command[1])
			}
			if name == "watched_batch_replay" || name == "duplicate_watched_batch" {
				first := readTestBegin(t, r, command)
				_ = first
				ids := bson.A{"held", "healthy"}
				if name == "duplicate_watched_batch" {
					ids = bson.A{"held", "held"}
				}
				selector := bson.D{{Key: "$in", Value: ids}}
				filter := bson.D{{Key: "_id", Value: selector}}
				command[1].Value = filter
			}
			before := w.Snapshot()
			opts := readBeginOptions{message: ownedMessage(t, command), member: 1}
			matched, err := r.begin(opts)
			if name == "other_namespace" {
				if matched != nil || err != nil || w.Snapshot().Error != "" {
					t.Fatal("unrelated namespace intercepted")
				}
				return
			}
			if err == nil || w.Snapshot().Error == "" || w.Snapshot().SessionDigest != before.SessionDigest {
				t.Fatal("ambiguous watched selector evaded", w.Snapshot(), err)
			}
		})
	}
}
