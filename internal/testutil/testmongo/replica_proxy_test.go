package testmongo

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func replicaTestMap(t *testing.T) *replicaAddressMap {
	t.Helper()
	actual := [3]string{"localhost:27011", "127.0.0.1:27012", "127.0.0.1:27013"}
	frontend := [3]string{"localhost:28011", "127.0.0.1:28012", "127.0.0.1:28013"}
	mapping, err := newReplicaAddressMap(actual, frontend)
	if err != nil {
		t.Fatal(err)
	}
	return mapping
}

func replicaTestMessage(t *testing.T, opcode uint32, document bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 16)
	binary.LittleEndian.PutUint32(message[4:8], 42)
	binary.LittleEndian.PutUint32(message[12:16], opcode)
	switch opcode {
	case 2013:
		message = append(message, 0, 0, 0, 0, 0)
	case 2004:
		message = append(message, 4, 0, 0, 0)
		message = append(message, []byte("admin.$cmd\x00")...)
		message = append(message, 0, 0, 0, 0, 255, 255, 255, 255)
	case 1:
		binary.LittleEndian.PutUint32(message[4:8], 73)
		binary.LittleEndian.PutUint32(message[8:12], 42)
		message = append(message, make([]byte, 20)...)
		binary.LittleEndian.PutUint32(message[32:36], 1)
		// MongoDB 8.0.32 returns OP_REPLY AwaitCapable (8), not only flags zero.
		binary.LittleEndian.PutUint32(message[16:20], replicaReplyAwaitCapable)
	default:
		t.Fatal("unsupported test message")
	}
	message = append(message, raw...)
	binary.LittleEndian.PutUint32(message, uint32(len(message)))
	return message
}

func replicaTestReply(t *testing.T, opcode uint32, document bson.D) []byte {
	t.Helper()
	message := replicaTestMessage(t, opcode, document)
	binary.LittleEndian.PutUint32(message[4:8], 73)
	binary.LittleEndian.PutUint32(message[8:12], 42)
	return message
}

func TestReplicaProxyAddressRewrite(t *testing.T) {
	mapping := replicaTestMap(t)
	unknown := bson.D{{Key: "opaque", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255, 0}}}}
	document := bson.D{
		{Key: "ok", Value: 1.0}, {Key: "me", Value: mapping.actual[0]},
		{Key: "hosts", Value: bson.A{mapping.actual[0], mapping.actual[1], mapping.actual[2]}},
		{Key: "passives", Value: bson.A{mapping.actual[1]}}, {Key: "arbiters", Value: bson.A{}},
		{Key: "primary", Value: mapping.actual[2]}, {Key: "isWritablePrimary", Value: true},
		{Key: "setName", Value: "owned"}, {Key: "term", Value: int64(12)},
		{Key: "topologyVersion", Value: unknown}, {Key: "maxWireVersion", Value: int32(25)},
		{Key: "speculativeAuthenticate", Value: unknown}, {Key: "unknown", Value: unknown},
		{Key: "errorLabels", Value: bson.A{"fixture-label"}},
	}
	for _, command := range []string{"hello", "isMaster", "ismaster"} {
		for _, legacy := range []bool{false, true} {
			name := command + "/msg"
			requestOpcode, responseOpcode := uint32(2013), uint32(2013)
			if legacy {
				name, requestOpcode, responseOpcode = command+"/query", 2004, 1
			}
			t.Run(name, func(t *testing.T) {
				query := bson.D{{Key: command, Value: 1}}
				request := replicaTestMessage(t, requestOpcode, query)
				original := replicaTestReply(t, responseOpcode, document)
				rewritten, err := mapping.rewriteReply(request, original)
				if err != nil {
					t.Fatal(err)
				}
				before, offset, _ := replicaWireDocument(original, true)
				after, _, err := replicaWireDocument(rewritten, true)
				if err != nil || !bytes.Equal(original[4:offset], rewritten[4:offset]) {
					t.Fatal("wire correlation or flags changed")
				}
				fields, _ := before.Elements()
				for _, field := range fields {
					key := field.Key()
					switch key {
					case "me", "primary", "hosts", "passives", "arbiters":
						continue
					}
					value := after.Lookup(key)
					if value.Type != field.Value().Type || !bytes.Equal(value.Value, field.Value().Value) {
						t.Fatalf("non-address field changed: %s", key)
					}
				}
				if after.Lookup("me").StringValue() != mapping.frontend[0] || after.Lookup("primary").StringValue() != mapping.frontend[2] {
					t.Fatal("member address not rewritten")
				}
				for i, expected := range mapping.frontend {
					if after.Lookup("hosts").Array().Index(uint(i)).StringValue() != expected {
						t.Fatal("discovered member bypass")
					}
				}
				if after.Lookup("passives").Array().Index(0).StringValue() != mapping.frontend[1] {
					t.Fatal("passive address bypass")
				}
			})
		}
	}
	query := bson.D{{Key: "find", Value: "records"}}
	request := replicaTestMessage(t, 2013, query)
	business := replicaTestReply(t, 2013, document)
	unchanged, err := mapping.rewriteReply(request, business)
	if err != nil || !bytes.Equal(unchanged, business) {
		t.Fatal("ordinary business reply changed")
	}
}

func TestReplicaProxyRejectsWireAndUnmapped(t *testing.T) {
	mapping := replicaTestMap(t)
	hello := bson.D{{Key: "hello", Value: 1}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "me", Value: mapping.actual[0]}}
	cases := []struct {
		name string
		edit func([]byte, []byte) ([]byte, []byte)
	}{
		{name: "wrong responseTo", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(r[8:12], 99); return q, r }},
		{name: "zero requestID", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[4:8], 0); return q, r }},
		{name: "request responseTo", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[8:12], 42); return q, r }},
		{name: "wrong requestID", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[4:8], 99); return q, r }},
		{name: "bad length", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(r, 16); return q, r }},
		{name: "compressed request", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[12:16], 2012); return q, r }},
		{name: "compressed response", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(r[12:16], 2012); return q, r }},
		{name: "checksum", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(r[16:20], 1); return q, r }},
		{name: "exhaust", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[16:20], 65536); return q, r }},
		{name: "moreToCome", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(r[16:20], 2); return q, r }},
		{name: "unknown flags", edit: func(q, r []byte) ([]byte, []byte) { binary.LittleEndian.PutUint32(q[16:20], 8); return q, r }},
		{name: "invalid BSON", edit: func(q, r []byte) ([]byte, []byte) { r[len(r)-1] = 1; return q, r }},
		{name: "wrong section", edit: func(q, r []byte) ([]byte, []byte) { r[20] = 1; return q, r }},
		{name: "truncated", edit: func(q, r []byte) ([]byte, []byte) { return q, r[:18] }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := replicaTestMessage(t, 2013, hello)
			response := replicaTestReply(t, 2013, reply)
			request, response = test.edit(request, response)
			if rewritten, err := mapping.rewriteReply(request, response); err == nil || rewritten != nil {
				t.Fatal("invalid wire did not fail closed")
			}
		})
	}
	for _, bad := range []bson.D{
		{{Key: "ok", Value: 1}, {Key: "me", Value: "127.0.0.1:29999"}},
		{{Key: "ok", Value: 1}, {Key: "hosts", Value: bson.A{mapping.actual[0], "127.0.0.1:29999"}}},
		{{Key: "ok", Value: 1}, {Key: "primary", Value: int32(1)}},
		{{Key: "ok", Value: 1}, {Key: "me", Value: mapping.actual[0]}, {Key: "me", Value: mapping.actual[0]}},
		{{Key: "ok", Value: 1}, {Key: "hosts", Value: bson.A{mapping.actual[0], mapping.actual[0]}}},
		{{Key: "ok", Value: 1}, {Key: "hosts", Value: bson.A{mapping.actual[0], mapping.actual[1], mapping.actual[2], mapping.actual[0]}}},
	} {
		request := replicaTestMessage(t, 2013, hello)
		response := replicaTestReply(t, 2013, bad)
		if rewritten, err := mapping.rewriteReply(request, response); err == nil || rewritten != nil {
			t.Fatal("unmapped or invalid address field did not fail closed")
		}
	}
	request := replicaTestMessage(t, 2004, hello)
	response := replicaTestReply(t, 1, reply)
	for _, offset := range []int{16, 20, 28, 32} {
		bad := append([]byte(nil), response...)
		binary.LittleEndian.PutUint32(bad[offset:offset+4], 2)
		if _, err := mapping.rewriteReply(request, bad); err == nil {
			t.Fatal("unsupported OP_REPLY header accepted")
		}
	}
	for _, flags := range []uint32{1, 2, 4, 16, replicaReplyAwaitCapable | 2} {
		bad := append([]byte(nil), response...)
		binary.LittleEndian.PutUint32(bad[16:20], flags)
		if _, err := mapping.rewriteReply(request, bad); err == nil {
			t.Fatal("unsupported OP_REPLY flag accepted")
		}
	}
	for _, flags := range []uint32{2, 64} {
		bad := append([]byte(nil), request...)
		binary.LittleEndian.PutUint32(bad[16:20], flags)
		if _, err := mapping.rewriteReply(bad, response); err == nil {
			t.Fatal("unsupported OP_QUERY flag accepted")
		}
	}
	msgReply := replicaTestReply(t, 2013, reply)
	if _, err := mapping.rewriteReply(request, msgReply); err == nil {
		t.Fatal("mismatched response opcode accepted")
	}
}

func TestReplicaProxyMapAndCoverage(t *testing.T) {
	mapping := replicaTestMap(t)
	for _, mode := range []string{"duplicate", "nonloopback", "changed hostname", "same port", "invalid port"} {
		t.Run(mode, func(t *testing.T) {
			actual, frontend := mapping.actual, mapping.frontend
			switch mode {
			case "duplicate":
				actual[2] = actual[1]
			case "nonloopback":
				actual[0], frontend[0] = "example.invalid:27011", "example.invalid:28011"
			case "changed hostname":
				frontend[0] = "127.0.0.1:28011"
			case "same port":
				frontend[0] = actual[0]
			case "invalid port":
				frontend[0] = "localhost:0"
			}
			if _, err := newReplicaAddressMap(actual, frontend); err == nil {
				t.Fatal("invalid fixed mapping accepted")
			}
		})
	}
	var snapshots [3]ReplicaProxySnapshot
	for i := range snapshots {
		monitor := ReplicaProxyConnection{ID: 1, Commands: map[string]int{"hello": 2}, DiscoveryReplies: 2}
		data := ReplicaProxyConnection{ID: 2, Commands: map[string]int{"hello": 1, "find": 1, "bulkWrite": 2, "findAndModify": 1, "commitTransaction": 1}, DiscoveryReplies: 1}
		snapshots[i] = ReplicaProxySnapshot{Frontend: mapping.frontend[i], Upstream: mapping.actual[i], Connections: []ReplicaProxyConnection{monitor, data}}
	}
	opts := replicaCoverageOptions{mapping: mapping, discovered: mapping.frontend[:], snapshots: snapshots, primary: 1}
	if err := checkReplicaCoverage(opts); err != nil {
		t.Fatal(err)
	}
	// Even perfect frontend counters cannot conceal a seed-only proxy followed
	// by direct SDAM discovery. Exercise the same oracle used by the real adapter.
	opts.discovered = []string{mapping.frontend[0], mapping.actual[1], mapping.actual[2]}
	if err := checkReplicaCoverage(opts); err == nil {
		t.Fatal("seed-only proxy bypass escaped the coverage oracle")
	}
	opts.discovered = mapping.frontend[:]
	opts.snapshots[2].Connections = nil
	if err := checkReplicaCoverage(opts); err == nil {
		t.Fatal("nonseed monitor coverage was optional")
	}
}

func replicaTestTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "new owned proxy unit CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, parsed, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key}
	server := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	client := &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	return server, client
}

func TestReplicaProxyVerifiedTLSBothLegs(t *testing.T) {
	for _, mode := range []string{"valid", "frontend hostname", "frontend trust", "upstream hostname", "upstream trust"} {
		t.Run(mode, func(t *testing.T) {
			serverTLS, verified := replicaTestTLS(t)
			frontTLS, upstreamTLS := verified.Clone(), verified.Clone()
			switch mode {
			case "frontend hostname":
				frontTLS.ServerName = "127.0.0.1"
			case "frontend trust":
				frontTLS.RootCAs = x509.NewCertPool()
			case "upstream hostname":
				upstreamTLS.ServerName = "127.0.0.1"
			case "upstream trust":
				upstreamTLS.RootCAs = x509.NewCertPool()
			}
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			upstream := tls.NewListener(listener, serverTLS)
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := upstream.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := readMessage(conn); err == nil {
					document := bson.D{{Key: "ok", Value: 1}}
					message := replicaTestReply(t, 2013, document)
					_, _ = conn.Write(message)
				}
			}()
			t.Cleanup(func() {
				_ = upstream.Close()
				select {
				case <-done:
				case <-time.After(4 * time.Second):
					t.Error("owned TLS unit worker did not join")
				}
			})
			opts := proxyOptions{uri: "mongodb://" + listener.Addr().String(), serverTLS: serverTLS, clientTLS: upstreamTLS, discovery: replicaTestMap(t)}
			proxy := startProxy(t, opts)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			network := &net.Dialer{Timeout: time.Second}
			dialer := &tls.Dialer{NetDialer: network, Config: frontTLS}
			connection, err := dialer.DialContext(ctx, "tcp", proxy.listener.Addr().String())
			if err == nil {
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
				document := bson.D{{Key: "ping", Value: 1}}
				request := replicaTestMessage(t, 2013, document)
				_, err = connection.Write(request)
				if err == nil {
					_, err = readMessage(connection)
				}
			}
			if (err == nil) != (mode == "valid") {
				t.Fatal("actual relay TLS verification result incorrect", mode, err)
			}
			proxy.Close()
			if current, _ := proxy.Sockets(); current != 0 {
				t.Fatal("TLS relay retained upstream")
			}
			if mode == "valid" {
				observed := proxy.replicaSnapshot(proxy.listener.Addr().String())
				if len(observed.Connections) != 1 || !observed.Connections[0].FrontendTLS || !observed.Connections[0].UpstreamTLS || len(proxy.ForwardedCommands()) != 1 {
					t.Fatal("verified relay coverage absent")
				}
			} else if len(proxy.ForwardedCommands()) != 0 {
				t.Fatal("unverified connection forwarded a business command")
			}
		})
	}
}

func TestReplicaProxyReadMessageBounds(t *testing.T) {
	for _, length := range []uint32{0, 15, replicaProxyMaxMessage + 1} {
		header := make([]byte, 16)
		binary.LittleEndian.PutUint32(header, length)
		if _, err := readMessage(bytes.NewReader(header)); err == nil {
			t.Fatal("invalid message length accepted")
		}
	}
	if _, err := readMessage(io.LimitReader(bytes.NewReader(make([]byte, 16)), 15)); err == nil {
		t.Fatal("short header accepted")
	}
}

func replicaTestSequence(t *testing.T, message []byte, identifier string, document bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	sequence := make([]byte, 4)
	sequence = append(sequence, []byte(identifier)...)
	sequence = append(sequence, 0)
	sequence = append(sequence, raw...)
	binary.LittleEndian.PutUint32(sequence, uint32(len(sequence)))
	message = append(message, 1)
	message = append(message, sequence...)
	binary.LittleEndian.PutUint32(message, uint32(len(message)))
	return message
}

func TestReplicaProxyBulkSequences(t *testing.T) {
	mapping := replicaTestMap(t)
	body := bson.D{{Key: "bulkWrite", Value: 1}}
	operation := bson.D{{Key: "insert", Value: 0}}
	namespace := bson.D{{Key: "ns", Value: "owned.records"}}
	request := replicaTestMessage(t, 2013, body)
	request = replicaTestSequence(t, request, "ops", operation)
	request = replicaTestSequence(t, request, "nsInfo", namespace)
	if _, _, err := replicaWireDocument(request, false); err != nil {
		t.Fatal("pinned bulk document sequences rejected", err)
	}
	reply := bson.D{{Key: "ok", Value: 1}}
	response := replicaTestReply(t, 2013, reply)
	result, err := mapping.rewriteReply(request, response)
	if err != nil || !bytes.Equal(result, response) {
		t.Fatal("bulk response changed")
	}
	for _, mode := range []string{"duplicate sequence", "unknown identifier", "bad section size", "bad sequence BSON", "response sequence"} {
		t.Run(mode, func(t *testing.T) {
			bad := append([]byte(nil), request...)
			switch mode {
			case "duplicate sequence":
				bad = replicaTestSequence(t, bad, "ops", operation)
			case "unknown identifier":
				bad = replicaTestSequence(t, bad, "arbitrary", operation)
			case "bad section size":
				_, offset, _ := replicaBSONAt(bad, 21)
				binary.LittleEndian.PutUint32(bad[offset+1:offset+5], ^uint32(0))
			case "bad sequence BSON":
				bad[len(bad)-1] = 1
			case "response sequence":
				bad = replicaTestSequence(t, append([]byte(nil), response...), "ops", operation)
			}
			if mode == "response sequence" {
				_, err = mapping.rewriteReply(request, bad)
			} else {
				_, _, err = replicaWireDocument(bad, false)
			}
			if err == nil {
				t.Fatal("unsupported sequence accepted")
			}
		})
	}
}

func TestReplicaProxyObservationBounds(t *testing.T) {
	proxy := &Proxy{}
	for index := range replicaProxyMaxConnections {
		if proxy.beginReplicaConnection() != index {
			t.Fatal("connection count changed")
		}
	}
	if proxy.beginReplicaConnection() != -1 {
		t.Fatal("connection metadata exceeded bound")
	}
	for range replicaProxyMaxCommands {
		if !proxy.reserveReplicaRequest("find") {
			t.Fatal("request count changed")
		}
		proxy.recordReplicaCommand(0, "find")
	}
	if proxy.reserveReplicaRequest("find") {
		t.Fatal("request metadata exceeded bound")
	}
	snapshot := proxy.replicaSnapshot("owned")
	snapshot.Connections[0].Commands["find"] = 0
	if proxy.replicaSnapshot("owned").Connections[0].Commands["find"] != replicaProxyMaxCommands {
		t.Fatal("snapshot exposed mutable observer metadata")
	}
}

func TestReplicaProxyCloseJoinsHeldRelay(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reply := bson.D{{Key: "ok", Value: 1}}
	response := replicaTestReply(t, 2013, reply)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readMessage(conn); err == nil {
			_, _ = conn.Write(response)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("owned unit upstream did not join")
		}
	})
	opts := proxyOptions{uri: "mongodb://" + listener.Addr().String(), discovery: replicaTestMap(t)}
	proxy := startProxy(t, opts)
	held, acknowledged := make(chan struct{}), make(chan struct{}, 1)
	fault := ProxyFault{Command: "ping", After: held, Acknowledged: acknowledged}
	proxy.ArmFault(fault)
	connection, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	command := bson.D{{Key: "ping", Value: 1}}
	request := replicaTestMessage(t, 2013, command)
	if _, err := connection.Write(request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acknowledged:
	case <-time.After(3 * time.Second):
		t.Fatal("reply barrier not reached")
	}
	started := time.Now()
	proxy.Close()
	if time.Since(started) > 3*time.Second {
		t.Fatal("held relay did not join promptly")
	}
	if current, _ := proxy.Sockets(); current != 0 {
		t.Fatal("joined relay retained upstream")
	}
	select {
	case <-held:
		t.Fatal("cleanup released held reply to manufacture retirement")
	default:
	}
}
