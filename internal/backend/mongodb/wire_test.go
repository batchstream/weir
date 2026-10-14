package mongodb

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func scanWireMessage(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 21, len(raw)+21)
	binary.LittleEndian.PutUint32(message, uint32(len(raw)+21))
	binary.LittleEndian.PutUint32(message[12:16], 2013)
	return append(message, raw...)
}
func TestMongoWireBoundsBeforeDriverHeader(t *testing.T) {
	for _, mode := range []string{"good", "huge_length", "metadata_bytes", "metadata_nodes", "envelope_nodes", "large_data", "large_scalar", "duplicate", "bad_flags", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			doc := bson.D{{Key: "ok", Value: 1.0}}
			switch mode {
			case "metadata_bytes":
				diagnostic := bson.D{{Key: "code", Value: 91}, {Key: "errmsg", Value: strings.Repeat("x", 64<<10)}}
				field := bson.E{Key: "writeConcernError", Value: diagnostic}
				doc = append(doc, field)
			case "large_data":
				databases := make(bson.A, 2000)
				for i := range databases {
					database := bson.D{{Key: "name", Value: fmt.Sprintf("weir_database_%d", i)}, {Key: "sizeOnDisk", Value: int64(i)}, {Key: "empty", Value: false}}
					databases[i] = database
				}
				field := bson.E{Key: "databases", Value: databases}
				doc = append(doc, field)
			case "large_scalar":
				field := bson.E{Key: "errmsg", Value: strings.Repeat("x", 96<<10)}
				doc = append(doc, field)
			case "metadata_nodes":
				array := make(bson.A, 4097)
				for i := range array {
					array[i] = "a"
				}
				field := bson.E{Key: "errorLabels", Value: array}
				doc = append(doc, field)
			case "envelope_nodes":
				for i := 0; i < 33; i++ {
					field := bson.E{Key: string(rune('a' + i)), Value: true}
					doc = append(doc, field)
				}
			case "duplicate":
				field := bson.E{Key: "ok", Value: 1.0}
				doc = append(doc, field)
			}
			message := scanWireMessage(t, doc)
			switch mode {
			case "huge_length":
				message = message[:16]
				binary.LittleEndian.PutUint32(message, uint32(scanNativeLimit+1))
			case "bad_flags":
				message[16] = 2
			case "truncated":
				message = message[:len(message)-1]
			}
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(time.Second))
			sent := make(chan struct{})
			go func() { defer close(sent); _, _ = server.Write(message); _ = server.Close() }()
			guard := &boundedConn{Conn: client}
			header := make([]byte, 4)
			n, err := guard.Read(header)
			if mode == "good" || mode == "metadata_nodes" || mode == "envelope_nodes" || mode == "large_data" || mode == "large_scalar" {
				if err != nil || n != 4 {
					t.Fatal(n, err)
				}
				rest := make([]byte, len(message)-4)
				if _, err := io.ReadFull(guard, rest); err != nil {
					t.Fatal(err)
				}
				if guard.reply != nil {
					t.Fatal("wire buffer retained after consumption")
				}
			} else if n != 0 || err == nil {
				t.Fatal("unqualified response exposed header to driver", n, err)
			}
			<-sent
		})
	}
}

func TestMongoWireBoundsOnlyExpandedDiagnosticValues(t *testing.T) {
	for _, name := range []string{"writeErrors", "errorLabels", "writeConcernError", "topologyVersion"} {
		t.Run(name, func(t *testing.T) {
			text := strings.Repeat("x", 64<<10)
			var value any
			switch name {
			case "writeErrors", "errorLabels":
				value = bson.A{text}
			default:
				value = bson.D{{Key: "diagnostic", Value: text}}
			}
			document := bson.D{{Key: "ok", Value: 1}, {Key: name, Value: value}}
			message := scanWireMessage(t, document)
			if boundedReply(message) {
				t.Fatal("driver expansion exceeded diagnostic byte budget", name)
			}
			// The same name with a scalar value is not expanded by the driver.
			document[1].Value = text
			message = scanWireMessage(t, document)
			if !boundedReply(message) {
				t.Fatal("non-expanded field was constrained as diagnostics", name)
			}
		})
	}
}
