package mongostore

import (
	"encoding/binary"
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
	for _, mode := range []string{"good", "huge_length", "metadata_bytes", "metadata_nodes", "envelope_nodes", "duplicate", "bad_flags", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			doc := bson.D{{Key: "ok", Value: 1.0}}
			switch mode {
			case "metadata_bytes":
				field := bson.E{Key: "errmsg", Value: strings.Repeat("x", (64<<10)+1)}
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
			if mode == "good" {
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
