package mongodb

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var errWireBound = errors.New("MongoDB reply exceeds qualified wire/envelope bounds")

// The driver expands top-level error arrays before RunCommand.Raw can inspect
// them. This fixed uncompressed OP_REPLY/OP_MSG profile checks that envelope
// before exposing any header to the driver. It does not decode result documents,
// interpret selectors, issue commands or retry. One buffer, at most 48 MiB, per
// socket; the driver may concurrently allocate its own equally bounded copy.
type boundedDialer struct {
	dialer    net.Dialer
	tlsConfig *tls.Config
}

func newBoundedDialer() *boundedDialer {
	d := &boundedDialer{dialer: net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}}
	return d
}
func (d *boundedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, mongoConnectTimeout)
	defer cancel()
	conn, err := d.dialTCP(ctx, network, address)
	if err != nil {
		return nil, err
	}
	raw := conn
	if d.tlsConfig != nil {
		secured, err := mongoTLS(ctx, conn, d.tlsConfig, address)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = secured
	}
	bounded := &boundedConn{Conn: conn, raw: raw}
	return bounded, nil
}

// Shared by Mongo TCP and the bounded OCSP HTTP transport; DNS is pure Go and
// cancellation closes its sockets, including a resolver read already in flight.
func (d *boundedDialer) dialTCP(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, mongoConnectTimeout)
	defer cancel()
	dialer := d.dialer
	source := dialer.Resolver
	resolver := &net.Resolver{PreferGo: true, StrictErrors: true}
	resolver.Dial = func(_ context.Context, network, address string) (net.Conn, error) {
		// Go's DNS read may outlive lookup cancellation until its socket deadline.
		// Bind each DNS socket to this same total connect budget and cancellation.
		var conn net.Conn
		var err error
		if source != nil && source.Dial != nil {
			conn, err = source.Dial(ctx, network, address)
		} else {
			dnsDialer := net.Dialer{Timeout: mongoConnectTimeout}
			conn, err = dnsDialer.DialContext(ctx, network, address)
		}
		if err != nil {
			return nil, err
		}
		deadline, _ := ctx.Deadline()
		if err := conn.SetDeadline(deadline); err != nil {
			conn.Close()
			return nil, err
		}
		context.AfterFunc(ctx, func() { conn.Close() })
		return conn, nil
	}
	dialer.Resolver = resolver
	return dialer.DialContext(ctx, network, address)
}

type boundedConn struct {
	net.Conn
	raw   net.Conn
	reply []byte
}

// Driver cancellation/Close must release TCP immediately. tls.Conn.Close can
// wait five seconds for close_notify; Mongo already has explicit frame lengths.
func (c *boundedConn) Close() error {
	if c.raw != nil {
		return c.raw.Close()
	}
	return c.Conn.Close()
}

func (c *boundedConn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(c.reply) == 0 {
		var header [16]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			_ = c.Close()
			return 0, err
		}
		size := int64(binary.LittleEndian.Uint32(header[:4]))
		if size < 21 || size > scanNativeLimit {
			_ = c.Close()
			return 0, errWireBound
		}
		message := make([]byte, int(size))
		copy(message, header[:])
		if _, err := io.ReadFull(c.Conn, message[16:]); err != nil {
			_ = c.Close()
			return 0, err
		}
		if !boundedReply(message) {
			_ = c.Close()
			return 0, errWireBound
		}
		c.reply = message
	}
	n := copy(dst, c.reply)
	c.reply = c.reply[n:]
	if len(c.reply) == 0 {
		c.reply = nil
	}
	return n, nil
}
func boundedReply(message []byte) bool {
	if len(message) < 16 || int64(binary.LittleEndian.Uint32(message[:4])) != int64(len(message)) {
		return false
	}
	var raw []byte
	switch binary.LittleEndian.Uint32(message[12:16]) {
	case 2013:
		// No exhaust, compressed reply, document sequence or checksum is negotiated
		// by this qualified profile. Reject unknown framing instead of weakening it.
		if len(message) < 26 || binary.LittleEndian.Uint32(message[16:20]) != 0 || message[20] != 0 {
			return false
		}
		raw = message[21:]
	case 1:
		if len(message) < 41 || binary.LittleEndian.Uint32(message[16:20]) & ^uint32(8) != 0 || binary.LittleEndian.Uint64(message[20:28]) != 0 || binary.LittleEndian.Uint32(message[32:36]) != 1 {
			return false
		}
		raw = message[36:]
	default:
		return false
	}
	fields, err := scanFields(raw)
	if err != nil {
		return false
	}
	metadataBytes := 0
	for key, value := range fields {
		// ExtractErrorFromServerResponse only enumerates this outer cursor value;
		// RunCommand.Raw does not expand its documents or batch array.
		if key == "cursor" || key == "value" && value.Type == bson.TypeEmbeddedDocument && len(value.Value) <= NativeResponseLimit {
			continue
		}
		metadataBytes += len(key) + len(value.Value) + 2
		if metadataBytes > 64<<10 {
			return false
		}
		if value.Type == bson.TypeArray || value.Type == bson.TypeEmbeddedDocument {
			nodes := 4096
			if !validScanBSON(value.Value, 0, &nodes) {
				return false
			}
		} else if value.Validate() != nil {
			return false
		}
	}
	return true
}
