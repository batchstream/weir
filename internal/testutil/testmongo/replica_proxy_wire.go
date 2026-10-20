package testmongo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const replicaProxyMaxMessage = 48 << 20

var errReplicaProxyWire = errors.New("unsupported or invalid owned replica proxy message")
var errReplicaProxyReplyHeader = errors.New("unsupported owned replica OP_REPLY flags or cursor header")

// The pinned mongod sets AwaitCapable on its ordinary legacy handshake reply.
// This capability bit does not permit exhaust, query failure or cursor replies.
const replicaReplyAwaitCapable uint32 = 8

type replicaAddressMap struct {
	actual, frontend [3]string
}

func newReplicaAddressMap(actual, frontend [3]string) (*replicaAddressMap, error) {
	seen := make(map[string]bool, 6)
	for i := range actual {
		actualHost, actualPort, err := net.SplitHostPort(actual[i])
		if err != nil || !replicaLoopback(actualHost) {
			return nil, errReplicaProxyWire
		}
		frontHost, frontPort, err := net.SplitHostPort(frontend[i])
		if err != nil || frontHost != actualHost {
			return nil, errReplicaProxyWire
		}
		for _, port := range []string{actualPort, frontPort} {
			number, err := strconv.Atoi(port)
			if err != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port {
				return nil, errReplicaProxyWire
			}
		}
		for _, address := range []string{actual[i], frontend[i]} {
			if seen[address] {
				return nil, errReplicaProxyWire
			}
			seen[address] = true
		}
	}
	mapping := &replicaAddressMap{actual: actual, frontend: frontend}
	return mapping, nil
}

func replicaLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func replicaHello(command string) bool {
	return command == "hello" || command == "isMaster" || command == "ismaster"
}

// Only the pinned polling/uncompressed wire forms are accepted. Checksums,
// compression, exhaust, reply cursors and unknown flags fail closed.
func replicaWireDocument(message []byte, response bool) (bson.Raw, int, error) {
	if len(message) < 16 || len(message) > replicaProxyMaxMessage || int(binary.LittleEndian.Uint32(message)) != len(message) {
		return nil, 0, errReplicaProxyWire
	}
	if !response && (binary.LittleEndian.Uint32(message[4:8]) == 0 || binary.LittleEndian.Uint32(message[8:12]) != 0) {
		return nil, 0, errReplicaProxyWire
	}
	opcode := binary.LittleEndian.Uint32(message[12:16])
	offset := 0
	switch opcode {
	case 2013:
		if len(message) < 26 || binary.LittleEndian.Uint32(message[16:20]) != 0 || message[20] != 0 {
			return nil, 0, errReplicaProxyWire
		}
		offset = 21
	case 2004:
		if response || len(message) < 34 || binary.LittleEndian.Uint32(message[16:20]) & ^uint32(4) != 0 {
			return nil, 0, errReplicaProxyWire
		}
		end := bytes.IndexByte(message[20:], 0)
		if end < 0 || end > 128 {
			return nil, 0, errReplicaProxyWire
		}
		end += 20
		if !bytes.HasSuffix(message[20:end], []byte(".$cmd")) || end+9 > len(message) {
			return nil, 0, errReplicaProxyWire
		}
		count := binary.LittleEndian.Uint32(message[end+5 : end+9])
		if binary.LittleEndian.Uint32(message[end+1:end+5]) != 0 || count != 1 && count != ^uint32(0) {
			return nil, 0, errReplicaProxyWire
		}
		offset = end + 9
	case 1:
		if !response || len(message) < 41 || binary.LittleEndian.Uint32(message[16:20]) & ^replicaReplyAwaitCapable != 0 || binary.LittleEndian.Uint64(message[20:28]) != 0 || binary.LittleEndian.Uint32(message[28:32]) != 0 || binary.LittleEndian.Uint32(message[32:36]) != 1 {
			return nil, 0, errReplicaProxyReplyHeader
		}
		offset = 36
	default:
		return nil, 0, errReplicaProxyWire
	}
	doc, end, err := replicaBSONAt(message, offset)
	if err != nil {
		return nil, 0, err
	}
	if !response {
		elements, _ := doc.Elements()
		if len(elements) == 0 || len(elements[0].Key()) > 64 || opcode == 2004 && !replicaHello(elements[0].Key()) {
			return nil, 0, errReplicaProxyWire
		}
	}
	// Client bulkWrite may carry the two pinned document sequences. They are
	// validated without changing request bytes or retaining their payloads.
	sequences := make(map[string]bool, 2)
	for end < len(message) {
		if opcode != 2013 || response || message[end] != 1 || len(message)-end < 6 {
			return nil, 0, errReplicaProxyWire
		}
		start := end + 1
		size := int(binary.LittleEndian.Uint32(message[start:]))
		if size < 6 || size > len(message)-start {
			return nil, 0, errReplicaProxyWire
		}
		limit := start + size
		identifierEnd := bytes.IndexByte(message[start+4:limit], 0)
		if identifierEnd < 0 {
			return nil, 0, errReplicaProxyWire
		}
		identifierEnd += start + 4
		identifier := string(message[start+4 : identifierEnd])
		if identifier != "ops" && identifier != "nsInfo" || sequences[identifier] {
			return nil, 0, errReplicaProxyWire
		}
		sequences[identifier] = true
		position := identifierEnd + 1
		for position < limit {
			_, position, err = replicaBSONAt(message[:limit], position)
			if err != nil {
				return nil, 0, err
			}
		}
		end = limit
	}
	return doc, offset, nil
}

func replicaBSONAt(message []byte, offset int) (bson.Raw, int, error) {
	if offset < 0 || len(message)-offset < 5 {
		return nil, 0, errReplicaProxyWire
	}
	size := int(binary.LittleEndian.Uint32(message[offset:]))
	if size < 5 || size > len(message)-offset {
		return nil, 0, errReplicaProxyWire
	}
	doc := bson.Raw(message[offset : offset+size])
	if doc.Validate() != nil {
		return nil, 0, errReplicaProxyWire
	}
	return doc, offset + size, nil
}

func (m *replicaAddressMap) rewriteReply(request, response []byte) ([]byte, error) {
	command, _, err := replicaWireDocument(request, false)
	if err != nil {
		return nil, err
	}
	reply, offset, err := replicaWireDocument(response, true)
	if err != nil {
		return nil, err
	}
	requestOpcode := binary.LittleEndian.Uint32(request[12:16])
	replyOpcode := binary.LittleEndian.Uint32(response[12:16])
	if !bytes.Equal(request[4:8], response[8:12]) || requestOpcode == 2004 && replyOpcode != 1 || requestOpcode == 2013 && replyOpcode != 2013 {
		return nil, errReplicaProxyWire
	}
	elements, _ := command.Elements()
	if !replicaHello(elements[0].Key()) {
		return response, nil
	}
	body := make([]byte, 4, len(reply))
	fields, _ := reply.Elements()
	seen := make(map[string]bool, 5)
	for _, field := range fields {
		name := field.Key()
		switch name {
		case "me", "primary", "hosts", "passives", "arbiters":
			if seen[name] {
				return nil, errReplicaProxyWire
			}
			seen[name] = true
		default:
			body = append(body, field...)
			continue
		}
		if name == "me" || name == "primary" {
			address, ok := field.Value().StringValueOK()
			if !ok {
				return nil, errReplicaProxyWire
			}
			mapped, err := m.frontendFor(address)
			if err != nil {
				return nil, err
			}
			body = bsoncore.AppendStringElement(body, name, mapped)
			continue
		}
		array, ok := field.Value().ArrayOK()
		if !ok {
			return nil, errReplicaProxyWire
		}
		entries, err := bson.Raw(array).Elements()
		if err != nil || len(entries) > 3 {
			return nil, errReplicaProxyWire
		}
		mappedArray := make([]byte, 4)
		addresses := make(map[string]bool, 3)
		for i, entry := range entries {
			address, ok := entry.Value().StringValueOK()
			if !ok || entry.Key() != strconv.Itoa(i) || addresses[address] {
				return nil, errReplicaProxyWire
			}
			addresses[address] = true
			mapped, err := m.frontendFor(address)
			if err != nil {
				return nil, err
			}
			mappedArray = bsoncore.AppendStringElement(mappedArray, entry.Key(), mapped)
		}
		mappedArray = append(mappedArray, 0)
		binary.LittleEndian.PutUint32(mappedArray, uint32(len(mappedArray)))
		body = bsoncore.AppendArrayElement(body, name, mappedArray)
	}
	body = append(body, 0)
	binary.LittleEndian.PutUint32(body, uint32(len(body)))
	if offset+len(body) > replicaProxyMaxMessage {
		return nil, errReplicaProxyWire
	}
	rewritten := append([]byte(nil), response[:offset]...)
	rewritten = append(rewritten, body...)
	binary.LittleEndian.PutUint32(rewritten, uint32(len(rewritten)))
	return rewritten, nil
}

func (m *replicaAddressMap) frontendFor(actual string) (string, error) {
	for i, address := range m.actual {
		if address == actual {
			return m.frontend[i], nil
		}
	}
	return "", errReplicaProxyWire
}
