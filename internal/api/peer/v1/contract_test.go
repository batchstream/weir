package peerv1

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type fieldContract struct {
	name     protoreflect.Name
	kind     protoreflect.Kind
	repeated bool
}

func assertFields(t *testing.T, message protoreflect.MessageDescriptor, expected []fieldContract) {
	t.Helper()
	fields := message.Fields()
	if fields.Len() != len(expected) {
		t.Fatalf("%s has %d fields, want %d", message.FullName(), fields.Len(), len(expected))
	}
	for i, contract := range expected {
		field := fields.Get(i)
		if field.Name() != contract.name || field.Number() != protoreflect.FieldNumber(i+1) || field.Kind() != contract.kind || field.IsList() != contract.repeated {
			t.Fatalf("%s field %d changed: %s number=%d kind=%s list=%v", message.FullName(), i, field.Name(), field.Number(), field.Kind(), field.IsList())
		}
	}
}

func TestInternalPeerContract(t *testing.T) {
	file := File_internal_api_peer_v1_peer_proto
	if file.Path() != "internal/api/peer/v1/peer.proto" || file.Package() != "weir.peer.v1" || file.Services().Len() != 1 || file.Messages().Len() != 3 || file.Enums().Len() != 0 {
		t.Fatal("internal package or message contract changed", file.Path(), file.Package())
	}
	service := file.Services().Get(0)
	if service.FullName() != "weir.peer.v1.PeerDiscoveryService" || service.Methods().Len() != 1 {
		t.Fatal("unexpected internal service", service.FullName(), service.Methods().Len())
	}
	method := service.Methods().Get(0)
	if method.Name() != "SyncDirectory" || method.Input().FullName() != "weir.peer.v1.SyncDirectoryRequest" || method.Output().FullName() != "weir.peer.v1.SyncDirectoryResponse" || method.IsStreamingClient() || method.IsStreamingServer() {
		t.Fatal("SyncDirectory RPC contract changed", method)
	}
	if PeerDiscoveryService_SyncDirectory_FullMethodName != "/weir.peer.v1.PeerDiscoveryService/SyncDirectory" {
		t.Fatal("generated internal method path changed")
	}
	fields := []fieldContract{{name: "announcements", kind: protoreflect.MessageKind, repeated: true}}
	assertFields(t, method.Input(), fields)
	assertFields(t, method.Output(), fields)
	for _, message := range []protoreflect.MessageDescriptor{method.Input(), method.Output()} {
		if message.Fields().Get(0).Message().FullName() != "weir.peer.v1.NodeAnnouncement" {
			t.Fatal("internal snapshot references a foreign message", message)
		}
	}
	announcement := []fieldContract{
		{name: "incarnation_id", kind: protoreflect.StringKind},
		{name: "revision", kind: protoreflect.Uint64Kind},
		{name: "peer_endpoint", kind: protoreflect.StringKind},
		{name: "replica_group", kind: protoreflect.StringKind},
		{name: "store_names", kind: protoreflect.StringKind, repeated: true},
		{name: "store_endpoints", kind: protoreflect.StringKind, repeated: true},
		{name: "lease_remaining_ms", kind: protoreflect.Uint64Kind},
		{name: "withdrawn", kind: protoreflect.BoolKind},
	}
	assertFields(t, file.Messages().ByName("NodeAnnouncement"), announcement)
}

func TestPeerDescriptorClosureExcludesBusinessDTOs(t *testing.T) {
	pending := []protoreflect.FileDescriptor{File_internal_api_peer_v1_peer_proto}
	seen := make(map[string]bool)
	for len(pending) > 0 {
		file := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[file.Path()] {
			continue
		}
		seen[file.Path()] = true
		if file.Package() == "weir.v1" || strings.HasPrefix(file.Path(), "api/weir/") {
			t.Fatal("internal peer descriptor imports public business protocol", file.Path(), file.Package())
		}
		for _, name := range []protoreflect.Name{"Call", "Event", "Document", "Operation", "Result", "ResolveStoreRequest", "ResolveStoreResponse", "ExecuteRequest", "ExecuteResponse"} {
			if file.Messages().ByName(name) != nil {
				t.Fatal("business DTO exists in internal descriptor closure", file.Path(), name)
			}
		}
		imports := file.Imports()
		for i := 0; i < imports.Len(); i++ {
			pending = append(pending, imports.Get(i).FileDescriptor)
		}
	}
}
