package weirv1

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

func TestPublicStoreContract(t *testing.T) {
	file := File_api_weir_v1_store_proto
	if file.Path() != "api/weir/v1/store.proto" || file.Package() != "weir.v1" || file.Services().Len() != 1 {
		t.Fatal("public package or service contract changed", file.Path(), file.Package(), file.Services().Len())
	}
	service := file.Services().Get(0)
	if service.FullName() != "weir.v1.StoreService" || service.Methods().Len() != 2 {
		t.Fatal("unexpected public service", service.FullName(), service.Methods().Len())
	}
	resolve := service.Methods().Get(0)
	execute := service.Methods().Get(1)
	if resolve.Name() != "ResolveStore" || resolve.Input().FullName() != "weir.v1.ResolveStoreRequest" || resolve.Output().FullName() != "weir.v1.ResolveStoreResponse" || resolve.IsStreamingClient() || resolve.IsStreamingServer() {
		t.Fatal("ResolveStore RPC contract changed", resolve)
	}
	if execute.Name() != "Execute" || execute.Input().FullName() != "weir.v1.ExecuteRequest" || execute.Output().FullName() != "weir.v1.ExecuteResponse" || !execute.IsStreamingClient() || !execute.IsStreamingServer() {
		t.Fatal("Execute RPC contract changed", execute)
	}
	if StoreService_ResolveStore_FullMethodName != "/weir.v1.StoreService/ResolveStore" || StoreService_Execute_FullMethodName != "/weir.v1.StoreService/Execute" {
		t.Fatal("generated public method paths changed")
	}
	request := []fieldContract{{name: "store_name", kind: protoreflect.StringKind}}
	assertFields(t, resolve.Input(), request)
	response := []fieldContract{{name: "store_name", kind: protoreflect.StringKind}, {name: "endpoints", kind: protoreflect.StringKind, repeated: true}, {name: "cache_ttl_ms", kind: protoreflect.Uint64Kind}}
	assertFields(t, resolve.Output(), response)
	executeRequest := []fieldContract{{name: "request_id", kind: protoreflect.Uint64Kind}, {name: "store_name", kind: protoreflect.StringKind}, {name: "call_payload", kind: protoreflect.BytesKind}}
	assertFields(t, execute.Input(), executeRequest)
	executeResponse := []fieldContract{{name: "request_id", kind: protoreflect.Uint64Kind}, {name: "event_fragment", kind: protoreflect.BytesKind}, {name: "request_complete", kind: protoreflect.BoolKind}}
	assertFields(t, execute.Output(), executeResponse)
	if resolve.Output().Fields().ByName("group") != nil || resolve.Output().Fields().ByName("replica_group") != nil {
		t.Fatal("peer replica group leaked into public response")
	}
	for _, name := range []protoreflect.Name{"Weir", "Directory"} {
		if file.Services().ByName(name) != nil {
			t.Fatal("retired service remains public", name)
		}
	}
	for _, name := range []protoreflect.Name{"NodeAnnouncement", "NodeAdvertisement", "SyncDirectoryRequest", "SyncDirectoryResponse", "ExchangeRequest", "ExchangeResponse", "ResolveRequest", "ResolveResponse", "RouteRequest", "RouteResponse"} {
		if file.Messages().ByName(name) != nil {
			t.Fatal("internal or retired message remains public", name)
		}
	}
}

func TestPublicDescriptorClosureExcludesPeers(t *testing.T) {
	pending := []protoreflect.FileDescriptor{File_api_weir_v1_store_proto}
	seen := make(map[string]bool)
	for len(pending) > 0 {
		file := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[file.Path()] {
			continue
		}
		seen[file.Path()] = true
		if strings.HasPrefix(string(file.Package()), "weir.peer") || strings.HasPrefix(file.Path(), "internal/") {
			t.Fatal("public descriptor imports internal peer protocol", file.Path(), file.Package())
		}
		imports := file.Imports()
		for i := 0; i < imports.Len(); i++ {
			pending = append(pending, imports.Get(i).FileDescriptor)
		}
	}
}
