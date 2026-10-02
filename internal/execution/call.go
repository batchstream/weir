package execution

import (
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// NormalizeCall adds the configured Store only after verifying the wire target.
// It copies envelopes while borrowing immutable document/body buffers.
func NormalizeCall(call *pb.Call, store string) (*pb.Call, *pb.Failure) {
	if call == nil || call.Version != 1 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unsupported or missing Call version")
	}
	normalized := &pb.Call{Version: 1}
	var resource string
	switch value := call.Operation.(type) {
	case *pb.Call_Read:
		if value.Read == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing read")
		}
		resource = value.Read.Resource
		request := &pb.ReadRequest{Resource: "weir://" + store + "/" + resource, ReadMediaType: value.Read.ReadMediaType, AdapterOptions: value.Read.AdapterOptions}
		normalized.Operation = &pb.Call_Read{Read: request}
	case *pb.Call_Mutate:
		if value.Mutate == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing mutation")
		}
		resource = value.Mutate.Resource
		request := &pb.MutateRequest{Resource: "weir://" + store + "/" + resource, AdapterOptions: value.Mutate.AdapterOptions, Action: value.Mutate.Action}
		normalized.Operation = &pb.Call_Mutate{Mutate: request}
	case *pb.Call_Scan:
		if value.Scan == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing scan")
		}
		resource = value.Scan.Resource
		request := &pb.ScanRequest{Resource: "weir://" + store + "/" + resource, Selector: value.Scan.Selector, ReadMediaType: value.Scan.ReadMediaType, PageSize: value.Scan.PageSize, ContinuationToken: value.Scan.ContinuationToken}
		normalized.Operation = &pb.Call_Scan{Scan: request}
	case *pb.Call_Native:
		if value.Native == nil || value.Native.Open == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing native request")
		}
		resource = value.Native.Open.Resource
		request := &pb.NativeOpen{Resource: "weir://" + store + "/" + resource, Descriptor_: value.Native.Open.Descriptor_, BodyMediaType: value.Native.Open.BodyMediaType}
		native := &pb.NativeCall{Open: request, Body: value.Native.Body}
		normalized.Operation = &pb.Call_Native{Native: native}
	default:
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing Call operation")
	}
	if resource == "" || strings.HasPrefix(resource, "/") || strings.Contains(resource, "://") {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "target must be a canonical relative Store path")
	}
	_, parts, err := protocol.ParseResource("weir://" + store + "/" + resource)
	if err != nil || len(parts) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid relative target")
	}
	return normalized, nil
}

func RecordOperation(id uint64, call *pb.Call) *pb.Operation {
	operation := &pb.Operation{Index: id}
	if read := call.GetRead(); read != nil {
		operation.Operation = &pb.Operation_Read{Read: read}
	} else if mutation := call.GetMutate(); mutation != nil {
		operation.Operation = &pb.Operation_Mutate{Mutate: mutation}
	}
	return operation
}
