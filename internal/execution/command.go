package execution

import (
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// NormalizeCommand adds the configured Store only after verifying the wire target.
// It copies envelopes while borrowing immutable document/body buffers.
func NormalizeCommand(call *pb.Command, store string) (*pb.Command, *pb.Failure) {
	if call == nil || call.Version != 1 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unsupported or missing Command version")
	}
	normalized := &pb.Command{Version: 1}
	var resource string
	switch value := call.Operation.(type) {
	case *pb.Command_Scan:
		if value.Scan == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing scan")
		}
		resource = value.Scan.Resource
		request := &pb.ScanRequest{Resource: "weir://" + store + "/" + resource, Selector: value.Scan.Selector, ReadMediaType: value.Scan.ReadMediaType, PageSize: value.Scan.PageSize, ContinuationToken: value.Scan.ContinuationToken}
		normalized.Operation = &pb.Command_Scan{Scan: request}
	case *pb.Command_Native:
		if value.Native == nil || value.Native.Open == nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing native request")
		}
		resource = value.Native.Open.Resource
		request := &pb.NativeOpen{Resource: "weir://" + store + "/" + resource, Descriptor_: value.Native.Open.Descriptor_, BodyMediaType: value.Native.Open.BodyMediaType}
		native := &pb.NativeRequest{Open: request, Body: value.Native.Body}
		normalized.Operation = &pb.Command_Native{Native: native}
	default:
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing Command operation")
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
