package execution

import (
	"net/url"
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Record borrows one validated request in the current bounded execution window.
// Its request and decoded segments are borrowed immutably by the backend plan.
type Record struct {
	operation *Operation
	store     string
	segments  []string
	key       string
	bytes     int
}

func (r *Record) Operation() *Operation { return r.operation }
func (r *Record) StoreName() string     { return r.store }
func (r *Record) Segments() []string    { return r.segments }
func (r *Record) Key() string           { return r.key }
func (r *Record) Bytes() int            { return r.bytes }

func NewReadRecords(store string, requests []*pb.ReadRequest, byteLimit int) ([]*Record, *pb.Failure) {
	for _, request := range requests {
		if err := protocol.ValidateReadRequest(request); err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
		}
	}
	if !protocol.ValidStoreName(store) || len(requests) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Store or empty record window")
	}
	bytes := 0
	for _, read := range requests {
		bytes += recordBytes(store, read.Resource, proto.Size(read)+16)
		if bytes > byteLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch preparation metadata exceeds byte budget")
		}
	}
	records := make([]*Record, len(requests))
	for i, read := range requests {
		operation := &Operation{Index: uint64(i + 1), Read: read}
		record, err := NewRecord(store, operation)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "validated record path could not be decoded")
		}
		records[i] = record
	}
	return records, nil
}

func NewMutationRecords(store string, requests []*pb.MutateRequest, byteLimit int) ([]*Record, *pb.Failure) {
	for _, request := range requests {
		if err := protocol.ValidateMutationRequest(request); err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
		}
	}
	if !protocol.ValidStoreName(store) || len(requests) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Store or empty record window")
	}
	bytes := 0
	for _, mutation := range requests {
		bytes += recordBytes(store, mutation.Resource, proto.Size(mutation)+16)
		if bytes > byteLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch preparation metadata exceeds byte budget")
		}
	}
	records := make([]*Record, len(requests))
	for i, mutation := range requests {
		operation := &Operation{Index: uint64(i + 1), Mutate: mutation}
		record, err := NewRecord(store, operation)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "validated record path could not be decoded")
		}
		records[i] = record
	}
	return records, nil
}

// Count the retained request, model envelope and decoded path before allocating
// a batch. Sixteen bytes cover the retained internal positional metadata.
// The Store pending bound accounts for prepared window metadata.
func recordBytes(store, resource string, operationBytes int) int {
	return operationBytes + len(store) + 2*len(resource) + 1024 + 16*(strings.Count(resource, "/")+1)
}

func NewRecord(store string, operation *Operation) (*Record, error) {
	resource := operation.Resource()
	parts := make([]string, 0, strings.Count(resource, "/")+1)
	remaining := resource
	for {
		piece, rest, more := strings.Cut(remaining, "/")
		// Public validation has checked canonical spelling, UTF-8, length and
		// forbidden segments. This step only retains their decoded values.
		decoded, err := url.PathUnescape(piece)
		if err != nil {
			return nil, err
		}
		parts = append(parts, decoded)
		if !more {
			break
		}
		remaining = rest
	}
	bytes := recordBytes(store, resource, operation.RequestBytes())
	record := &Record{operation: operation, store: store, segments: parts, key: resource, bytes: bytes}
	return record, nil
}
