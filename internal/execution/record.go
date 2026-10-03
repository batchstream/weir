package execution

import (
	"net/url"
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Record is constructed only after its complete public batch passes validation.
// Its request and decoded segments are borrowed immutably by the backend plan.
type Record struct {
	operation *pb.Operation
	store     string
	segments  []string
	key       string
	bytes     int
}

func (r *Record) Operation() *pb.Operation { return r.operation }
func (r *Record) StoreName() string        { return r.store }
func (r *Record) Segments() []string       { return r.segments }
func (r *Record) Key() string              { return r.key }
func (r *Record) Bytes() int               { return r.bytes }

func NewReadRecords(request *pb.ReadBatchRequest, byteLimit int) ([]*Record, *pb.Failure) {
	if err := protocol.ValidateReadBatchRequest(request); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	byteLimit = min(byteLimit, protocol.MaxBatchRequestBytes)
	bytes := 0
	for _, read := range request.Requests {
		bytes += recordBytes(request.StoreName, read.Resource, proto.Size(read)+16)
		if bytes > byteLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch preparation metadata exceeds byte budget")
		}
	}
	records := make([]*Record, len(request.Requests))
	for i, read := range request.Requests {
		variant := &pb.Operation_Read{Read: read}
		operation := &pb.Operation{Index: uint64(i + 1), Operation: variant}
		record, err := newRecord(request.StoreName, operation)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "validated record path could not be decoded")
		}
		records[i] = record
	}
	return records, nil
}

func NewMutationRecords(request *pb.MutateBatchRequest, byteLimit int) ([]*Record, *pb.Failure) {
	if err := protocol.ValidateMutateBatchRequest(request); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	byteLimit = min(byteLimit, protocol.MaxBatchRequestBytes)
	bytes := 0
	for _, mutation := range request.Requests {
		bytes += recordBytes(request.StoreName, mutation.Resource, proto.Size(mutation)+16)
		if bytes > byteLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch preparation metadata exceeds byte budget")
		}
	}
	records := make([]*Record, len(request.Requests))
	for i, mutation := range request.Requests {
		variant := &pb.Operation_Mutate{Mutate: mutation}
		operation := &pb.Operation{Index: uint64(i + 1), Operation: variant}
		record, err := newRecord(request.StoreName, operation)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "validated record path could not be decoded")
		}
		records[i] = record
	}
	return records, nil
}

// Count the retained request, model envelope and decoded path before allocating
// a batch. Sixteen bytes cover the internal Operation index and message wrapper.
// The Store's pending bound and existing RPC envelope both remain effective.
func recordBytes(store, resource string, operationBytes int) int {
	prefixBytes := len("weir://") + len(store) + 1
	fullResourceBytes := prefixBytes + len(resource)
	return operationBytes + prefixBytes + 2*fullResourceBytes + 1024 + 16*(strings.Count(resource, "/")+1)
}

func newRecord(store string, operation *pb.Operation) (*Record, error) {
	resource := protocol.Resource(operation)
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
	bytes := recordBytes(store, resource, proto.Size(operation))
	record := &Record{operation: operation, store: store, segments: parts, key: resource, bytes: bytes}
	return record, nil
}
