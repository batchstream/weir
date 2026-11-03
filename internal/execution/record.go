package execution

import (
	"net/url"
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Record borrows one validated immutable request and its decoded resource path.
type Record struct {
	Index    uint64
	Command  *pb.Command
	Segments []string
	Key      string
	Bytes    int
}

func NewRecord(index uint64, command *pb.Command) (*Record, error) {
	resource := command.GetMutate().GetResource()
	if read := command.GetRead(); read != nil {
		resource = read.Resource
	}
	// Mongo accepts three segments and Search two. A fourth marker preserves
	// the adapters' existing arity rejection without allocating a slice or
	// decoding a tree proportional to an unsupported resource's slash count.
	parts := make([]string, 0, 4)
	remaining := resource
	for {
		if len(parts) == 3 {
			parts = append(parts, "")
			break
		}
		piece, rest, more := strings.Cut(remaining, "/")
		// Public validation checks canonical spelling, UTF-8 and forbidden segments.
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
	record := &Record{Index: index, Command: command, Segments: parts, Key: resource, Bytes: proto.Size(command)}
	return record, nil
}
