package execution

import (
	"net/url"
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Record borrows one validated immutable request and its decoded resource path.
type Record struct {
	index    uint64
	command  *pb.Command
	store    string
	segments []string
	key      string
	bytes    int
}

func (r *Record) Index() uint64        { return r.index }
func (r *Record) Command() *pb.Command { return r.command }
func (r *Record) StoreName() string    { return r.store }
func (r *Record) Segments() []string   { return r.segments }
func (r *Record) Key() string          { return r.key }
func (r *Record) Bytes() int           { return r.bytes }

func NewRecord(store string, index uint64, command *pb.Command) (*Record, error) {
	resource := command.GetMutate().GetResource()
	if read := command.GetRead(); read != nil {
		resource = read.Resource
	}
	parts := make([]string, 0, strings.Count(resource, "/")+1)
	remaining := resource
	for {
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
	// Retained request, ordinal, decoded path, ticket and cancellation metadata.
	bytes := proto.Size(command) + 16 + len(store) + 2*len(resource) + 1024 + 16*len(parts)
	record := &Record{index: index, command: command, store: store, segments: parts, key: resource, bytes: bytes}
	return record, nil
}
