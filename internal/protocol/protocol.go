// Package protocol validates wire envelopes without interpreting document fields.
package protocol

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

const (
	MaxDocument    = 256 << 10
	MaxFrame       = 300 << 10
	MaxURI         = 4096
	ResultOverhead = 512
	EntryOverhead  = 512
)

var storePattern = regexp.MustCompile(`^[a-z](?:[a-z0-9]|-[a-z0-9]){0,62}$`)
var mediaPattern = regexp.MustCompile(`^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$`)

func ParseResource(raw string) (string, []string, error) {
	if len(raw) > MaxURI || !strings.HasPrefix(raw, "weir://") {
		return "", nil, fmt.Errorf("invalid resource")
	}
	pieces := strings.Split(strings.TrimPrefix(raw, "weir://"), "/")
	if len(pieces[0]) > 63 || !storePattern.MatchString(pieces[0]) {
		return "", nil, fmt.Errorf("invalid store")
	}
	segments := make([]string, 0, len(pieces)-1)
	for _, p := range pieces[1:] {
		s, err := url.PathUnescape(p)
		if err != nil || s == "" || s == "." || s == ".." || !utf8.ValidString(s) {
			return "", nil, fmt.Errorf("invalid segment")
		}
		for _, r := range s {
			if unicode.IsControl(r) {
				return "", nil, fmt.Errorf("control character")
			}
		}
		if EncodeSegment(s) != p {
			return "", nil, fmt.Errorf("noncanonical segment")
		}
		segments = append(segments, s)
	}
	return pieces[0], segments, nil
}
func EncodeSegment(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~:", rune(c)) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}
func Fail(code pb.FailureCode, message string) *pb.Failure {
	if len(message) > 1024 {
		message = message[:1024]
	}
	f := &pb.Failure{Code: code, Message: message}
	return f
}
func ContextFailure(ctx context.Context) *pb.Failure {
	if ctx.Err() == context.DeadlineExceeded {
		return Fail(pb.FailureCode_DEADLINE_EXCEEDED, "deadline exceeded")
	}
	return Fail(pb.FailureCode_CANCELLED, "cancelled")
}
func Mutation(outcome pb.MutationOutcome, failure *pb.Failure) *pb.MutationResult {
	r := &pb.MutationResult{Outcome: outcome, Failure: failure}
	return r
}
func ReadFailure(f *pb.Failure) *pb.ReadResult {
	v := &pb.ReadResult_Failure{Failure: f}
	r := &pb.ReadResult{Result: v}
	return r
}
func ReadDocument(d *pb.Document) *pb.ReadResult {
	v := &pb.ReadResult_Document{Document: d}
	r := &pb.ReadResult{Result: v}
	return r
}
func Missing() *pb.ReadResult {
	e := &pb.Empty{}
	v := &pb.ReadResult_Missing{Missing: e}
	r := &pb.ReadResult{Result: v}
	return r
}
func ResultError(op *pb.BulkOperation, outcome pb.MutationOutcome, f *pb.Failure) *pb.BulkResult {
	r := &pb.BulkResult{Index: op.GetIndex()}
	if op.GetRead() != nil {
		r.Result = &pb.BulkResult_Read{Read: ReadFailure(f)}
	} else {
		r.Result = &pb.BulkResult_Mutation{Mutation: Mutation(outcome, f)}
	}
	return r
}
func Validate(op *pb.BulkOperation, store string) *pb.Failure {
	if op == nil || proto.Size(op) > MaxFrame {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing or oversized operation")
	}
	var resource string
	var docs []*pb.Document
	if r := op.GetRead(); r != nil {
		resource = r.Resource
		if r.ReadMediaType != "" && !validMedia(r.ReadMediaType) {
			return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid media type")
		}
		if r.AdapterOptions != nil {
			docs = append(docs, r.AdapterOptions)
		}
	} else if m := op.GetMutate(); m != nil {
		resource = m.Resource
		if m.Action == nil {
			return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing action")
		}
		if m.AdapterOptions != nil {
			docs = append(docs, m.AdapterOptions)
		}
		switch a := m.Action.(type) {
		case *pb.MutateRequest_Put:
			docs = append(docs, a.Put)
		case *pb.MutateRequest_Create:
			docs = append(docs, a.Create)
		case *pb.MutateRequest_Replace:
			docs = append(docs, a.Replace)
		case *pb.MutateRequest_Delete:
			if a.Delete == nil {
				return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing delete")
			}
		case *pb.MutateRequest_AtomicTransform:
			return Fail(pb.FailureCode_UNSUPPORTED, "AtomicTransform is not enabled in milestone 1")
		default:
			return Fail(pb.FailureCode_INVALID_ARGUMENT, "unknown action")
		}
	} else {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing operation")
	}
	name, segments, err := ParseResource(resource)
	if err != nil || len(segments) == 0 || name != store {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or wrong-store resource")
	}
	for _, d := range docs {
		if d == nil || !validMedia(d.MediaType) || len(d.Data) > MaxDocument {
			return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid document envelope")
		}
	}
	return nil
}
func validMedia(s string) bool { return len(s) <= 127 && mediaPattern.MatchString(s) }
func Resource(op *pb.BulkOperation) string {
	if r := op.GetRead(); r != nil {
		return r.Resource
	}
	return op.GetMutate().GetResource()
}
