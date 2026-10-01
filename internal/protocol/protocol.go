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
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	MaxDocument      = 2 << 20
	MaxPayload       = 9 << 20
	MaxFrame         = MaxPayload + 128
	MaxResponse      = (64 << 10) + 32
	MaxEvent         = MaxDocument + (8 << 10)
	RouteOutstanding = 8
	RouteBytes       = 16 << 20
	MaxURI           = 4096
	ResultOverhead   = 512
	EntryOverhead    = 512
	MaxExpression    = 16 << 10
	MaxSelector      = 16 << 10
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
		for !utf8.ValidString(message) && len(message) > 0 {
			message = message[:len(message)-1]
		}
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

func ResultError(op *pb.Operation, outcome pb.MutationOutcome, f *pb.Failure) *pb.Result {
	r := &pb.Result{Index: op.GetIndex()}
	if op.GetRead() != nil {
		r.Result = &pb.Result_Read{Read: ReadFailure(f)}
	} else {
		r.Result = &pb.Result_Mutation{Mutation: Mutation(outcome, f)}
	}
	return r
}

func Validate(op *pb.Operation, store string) *pb.Failure {
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
			if a.AtomicTransform == nil || a.AtomicTransform.Form == nil {
				return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing transform")
			}
			switch transform := a.AtomicTransform.Form.(type) {
			case *pb.Transform_Program:
				program := transform.Program
				if program == nil ||
					program.Runtime == "" ||
					len(program.Source) == 0 ||
					len(program.Source) > MaxExpression ||
					!utf8.Valid(program.Source) ||
					strings.IndexByte(string(program.Source), 0) >= 0 ||
					strings.HasPrefix(string(program.Source), "\x1bLua") {
					return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing or oversized program transform")
				}
				if program.Runtime != "lua.v1" {
					return Fail(pb.FailureCode_UNSUPPORTED, "program runtime is unsupported")
				}
				if program.Input != nil {
					docs = append(docs, program.Input)
				}
			case *pb.Transform_BackendExpression:
				if transform.BackendExpression == nil || len(transform.BackendExpression.Data) == 0 || len(transform.BackendExpression.Data) > MaxExpression {
					return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing or oversized backend expression")
				}
				docs = append(docs, transform.BackendExpression)
			default:
				return Fail(pb.FailureCode_INVALID_ARGUMENT, "unknown transform")
			}
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

func Resource(op *pb.Operation) string {
	if r := op.GetRead(); r != nil {
		return r.Resource
	}
	return op.GetMutate().GetResource()
}

func ValidateScan(req *pb.ScanRequest, store string) *pb.Failure {
	if req == nil || proto.Size(req) > MaxFrame {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "missing or oversized Scan")
	}
	name, segments, err := ParseResource(req.Resource)
	if err != nil || name != store || len(segments) == 0 {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or wrong-store Scan resource")
	}
	if req.ReadMediaType != "" && !validMedia(req.ReadMediaType) {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Scan media type")
	}
	if d := req.Selector; d != nil && (!validMedia(d.MediaType) || len(d.Data) > MaxSelector) {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or oversized selector")
	}
	return nil
}

const NativeChunk = 64 << 10
const NativeDescriptor = 64 << 10

func ValidateNative(open *pb.NativeOpen, store string) *pb.Failure {
	if open == nil || proto.Size(open) > NativeDescriptor+MaxURI+256 || open.Descriptor_ == nil || len(open.Descriptor_.Data) > NativeDescriptor {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native Open bounds")
	}
	name, _, err := ParseResource(open.Resource)
	if err != nil || name != store {
		return Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native resource")
	}
	return nil
}

func NativeFailure(started bool, failure *pb.Failure) *pb.NativeEnd {
	completion := pb.NativeCompletion_NATIVE_NOT_STARTED
	if started {
		completion = pb.NativeCompletion_RESPONSE_INCOMPLETE
	}
	end := &pb.NativeEnd{Completion: completion, Failure: failure}
	return end
}

// ValidateRequest checks only the routing envelope. Payload interpretation is
// restricted to the final node selected by destination configuration.
func ValidateRequest(req *pb.Request, destination string, lastID uint64) error {
	if req == nil || req.Id == 0 || req.Id <= lastID || proto.Size(req) > MaxFrame || len(req.Payload) == 0 || len(req.Payload) > MaxPayload {
		return fmt.Errorf("invalid request ID or payload bounds")
	}
	if !storePattern.MatchString(req.Destination) || len(req.Destination) > 63 || destination != "" && req.Destination != destination {
		return fmt.Errorf("invalid or inconsistent destination")
	}
	if len(req.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("unknown request fields")
	}
	return nil
}

func ValidateResponse(response *pb.Response) error {
	if response == nil || response.Id == 0 || proto.Size(response) > MaxResponse || len(response.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("invalid response envelope")
	}
	if response.End {
		if len(response.Payload) != 0 {
			return fmt.Errorf("terminal response must have empty payload")
		}
	} else if len(response.Payload) == 0 || len(response.Payload) > NativeChunk {
		return fmt.Errorf("invalid response fragment")
	}
	return nil
}

func DecodeCall(payload []byte) (*pb.Call, error) {
	if len(payload) == 0 || len(payload) > MaxPayload {
		return nil, fmt.Errorf("invalid call bounds")
	}
	call := &pb.Call{}
	if err := proto.Unmarshal(payload, call); err != nil {
		return nil, fmt.Errorf("invalid call encoding")
	}
	if call.Version != 1 || call.Operation == nil || hasUnknown(call.ProtoReflect()) {
		return nil, fmt.Errorf("unsupported call version or fields")
	}
	var target string
	switch operation := call.Operation.(type) {
	case *pb.Call_Read:
		if operation.Read != nil {
			target = operation.Read.Resource
		}
	case *pb.Call_Mutate:
		if operation.Mutate != nil {
			target = operation.Mutate.Resource
		}
	case *pb.Call_Scan:
		if operation.Scan != nil {
			target = operation.Scan.Resource
		}
	case *pb.Call_Native:
		if operation.Native != nil && operation.Native.Open != nil {
			target = operation.Native.Open.Resource
		}
	}
	_, segments, err := ParseResource("weir://target/" + target)
	if err != nil || len(segments) == 0 {
		return nil, fmt.Errorf("call requires a canonical relative target")
	}
	return call, nil
}

// Unknown fields are rejected throughout the typed payload. Adding fields to
// version 1 therefore requires every client and executor to update together.
func hasUnknown(message protoreflect.Message) bool {
	if len(message.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsList() && field.Message() != nil {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if hasUnknown(list.Get(i).Message()) {
					unknown = true
					return false
				}
			}
		} else if field.IsMap() && field.MapValue().Message() != nil {
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				unknown = hasUnknown(entry.Message())
				return !unknown
			})
		} else if field.Message() != nil {
			unknown = hasUnknown(value.Message())
		}
		return !unknown
	})
	return unknown
}

// MarshalEvent returns one bounded length-delimited event. The caller splits
// these bytes into response fragments and releases them before the next event.
func MarshalEvent(event *pb.Event) ([]byte, error) {
	if err := ValidateEvent(event); err != nil {
		return nil, err
	}
	size := proto.Size(event)
	encoded := make([]byte, 0, protowire.SizeVarint(uint64(size))+size)
	encoded = protowire.AppendVarint(encoded, uint64(size))
	opts := proto.MarshalOptions{}
	return opts.MarshalAppend(encoded, event)
}

func ValidateEvent(event *pb.Event) error {
	if event == nil || event.Version != 1 || event.Value == nil || hasUnknown(event.ProtoReflect()) || proto.Size(event) > MaxEvent {
		return fmt.Errorf("invalid event version or bounds")
	}
	valid := false
	switch value := event.Value.(type) {
	case *pb.Event_Result:
		result := value.Result
		if result != nil && result.Index != 0 {
			if read := result.GetRead(); read != nil {
				switch item := read.Result.(type) {
				case *pb.ReadResult_Document:
					valid = validDocument(item.Document, MaxDocument)
				case *pb.ReadResult_Missing:
					valid = item.Missing != nil
				case *pb.ReadResult_Failure:
					valid = item.Failure != nil && validFailure(item.Failure)
				}
			} else if mutation := result.GetMutation(); mutation != nil {
				valid = mutation.Outcome >= pb.MutationOutcome_NOT_STARTED && mutation.Outcome <= pb.MutationOutcome_UNKNOWN && validFailure(mutation.Failure)
				if mutation.Outcome == pb.MutationOutcome_APPLIED {
					valid = valid && mutation.Failure == nil
				} else {
					valid = valid && mutation.Failure != nil
				}
			}
		}
	case *pb.Event_Document:
		valid = validDocument(value.Document, MaxDocument)
	case *pb.Event_Head:
		head := value.Head
		valid = head != nil && (head.BodyMediaType == "" || validMedia(head.BodyMediaType)) && (head.Metadata == nil || validDocument(head.Metadata, NativeDescriptor))
	case *pb.Event_Chunk:
		valid = len(value.Chunk) > 0 && len(value.Chunk) <= NativeChunk
	case *pb.Event_ScanEnd:
		valid = value.ScanEnd != nil && validFailure(value.ScanEnd.Failure)
	case *pb.Event_NativeEnd:
		end := value.NativeEnd
		if end != nil {
			valid = end.Completion >= pb.NativeCompletion_NATIVE_NOT_STARTED && end.Completion <= pb.NativeCompletion_RESPONSE_INCOMPLETE && validFailure(end.Failure)
			if end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				valid = valid && end.Failure == nil
			} else {
				valid = valid && end.Failure != nil
			}
		}
	}
	if !valid {
		return fmt.Errorf("invalid event value")
	}
	return nil
}

func validFailure(failure *pb.Failure) bool {
	return failure == nil || failure.Code >= pb.FailureCode_INVALID_ARGUMENT && failure.Code <= pb.FailureCode_INTERNAL && len(failure.Message) <= 1024 && utf8.ValidString(failure.Message)
}
func validDocument(document *pb.Document, limit int) bool {
	return document != nil && validMedia(document.MediaType) && len(document.Data) <= limit
}
