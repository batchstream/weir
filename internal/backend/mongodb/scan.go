package mongodb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

// The connection guard caps wire bytes and metadata expansion before the driver
// allocates/decodes its copy. RunCommand.Raw retains the driver buffer without
// cursor batch decoding. Reserve both wire buffers plus bounded metadata/framing;
// this is separate from the one output-frame credit, not a claim about RSS.
const scanPageBudget = 24 << 20
const scanNativeLimit = (16 << 20) + (64 << 10)

type scanPlan struct {
	count             uint64
	target            namespace
	options           bson.D
	filter            bson.RawValue
	last              bson.Raw
	fingerprint       string
	pageSize          uint64
	qualified, closed bool
}

func (a *Adapter) prepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateScan(req, a.config.Store); f != nil {
		return nil, f
	}
	_, parts, _ := protocol.ParseResource(req.Resource)
	if len(parts) != 2 || !validNamespace(parts) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid MongoDB Scan target")
	}
	if req.ReadMediaType != "" && req.ReadMediaType != "application/bson" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan outputs native BSON")
	}
	target := namespace{database: parts[0], collection: parts[1]}
	native := &scanPlan{target: target, pageSize: protocol.ScanPageSize(req), fingerprint: protocol.ScanFingerprint(req, "mongodb")}
	if d := req.Selector; d != nil {
		if d.MediaType != "application/bson" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "find selector requires BSON")
		}
		// Existing bounded codec validates selector structure before any materialized
		// BSON option list. Scalar types have their original native BSON semantics.
		if _, err := Decode(d.Data); err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive BSON selector")
		}
		fields, err := scanFields(d.Data)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid selector")
		}
		// Maintain selector field order. No caller command, lifecycle or partial flags.
		elements, _ := bson.Raw(d.Data).Elements()
		for _, e := range elements {
			key := e.Key()
			if key != "filter" && key != "sort" && key != "projection" {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unsupported find selector option")
			}
			if fields[key].Type != bson.TypeEmbeddedDocument {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "find option must be a document")
			}
			switch key {
			case "filter":
				native.filter = e.Value()
			case "sort":
				sort, err := scanFields(e.Value().Value)
				if err != nil || len(sort) != 0 && (len(sort) != 1 || !scanOK(sort["_id"])) {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan requires ascending _id order")
				}
			case "projection":
				projection, err := scanFields(e.Value().Value)
				if err != nil {
					return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Scan projection")
				}
				for name, value := range projection {
					if strings.HasPrefix(name, "_id.") || name == "_id" && !scanOK(value) && !(value.Type == bson.TypeBoolean && value.Boolean()) {
						return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan projection must preserve the original _id")
					}
				}
				option := bson.E{Key: key, Value: e.Value()}
				native.options = append(native.options, option)
			}
		}
	}
	if len(req.ContinuationToken) != 0 {
		state, err := protocol.DecodeScanToken(req.ContinuationToken, "mongodb", native.fingerprint)
		if err != nil || !validScanID(state) {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or mismatched MongoDB Scan continuation")
		}
		native.last = state
	}
	p := &execution.Plan{
		Singleton:    true,
		Key:          req.Resource,
		Bytes:        proto.Size(req) + protocol.EntryOverhead + 4096,
		ResultBytes:  protocol.MaxDocument + protocol.ResultOverhead,
		WorkingBytes: scanPageBudget,
		Backend:      native,
	}
	return p, nil
}

func scanFindCommand(n *scanPlan) bson.D {
	order := bson.D{{Key: "_id", Value: int32(1)}}
	command := bson.D{
		{Key: "find", Value: n.target.collection},
		{Key: "sort", Value: order},
		{Key: "hint", Value: order},
		{Key: "limit", Value: int64(1)},
		{Key: "batchSize", Value: int32(1)},
		{Key: "singleBatch", Value: true},
		{Key: "allowPartialResults", Value: false},
	}
	command = append(command, n.options...)
	filter := any(n.filter)
	if n.filter.Type == 0 {
		filter = bson.D{}
	}
	if len(n.last) != 0 {
		id := n.last.Lookup("_id")
		// Expression comparison uses the full BSON order. Native $gt applies
		// type bracketing, while find.min has an exclusive MaxKey upper bound.
		// $literal preserves dollar-prefixed strings and embedded document IDs.
		literal := bson.D{{Key: "$literal", Value: id}}
		greater := bson.D{{Key: "$gt", Value: bson.A{"$_id", literal}}}
		boundary := bson.D{{Key: "$expr", Value: greater}}
		both := bson.D{{Key: "$and", Value: bson.A{filter, boundary}}}
		filter = both
	}
	element := bson.E{Key: "filter", Value: filter}
	command = append(command, element)
	return command
}

func (a *Adapter) fetchScan(ctx context.Context, p *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	n := p.Backend.(*scanPlan)
	page := &execution.ScanPage{}
	if n.closed || ctx.Err() != nil {
		page.Failure = protocol.ContextFailure(ctx)
		return page, execution.Neutral
	}
	if !n.qualified {
		if failure, signal := a.qualifyTarget(ctx, n.target); failure != nil {
			page.Failure = failure
			return page, signal
		}
		n.qualified = true
	}
	command := scanFindCommand(n)
	raw, err := a.client.Database(n.target.database).RunCommand(ctx, command).Raw()
	if err != nil {
		page.Failure = backendFailure(ctx, err)
		return page, feedback(ctx, err)
	}
	cursor := &recordCursor{target: n.target, items: 1}
	page = a.recordCursorReply(raw, cursor, true)
	if page.Failure != nil {
		return page, execution.Neutral
	}
	if cursor.cursor != 0 {
		page.Documents = nil
		page.Exhausted = false
		page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "single-batch Scan retained a cursor")
		return page, execution.Neutral
	}
	page.Exhausted = len(page.Documents) == 0
	if len(page.Documents) != 0 {
		id := bson.Raw(page.Documents[0].Data).Lookup("_id")
		identity := bson.D{{Key: "_id", Value: id}}
		state, err := bson.Marshal(identity)
		if err != nil || !validScanID(state) {
			page.Documents = nil
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan _id is invalid or exceeds continuation bound")
			return page, execution.Neutral
		}
		n.last = state
	}
	if !page.Exhausted && n.count+uint64(len(page.Documents)) >= n.pageSize {
		token, err := protocol.EncodeScanToken("mongodb", n.fingerprint, n.last)
		if err != nil {
			page.Documents = nil
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan continuation exceeds bound")
			return page, execution.Neutral
		}
		page.Complete = true
		page.NextContinuationToken = token
	}
	return page, execution.Healthy
}

func validScanID(raw []byte) bool {
	if len(raw) > protocol.MaxScanState {
		return false
	}
	nodes := 65536
	if !validScanBSON(raw, 0, &nodes) {
		return false
	}
	fields, err := scanFields(raw)
	if err != nil || len(fields) != 1 {
		return false
	}
	id, exists := fields["_id"]
	return exists && id.Type != bson.TypeArray && id.Type != bson.TypeRegex && id.Type != bson.TypeUndefined && id.Type != 0
}

func (a *Adapter) closeScan(_ context.Context, p *execution.Plan) *pb.Failure {
	state := p.Backend.(*scanPlan)
	state.closed = true
	state.options = nil
	state.filter = bson.RawValue{}
	state.last = nil
	return nil
}

func scanOK(value bson.RawValue) bool {
	switch value.Type {
	case bson.TypeDouble:
		return value.Double() == 1
	case bson.TypeInt32:
		return value.Int32() == 1
	case bson.TypeInt64:
		return value.Int64() == 1
	}
	return false
}

// Validation walks bounded raw bytes; it never builds a native document tree or
// converts BSON types. MongoDB's native maximum nesting depth is 100.
func validScanBSON(raw []byte, depth int, nodes *int) bool {
	if depth > 100 || !scanFraming(raw) {
		return false
	}
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		*nodes--
		if *nodes < 0 {
			return false
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return false
		}
		rest = tail
		value, err := element.ValueErr()
		if err != nil {
			return false
		}
		switch value.Type {
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			if !validScanBSON(value.Data, depth+1, nodes) {
				return false
			}
		case bsoncore.TypeCodeWithScope:
			_, scope, ok := value.CodeWithScopeOK()
			if !ok || !validScanBSON(scope, depth+1, nodes) {
				return false
			}
		default:
			if value.Validate() != nil {
				return false
			}
		}
	}
	return true
}

func scanFraming(raw []byte) bool {
	return len(raw) >= 5 && int64(binary.LittleEndian.Uint32(raw)) == int64(len(raw)) && raw[len(raw)-1] == 0
}

func scanFields(raw []byte) (map[string]bson.RawValue, error) {
	if !scanFraming(raw) {
		return nil, fmt.Errorf("invalid BSON framing")
	}
	fields := make(map[string]bson.RawValue)
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		if len(fields) >= 32 {
			return nil, fmt.Errorf("envelope field limit")
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return nil, fmt.Errorf("invalid element")
		}
		rest = tail
		key, err := element.KeyErr()
		if err != nil {
			return nil, err
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate field")
		}
		value, err := element.ValueErr()
		if err != nil {
			return nil, err
		}
		fields[key] = bson.RawValue{Type: bson.Type(value.Type), Value: value.Data}
	}
	return fields, nil
}
