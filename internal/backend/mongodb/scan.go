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

// A Scan holds two bounded native wire buffers, the copied output prefix and
// metadata/framing while validating the entire response.
const scanWorkingBytes = 48 << 20

type scanPlan struct {
	execution.ScanProgress
	target            namespace
	options           bson.D
	filter            bson.RawValue
	last              bson.Raw
	fingerprint       string
	pageSize          uint64
	batchSize         int
	qualified, closed bool
	includeID         bool
}

type scanCheckpoint struct {
	Last      bson.Raw `bson:"last"`
	BatchSize int32    `bson:"batch_size"`
}

func (a *Adapter) prepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateScan(req); f != nil {
		return nil, f
	}
	parts, _ := protocol.ParseRelativeResource(req.Resource)
	if len(parts) != 2 || !validNamespace(parts) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid MongoDB Scan target")
	}
	target := namespace{database: parts[0], collection: parts[1]}
	native := &scanPlan{target: target, pageSize: protocol.ScanPageSize(req), batchSize: execution.ScanBatchDocuments, fingerprint: protocol.ScanFingerprint(req, a.config.Store, "mongodb"), includeID: true}
	if d := req.Filter; d != nil {
		if d.ContentType != "application/bson" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Scan filter requires BSON")
		}
		nodes := 4096
		if !validScanBSON(d.Data, 0, &nodes) {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive BSON filter")
		}
		native.filter = bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d.Data}
	}
	if projection := req.Projection; projection != nil {
		fields := bson.D{}
		include := projection.Mode == pb.ProjectionMode_INCLUDE
		native.includeID = !include
		for _, name := range projection.Fields {
			for _, segment := range strings.Split(name, ".") {
				if strings.HasPrefix(segment, "$") {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Scan projection does not support dollar-prefixed fields")
				}
			}
			if strings.HasPrefix(name, "_id.") {
				return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Scan projects the complete _id or excludes it")
			}
			if name == "_id" {
				native.includeID = include
				continue
			}
			flag := int32(0)
			if include {
				flag = 1
			}
			field := bson.E{Key: name, Value: flag}
			fields = append(fields, field)
		}
		if include {
			identity := bson.E{Key: "_id", Value: int32(1)}
			fields = append(fields, identity)
		}
		if len(fields) != 0 {
			option := bson.E{Key: "projection", Value: fields}
			native.options = append(native.options, option)
		}
	}
	if len(req.ContinuationToken) != 0 {
		state, err := protocol.DecodeScanToken(req.ContinuationToken, "mongodb", native.fingerprint)
		fields, fieldsErr := scanFields(state)
		last, lastOK := fields["last"].DocumentOK()
		batchSize, batchOK := fields["batch_size"].Int32OK()
		if err != nil || fieldsErr != nil || len(fields) != 2 || !lastOK || !validScanID(last) || !batchOK || batchSize < 1 || batchSize > execution.ScanBatchDocuments {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or mismatched MongoDB Scan continuation")
		}
		native.last = last
		native.batchSize = int(batchSize)
	}
	p := &execution.Plan{
		Singleton:    true,
		Key:          req.Resource,
		Bytes:        proto.Size(req) + execution.EntryOverheadBytes + 4096,
		ResultBytes:  execution.ScanResultBytes,
		WorkingBytes: scanWorkingBytes,
		Backend:      native,
	}
	return p, nil
}

func scanFindCommand(n *scanPlan) bson.D {
	batchSize := min(n.pageSize-n.Count, uint64(execution.ScanBatchDocuments), uint64(n.batchSize))
	order := bson.D{{Key: "_id", Value: int32(1)}}
	command := bson.D{
		{Key: "find", Value: n.target.collection},
		{Key: "sort", Value: order},
		{Key: "hint", Value: order},
		{Key: "limit", Value: int64(batchSize)},
		{Key: "batchSize", Value: int32(batchSize)},
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
	if n.Count >= n.pageSize {
		page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "Scan fetched beyond its logical page")
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
	batchSize := min(n.pageSize-n.Count, uint64(execution.ScanBatchDocuments), uint64(n.batchSize))
	cursor := &recordCursor{target: n.target, items: int(batchSize), outputBytes: execution.ScanBatchBytes}
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
	// singleBatch closes even a byte-truncated native batch. Only an empty
	// response proves exhaustion; any discarded tail is fetched again from
	// the last accepted identity.
	page.Exhausted = len(page.Documents) == 0 && !cursor.limited
	if len(page.Documents) != 0 {
		id := bson.Raw(page.Documents[len(page.Documents)-1].Data).Lookup("_id")
		identity := bson.D{{Key: "_id", Value: id}}
		state, err := bson.Marshal(identity)
		if err != nil || !validScanID(state) {
			page.Documents = nil
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan _id is invalid or exceeds continuation bound")
			return page, execution.Neutral
		}
		n.last = state
	}
	if !n.includeID {
		for _, document := range page.Documents {
			start, output := bsoncore.AppendDocumentStart(nil)
			remaining := document.Data[4 : len(document.Data)-1]
			for len(remaining) != 0 {
				element, tail, _ := bsoncore.ReadElement(remaining)
				remaining = tail
				if element.Key() != "_id" {
					output = append(output, element...)
				}
			}
			output, _ = bsoncore.AppendDocumentEnd(output, start)
			document.Data = output
		}
	}
	if cursor.limited {
		// Learn from the accepted byte-bounded prefix, not a short logical page.
		// The next request and its continuation reuse this conservative capacity.
		n.batchSize = max(1, len(page.Documents))
	}
	if !page.Exhausted && n.Count+uint64(len(page.Documents)) >= n.pageSize {
		checkpoint := scanCheckpoint{Last: n.last, BatchSize: int32(n.batchSize)}
		state, err := bson.Marshal(checkpoint)
		var token []byte
		if err == nil {
			token, err = protocol.EncodeScanToken("mongodb", n.fingerprint, state)
		}
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
