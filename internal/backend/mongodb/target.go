package mongodb

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// namespace is copied into a prepared request, never attached to the client.
type namespace struct {
	database   string
	collection string
}

func validNamespace(parts []string) bool {
	return len(parts) >= 2 && validDatabaseName(parts[0]) && validCollectionName(parts[1]) && len(parts[0])+1+len(parts[1]) <= 255
}

func validDatabaseName(name string) bool {
	if len(name) == 0 || len(name) >= 64 || strings.ContainsAny(name, "/\\. \"$*<>:|?") || !utf8.ValidString(name) {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validCollectionName(name string) bool {
	if name == "" || strings.ContainsRune(name, '$') || strings.HasPrefix(name, "system.") || strings.Contains(name, ".system.") || !utf8.ValidString(name) {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func (n namespace) String() string {
	return n.database + "." + n.collection
}

// Collection structure is a deployment prerequisite and stays stable while the
// Store is open. Actual read/write commands still enforce backend permissions.
func (a *Adapter) qualifyTarget(ctx context.Context, target namespace) (*pb.Failure, execution.Feedback) {
	lookup, err := a.targets.Acquire(ctx, target)
	if err != nil {
		return protocol.ContextFailure(ctx), execution.Neutral
	}
	if lookup.Cached {
		return nil, execution.Healthy
	}
	failure, signal := a.inspectTarget(ctx, target)
	empty := struct{}{}
	a.targets.Complete(target, lookup, empty, failure == nil && ctx.Err() == nil)
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx), execution.Neutral
	}
	return failure, signal
}

func (a *Adapter) inspectTarget(ctx context.Context, target namespace) (*pb.Failure, execution.Feedback) {
	filter := bson.D{{Key: "name", Value: target.collection}}
	specs, err := a.client.Database(target.database).ListCollectionSpecifications(ctx, filter)
	if err != nil {
		return backendFailure(ctx, err), feedback(ctx, err)
	}
	if len(specs) == 0 {
		return protocol.Fail(pb.FailureCode_TARGET_NOT_FOUND, "target collection does not exist"), execution.Neutral
	}
	if len(specs) != 1 || specs[0].Name != target.collection || specs[0].Type != "collection" {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "one concrete collection required"), execution.Neutral
	}
	if collation := specs[0].Options.Lookup("collation"); collation.Type != 0 {
		document, valid := collation.DocumentOK()
		locale, validLocale := document.Lookup("locale").StringValueOK()
		if !valid || !validLocale || locale != "simple" {
			return protocol.Fail(pb.FailureCode_UNSUPPORTED, "simple collation required"), execution.Neutral
		}
	}
	if capped := specs[0].Options.Lookup("capped"); capped.Type != 0 {
		value, valid := capped.BooleanOK()
		if !valid || value {
			return protocol.Fail(pb.FailureCode_UNSUPPORTED, "capped collections unsupported"), execution.Neutral
		}
	}
	return nil, execution.Healthy
}

// A cancelled group never contacts MongoDB. Read and write commands within one
// Execute share this qualification; later calls reuse successful target checks.
// Command builders recheck callers because metadata I/O can outlive them.
func (a *Adapter) qualifyRecordBatch(ctx context.Context, plans []*execution.Plan) ([]*pb.Event, execution.Feedback) {
	var failure *pb.Failure
	signal := execution.Neutral
	for _, work := range plans {
		if unstarted(ctx, work) == nil {
			failure, signal = a.qualifyTarget(ctx, work.Backend.(*plan).target)
			if failure == nil {
				return nil, signal
			}
			break
		}
	}
	results := make([]*pb.Event, len(plans))
	for i, work := range plans {
		result := unstarted(ctx, work)
		if result == nil {
			result = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
		}
		results[i] = result
	}
	return results, signal
}
