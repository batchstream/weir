package mongodb

import (
	"context"
	"regexp"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// namespace is copied into a prepared request, never attached to the client.
type namespace struct {
	database   string
	collection string
}

var namespacePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,62}$`)

func validNamespace(parts []string) bool {
	return len(parts) >= 2 && namespacePattern.MatchString(parts[0]) && namespacePattern.MatchString(parts[1])
}

func (n namespace) String() string {
	return n.database + "." + n.collection
}

// Qualify the requested collection before dispatch. Each execution rechecks the
// target instead of retaining a namespace cache.
func (a *Adapter) qualifyTarget(ctx context.Context, target namespace) (*pb.Failure, execution.Feedback) {
	filter := bson.D{{Key: "name", Value: target.collection}}
	specs, err := a.client.Database(target.database).ListCollectionSpecifications(ctx, filter)
	if err != nil {
		return backendFailure(ctx, err), feedback(ctx, err)
	}
	if len(specs) != 1 || specs[0].Name != target.collection || specs[0].Type != "collection" {
		return protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "create the target collection before executing requests"), execution.Neutral
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

// A cancelled group never contacts MongoDB. Recheck each caller after this
// qualification in the command builder, because metadata I/O can outlive it.
func (a *Adapter) qualifyRecordBatch(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
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
	results := make([]*pb.BulkResult, len(plans))
	for i, work := range plans {
		result := unstarted(ctx, work)
		if result == nil {
			result = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_STARTED, failure)
		}
		results[i] = result
	}
	return results, signal
}
