package mongodb

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// programFolds holds the chains of Lua programs of one record that a program
// batch runs as one, each under a plan that stands for the whole chain.
type programFolds struct {
	chains map[*execution.Plan][]*execution.Plan
	// own holds, from a chain's last evaluation, the result of each program
	// that did not take part in its write, and nil for those that did.
	own map[*execution.Plan][]*pb.MutationResult
	// unfold marks chains whose write MongoDB rejected; none of their
	// programs applied, so they run again one at a time.
	unfold   map[*execution.Plan]bool
	releases []func()
}

func (f *programFolds) chain(stand *execution.Plan) []*execution.Plan {
	if f == nil {
		return nil
	}
	return f.chains[stand]
}

func (f *programFolds) release() {
	for _, release := range f.releases {
		release()
	}
}

// foldPrograms links the programs of one record into chains and returns the
// plans a program batch runs: each single program, and for each chain a plan
// whose caller remains while any of its programs' callers does.
func foldPrograms(plans []*execution.Plan) ([]*execution.Plan, *programFolds) {
	members := make(map[string][]*execution.Plan, len(plans))
	order := make([]string, 0, len(plans))
	for _, work := range plans {
		if _, seen := members[work.Key]; !seen {
			order = append(order, work.Key)
		}
		members[work.Key] = append(members[work.Key], work)
	}
	if len(order) == len(plans) {
		return plans, nil
	}
	folds := &programFolds{
		chains: make(map[*execution.Plan][]*execution.Plan),
		own:    make(map[*execution.Plan][]*pb.MutationResult),
		unfold: make(map[*execution.Plan]bool),
	}
	stands := make([]*execution.Plan, 0, len(order))
	for _, key := range order {
		chain := members[key]
		if len(chain) == 1 || !chain[0].Folds {
			stands = append(stands, chain...)
			continue
		}
		stand := *chain[0]
		ctx, release := chainContext(chain)
		stand.Context = ctx
		folds.releases = append(folds.releases, release)
		folds.chains[&stand] = chain
		stands = append(stands, &stand)
	}
	return stands, folds
}

// chainContext ends once every member's context has ended, and no later than
// the latest member deadline when every member has one.
func chainContext(chain []*execution.Plan) (context.Context, func()) {
	var latest time.Time
	bounded := true
	for _, work := range chain {
		deadline, ok := work.Context.Deadline()
		bounded = bounded && ok
		if deadline.After(latest) {
			latest = deadline
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	if bounded {
		ctx, cancel = context.WithDeadline(context.Background(), latest)
	}
	var live atomic.Int64
	live.Store(int64(len(chain)))
	stops := make([]func() bool, 0, len(chain))
	for _, work := range chain {
		stop := context.AfterFunc(work.Context, func() {
			if live.Add(-1) == 0 {
				cancel()
			}
		})
		stops = append(stops, stop)
	}
	release := func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
	return ctx, release
}

// transformChain evaluates a chain's programs in queue order, each on the
// document the programs before it left, and returns one write from the read
// document to the last. A program that does not change the document, or that
// fails, leaves it to the next.
func (a *Adapter) transformChain(batch context.Context, folds *programFolds, stand *execution.Plan, raw bson.Raw) (*execution.Plan, *pb.MutationResult) {
	chain := folds.chains[stand]
	own := make([]*pb.MutationResult, len(chain))
	folds.own[stand] = own
	state := raw
	for i, work := range chain {
		write, result := a.transformProgram(batch, work, state)
		switch {
		case result == nil && write.Backend.(*plan).action == "delete":
			state = nil
		case result == nil:
			state = write.Backend.(*plan).document
		case result.Outcome != pb.MutationOutcome_APPLIED || result.Failure != nil:
			own[i] = result
		}
	}
	native := stand.Backend.(*plan)
	prepared := &plan{target: native.target, id: native.id}
	switch {
	case raw == nil && state == nil, raw != nil && state != nil && bytes.Equal(raw, state):
		unchanged := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
		return nil, unchanged
	case state == nil:
		prepared.action = "delete"
	case raw == nil:
		prepared.action, prepared.document = "create", state
	default:
		prepared.action, prepared.document = "replace", state
	}
	write := &execution.Plan{Command: stand.Command, Backend: prepared}
	return write, nil
}

// unfoldPrograms runs each program of a rejected chain alone, in queue order.
func (a *Adapter) unfoldPrograms(ctx context.Context, chain []*execution.Plan) []*pb.MutationResult {
	results := make([]*pb.MutationResult, len(chain))
	for i, work := range chain {
		single := []*execution.Plan{work}
		batch := &programBatch{plans: single, results: make([]*pb.MutationResult, 1)}
		a.runPrograms(ctx, batch)
		results[i] = batch.results[0]
	}
	return results
}

// chainResult is a program's result from its chain's result: a caller that
// left before the chain ran keeps its own cause.
func chainResult(work *execution.Plan, result *pb.MutationResult) *pb.MutationResult {
	code := result.GetFailure().GetCode()
	unstarted := result.GetOutcome() == pb.MutationOutcome_NOT_STARTED || result.GetOutcome() == pb.MutationOutcome_NOT_APPLIED
	if unstarted && work.Context.Err() != nil && (code == pb.FailureCode_CANCELLED || code == pb.FailureCode_DEADLINE_EXCEEDED) {
		own := protocol.Mutation(result.GetOutcome(), protocol.ContextFailure(work.Context))
		return own
	}
	return result
}

// foldWrites keeps, of each chain of puts and deletes of one record, the last
// live one, which leaves the record as the whole chain would since each
// replaces or removes it whatever it held. It returns the kept writes and,
// for each kept write that stands for others, all their positions.
func foldWrites(active []*execution.Plan, positions []int, writes []bulkWrite) ([]*execution.Plan, []int, []bulkWrite, map[int][]int) {
	indexes := make(map[string][]int, len(active))
	for i, work := range active {
		if work.Folds {
			indexes[work.Key] = append(indexes[work.Key], i)
		}
	}
	chains := make(map[string][]int)
	for key, chain := range indexes {
		if len(chain) < 2 {
			continue
		}
		members := make([]int, len(chain))
		for j, i := range chain {
			members[j] = positions[i]
		}
		chains[key] = members
	}
	if len(chains) == 0 {
		return active, positions, writes, nil
	}
	kept := 0
	stands := make(map[int][]int, len(chains))
	for i, work := range active {
		if members := chains[work.Key]; work.Folds && members != nil {
			chain := indexes[work.Key]
			if chain[len(chain)-1] != i {
				continue
			}
			stands[kept] = members
		}
		active[kept], positions[kept], writes[kept] = work, positions[i], writes[i]
		kept++
	}
	return active[:kept], positions[:kept], writes[:kept], stands
}
