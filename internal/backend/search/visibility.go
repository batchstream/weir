package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func (b *recordBatch) queueVisibilityBarrier(index string, position int) {
	if b.barriers == nil {
		b.barriers = make(map[string][]int)
	}
	b.barriers[index] = append(b.barriers[index], position)
}

// A noop update and a delete of an absent record may not install a refresh
// listener. Their successful real-time state still needs a search barrier.
func (b *recordBatch) queueBulkVisibilityBarriers(works []*execution.Plan, positions []int, replies []*pb.Event, raw []byte) {
	if !works[0].Backend.(*plan).waitUntilVisible {
		return
	}
	var envelope struct {
		Items []map[string]struct{ Result string } `json:"items"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Items) != len(works) {
		return
	}
	for i, work := range works {
		mutation := replies[i].GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
			continue
		}
		native := work.Backend.(*plan)
		result := envelope.Items[i][native.bulkAction()].Result
		if result == "noop" || result == "not_found" {
			b.queueVisibilityBarrier(native.index, positions[i])
		}
	}
}

func (b *recordBatch) flushVisibilityBarriers() {
	for index, positions := range b.barriers {
		active := positions[:0]
		for _, position := range positions {
			work := b.works[position]
			if failure := b.callerFailure(work); failure != nil {
				b.results[position] = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, failure)
				continue
			}
			active = append(active, position)
		}
		if len(active) == 0 {
			continue
		}
		failure := b.adapter.refreshBarrier(b.ctx, index)
		if failure != nil {
			for _, position := range active {
				b.results[position] = execution.FailedEvent(b.works[position].Command, pb.MutationOutcome_APPLIED, failure)
			}
		}
	}
	clear(b.barriers)
}

func (a *Adapter) refreshBarrier(ctx context.Context, index string) *pb.Failure {
	call := exchange{path: "/" + url.PathEscape(index) + "/_refresh", method: http.MethodPost}
	status, raw, err := a.request(ctx, call)
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	switch err {
	case errTimeout:
		return protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "query visibility deadline exceeded")
	case errCanceled:
		return protocol.Fail(pb.FailureCode_CANCELLED, "query visibility canceled")
	}
	if err != nil {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "query visibility acknowledgement unavailable or incomplete")
	}
	if status != http.StatusOK {
		return a.nativeResponseFailure(ctx, status, raw)
	}
	var reply struct {
		Shards *struct{ Total, Successful, Failed *int } `json:"_shards"`
	}
	if json.Unmarshal(raw, &reply) == nil && reply.Shards != nil {
		shards := reply.Shards
		if shards.Total != nil && shards.Successful != nil && shards.Failed != nil && *shards.Total > 0 && *shards.Successful == *shards.Total && *shards.Failed == 0 {
			return nil
		}
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "query visibility acknowledgement unavailable or incomplete")
}
