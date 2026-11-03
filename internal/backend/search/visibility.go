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
func (b *recordBatch) queueBulkVisibilityBarriers(works []*execution.Plan, positions []int, replies []*pb.Event, items []bulkItem) {
	for i := range items {
		mutation := replies[i].GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
			continue
		}
		if result := items[i].result; result == "noop" || result == "not_found" {
			b.queueVisibilityBarrier(works[i].Backend.(*plan).index, positions[i])
		}
	}
}

func (b *recordBatch) flushVisibilityBarriers() {
	for index, positions := range b.barriers {
		active := positions[:0]
		for _, position := range positions {
			work := b.works[position]
			if failure := work.Ended(b.ctx); failure != nil {
				b.fail(position, pb.MutationOutcome_APPLIED, failure)
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
				b.fail(position, pb.MutationOutcome_APPLIED, failure)
			}
		}
	}
	clear(b.barriers)
}

func (a *Adapter) refreshBarrier(ctx context.Context, index string) *pb.Failure {
	call := exchange{path: "/" + url.PathEscape(index) + "/_refresh", method: http.MethodPost, operation: "refresh"}
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
