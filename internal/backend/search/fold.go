package search

import (
	"bytes"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// reader returns the work that reads a chain's record: its first mutation
// whose caller remains, so that one caller leaving does not fail the read.
func (b *recordBatch) reader(head int) *execution.Plan {
	for _, member := range b.chains[head] {
		if b.works[member].Ended(b.ctx) == nil {
			return b.works[member]
		}
	}
	return b.works[head]
}

// live reports whether any caller of a chain remains.
func (b *recordBatch) live(head int) bool {
	for _, member := range b.chains[head] {
		if b.works[member].Ended(b.ctx) == nil {
			return true
		}
	}
	return false
}

// mutateChain applies a chain's mutations in queue order to the record's
// state and frames one write that takes the record from the state it read, or
// from any state when the first mutation is a put or delete, to the last. A
// mutation that is rejected leaves the state to the next one.
func (b *recordBatch) mutateChain(head int, current *getReply) {
	if current == nil && b.preReads(head) {
		// The mutation that made the read unnecessary lost its caller.
		b.retry = append(b.retry, head)
		return
	}
	b.attempts[head]++
	found, source := false, []byte(nil)
	if current != nil {
		found, source = *current.Found, current.Source
	}
	// changed reports whether the state may differ from the record as read;
	// without a read it is not known.
	changed := current == nil
	var applied, dependent []int
	for _, i := range b.chains[head] {
		work := b.works[i]
		if denied := work.Ended(b.ctx); denied != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, denied)
			continue
		}
		step := work.Backend.(*plan)
		var rejected *pb.MutationResult
		switch step.action {
		case "index":
			found, source = true, step.source
		case "delete":
			found, source = false, nil
		case "create":
			if found {
				rejected = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record already exists"))
				break
			}
			found, source = true, step.source
		case "replace":
			if !found {
				rejected = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing"))
				break
			}
			source = step.source
		case "program":
			present := found
			state := &getReply{Found: &present, Source: source}
			next, mutation := b.adapter.evaluateProgram(work, state)
			if denied := work.Ended(b.ctx); denied != nil {
				b.fail(i, pb.MutationOutcome_NOT_APPLIED, denied)
				continue
			}
			switch {
			case mutation == nil && next.action == "delete":
				found, source = false, nil
			case mutation == nil:
				found, source = true, next.source
			case mutation.Outcome != pb.MutationOutcome_APPLIED || mutation.Failure != nil:
				rejected = mutation
			}
		}
		if rejected != nil {
			b.fail(i, rejected.Outcome, rejected.Failure)
			if changed {
				dependent = append(dependent, i)
			}
			continue
		}
		if !changed && found == *current.Found && (!found || bytes.Equal(source, current.Source)) {
			// Decided on the record as read and left it unchanged, so the
			// result does not depend on the chain's write.
			b.fail(i, pb.MutationOutcome_APPLIED, nil)
			if step.waitUntilVisible {
				b.queueVisibilityBarrier(step.index, i)
			}
			continue
		}
		changed = true
		applied = append(applied, i)
	}
	if len(applied) == 0 {
		return
	}
	native := b.works[head].Backend.(*plan)
	read := current != nil
	var action string
	switch {
	case read && !*current.Found && !found:
	case read && *current.Found && found && bytes.Equal(source, current.Source):
	case !found:
		action = "delete"
	case read && !*current.Found:
		action = "create"
	default:
		action = "index"
	}
	if action == "" {
		// The record already holds the chain's result.
		for _, i := range applied {
			b.fail(i, pb.MutationOutcome_APPLIED, nil)
			if native.waitUntilVisible {
				b.queueVisibilityBarrier(native.index, i)
			}
		}
		return
	}
	write := &plan{index: native.index, id: native.id, action: action, waitUntilVisible: native.waitUntilVisible, conditional: read}
	if found {
		write.source = source
	}
	framed := *b.works[head]
	framed.Backend = write
	if b.applied == nil {
		b.applied = make(map[int][]int)
		b.dependent = make(map[int][]int)
	}
	b.applied[head] = applied
	b.dependent[head] = dependent
	b.appendWrite(&framed, head, current)
}

// departed reports whether a mutation whose effect a framed chain's write
// carries has lost its caller.
func (b *recordBatch) departed(members []int) bool {
	for _, i := range members {
		if b.works[i].Ended(b.ctx) != nil {
			return true
		}
	}
	return false
}

// settleChain gives a chain's write result to each mutation the write
// carried. A conflict evaluates the chain again from a fresh read, and a
// rejected document runs its mutations one at a time, since none applied. A
// write that did not apply also fails the mutations rejected on the state it
// would have made real.
func (b *recordBatch) settleChain(head int, reply *pb.Event) {
	members, dependent := b.applied[head], b.dependent[head]
	delete(b.applied, head)
	delete(b.dependent, head)
	mutation := reply.GetMutationResult()
	outcome, failure := mutation.GetOutcome(), mutation.GetFailure()
	if outcome == pb.MutationOutcome_NOT_APPLIED {
		switch failure.GetCode() {
		case pb.FailureCode_PRECONDITION_FAILED:
			b.unfold = append(b.unfold, head)
			return
		case pb.FailureCode_CONFLICT:
			if b.attempts[head] < b.adapter.options().RetryAttempts {
				b.retry = append(b.retry, head)
				return
			}
			failure = protocol.Fail(pb.FailureCode_CONFLICT, "Search record changed during every transform attempt")
		}
	}
	for _, i := range members {
		b.fail(i, outcome, failure)
	}
	if outcome == pb.MutationOutcome_APPLIED {
		return
	}
	for _, i := range dependent {
		b.fail(i, pb.MutationOutcome_NOT_APPLIED, failure)
	}
}
