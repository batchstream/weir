package execution

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// A Scan fetch owns one bounded result batch while its backend permit is free
// during publication. These bounds apply to retained output, not database pages.
const ScanBatchDocuments = 128
const ScanBatchBytes = 4 << 20
const ScanResultBytes = ScanBatchBytes + ScanBatchDocuments*ResultOverheadBytes + protocol.MaxScanToken + ResultOverheadBytes

// ScanProgress retains publication state across bounded backend fetches.
type ScanProgress struct {
	Count uint64
}

// PublishPage counts documents accepted by emit, terminates failed pages without
// a checkpoint and schedules nonterminal pages to continue. A true result means
// emit accepted a continuation token; it does not confirm delivery to the caller.
func (s *ScanProgress) PublishPage(ctx context.Context, work *Plan, page *ScanPage, emit Emit) bool {
	work.Continue = false
	for _, document := range page.Documents {
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Value: value}
		if err := emit(work, event); err != nil {
			page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "Scan result publication failed")
			if ctx.Err() != nil {
				page.Failure = protocol.ContextFailure(ctx)
			}
			break
		}
		s.Count++
	}
	if page.Failure == nil && !page.Exhausted && !page.Complete {
		work.Continue = true
		return false
	}
	end := &pb.ScanEnd{DocumentCount: s.Count, Failure: page.Failure}
	if page.Failure == nil {
		end.Exhausted = page.Exhausted
		end.NextContinuationToken = page.NextContinuationToken
	}
	value := &pb.Event_ScanEnd{ScanEnd: end}
	event := &pb.Event{Value: value}
	err := emit(work, event)
	return err == nil && len(end.NextContinuationToken) != 0
}
