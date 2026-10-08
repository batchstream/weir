package execution

import (
	"context"
	"fmt"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// A Scan fetch owns one bounded result batch while its backend permit is free
// during publication. These bounds apply to retained output, not database pages.
const ScanBatchDocuments = 128
const ScanBatchBytes = 4 << 20
const ScanResultBytes = ScanBatchBytes + ScanBatchDocuments*ResultOverheadBytes + protocol.MaxScanToken + ResultOverheadBytes

// ScanLimits bounds one retained output batch, independently of the logical page.
type ScanLimits struct{ Documents, Bytes int }

func DefaultScanLimits() ScanLimits {
	limits := ScanLimits{Documents: ScanBatchDocuments, Bytes: ScanBatchBytes}
	return limits
}

func (limits ScanLimits) Validate() error {
	if limits.Documents < 1 || limits.Documents > protocol.MaxScanPageSize || limits.Bytes < protocol.MaxDocument || limits.Bytes > int(^uint(0)>>1)-limits.Documents*ResultOverheadBytes-protocol.MaxScanToken-ResultOverheadBytes {
		return fmt.Errorf("invalid Scan batch bounds")
	}
	return nil
}

func (limits ScanLimits) ResultBytes() int {
	return limits.Bytes + limits.Documents*ResultOverheadBytes + protocol.MaxScanToken + ResultOverheadBytes
}

// ScanProgress retains publication state across bounded backend fetches.
type ScanProgress struct {
	Count uint64
}

// PublishPage counts documents accepted by emit, terminates failed pages without
// a checkpoint. The first result schedules another bounded backend fetch. The
// second means emit accepted a token, not that the caller received it.
func (s *ScanProgress) PublishPage(ctx context.Context, work *Plan, page *ScanPage, emit Emit) (continuation, transferred bool) {
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
		return true, false
	}
	end := &pb.ScanEnd{DocumentCount: s.Count, Failure: page.Failure}
	if page.Failure == nil {
		end.Exhausted = page.Exhausted
		end.NextContinuationToken = page.NextContinuationToken
	}
	value := &pb.Event_ScanEnd{ScanEnd: end}
	event := &pb.Event{Value: value}
	err := emit(work, event)
	return false, err == nil && len(end.NextContinuationToken) != 0
}
