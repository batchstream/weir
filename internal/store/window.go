package store

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// PrepareWindow retains only the next database window from the current input
// frame. Result credits use each adapter's configured maximum read size.
func (r *Runtime) PrepareWindow(store string, command *pb.Command, start int) (*PreparedBatch, int, *pb.Failure) {
	prepared := &PreparedBatch{}
	count := 0
	if read := command.GetRead(); read != nil {
		count = len(read.Requests)
	} else if mutations := command.GetMutate(); mutations != nil {
		count = len(mutations.Requests)
	}
	if start < 0 || start >= count {
		return nil, 0, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record window offset")
	}
	for offset := start; offset < count && len(prepared.plans) < r.limits.BatchOperations; offset++ {
		operation := &execution.Operation{Index: uint64(len(prepared.plans) + 1)}
		if read := command.GetRead(); read != nil {
			operation.Read = read.Requests[offset]
		} else {
			operation.Mutate = command.GetMutate().Requests[offset]
		}
		record, err := execution.NewRecord(store, operation)
		if err != nil {
			return nil, 0, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
		}
		plan, failure := r.adapter.PrepareRecord(record)
		if failure != nil {
			r.metrics.rejections.WithLabelValues("prepare").Inc()
			return nil, 0, failure
		}
		if plan == nil || plan.Operation == nil || plan.Streaming || plan.CleanupRequired || plan.ID != operation.Index {
			return nil, 0, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
		}
		plan.Bytes = max(plan.Bytes, record.Bytes())
		resultBytes := max(plan.ResultBytes, execution.ResultOverheadBytes)
		if plan.Bytes > r.limits.PendingBytes || resultBytes > r.limits.ResultBytes || plan.WorkingBytes > r.limits.WorkingBytes {
			return nil, 0, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record cannot fit Store memory bounds")
		}
		resultLimit := max(resultBytes, r.limits.ResultBytes/r.limits.Concurrency)
		inputLimit := min(r.limits.BatchBytes, r.limits.PendingBytes)
		if len(prepared.plans) != 0 && (plan.Bytes > inputLimit-prepared.bytes || resultBytes > resultLimit-prepared.resultBytes) {
			break
		}
		prepared.plans = append(prepared.plans, plan)
		prepared.bytes += plan.Bytes
		prepared.resultBytes += resultBytes
		prepared.workingBytes = max(prepared.workingBytes, plan.WorkingBytes)
	}
	return prepared, len(prepared.plans), nil
}
