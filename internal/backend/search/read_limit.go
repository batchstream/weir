package search

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/execution"
)

func (a *Adapter) maxReadSize() int {
	if a.config.MaxReadSize == 0 {
		return execution.DefaultMaxReadSize
	}
	return a.config.MaxReadSize
}

func (a *Adapter) sourceLimit(work *execution.Plan) int {
	if work.Backend.(*plan).action == "read" {
		return a.maxReadSize()
	}
	return protocol.MaxDocument
}

// Account for the largest permitted read sub-batch, wire body, decoded source
// and framing scratch. Transform pre-reads retain the global document budget.
func (a *Adapter) readWorkingBytes() int {
	// One body, one decoded source set and parsing scratch are bounded by
	// the complete response cap, independent of the declared single record size.
	return 3 * batchBodyLimit
}
