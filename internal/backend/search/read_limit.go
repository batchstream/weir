package search

import (
	"github.com/batchstream/weir/api/protocol"
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
	if a.maxReadSize() == protocol.MaxDocument {
		return scanPageBudget
	}
	item := a.maxReadSize() + getFramingLimit
	items := min(batchOperationLimit, (batchBodyLimit-getFramingLimit)/item)
	response := getFramingLimit + items*item
	return 3 * max(metadataLimit, response)
}
