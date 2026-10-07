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
