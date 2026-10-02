package mongodb

import "github.com/batchstream/weir/internal/execution"

func (a *Adapter) maxReadSize() int {
	if a.config.MaxReadSize == 0 {
		return execution.DefaultMaxReadSize
	}
	return a.config.MaxReadSize
}

// The driver and the socket guard can each retain an 8 MiB reply even when a
// stored record exceeds the declared read limit. Keep both buffers budgeted.
func (a *Adapter) readWorkingBytes() int {
	return 2*scanNativeLimit + max(1<<20, 4*a.maxReadSize())
}
