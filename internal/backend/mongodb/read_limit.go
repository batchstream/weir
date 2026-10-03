package mongodb

import "github.com/batchstream/weir/internal/execution"

func (a *Adapter) maxReadSize() int {
	if a.config.MaxReadSize == 0 {
		return execution.DefaultMaxReadSize
	}
	return a.config.MaxReadSize
}

// The driver and socket guard can each retain a native 16 MiB BSON reply plus
// framing even when a record exceeds the declared read limit. Budget both buffers.
func (a *Adapter) readWorkingBytes() int {
	return 2*scanNativeLimit + max(1<<20, 4*a.maxReadSize())
}
