package search

import (
	"bytes"
	"math"
	"sync"

	"github.com/batchstream/weir/internal/execution"
)

type nativeHeaderKey struct{}

// Header bytes first enter net/http's existing 4 KiB read buffer. Before those
// bytes reach its parser, reserve the parser's expanding line buffers, strings,
// Header map and the eventual metadata copy. 512 bytes per line conservatively
// exceeds Go's documented 200-byte MIME map-entry accounting; eight copies of
// wire bytes cover growing line/metadata buffers and encoded output. This is a
// memory bound, without imposing a new fixed HTTP header-size/count quota.
type nativeHeaderMemory struct {
	mu        sync.Mutex
	io        execution.NativeIO
	base      int
	bytes     int
	lines     int
	completed bool
}

func (m *nativeHeaderMemory) read(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.completed || m.io == nil {
		return nil
	}
	lines := bytes.Count(data, []byte{'\n'})
	if len(data) > (math.MaxInt-8192)/8-m.bytes || lines > (math.MaxInt-8192)/512-m.lines {
		return execution.ErrNativeMemory
	}
	m.bytes += len(data)
	m.lines += lines
	if m.base > math.MaxInt-8192-m.lines*512 || m.bytes > (math.MaxInt-8192-m.lines*512-m.base)/8 {
		return execution.ErrNativeMemory
	}
	return m.io.GrowIO(m.base + 8192 + m.bytes*8 + m.lines*512)
}

func (m *nativeHeaderMemory) finish() {
	m.mu.Lock()
	m.completed = true
	// Detached HTTP transport contexts must not retain the Runtime or input.
	m.io = nil
	m.mu.Unlock()
}
