package search

import (
	"bytes"
	"context"
	"math"
	"sync"

	"github.com/batchstream/weir/internal/execution"
)

type nativeHeaderKey struct{}

// Header bytes first enter net/http's existing 4 KiB read buffer. Before those
// bytes reach its parser, reserve the parser's expanding line buffers, strings,
// Header map and the eventual metadata copy. 512 bytes per line/element conservatively
// exceeds Go's documented 200-byte MIME map-entry accounting; eight copies of
// wire bytes cover growing line/metadata buffers and encoded output. This is a
// memory bound, without imposing a new fixed HTTP header-size/count quota.
type nativeHeaderMemory struct {
	ctx           context.Context
	mu            sync.Mutex
	io            execution.NativeIO
	memory        execution.Memory
	base          int
	minimum       int
	bytes         int
	lines         int
	elements      int
	completed     bool
	records       *recordMemory
	body          int
	trailer       int
	window        int
	currentBytes  int
	currentLines  int
	previousBytes int
	previousLines int
}

func (m *nativeHeaderMemory) read(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.completed || m.io == nil && m.memory == nil {
		return nil
	}
	// Go readTrailer first requires its complete MIME block to fit in the
	// transport's 4 KiB bufio buffer. Any such block lies in two adjacent
	// 4 KiB wire intervals, including bytes prefetched with the response head.
	// Keep their largest parser bound until the response is released. Large
	// direct body reads are scanned in place; body commas are not declarations.
	// Before Do returns, read-ahead is conservatively charged as response head.
	remaining := data
	for len(remaining) != 0 {
		count := min(responseReadBuffer-m.currentBytes, len(remaining))
		m.currentBytes += count
		m.currentLines += bytes.Count(remaining[:count], []byte{'\n'})
		window := 8192 + 8*(m.previousBytes+m.currentBytes) + 512*(m.previousLines+m.currentLines)
		m.window = max(m.window, window)
		remaining = remaining[count:]
		if m.currentBytes == responseReadBuffer {
			m.previousBytes, m.previousLines = m.currentBytes, m.currentLines
			m.currentBytes, m.currentLines = 0, 0
		}
	}
	if m.body != 0 {
		return m.setBody(m.body)
	}
	lines := bytes.Count(data, []byte{'\n'})
	elements := bytes.Count(data, []byte{','})
	if len(data) > math.MaxInt-m.bytes || lines > math.MaxInt-m.lines || elements > math.MaxInt-m.elements {
		return execution.ErrExecutionMemory
	}
	m.bytes += len(data)
	m.lines += lines
	m.elements += elements
	if m.base > math.MaxInt-8192 || m.lines > (math.MaxInt-8192-m.base)/512 || m.elements > (math.MaxInt-8192-m.base)/512-m.lines {
		return execution.ErrExecutionMemory
	}
	entries := m.lines + m.elements
	if m.bytes > (math.MaxInt-8192-m.base-entries*512)/8 {
		return execution.ErrExecutionMemory
	}
	// fixTrailer allocates a map entry for every comma-separated declaration,
	// even when one physical Trailer line contains thousands of short names.
	working := m.base + 8192 + m.bytes*8 + entries*512
	if m.memory != nil {
		return m.memory.SetWorking(m.ctx, max(m.minimum, working))
	}
	return m.io.GrowIO(working)
}

// Called under mu by both raw connection reads and body buffer reservations.
func (m *nativeHeaderMemory) setBody(scratch int) error {
	if scratch < 0 || m.window > math.MaxInt-scratch || m.trailer > (math.MaxInt-scratch-m.window)/2 {
		return execution.ErrExecutionMemory
	}
	// Declared trailers stay in scratch. Undeclared EOF keys can grow that
	// destination map while Go also holds its temporary MIME map: the wire
	// window covers new entries/strings; two existing map capacities cover
	// destination growth without assuming a favorable map load factor.
	memoryErr := m.records.set(m.ctx, m.records.sources, m.records.frame, scratch+m.window+2*m.trailer)
	if memoryErr != nil {
		return memoryErr
	}
	m.body = scratch
	return nil
}

func (m *nativeHeaderMemory) finish() {
	m.mu.Lock()
	m.completed = true
	// Detached HTTP transport contexts must not retain the Runtime or input.
	m.ctx = nil
	m.io = nil
	m.memory = nil
	m.records = nil
	m.mu.Unlock()
}
