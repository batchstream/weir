package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func BenchmarkSelectorDrain(b *testing.B) {
	const operations = 1024
	for _, namespaces := range []int{1, 8, 64, 1024} {
		b.Run(fmt.Sprintf("namespaces=%d", namespaces), func(b *testing.B) {
			limits := DefaultLimits()
			works := make([]*execution.Plan, operations)
			for index := range works {
				work := plan(uint64(index), fmt.Sprintf("record-%d", index), false)
				work.BatchKey = fmt.Sprintf("namespace-%d", index%namespaces)
				works[index] = work
			}
			b.ReportAllocs()
			b.ResetTimer()
			batches := 0
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				runtime := newRuntime(nil, limits)
				now := time.Now()
				for _, work := range works {
					ticket := &Ticket{runtime: runtime, ctx: b.Context(), plan: work, queuedAt: now}
					runtime.queue = append(runtime.queue, ticket)
					runtime.pendingBytes += work.Bytes
				}
				b.StartTimer()
				batches = 0
				for len(runtime.queue) != 0 {
					batch, _ := runtime.selectLocked(now)
					if batch == nil {
						b.Fatal("ready queue failed to drain")
					}
					batch.cancel()
					delete(runtime.batches, batch)
					batches++
				}
			}
			b.ReportMetric(float64(batches), "batches/drain")
			b.ReportMetric(operations, "operations/drain")
		})
	}
}
