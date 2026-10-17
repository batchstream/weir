package luaengine

import (
	"testing"

	"github.com/batchstream/weir/internal/value"
)

// Report real Go allocation bytes beside the promised scope bytes. This is
// diagnostic evidence, not an assertion based on process-global GC counters.
func BenchmarkEvaluateAllocationAccounting(b *testing.B) {
	cases := []struct {
		name   string
		source string
	}{
		{name: "tiny", source: `return function() return weir.keep() end`},
		{name: "format", source: `return function() local s='abcdefghijklmnop'; for i=1,8 do s=s..s end; return {text=string.format('%s:%d',s,7)} end`},
		{name: "deep", source: `return function() local t={}; for i=1,1000 do t={x=t} end; return t end`},
		{name: "wide", source: `return function() local a={}; for i=1,2500 do a[i]=i end; return {a=a} end`},
		{name: "bytes", source: `return function() local s='abcdefghijklmnop'; for i=1,8 do s=s..s end; return {data=weir.bytes(s)} end`},
		{name: "shared DAG", source: `return function() local t={value=1}; for i=1,8 do t={a=t,b=t} end; return t end`},
	}
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			reserved, total := 0, uint64(0)
			reserve := func(bytes int) error {
				reserved = bytes
				return nil
			}
			program := Program{Source: test.source, Reserve: reserve}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				reserved = 0
				result, err := Evaluate(b.Context(), program)
				if err != nil || result.Action != Keep && result.Value.Kind != value.Object {
					b.Fatal("accounting fixture failed", err)
				}
				total += uint64(reserved)
			}
			b.ReportMetric(float64(total)/float64(b.N), "reserved-B/op")
		})
	}
}
