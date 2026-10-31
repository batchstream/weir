package mongodb

import (
	"testing"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestMongoLuaEvaluationsAreTimedByOutcome(t *testing.T) {
	adapter := &Adapter{lua: luaengine.NewMetrics()}
	for _, source := range []string{`return function() return weir.keep() end`, `invalid lua`} {
		program := &luaengine.Program{Source: source}
		native := &plan{id: "item", program: program}
		work := &execution.Plan{Backend: native, Context: t.Context()}
		_, _ = adapter.transformProgram(t.Context(), work, nil)
	}
	families := testmetrics.Gather(t, adapter)
	for _, outcome := range []string{"success", "error"} {
		labels := map[string]string{"outcome": outcome}
		metric := testmetrics.Sample(families, "weir_lua_evaluation_seconds", labels)
		if metric.GetHistogram().GetSampleCount() != 1 {
			t.Fatal("Lua evaluation missing or counted twice", outcome, metric)
		}
	}
}
