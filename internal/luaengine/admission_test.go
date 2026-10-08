package luaengine

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestEvaluateHonorsCallerCancellation(t *testing.T) {
	program := Program{Source: "return function() return weir.keep() end"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Evaluate(ctx, program); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation lost: %v", err)
	}
}

func TestEvaluateConcurrentFailuresDoNotBlockOtherCalls(t *testing.T) {
	sources := []string{"return function( end", "return nil", "return function() error('failed') end", "while true do end", "return function() while true do end end"}
	var workers sync.WaitGroup
	for range 8 {
		for _, source := range sources {
			workers.Go(func() {
				limits := DefaultLimits()
				limits.MaxInstructions = 1_000_000
				program := Program{Source: source, Limits: &limits}
				if _, err := Evaluate(t.Context(), program); err == nil {
					t.Error("invalid Lua accepted")
				}
			})
		}
	}
	workers.Wait()
	program := Program{Source: "return function() return weir.keep() end"}
	if _, err := Evaluate(t.Context(), program); err != nil {
		t.Fatal(err)
	}
}
