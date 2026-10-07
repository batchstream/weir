package luaengine

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEvaluateAdmissionHonorsCancellation(t *testing.T) {
	for range maxConcurrent {
		evaluations <- struct{}{}
	}
	defer func() {
		for range maxConcurrent {
			<-evaluations
		}
	}()
	program := Program{Source: "return function() return weir.keep() end"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := Evaluate(ctx, program)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission did not honor the caller deadline: %v", err)
	}
	if len(evaluations) != maxConcurrent {
		t.Fatal("canceled admission changed the active evaluation count")
	}
}

func TestEvaluateReturnsAdmissionAfterEveryFailure(t *testing.T) {
	sources := []string{
		"return function( end",
		"return nil",
		"return function() error('failed') end",
		"while true do end",
		"return function() while true do end end",
	}
	for _, source := range sources {
		program := Program{Source: source}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, err := Evaluate(ctx, program)
		cancel()
		if err == nil || len(evaluations) != 0 {
			t.Fatalf("failure retained admission: source=%q err=%v active=%d", source, err, len(evaluations))
		}
	}
	program := Program{Source: "return function() return weir.keep() end"}
	if _, err := Evaluate(context.Background(), program); err != nil || len(evaluations) != 0 {
		t.Fatalf("admission did not recover: err=%v active=%d", err, len(evaluations))
	}
}
