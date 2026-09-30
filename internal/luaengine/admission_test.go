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
	program := Program{Source: "while true do end"}
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
