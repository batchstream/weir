package execution

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestLuaFailurePreservesCallerStatusAndSanitizesErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, release := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer release()
	private := fmt.Errorf("private source detail")
	timeout := fmt.Errorf("private source detail: %w", context.DeadlineExceeded)
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		code pb.FailureCode
	}{
		{name: "evaluation error", ctx: t.Context(), err: private, code: pb.FailureCode_INVALID_ARGUMENT},
		{name: "execution limit", ctx: t.Context(), err: timeout, code: pb.FailureCode_DEADLINE_EXCEEDED},
		{name: "caller cancellation overrides limit", ctx: canceled, err: timeout, code: pb.FailureCode_CANCELLED},
		{name: "caller deadline overrides evaluation error", ctx: expired, err: private, code: pb.FailureCode_DEADLINE_EXCEEDED},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := LuaFailure(test.ctx, test.err)
			failure := result.GetFailure()
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || failure == nil || failure.Code != test.code {
				t.Fatalf("lost pre-write failure evidence: %v", result)
			}
			if failure.Message == "" || strings.Contains(failure.Message, "private source detail") {
				t.Fatalf("Lua error was not sanitized: %v", failure)
			}
		})
	}
}
