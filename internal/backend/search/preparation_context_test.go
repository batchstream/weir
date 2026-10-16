package search

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// Cancel on a work checkpoint, without a timing race or an arbitrary sleep.
type checkpointCancelContext struct {
	context.Context
	remaining atomic.Int64
	cancel    context.CancelFunc
}

func (c *checkpointCancelContext) Err() error {
	if c.remaining.Add(-1) == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestSearchPreparationObservesInFlightCancellation(t *testing.T) {
	cfg := Config{Store: "search"}
	adapter := &Adapter{config: cfg, dialect: ElasticsearchProduct}
	raw := []byte(`{"payload":"` + strings.Repeat("x", 4<<20) + `"}`)
	for _, action := range []string{"put", "program", "expression"} {
		t.Run(action, func(t *testing.T) {
			inner, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &checkpointCancelContext{Context: inner, cancel: cancel}
			ctx.remaining.Store(20)
			var failure *pb.Failure
			work := batchTestPlan(t, adapter, action, "records/s:item")
			mutation := work.Command.GetMutate()
			switch action {
			case "put":
				mutation.GetPut().Data = raw
			case "program":
				input := &pb.Document{ContentType: "application/json", Data: raw}
				mutation.GetAtomicTransform().GetLua().Input = input
			case "expression":
				mutation.GetAtomicTransform().GetBackendExpression().Data = append(append([]byte(`{"doc":`), raw...), '}')
			}
			record, err := execution.NewRecord("search", 1, work.Command)
			if err != nil {
				t.Fatal(err)
			}
			_, failure = adapter.PrepareRecord(ctx, record)
			if !errors.Is(ctx.Err(), context.Canceled) || failure.GetCode() != pb.FailureCode_CANCELLED {
				t.Fatal("cancellation became an argument error or was ignored", failure)
			}
		})
	}
}

func TestSearchJSONSyntaxDecoderCanCancelLongScalar(t *testing.T) {
	raw := []byte(`{"payload":"` + strings.Repeat("x", 4<<20) + `"}`)
	inner, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &checkpointCancelContext{Context: inner, cancel: cancel}
	// Allow the UTF-8/surrogate scan to finish, then cancel while the standard
	// syntax decoder is reading the scalar in chunks.
	ctx.remaining.Store(int64((len(raw)+4095)/4096 + 20))
	if err := validateJSON(ctx, raw); !errors.Is(err, context.Canceled) {
		t.Fatal("long scalar syntax validation ignored cancellation", err)
	}
}

func TestSearchJSONCancellationAcrossUTF8AndEscapeBoundaries(t *testing.T) {
	patterns := map[string]string{
		"multibyte":      "😀",
		"surrogate-pair": `\ud83d\ude00`,
	}
	for name, pattern := range patterns {
		t.Run(name, func(t *testing.T) {
			for _, prefix := range []string{`"`, `{`} {
				inner, cancel := context.WithCancel(t.Context())
				ctx := &checkpointCancelContext{Context: inner, cancel: cancel}
				ctx.remaining.Store(20)
				raw := []byte(prefix + strings.Repeat(pattern, 64<<10) + `"`)
				err := validateJSON(ctx, raw)
				cancel()
				if !errors.Is(err, context.Canceled) {
					t.Fatal("jumping over byte 4096 skipped cancellation", name, err)
				}
			}
		})
	}
}
