package app

import (
	"context"
	"math"
	"net"
	"os"
	"runtime/debug"
	"testing"
)

func TestOpenAndFailedOpenLeaveHostRuntimeMemoryLimitUnchanged(t *testing.T) {
	prior := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(prior)
	value, present := os.LookupEnv("GOMEMLIMIT")
	if err := os.Unsetenv("GOMEMLIMIT"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			if err := os.Setenv("GOMEMLIMIT", value); err != nil {
				t.Error(err)
			}
		}
	})
	for _, current := range []int64{math.MaxInt64, 256 << 20} {
		debug.SetMemoryLimit(current)
		cfg := DefaultConfig()
		cfg.Basic.Listeners.Application = "127.0.0.1:0"
		cfg.Basic.Listeners.Peer = ""
		cfg.Basic.Diagnostics.Address = ""
		node, err := Open(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot := node.guard.Snapshot(); snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != current {
			t.Fatal("embedded Open changed its host runtime", snapshot, current)
		}
		if err := node.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := node.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if debug.SetMemoryLimit(-1) != current {
			t.Fatal("embedded Start/Close changed its host runtime")
		}
		cfg.Basic.Overload.Memory.LowWatermark = 0
		if _, err := Open(t.Context(), cfg); err == nil {
			t.Fatal("zero recovery watermark accepted")
		}
		if debug.SetMemoryLimit(-1) != current {
			t.Fatal("failed Open changed its host runtime")
		}
		cfg.Basic.Overload.Memory.LowWatermark = 70
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Basic.Diagnostics.Address = occupied.Addr().String()
		failed, openErr := Open(t.Context(), cfg)
		if err := occupied.Close(); err != nil {
			t.Fatal(err)
		}
		if openErr == nil {
			_ = failed.Close(context.Background())
			t.Fatal("occupied diagnostics listener did not fail after Guard construction")
		}
		if debug.SetMemoryLimit(-1) != current {
			t.Fatal("partial Open cleanup changed its host runtime")
		}
	}
}
