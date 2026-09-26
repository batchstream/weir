package overload

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type guardTarget struct{ latched atomic.Bool }

func (t *guardTarget) SetOverloaded(value bool) { t.latched.Store(value) }
func TestGuardObservationUsesExistingHysteresis(t *testing.T) {
	target := &guardTarget{}
	guard := New([]Target{target}, 1000)
	if s := guard.Snapshot(); s.Observed || s.Source != "unobserved" || s.Bytes != 0 {
		t.Fatal(s)
	}
	for _, n := range []uint64{800, 750, 700} {
		guard.sample(n, "go_sys_minus_released")
		s := guard.Snapshot()
		if s.Latched != (n != 700) || target.latched.Load() != s.Latched || s.Bytes != n || !s.Observed {
			t.Fatal(s)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); guard.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done
	s := guard.Snapshot()
	if !s.Observed || s.Bytes == 0 || runtime.GOOS != "linux" && s.Source != "go_sys_minus_released" {
		t.Fatal(s)
	}
}
