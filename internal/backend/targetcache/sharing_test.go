package targetcache

import (
	"context"
	"testing"
	"testing/synctest"
)

func TestDisabledRetentionStillSharesPendingSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := New[string, int](0)
		owner, err := cache.Acquire(context.Background(), "records")
		if err != nil {
			t.Fatal(err)
		}
		received := make(chan Lookup[int], 1)
		go func() {
			lookup, err := cache.Acquire(context.Background(), "records")
			if err != nil {
				t.Error(err)
			}
			received <- lookup
		}()
		// Wait until the second caller is blocked on the pending check.
		synctest.Wait()
		cache.Complete("records", owner, 42, true)
		waiter := <-received
		if !waiter.Cached || waiter.Value != 42 {
			t.Fatal("pending waiter repeated metadata I/O with retention disabled", waiter)
		}
	})
}
