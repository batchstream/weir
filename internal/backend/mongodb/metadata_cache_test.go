package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
)

func TestTargetCacheSharesPendingSuccess(t *testing.T) {
	for _, capacity := range []int{0, 2} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cache := targetCache{entries: make(map[namespace]*targetCheck), capacity: capacity}
				target := namespace{database: "db", collection: "records"}
				owner, err := cache.acquire(t.Context(), target)
				if err != nil || owner == nil {
					t.Fatal("first caller did not own the check", err)
				}
				done := make(chan struct{}, 32)
				for range 32 {
					go func() {
						pending, err := cache.acquire(t.Context(), target)
						if err != nil || pending != nil {
							t.Error("waiter repeated successful metadata I/O", err)
						}
						done <- struct{}{}
					}()
				}
				synctest.Wait()
				cache.complete(target, owner, true)
				for range 32 {
					<-done
				}
				next, err := cache.acquire(t.Context(), target)
				if err != nil || (next == nil) != (capacity > 0) {
					t.Fatal("completed retention did not follow capacity", err)
				}
				cache.complete(target, next, true)
			})
		})
	}
}

func TestTargetCacheRetriesFailureAndIgnoresOldOwners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cache targetCache
		target := namespace{database: "db", collection: "records"}
		failed, err := cache.acquire(t.Context(), target)
		if err != nil || failed == nil {
			t.Fatal(err)
		}
		received := make(chan *targetCheck, 1)
		go func() {
			pending, err := cache.acquire(t.Context(), target)
			if err != nil {
				t.Error(err)
			}
			received <- pending
		}()
		synctest.Wait()
		cache.complete(target, failed, false)
		retry := <-received
		if retry == nil || retry == failed {
			t.Fatal("failed check was retained")
		}
		cache.complete(target, failed, true)
		cache.complete(target, retry, true)
		cache.complete(target, retry, false)
		hot, err := cache.acquire(t.Context(), target)
		if err != nil || hot != nil {
			t.Fatal("old or repeated completion discarded a successful check", err)
		}
	})
}

func TestTargetCacheCancellationDoesNotCancelOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cache targetCache
		target := namespace{database: "db", collection: "records"}
		owner, err := cache.acquire(t.Context(), target)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := cache.acquire(ctx, target)
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("waiter ignored cancellation", err)
		}
		cache.complete(target, owner, true)
		if _, err := cache.acquire(ctx, target); !errors.Is(err, context.Canceled) {
			t.Fatal("cached check ignored cancellation", err)
		}
		hot, err := cache.acquire(t.Context(), target)
		if err != nil || hot != nil {
			t.Fatal("canceled waiter discarded the owner's check", err)
		}
	})
}

func TestTargetCacheBoundsCompletedChecksWithoutBlockingColdTargets(t *testing.T) {
	cache := targetCache{entries: make(map[namespace]*targetCheck), capacity: 2}
	owners := make([]*targetCheck, 65)
	targets := make([]namespace, len(owners))
	for i := range owners {
		targets[i] = namespace{database: "db", collection: fmt.Sprint(i)}
		pending, err := cache.acquire(t.Context(), targets[i])
		if err != nil || pending == nil {
			t.Fatal("cold check waited for completed cache capacity", err)
		}
		owners[i] = pending
	}
	for i, owner := range owners {
		cache.complete(targets[i], owner, true)
	}
	if len(cache.entries) != 2 || len(cache.order) != 2 {
		t.Fatal("completed checks exceeded capacity")
	}
	for _, target := range targets[len(targets)-2:] {
		pending, err := cache.acquire(t.Context(), target)
		if err != nil || pending != nil {
			t.Fatal("newest completed check was evicted", err)
		}
	}
	pending, err := cache.acquire(t.Context(), targets[0])
	if err != nil || pending == nil {
		t.Fatal("oldest completed check was retained", err)
	}
	cache.complete(targets[0], pending, true)
}
