package mongodb

import "context"

// bindLifetime preserves the caller's values, deadline and cancellation while
// also retiring I/O on the immutable adapter lifetime. AfterFunc must be stopped
// on release, and an already-closed rail must cancel synchronously before I/O.
func (a *Adapter) bindLifetime(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(a.lifetime, cancel)
	if a.lifetime.Err() != nil {
		cancel()
	}
	release := func() { stop(); cancel() }
	return ctx, release
}

// Ordinary caller cancellation does not shorten detached cleanup. Adapter
// teardown, including cleanup started after Close, never opens a new window.
func (a *Adapter) cleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(parent), a.options().CleanupTimeout)
	ctx, release := a.bindLifetime(bounded)
	stop := func() { release(); cancel() }
	return ctx, stop
}
