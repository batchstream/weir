package mongodb

import "context"

// Driver v2.9.1 changes required commit retry-once into unlimited retries when
// Deadline is present (CSOT). Bridge the parent's cancellation into a deadline-free
// context, retaining session values and native retry-once. The driver's socket
// listener only closes on Canceled, not DeadlineExceeded without a socket deadline.
// The enclosing operation retains the original deadline/error. No client-level
// Timeout is configured. Tests qualify both actual reply loss and blocked commits.
func nativeAttemptContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, cancel)
	if parent.Err() != nil {
		cancel()
	}
	release := func() { stop(); cancel() }
	return ctx, release
}
