// Package lease provides a reusable lease heartbeat primitive for
// background runners that need periodic lease renewal during long-running
// operations (e.g. forwarding polling, delivery uploads).
//
// The Heartbeat is intentionally small and does not depend on any store
// implementation — callers inject Renew and OnLost callbacks. This keeps
// the primitive testable in isolation and reusable across the forwarding
// and delivery runners.
package lease

import (
	"context"
	"time"
)

// Heartbeat periodically calls a Renew function. If Renew returns an
// error, the OnLost callback is invoked and the heartbeat loop stops.
//
// Usage:
//
//	h := &lease.Heartbeat{
//	    Interval: leaseDuration / 3,
//	    Renew: func(ctx context.Context) error {
//	        return store.RenewLease(ctx, ...)
//	    },
//	    OnLost: func(err error) {
//	        cancel() // interrupt the upload
//	    },
//	}
//	go h.Run(ctx)
//
// The caller cancels ctx to stop the heartbeat gracefully after the
// protected operation completes. Heartbeat does NOT own the context
// lifecycle; the caller decides when to cancel.
type Heartbeat struct {
	// Interval between renewal attempts. Must be > 0. A typical value
	// is leaseDuration / 3.
	Interval time.Duration
	// Renew is called at each Interval. The ctx passed to Renew is the
	// same ctx passed to Run. If the renew operation needs to survive
	// after ctx is cancelled (e.g. a final write), use context.Background()
	// or a detached context inside the callback.
	Renew func(ctx context.Context) error
	// OnLost is called when Renew returns a non-nil error. The error
	// from Renew is passed through. May be nil.
	OnLost func(error)
}

// Run starts the heartbeat loop. It blocks until ctx is cancelled or
// Renew returns an error. Callers should run this in a goroutine and
// communicate completion via a done channel or sync.WaitGroup.
//
// The first renewal happens after one Interval (not immediately).
func (h *Heartbeat) Run(ctx context.Context) {
	if h.Interval <= 0 {
		h.Interval = 30 * time.Second
	}
	if h.Renew == nil {
		return
	}
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.Renew(ctx); err != nil {
				if h.OnLost != nil {
					h.OnLost(err)
				}
				return
			}
		}
	}
}
