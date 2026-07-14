// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/supervisor"
)

// Run is the durable tick loop. It blocks until ctx is cancelled or Stop is
// called. The loop polls the database at cfg.PollInterval, claims up to
// ClaimBatch claimable forwardings per cycle, and dispatches each to
// processLease with bounded concurrency.
//
// Verdetto P1 #10 (Blocco 4): tick errors are CLASSIFIED rather than
// logged-and-continued. Per-element errors (one bad forwarding) are
// persisted on the row by processLease/handleRetry and don't count.
// Lease-lost is propagated via context cancellation by processLease.
// Infrastructure errors (DB closed, sql.ErrConnDone) accumulate in a
// supervisor.FailureTracker; once the consecutive-err threshold trips,
// Run returns the wrapped ErrInfrastructure to the BackgroundSupervisor
// so the ClassRestartable / ClassCritical restart machinery kicks in.
func (r *CreatorForwardingRunner) Run(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("forwarding: nil runner")
	}
	defer close(r.stoppedCh)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	// Metrics refresh runs on a separate, slower cadence (every 30s)
	// to avoid hitting the DB with COUNT/strftime queries every 5s.
	metricsTicker := time.NewTicker(30 * time.Second)
	defer metricsTicker.Stop()

	tracker := supervisor.NewFailureTrackerWithClock(supervisor.DefaultRetryPolicy(), supervisor.RealClock{})

	// Initial metrics snapshot on startup.
	r.refreshMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		case <-ticker.C:
			err := r.tick(ctx)
			if err == nil {
				tracker.Reset()
				continue
			}
			classified := supervisor.ClassifyError(err)
			if escalated := tracker.Record(classified); escalated != nil {
				return fmt.Errorf("forwarding runner: %w", escalated)
			}
			// Per-element errors are already persisted on disk by
			// processLease / handleRetry / handleEnqueueRetry.
			// Lease-lost cancels the in-flight context. Neither
			// needs a log-and-continue entry; the runner silently
			// proceeds to the next tick.
		case <-metricsTicker.C:
			r.refreshMetrics(ctx)
		}
	}
}
