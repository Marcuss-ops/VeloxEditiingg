// Package taskgraph / reaper.go — TaskLeaseReaper extracted as a named
// runner.
//
// History: PR-05 originally inlined the lease-reaper sweep inside the
// `taskgraph-dispatcher` supervisor tick (every 15 ticks = ~30 s). The
// inlined approach worked but coupled two unrelated concerns (PENDING→READY
// readiness promotion and expired-lease sweeping) inside a single runner.
// PR-05 follow-up splits them into independent runners so each has its own
// ticker, its own log prefix, and its own lifecycle handle in the supervisor.
//
// TaskLeaseReaper is the canonical master-side lease enforcement. PR-13's
// job-side reaper (with the VELOX_DISABLE_JOB_REAPER gate) is now a
// deprecated no-op: once this reaper is registered, Job.LeaseReaperSafe
// becomes redundant.
package taskgraph

import (
	"context"
	"log"
	"time"
)

// TaskLeaseReaper sweeps tasks whose lease has expired and resets them
// to READY for re-claim. The reaper owns its own ticker and goroutine so
// its cadence is decoupled from the readiness dispatcher.
//
// Default cadence: 30 s. The 30-s interval matches the master-side default
// task lease TTL (30 * time.Minute) so a freshly leased task waits at
// most TTL + 30 s before being reaped if its worker crashes mid-flight —
// well within the audit §P0.4 budget "worker crash → Task returns to
// READY within 1× TTL".
type TaskLeaseReaper struct {
	repo   Repository
	ticker time.Duration
	limit  int
	now    func() time.Time
}

// NewTaskLeaseReaper constructs a TaskLeaseReaper. repo is required;
// ticker and limit get safe defaults if non-positive.
func NewTaskLeaseReaper(repo Repository) *TaskLeaseReaper {
	return NewTaskLeaseReaperWithConfig(repo, 30*time.Second, 100)
}

// NewTaskLeaseReaperWithConfig constructs a TaskLeaseReaper with explicit
// ticker and limit. Used by tests that want a sub-second cadence so the
// reaper can be observed under deterministic time.
func NewTaskLeaseReaperWithConfig(repo Repository, ticker time.Duration, limit int) *TaskLeaseReaper {
	if repo == nil {
		panic("taskgraph.NewTaskLeaseReaper: repo is required")
	}
	if ticker <= 0 {
		ticker = 30 * time.Second
	}
	if limit <= 0 {
		limit = 100
	}
	return &TaskLeaseReaper{
		repo:   repo,
		ticker: ticker,
		limit:  limit,
		now:    func() time.Time { return time.Now().UTC() },
	}
}

// SetClock replaces the now() function used by the reaper. Intended for
// tests; production code should rely on the default which returns
// time.Now().UTC() per tick.
func (r *TaskLeaseReaper) SetClock(now func() time.Time) {
	if now == nil {
		return
	}
	r.now = now
}

// Run blocks until ctx is cancelled. On each tick it calls
// repo.RequeueExpiredLeases and logs a single structured line with the
// reaped count (or zero on idle ticks). Errors are logged but never
// fatal — the reaper keeps running so a transient SQL hiccup clears on
// the next sweep.
//
// Returns ctx.Err() on cancellation as expected by supervisor.Run().
func (r *TaskLeaseReaper) Run(ctx context.Context) error {
	log.Printf("[TASK-LEASE-REAPER] started ticker=%s limit=%d", r.ticker, r.limit)
	ticker := time.NewTicker(r.ticker)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[TASK-LEASE-REAPER] stopped reason=%v", ctx.Err())
			return ctx.Err()
		case <-ticker.C:
			nowStr := r.now().Format(time.RFC3339)
			reaped, err := r.repo.RequeueExpiredLeases(ctx, nowStr, r.limit)
			if err != nil {
				log.Printf("[TASK-LEASE-REAPER] sweep error=%v", err)
				continue
			}
			if len(reaped) > 0 {
				log.Printf("[TASK-LEASE-REAPER] reaped=%d", len(reaped))
			}
		}
	}
}
