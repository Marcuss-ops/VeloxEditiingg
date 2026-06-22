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
// PR-04 / fix/task-expiry-atomic-transition transforms the sweep from a
// bulk reset into per-candidate atomic reap:
//   - RequeueExpiredLeases now SELECT-only (returns []RequeueCandidate)
//   - the reaper iterates candidates and calls LifecycleService.ExpireTaskLease
//     which performs Task CAS + Attempt TIMED_OUT close + retry budget +
//     post-commit Job aggregate update in one tx (audit §9.5, P0#4, P0#6).
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

// TaskLeaseReaper sweeps tasks whose lease has expired and runs the
// audit-mandated per-task atomic reap. The reaper owns its own ticker
// and goroutine so its cadence is decoupled from the readiness dispatcher.
//
// Default cadence: 30 s. The 30-s interval matches the master-side default
// task lease TTL (30 * time.Minute) so a freshly leased task waits at
// most TTL + 30 s before being reaped if its worker crashes mid-flight —
// well within the audit §P0.4 budget "worker crash → Task returns to
// READY within 1× TTL".
type TaskLeaseReaper struct {
	lifecycle *LifecycleService
	ticker    time.Duration
	limit     int
	now       func() time.Time
}

// NewTaskLeaseReaper constructs a TaskLeaseReaper. lifecycle is
// required (it owns the reap atomic + retry budget query + post-commit
// Job aggregate update); ticker and limit get safe defaults if non-positive.
func NewTaskLeaseReaper(lifecycle *LifecycleService) *TaskLeaseReaper {
	return NewTaskLeaseReaperWithConfig(lifecycle, 30*time.Second, 100)
}

// NewTaskLeaseReaperWithConfig constructs a TaskLeaseReaper with explicit
// ticker and limit. Used by tests that want a sub-second cadence so the
// reaper can be observed under deterministic time.
func NewTaskLeaseReaperWithConfig(lifecycle *LifecycleService, ticker time.Duration, limit int) *TaskLeaseReaper {
	if lifecycle == nil {
		panic("taskgraph.NewTaskLeaseReaper: lifecycle is required")
	}
	if ticker <= 0 {
		ticker = 30 * time.Second
	}
	if limit <= 0 {
		limit = 100
	}
	return &TaskLeaseReaper{
		lifecycle: lifecycle,
		ticker:    ticker,
		limit:     limit,
		now:       func() time.Time { return time.Now().UTC() },
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
// lifecycle.RequeueExpiredLeases (SELECT-only) and per-row
// lifecycle.ExpireTaskLease, then logs a structured summary with
// reaped / still-running counts. Per-candidate errors are logged but
// never fatal — the reaper keeps running so a transient SQL hiccup
// clears on the next sweep.
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
			candidates, err := r.lifecycle.RequeueExpiredLeases(ctx, nowStr, r.limit)
			if err != nil {
				log.Printf("[TASK-LEASE-REAPER] sweep error=%v", err)
				continue
			}
			if len(candidates) == 0 {
				continue
			}

			var reaped, exhausted, raceLost, reapErr int
			for _, c := range candidates {
				res, aerr := r.lifecycle.ExpireTaskLease(ctx, c)
				switch {
				case aerr != nil:
					// CAS raced out (worker just renewed, task already
					// terminal, etc.) — non-fatal, log and continue.
					reapErr++
					log.Printf("[TASK-LEASE-REAPER] task %s expired-lease reap non-fatal error: %v", c.ID, aerr)
					continue
				case res.AttemptsExhausted:
					exhausted++
				case !res.AttemptClosed:
					raceLost++
				default:
					reaped++
				}
			}

			log.Printf("[TASK-LEASE-REAPER] reaped=%d exhausted=%d no-active-attempt=%d race-lost=%d errors=%d",
				reaped, exhausted, raceLost, raceLost, reapErr)
		}
	}
}
