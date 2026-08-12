package metrics

// supervisor_tick.go: tickOnce + refreshMasterHealth — the per-tick
// body of the metrics Supervisor. Split out of supervisor.go; the
// struct + Run loop live in supervisor.go and the daily rollup in
// supervisor_rollup.go.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/taskattempts"
)

// tickOnce is the body of one supervisor tick. Extracted so tests
// can drive it deterministically without sleeping through ticker
// waits.
//
// Verdetto P1 #10 (Blocco 4): returns the FIRST infrastructure
// error encountered in the tick (RecentAttemptIDs failure or
// refreshMasterHealth outbox-gauge failure) so the Run loop can
// route it through the supervisor.FailureTracker. Per-attempt
// label / scan errors are ELEMENT-scoped: each affected row is
// logged once at the site and skipped without affecting the
// consecutive-error counter. Returns nil when the tick completes
// without infrastructure trouble.
func (s *Supervisor) tickOnce(ctx context.Context, now time.Time) error {
	s.tickMu.Lock()
	since := s.last
	s.last = now
	s.tickMu.Unlock()

	// 1. Pull newly-terminal attempts since the LAST tick.
	ids, err := s.attempts.RecentAttemptIDs(ctx, since, s.limit)
	if err != nil {
		// RecentAttemptIDs is the primary tick error — DB scan
		// failure is the canonical infrastructure signal. We
		// still attempt master-health refresh (RSI / goroutines
		// are independent of the attempts query) but the tick
		// error IS this one. Per-tick ambient log line for
		// operational visibility (single entry, not repeated).
		log.Printf("[METRICS-SUPERVISOR] recent attempts query failed since=%s: %v",
			since.Format(time.RFC3339), err)
		if mhErr := s.refreshMasterHealth(ctx, now); mhErr != nil {
			log.Printf("[METRICS-SUPERVISOR] master-health refresh: %v", mhErr)
		}
		return fmt.Errorf("metrics supervisor: recent attempts query since=%s: %w",
			since.Format(time.RFC3339), err)
	}
	if len(ids) == 0 {
		// Daily rollups are calendar-driven, not attempt-driven. They
		// must still run on quiet ticks so an idle master can cross
		// midnight and persist the previous day's aggregates.
		rollupErr := s.tryDailyRollup(ctx, now)
		if mhErr := s.refreshMasterHealth(ctx, now); mhErr != nil {
			log.Printf("[METRICS-SUPERVISOR] master-health refresh: %v", mhErr)
			if rollupErr != nil {
				return errors.Join(rollupErr, mhErr)
			}
			return mhErr
		}
		return rollupErr
	}

	log.Printf("[METRICS-SUPERVISOR] tick=%s since=%s — %d newly-terminal attempts",
		now.Format(time.RFC3339), since.Format(time.RFC3339), len(ids))

	// 2. Per-class aggregates (typed struct for clarity; cleared
	// per tick). Multiple attempts on the same class accumulate.
	aggByClass := make(map[string]costAggregate)

	for _, id := range ids {
		if id == "" {
			continue
		}
		// Dedup: skip if the supervisor already scanned this
		// attempt in a prior tick. The seenIDs GC below bounds
		// the map size — see gcSeenIDs.
		s.seenMu.Lock()
		if _, ok := s.seenIDs[id]; ok {
			s.seenMu.Unlock()
			continue
		}
		s.seenIDs[id] = now
		s.seenMu.Unlock()

		execID, execVer, workerClass, lerr := s.attempts.Labels(ctx, id)
		if lerr != nil {
			// Element-scoped: log once, scan with default
			// labels so the per-attempt counter still stamps.
			log.Printf("[METRICS-SUPERVISOR] labels resolve for %s: %v", id, lerr)
			execID, execVer, workerClass = "unknown", "0", "default"
		}

		// 2a. Stamp per-attempt metrics + compute-outcome counter
		// via ScanAttemptWithLabels. This is the same path ingest
		// service uses — supervisor and ingest service produce
		// identical per-attempt counter behaviour for the same
		// input.
		if scanErr := s.collector.ScanAttemptWithLabels(ctx, s.attempts, id, execID, execVer, workerClass); scanErr != nil {
			// Element-scoped: log once, skip aggregation.
			log.Printf("[METRICS-SUPERVISOR] scan %s: %v", id, scanErr)
			// Do not permanently consume an attempt whose primary read
			// failed. The next tick must be able to retry it after a
			// transient SQLite/reader outage.
			s.seenMu.Lock()
			delete(s.seenIDs, id)
			s.seenMu.Unlock()
			continue
		}

		// 2a-bis. Scorecard v2: stamp engine phase + segment
		// timings onto the per-phase and per-segment histograms.
		// Prefer detailed phase rows (component.action → duration)
		// from the extended task_phase_timings table; fall back to
		// the aggregate columns in AttemptMetrics only when no
		// detailed rows exist (older attempts predating migration
		// 070).  worker_id comes from the timing rows themselves.
		//
		// Fetch segment timings once up-front so we can derive
		// attemptWorkerID (the first non-empty WorkerID across
		// segments) and reuse it for the aggregate-fallback and
		// parallelism stamps below — both previously hardcoded
		// wid="unknown", collapsing all parallelism gauges onto a
		// single worker_id="unknown" label and making per-worker
		// PromQL comparisons impossible.
		var segs []taskattempts.SegmentTiming
		if fetched, err := s.attempts.GetSegmentTimings(ctx, id); err == nil {
			segs = fetched
		} else {
			log.Printf("[METRICS-SUPERVISOR] segment timings %s: %v", id, err)
		}
		attemptWorkerID := "unknown"
		for _, seg := range segs {
			if seg.WorkerID != "" {
				attemptWorkerID = seg.WorkerID
				break
			}
		}

		hasDetailed := false
		if pts, ptErr := s.attempts.GetPhaseTimingsDetailed(ctx, id); ptErr == nil && len(pts) > 0 {
			hasDetailed = true
			for _, pt := range pts {
				wid := pt.WorkerID
				if wid == "" {
					wid = "unknown"
				}
				s.collector.RecordEnginePhase(pt, execID, wid)
			}
		} else if ptErr != nil {
			log.Printf("[METRICS-SUPERVISOR] phase timings %s: %v", id, ptErr)
		}
		// Fall back to aggregate columns only when no detailed rows
		// exist. The aggregate columns don't carry worker_id, so use
		// attemptWorkerID derived from segment timings above instead
		// of the old hardcoded "unknown".
		if !hasDetailed {
			if am, amErr := s.attempts.GetMetrics(ctx, id); amErr == nil && am != nil {
				s.collector.RecordEngineAggregate(am, execID, attemptWorkerID)
			}
		}
		for _, seg := range segs {
			wid := seg.WorkerID
			if wid == "" {
				wid = "unknown"
			}
			s.collector.RecordEngineSegment(seg, execID, wid)
		}

		// 2a-ter. Parallelism telemetry (migration 098). Read the
		// computed task_attempt_parallelism row and stamp gauges.
		// worker_id comes from the segment timing rows (via
		// attemptWorkerID derived above) — NOT a hardcoded "unknown".
		if par, parErr := s.attempts.GetParallelism(ctx, id); parErr == nil && par != nil {
			s.collector.RecordParallelism(*par, execID, attemptWorkerID)
		} else if parErr != nil {
			log.Printf("[METRICS-SUPERVISOR] parallelism %s: %v", id, parErr)
		}

		// 2b. Cost aggregation: read AttemptCostBasis and roll
		// into per-class totals.
		cb, cbErr := s.attempts.GetCostBasis(ctx, id)
		if cbErr != nil || cb == nil {
			continue
		}
		a := aggByClass[workerClass]
		a.cpuSecs += cb.CPUTimeSecondsTotal
		a.networkGB += cb.NetworkGBEgressed
		a.storageGB += cb.StorageGBWritten
		a.outputMin += cb.OutputMinutesTotal
		aggByClass[workerClass] = a
	}

	// 3. Stamp the 4 cost gauges per worker_class, plus a
	// fleet-wide aggregate stamped under worker_class="all" so a
	// single PromQL panel can see total fleet cost.
	total := costAggregate{}
	for class, a := range aggByClass {
		s.collector.RecordAggregateCost(class, a.cpuSecs, a.networkGB, a.storageGB, a.outputMin, s.costFactors)
		total.cpuSecs += a.cpuSecs
		total.networkGB += a.networkGB
		total.storageGB += a.storageGB
		total.outputMin += a.outputMin
	}
	if len(aggByClass) > 0 {
		s.collector.RecordAggregateCost("all", total.cpuSecs, total.networkGB, total.storageGB, total.outputMin, s.costFactors)
	}

	// 4. Refresh master-side health gauges (best-effort).
	if mhErr := s.refreshMasterHealth(ctx, now); mhErr != nil {
		log.Printf("[METRICS-SUPERVISOR] master-health refresh: %v", mhErr)
		return mhErr
	}

	// 5. GC the seenIDs map so it doesn't grow unbounded.
	s.gcSeenIDs(now)

	// 6. Daily rollups: if we've crossed midnight since the last rollup,
	//    compute and persist yesterday's rollup.
	return s.tryDailyRollup(ctx, now)
}

// refreshMasterHealth refreshes the heartbeat-age + master-health
// gauges from a single tick. Hoisted out of tickOnce so error
// paths can still call it without re-entering the per-attempt pass.
//
// Verdetto P1 #10 (Blocco 4): the outbox-gauge error path now
// returns the error to the caller (tickOnce) instead of just
// logging. The outbox.Store is a separate handle so a failure
// here is more likely infrastructure (e.g. shared-cache lock
// exhaustion, sqlite contention) than per-attempt script. RSI /
// goroutines (AverageHeartbeatAge / RecordMasterHealth) remain
// in-memory and do not return errors.
func (s *Supervisor) refreshMasterHealth(ctx context.Context, now time.Time) error {
	s.collector.AverageHeartbeatAge(now)
	if s.outbox != nil {
		n, err := s.outbox.PendingCount(ctx)
		if err != nil {
			return fmt.Errorf("outbox.PendingCount: %w", err)
		}
		s.collector.RecordMasterHealth(int(n))
		return nil
	}
	s.collector.RecordMasterHealth(0)
	return nil
}
