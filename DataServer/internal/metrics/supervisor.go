// Package metrics / supervisor.go
//
// SPEC §14 follow-up: the periodic metrics supervisor. Runs a 15s
// tick (configurable) on a per-master goroutine and refreshes:
//
//  1. the 4 `velox_cost_*_per_output_minute` gauges — aggregating
//     cost + output_minutes across newly-terminal attempts in this
//     tick (set-to-current-value per tick; see cost_factors.go for
//     the math caveat on averaging these gauges);
//  2. `velox_master_worker_heartbeat_age_seconds{worker_id}` — per
//     worker via the existing AverageHeartbeatAge path;
//  3. `velox_master_outbox_pending`, `velox_master_memory_rss_bytes`,
//     `velox_master_goroutines` via RecordMasterHealth.
//
// Newly-terminal detection is delta-based: the supervisor queries
// `task_attempts WHERE status IN (terminal) AND updated_at >=
// lastTick` and dedups by attempt_id via an internal `seenIDs` map
// that is cleared when its length exceeds seenIDsCap (a pragmatic
// time-bounding compromise: per-tick the GC runs once and the
// real cap is observed-cumulative since-boot, so worst-case
// double-count window is bounded to one tick × (size-cap / limit)
// ≈ 10k / 1000 = 10 ticks — invisible because the cost gauge is a
// per-tick average that self-corrects within ~2 minutes).
//
// Bootstrap wire-up lives in cmd/server/bootstrap.go::buildSupervisor
// (registered as a BackgroundRunner named "metrics-supervisor").
package metrics

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"velox-server/internal/supervisor"
	"velox-server/internal/taskattempts"
)

// AttemptsDataSource is the unified per-attempt surface the
// supervisor depends on. It merges:
//   - the "which attempts are newly terminal since X" list;
//   - per-attempt label resolution (execID, execVer, workerClass);
//   - the canonical AttemptReader read surface so the supervisor can
//     pull the SCORE-CARD → compute-outcome → cost-basis path through
//     one and the same interface.
//
// Closing the loop: this is exactly what ingest service does for
// each hand-rolled TaskResult, so the supervisor and ingest service
// produce identical per-attempt counter behaviour. The only
// difference is the trigger source — ingest fires off worker
// reports, supervisor fires off a periodic DB scan for missed or
// pre-PR-2 ingestion paths.
type AttemptsDataSource interface {
	RecentAttemptIDs(ctx context.Context, since time.Time, limit int) ([]string, error)
	Labels(ctx context.Context, attemptID string) (execID, execVer, workerClass string, err error)
	// The remaining methods match taskattempts.Repository semantics
	// so the supervisor can plug straight into the existing
	// SQLiteTaskAttemptRepository for production and into a
	// fake for tests.
	GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error)
	GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error)
	GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error)
	GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error)
	// Scorecard v2: detailed engine phase + segment timings for the
	// per-phase/per-segment Prometheus histograms.
	GetPhaseTimingsDetailed(ctx context.Context, attemptID string) ([]taskattempts.PhaseTimingDetailed, error)
	GetSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error)
	// Parallelism telemetry (migration 098): derived concurrency/speedup
	// aggregates computed by the master from segment timing offsets.
	GetParallelism(ctx context.Context, attemptID string) (*taskattempts.AttemptParallelism, error)
	// Metrics Center / Step 2: daily metric rollups for historical trends.
	// ComputeDailyRollups aggregates attempt metrics into the
	// daily_metric_rollups table for the given UTC day (YYYY-MM-DD).
	// Idempotent — INSERT OR REPLACE per (day, metric_name, executor, worker).
	ComputeDailyRollups(ctx context.Context, day string) error
}

// OutboxGauge is the minimal contract the supervisor needs from the
// outbox package. Defined here (consumed-by-supervisor) so the
// outbox import graph stays one-way. The genuine implementation
// lives on *outbox.Store::PendingCount.
type OutboxGauge interface {
	PendingCount(ctx context.Context) (int64, error)
}

// Supervisor is the canonical 15s metrics-tick runner. One
// instance per master. Owns no state of its own beyond the dedup
// map and the LastTick wall-clock watermark.
type Supervisor struct {
	collector   *Collector
	attempts    AttemptsDataSource
	outbox      OutboxGauge
	costFactors CostFactors
	tick        time.Duration
	limit       int

	// seenIDs is the dedup map for attempt-ids already scanned in
	// past ticks. GC at seenIDs-cap; the supervisor's worst-case
	// double-count window is bounded to one tick × (cap / limit).
	seenMu  sync.Mutex
	seenIDs map[string]time.Time
	seenCap int

	// tickMu guards lastTick watermark updates.
	tickMu sync.Mutex
	last   time.Time

	// lastRollupDay tracks the last day for which daily rollups were
	// computed (UTC YYYY-MM-DD). The midnight trigger compares the
	// current tick's day against this value. Empty on first boot.
	lastRollupDay string
}

const (
	defaultSupervisorTick       = 15 * time.Second
	defaultSupervisorAttemptCap = 1000
	defaultSupervisorSeenIDsCap = 10_000
)

// NewSupervisor builds a Supervisor with default tick + cap
// settings. Bootstrap uses this; tests can call SetTick/SetLimit
// for fast ticks.
func NewSupervisor(c *Collector, attempts AttemptsDataSource, outbox OutboxGauge, f CostFactors) *Supervisor {
	if attempts == nil {
		// Defensive nil-check at construction so callers cannot
		// silently build a supervisor that does nothing at tick-time.
		panic("metrics.NewSupervisor: attempts data source is nil")
	}
	now := time.Now().UTC()
	return &Supervisor{
		collector:   c,
		attempts:    attempts,
		outbox:      outbox,
		costFactors: f,
		tick:        defaultSupervisorTick,
		limit:       defaultSupervisorAttemptCap,
		seenIDs:     make(map[string]time.Time),
		seenCap:     defaultSupervisorSeenIDsCap,
		last:        now,
	}
}

// SetTick adjusts the tick duration (useful in tests).
func (s *Supervisor) SetTick(d time.Duration) {
	if d > 0 {
		s.tick = d
	}
}

// SetLimit adjusts the recent-attempts cap per tick.
func (s *Supervisor) SetLimit(n int) {
	if n > 0 {
		s.limit = n
	}
}

// Run loops until ctx is done.
//
// Verdetto P1 #10 (Blocco 4): per-tick errors are CLASSIFIED rather
// than logged-and-continued. The primary RecentAttemptIDs scan is
// the infrastructure probe; if it fails repeatedly, the run
// goroutine returns the wrapped ErrInfrastructure to the
// BackgroundSupervisor so the ClassRestartable / ClassCritical
// restart machinery kicks in. Per-attempt label/scan failures are
// element-scoped (each row is logged once and skipped) and do not
// count toward the consecutive-error threshold.
//
// Returns ctx.Err() on graceful shutdown.
func (s *Supervisor) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	log.Printf("[METRICS-SUPERVISOR] starting — tick=%s, attempt_cap=%d, cost_factors: cpu=€%.6f/core·s network=€%.4f/GB storage=€%.6f/GB",
		s.tick, s.limit, s.costFactors.CPUCoreSecondEUR, s.costFactors.NetworkGBEUR, s.costFactors.StorageGBEUR)

	tracker := supervisor.NewFailureTrackerWithClock(supervisor.DefaultRetryPolicy(), supervisor.RealClock{})

	for {
		select {
		case <-ctx.Done():
			log.Printf("[METRICS-SUPERVISOR] exit: %v", ctx.Err())
			return ctx.Err()
		case tick := <-ticker.C:
			err := s.tickOnce(ctx, tick.UTC())
			if err == nil {
				tracker.Reset()
				continue
			}
			classified := supervisor.ClassifyError(err)
			if escalated := tracker.Record(classified); escalated != nil {
				return fmt.Errorf("metrics supervisor: %w", escalated)
			}
			// Single per-tick infra error (e.g. master-side
			// outbox gauge failure) is logged once at the
			// element-scoped site, NOT log-and-continued across
			// many ticks. The BackgroundSupervisor /ready probe
			// surfaces the state through RunnerState.Failed if
			// the streak survives consecutive tick boundaries.
		}
	}
}

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
		if mhErr := s.refreshMasterHealth(ctx, now); mhErr != nil {
			log.Printf("[METRICS-SUPERVISOR] master-health refresh: %v", mhErr)
			return mhErr
		}
		return nil
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
	s.tryDailyRollup(ctx, now)

	return nil
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

// tryDailyRollup checks if we've crossed midnight since the last
// rollup, and if so, computes the daily rollup for the day that just
// ended. Runs at most once per tick — the lastRollupDay watermark
// ensures idempotency across restarts.
func (s *Supervisor) tryDailyRollup(ctx context.Context, now time.Time) {
	today := now.UTC().Format("2006-01-02")
	if today == s.lastRollupDay {
		return
	}

	// Determine the range of days to roll up: from (lastRollupDay+1) up to
	// (today-1). On first boot (lastRollupDay empty), roll up only yesterday
	// so we don't backfill the entire history on first start.
	//
	// Iterating the full range handles extended downtime: if the supervisor
	// was down for 3 days, all 3 missing days get rolled up on the first
	// tick after recovery.
	var days []string
	if s.lastRollupDay != "" {
		// Normal path: roll up all days from lastRollupDay+1 to today-1.
		start, err := dateAddDay(s.lastRollupDay, 1)
		if err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup: bad lastRollupDay %q: %v", s.lastRollupDay, err)
			return
		}
		end, err := dateAddDay(today, -1)
		if err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup: bad today %q: %v", today, err)
			return
		}
		// Walk from start up to end inclusive.
		for d := start; d <= end; {
			days = append(days, d)
			next, err := dateAddDay(d, 1)
			if err != nil {
				log.Printf("[METRICS-SUPERVISOR] daily rollup: date arithmetic failed at %q: %v", d, err)
				break
			}
			d = next
		}
	} else {
		// First boot: roll up yesterday only.
		yesterday := now.Add(-24 * time.Hour).UTC().Format("2006-01-02")
		days = append(days, yesterday)
	}

	var errs []error
	for _, day := range days {
		log.Printf("[METRICS-SUPERVISOR] daily rollup for %s started", day)
		if err := s.attempts.ComputeDailyRollups(ctx, day); err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup for %s FAILED: %v", day, err)
			errs = append(errs, fmt.Errorf("day %s: %w", day, err))
			continue
		}
		log.Printf("[METRICS-SUPERVISOR] daily rollup for %s completed", day)
	}
	s.lastRollupDay = today
	if len(errs) > 0 {
		log.Printf("[METRICS-SUPERVISOR] daily rollup: %d/%d days failed", len(errs), len(days))
	}
}

// gcSeenIDs clears the seenIDs map when it exceeds seenCap. The
// intent is time-bounding (the thinker's critique) but a pure
// time-based sweep would risk re-processing attempts whose
// updated_at lands on the boundary. A size cap is the pragmatic
// compromise — see supervisor.go header for the worst-case
// double-count window analysis.
func (s *Supervisor) gcSeenIDs(now time.Time) {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if len(s.seenIDs) > s.seenCap {
		s.seenIDs = make(map[string]time.Time, len(s.seenIDs)/2)
	}
}

// costAggregate is the per-class rolling accumulator held in
// supervisor.tickOnce for a single tick.
type costAggregate struct {
	cpuSecs, networkGB, storageGB, outputMin float64
}

// SQLiteLabelResolver production implementation (RecentAttemptIDs,
// Labels, GetStatus/Metrics/CacheStats/CostBasis/PhaseTimingsDetailed/
// SegmentTimings + workerClassFromExecutorID/isNoSuchColumnErr helpers
// and the Compile-time guard) lives in supervisor_sqlite.go.
