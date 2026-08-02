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
//
// File split by responsibility:
//   - supervisor.go        → deps, struct, constructor, Run loop
//   - supervisor_tick.go   → tickOnce + refreshMasterHealth
//   - supervisor_rollup.go → tryDailyRollup + gcSeenIDs + costAggregate
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
	// ComputeRenderPerformanceDailyRollup persists cohort/version/phase
	// aggregates, p25 baselines, and recoverable phase time.
	ComputeRenderPerformanceDailyRollup(ctx context.Context, day string) error
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

// SQLiteLabelResolver production implementation (RecentAttemptIDs,
// Labels, GetStatus/Metrics/CacheStats/CostBasis/PhaseTimingsDetailed/
// SegmentTimings + workerClassFromExecutorID/isNoSuchColumnErr helpers
// and the Compile-time guard) lives in supervisor_sqlite.go.
