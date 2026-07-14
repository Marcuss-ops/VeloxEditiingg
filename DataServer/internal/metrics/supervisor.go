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
	"database/sql"
	"fmt"
	"log"
	"strings"
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
		// Fall back to aggregate columns only when no detailed rows exist.
		if !hasDetailed {
			if am, amErr := s.attempts.GetMetrics(ctx, id); amErr == nil && am != nil {
				wid := "unknown"
				// Try to get worker_id from segment rows (the
				// aggregate columns don't carry worker_id).
				s.collector.RecordEngineAggregate(am, execID, wid)
			}
		}
		if segs, segErr := s.attempts.GetSegmentTimings(ctx, id); segErr == nil {
			for _, seg := range segs {
				wid := seg.WorkerID
				if wid == "" {
					wid = "unknown"
				}
				s.collector.RecordEngineSegment(seg, execID, wid)
			}
		} else {
			log.Printf("[METRICS-SUPERVISOR] segment timings %s: %v", id, segErr)
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

// workerClassFromExecutorID is the heuristic the supervisor /
// SQLiteLabelResolver fall back to when the workers table has no
// resource_class column or the JOIN misses. Pure string-match —
// matches the canonical costmodel enum verbatim (cpu | mixed | io
// | gpu). Empty / unknown → "default". This operator-friendly
// compromise keeps the supervisor running on legacy schemas that
// predate the typed resource_class column.
func workerClassFromExecutorID(executorID string) string {
	id := strings.ToLower(strings.TrimSpace(executorID))
	switch {
	case id == "":
		return "default"
	case strings.Contains(id, "gpu"):
		return "gpu"
	case strings.Contains(id, "scene.composite") || strings.Contains(id, "composite"):
		return "mixed"
	case strings.Contains(id, "io"):
		return "io"
	case strings.Contains(id, "transcode") || strings.Contains(id, "process"):
		return "cpu"
	default:
		return "default"
	}
}

func isNoSuchColumnErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such column")
}

// SQLiteLabelResolver is the production-grade AttemptsDataSource
// implementation. Backed by a raw *sql.DB on the canonical velox
// schema (task_attempts + tasks + workers). One humble query per
// method — the resolver is read-only and pure, so it can be shared
// across multiple supervisors if necessary.
type SQLiteLabelResolver struct {
	DB *sql.DB
}

// Compile-time guard: SQLiteLabelResolver satisfies
// AttemptsDataSource. Wiring mistakes break loudly.
var _ AttemptsDataSource = (*SQLiteLabelResolver)(nil)

// NewSQLiteLabelResolver builds the default resolver backed by
// `db`. Bootstrap wires this: velmetrics.NewSQLiteLabelResolver(p.SQLite.DB()).
func NewSQLiteLabelResolver(db *sql.DB) *SQLiteLabelResolver {
	if db == nil {
		panic("metrics.NewSQLiteLabelResolver: db is nil")
	}
	return &SQLiteLabelResolver{DB: db}
}

// RecentAttemptIDs returns IDs of attempts whose status is terminal
// (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT) AND whose updated_at is
// >= since. limit caps the response (0/negative ⇒ defaultCap).
//
// Order is updated_at ASC so older newly-terminal attempts are
// processed first within a tick — protects the dedup map against
// a long backlog at startup where the wall-clock watermark is
// initialised to "now" and RecentAttemptIDs picks up attempts
// that completed BEFORE the supervisor ever started.
func (r *SQLiteLabelResolver) RecentAttemptIDs(ctx context.Context, since time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultSupervisorAttemptCap
	}
	sinceStr := since.UTC().Format(time.RFC3339Nano)
	rows, err := r.DB.QueryContext(ctx, `
		SELECT id FROM task_attempts
		WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		  AND updated_at >= ?
		ORDER BY updated_at ASC
		LIMIT ?`,
		sinceStr, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: recent attempts query: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("supervisor: recent attempts scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("supervisor: recent attempts rows: %w", err)
	}
	return ids, nil
}

// Labels resolves (execID, execVer, workerClass) via a single JOIN
// over task_attempts + tasks + workers. The JOIN returns the
// executor identity from the canonical taskgraph row (PR-5
// typed-metrics cutover contract) and the resource classification
// from the workers row / executor_id heuristic when the workers
// schema lacks a typed column.
//
// On SQL miss (DELETE before supervisor query) the resolver
// returns the historical defaults (unknown / 0 / default) so the
// downstream ScanAttemptWithLabels call stamps with non-empty
// labels (collector.go's label-len panic is never triggered).
func (r *SQLiteLabelResolver) Labels(ctx context.Context, attemptID string) (string, string, string, error) {
	if attemptID == "" {
		return "unknown", "0", "default", nil
	}
	var execID, execVer, resourceClass sql.NullString
	err := r.DB.QueryRowContext(ctx, `
		SELECT
		    COALESCE(t.executor_id, ''),
		    COALESCE(CAST(t.executor_version AS TEXT), '0'),
		    COALESCE(w.resource_class, '')
		FROM task_attempts a
		LEFT JOIN tasks t ON t.task_id = a.task_id
		LEFT JOIN workers w ON w.worker_id = a.worker_id
		WHERE a.id = ?`,
		attemptID,
	).Scan(&execID, &execVer, &resourceClass)
	if isNoSuchColumnErr(err) {
		err = r.DB.QueryRowContext(ctx, `
			SELECT
			    COALESCE(t.executor_id, ''),
			    COALESCE(CAST(t.executor_version AS TEXT), '0'),
			    COALESCE(w.worker_class, '')
			FROM task_attempts a
			LEFT JOIN tasks t ON t.task_id = a.task_id
			LEFT JOIN workers w ON w.worker_id = a.worker_id
			WHERE a.id = ?`,
			attemptID,
		).Scan(&execID, &execVer, &resourceClass)
	}
	if err == sql.ErrNoRows {
		return "unknown", "0", "default", nil
	}
	if err != nil {
		return "", "", "", fmt.Errorf("supervisor: labels query: %w", err)
	}
	execIDStr := execID.String
	if execIDStr == "" {
		execIDStr = "unknown"
	}
	execVerStr := execVer.String
	if execVerStr == "" {
		execVerStr = "0"
	}
	class := resourceClass.String
	if class == "" {
		// Fall back to the executor-id heuristic — operators
		// with a typed resource_class column rarely hit this
		// path, and the fallback keeps legacy schemas running.
		class = workerClassFromExecutorID(execIDStr)
	}
	return execIDStr, execVerStr, class, nil
}

// GetStatus / GetMetrics / GetCacheStats / GetCostBasis mirror
// the SQLiteTaskAttemptRepository contract. They are kept inline
// (rather than wrapping the repository struct) so the supervisor
// can compile in unit tests without a fully-wired store bundle —
// see supervisor_test.go.
func (r *SQLiteLabelResolver) GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error) {
	if attemptID == "" {
		return taskattempts.AttemptStatusPending, nil
	}
	var status string
	err := r.DB.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`, attemptID,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return taskattempts.AttemptStatusPending, nil
	}
	if err != nil {
		return taskattempts.AttemptStatusPending, fmt.Errorf("supervisor: get status: %w", err)
	}
	return taskattempts.AttemptStatus(status), nil
}

func (r *SQLiteLabelResolver) GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	if attemptID == "" {
		return nil, nil
	}
	var m taskattempts.AttemptMetrics
	var concatMode string
	var streamCopy int
	var ffprobeValid, hasVideo, hasAudio, errorRetryable int
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, input_bytes, output_bytes,
		       bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
		       cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
		       frames_decoded, frames_composited, frames_encoded,
		       ffmpeg_speed_ratio, encode_passes,
		       final_concat_stream_copy, concat_mode,
		       temp_bytes_written, duplicate_download_bytes,
		       media_duration_seconds, wall_clock_seconds,
		       pipeline_resolve_ms, pipeline_validate_ms, pipeline_compile_ms,
		       pipeline_render_ms, pipeline_total_ms,
		       native_total_ms, native_process_wait_ms,
		       engine_asset_download_ms, engine_segment_build_ms,
		       engine_concat_ms, engine_audio_download_ms,
		       engine_mux_audio_ms, engine_copy_final_ms,
		       ffprobe_valid, duration_diff_sec,
		       has_video_stream, has_audio_stream,
		       output_file_size, black_frame_ratio, audio_sync_offset_ms,
		       cpu_percent_peak, rss_peak_bytes,
		       disk_read_bytes, disk_write_bytes,
		       network_rx_bytes, network_tx_bytes,
		       iowait_ms, open_fds_peak,
		       queue_ms, lease_wait_ms,
		       time_to_first_worker_ms, pending_tasks_at_start,
		       active_workers_at_start,
		       scene_count, segment_count, total_input_duration_sec,
		       resolution_width, resolution_height, fps,
		       audio_track_count, subtitle_count, template_id,
		       error_component, error_phase,
		       error_retryable, error_message_hash,
		       retry_count, wasted_cpu_ms, wasted_download_bytes,
		       wasted_cost_estimate,
		       asset_cache_hit_count, asset_cache_miss_count,
		       blob_cache_hit_count, blob_cache_miss_count,
		       render_cache_hit_count,
		       output_sha256
		FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	).Scan(
		&m.AttemptID, &m.InputBytes, &m.OutputBytes,
		&m.BytesFromDrive, &m.BytesFromBlobstore, &m.BytesFromLocalCache,
		&m.CPUTimeMS, &m.GPUTimeMS, &m.PeakRSSBytes, &m.PeakVRAMBytes,
		&m.FramesDecoded, &m.FramesComposited, &m.FramesEncoded,
		&m.FFmpegSpeedRatio, &m.EncodePasses,
		&streamCopy, &concatMode,
		&m.TempBytesWritten, &m.DuplicateDownloadBytes,
		&m.MediaDurationSeconds, &m.WallClockSeconds,
		&m.PipelineResolveMs, &m.PipelineValidateMs, &m.PipelineCompileMs,
		&m.PipelineRenderMs, &m.PipelineTotalMs,
		&m.NativeTotalMs, &m.NativeProcessWaitMs,
		&m.EngineAssetDownloadMs, &m.EngineSegmentBuildMs,
		&m.EngineConcatMs, &m.EngineAudioDownloadMs,
		&m.EngineMuxAudioMs, &m.EngineCopyFinalMs,
		&ffprobeValid, &m.DurationDiffSec,
		&hasVideo, &hasAudio,
		&m.OutputFileSize, &m.BlackFrameRatio, &m.AudioSyncOffsetMS,
		&m.CPUPercentPeak, &m.RSSPeakBytes,
		&m.DiskReadBytes, &m.DiskWriteBytes,
		&m.NetworkRxBytes, &m.NetworkTxBytes,
		&m.IOWaitMS, &m.OpenFDsPeak,
		&m.QueueMS, &m.LeaseWaitMS,
		&m.TimeToFirstWorkerMS, &m.PendingTasksAtStart,
		&m.ActiveWorkersAtStart,
		&m.SceneCount, &m.SegmentCount, &m.TotalInputDurationSec,
		&m.ResolutionWidth, &m.ResolutionHeight, &m.FPS,
		&m.AudioTrackCount, &m.SubtitleCount, &m.TemplateID,
		&m.ErrorComponent, &m.ErrorPhase,
		&errorRetryable, &m.ErrorMessageHash,
		&m.RetryCount, &m.WastedCPUMS, &m.WastedDownloadBytes,
		&m.WastedCostEstimate,
		&m.AssetCacheHitCount, &m.AssetCacheMissCount,
		&m.BlobCacheHitCount, &m.BlobCacheMissCount,
		&m.RenderCacheHitCount,
		&m.OutputSHA256,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get metrics: %w", err)
	}
	m.FinalConcatStreamCopy = streamCopy != 0
	m.ConcatMode = concatMode
	m.FFprobeValid = ffprobeValid
	m.HasVideoStream = hasVideo != 0
	m.HasAudioStream = hasAudio != 0
	m.ErrorRetryable = errorRetryable != 0
	return &m, nil
}

func (r *SQLiteLabelResolver) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if attemptID == "" {
		return nil, nil
	}
	var s taskattempts.AttemptCacheStats
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, cache_hits, cache_misses, cache_evictions,
		       cache_corruptions, cache_bytes_used, cache_entries
		FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	).Scan(&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get cache stats: %w", err)
	}
	return &s, nil
}

func (r *SQLiteLabelResolver) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	if attemptID == "" {
		return nil, nil
	}
	var b taskattempts.AttemptCostBasis
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
		       cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		FROM task_attempt_cost_basis WHERE attempt_id = ?`,
		attemptID,
	).Scan(&b.AttemptID, &b.CPUPricePerSecond, &b.StoragePricePerGB, &b.NetworkPricePerGB,
		&b.CPUTimeSecondsTotal, &b.StorageGBWritten, &b.NetworkGBEgressed, &b.OutputMinutesTotal)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get cost basis: %w", err)
	}
	b.Compute()
	return &b, nil
}

// GetPhaseTimingsDetailed returns all detailed phase timing rows for an
// attempt from the extended task_phase_timings table (migration 070).
// Returns an empty slice when no rows exist (not an error — older
// attempts predating migration 070 have no detailed rows).
func (r *SQLiteLabelResolver) GetPhaseTimingsDetailed(ctx context.Context, attemptID string) ([]taskattempts.PhaseTimingDetailed, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT attempt_id, phase, duration_ms, wall_start, wall_end,
		       phase_order, component, action,
		       status, error_code, error_message,
		       bytes_in, bytes_out, frames, metadata_json
		FROM task_phase_timings WHERE attempt_id = ? ORDER BY phase_order ASC, wall_start ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: get phase timings detailed: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.PhaseTimingDetailed
	for rows.Next() {
		var pt taskattempts.PhaseTimingDetailed
		pt.AttemptID = attemptID
		var wallStart, wallEnd string
		var phase string
		if err := rows.Scan(&pt.AttemptID, &phase, &pt.DurationMS, &wallStart, &wallEnd,
			&pt.PhaseOrder, &pt.Component, &pt.Action,
			&pt.Status, &pt.ErrorCode, &pt.ErrorMessage,
			&pt.BytesIn, &pt.BytesOut, &pt.Frames, &pt.MetadataJSON); err != nil {
			continue
		}
		pt.StartedAt, _ = time.Parse(time.RFC3339, wallStart)
		pt.CompletedAt, _ = time.Parse(time.RFC3339, wallEnd)
		results = append(results, pt)
	}
	return results, rows.Err()
}

// GetSegmentTimings returns all segment timing rows for an attempt from
// the task_attempt_segment_timings table (migration 070). Returns an
// empty slice when no rows exist.
func (r *SQLiteLabelResolver) GetSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT attempt_id, job_id, task_id, worker_id,
		       segment_index, scene_worker_index, source_type,
		       duration_ms, asset_download_ms, ffmpeg_encode_ms,
		       source_bytes, output_bytes, frames_encoded,
		       codec, preset, ffmpeg_threads,
		       status, error_code, error_message,
		       source_url_hash, cache_key,
		       input_duration_ms, output_duration_ms,
		       metadata_json
		FROM task_attempt_segment_timings WHERE attempt_id = ? ORDER BY segment_index ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: get segment timings: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.SegmentTiming
	for rows.Next() {
		var seg taskattempts.SegmentTiming
		if err := rows.Scan(&seg.AttemptID, &seg.JobID, &seg.TaskID, &seg.WorkerID,
			&seg.SegmentIndex, &seg.SceneWorkerIndex, &seg.SourceType,
			&seg.DurationMS, &seg.AssetDownloadMS, &seg.FfmpegEncodeMS,
			&seg.SourceBytes, &seg.OutputBytes, &seg.FramesEncoded,
			&seg.Codec, &seg.Preset, &seg.FfmpegThreads,
			&seg.Status, &seg.ErrorCode, &seg.ErrorMessage,
			&seg.SourceURLHash, &seg.CacheKey,
			&seg.InputDurationMS, &seg.OutputDurationMS,
			&seg.MetadataJSON); err != nil {
			continue
		}
		results = append(results, seg)
	}
	return results, rows.Err()
}


// Daily metric rollups (Step 2 / Velox Metrics Center) live in
// supervisor_rollups.go. The handlers invoked from
// Supervisor.tryDailyRollup (ComputeDailyRollups / rollupOneMetric /
// insertRollupRow / avgFloat64 / percentileFloat64 / dateAddDay) are
// all defined there on the SQLiteLabelResolver receiver.
