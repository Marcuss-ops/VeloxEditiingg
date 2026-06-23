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

// Run loops until ctx is done. Errors from individual ticks are
// logged and do NOT abort the run. Returns ctx.Err() on graceful
// shutdown.
func (s *Supervisor) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	log.Printf("[METRICS-SUPERVISOR] starting — tick=%s, attempt_cap=%d, cost_factors: cpu=€%.6f/core·s network=€%.4f/GB storage=€%.6f/GB",
		s.tick, s.limit, s.costFactors.CPUCoreSecondEUR, s.costFactors.NetworkGBEUR, s.costFactors.StorageGBEUR)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[METRICS-SUPERVISOR] exit: %v", ctx.Err())
			return ctx.Err()
		case tick := <-ticker.C:
			s.tickOnce(ctx, tick.UTC())
		}
	}
}

// tickOnce is the body of one supervisor tick. Extracted so tests
// can drive it deterministically without sleeping through ticker
// waits.
func (s *Supervisor) tickOnce(ctx context.Context, now time.Time) {
	s.tickMu.Lock()
	since := s.last
	s.last = now
	s.tickMu.Unlock()

	// 1. Pull newly-terminal attempts since the LAST tick.
	ids, err := s.attempts.RecentAttemptIDs(ctx, since, s.limit)
	if err != nil {
		log.Printf("[METRICS-SUPERVISOR] recent attempts query failed since=%s: %v",
			since.Format(time.RFC3339), err)
		// Still refresh master-side gauges even if the per-attempt
		// pass failed — RSS / goroutines / outbox are independent.
		s.refreshMasterHealth(ctx, now)
		return
	}
	if len(ids) == 0 {
		s.refreshMasterHealth(ctx, now)
		return
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
			log.Printf("[METRICS-SUPERVISOR] labels resolve for %s: %v", id, lerr)
			// DO NOT continue — keep scanning via
			// ScanAttemptWithLabels below with default labels
			// (the function fills "unknown/0/default" itself).
			execID, execVer, workerClass = "unknown", "0", "default"
		}

		// 2a. Stamp per-attempt metrics + compute-outcome counter
		// via ScanAttemptWithLabels. This is the same path ingest
		// service uses (ScanAttempt → RecordAttempt +
		// RecordAttemptOutcome) — supervisor and ingest service
		// produce identical per-attempt counter behaviour for the
		// same input.
		if scanErr := s.collector.ScanAttemptWithLabels(ctx, s.attempts, id, execID, execVer, workerClass); scanErr != nil {
			log.Printf("[METRICS-SUPERVISOR] scan %s: %v", id, scanErr)
		}

		// 2b. Cost aggregation: read AttemptCostBasis and roll
		// into per-class totals. The supervisor does NOT
		// divide-by-min per attempt; it sums first and divides
		// ONCE per class on the OUTBOUND cost-factors application
		// — this preserves the math caveat in cost_factors.go
		// (single-per-tick gauge, not incremental Inc that
		// averages-of-averages).
		cb, cbErr := s.attempts.GetCostBasis(ctx, id)
		if cbErr != nil || cb == nil {
			// Skip aggregation for this attempt — the
			// scan+outcome step above still stamps the
			// per-attempt counters.
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

	// 4. Refresh master-side health gauges.
	s.refreshMasterHealth(ctx, now)

	// 5. GC the seenIDs map so it doesn't grow unbounded.
	s.gcSeenIDs(now)
}

// refreshMasterHealth refreshes the heartbeat-age + master-health
// gauges from a single tick. Hoisted out of tickOnce so error
// paths can still call it without re-entering the per-attempt pass.
func (s *Supervisor) refreshMasterHealth(ctx context.Context, now time.Time) {
	s.collector.AverageHeartbeatAge(now)
	var pending int64
	if s.outbox != nil {
		n, err := s.outbox.PendingCount(ctx)
		if err != nil {
			log.Printf("[METRICS-SUPERVISOR] outbox.PendingCount: %v", err)
		} else {
			pending = n
		}
	}
	s.collector.RecordMasterHealth(int(pending))
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
		LEFT JOIN tasks t ON t.id = a.task_id
		LEFT JOIN workers w ON w.worker_id = a.worker_id
		WHERE a.id = ?`,
		attemptID,
	).Scan(&execID, &execVer, &resourceClass)
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
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, input_bytes, output_bytes,
		       bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
		       cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
		       frames_decoded, frames_composited, frames_encoded,
		       ffmpeg_speed_ratio, encode_passes,
		       final_concat_stream_copy, concat_mode,
		       temp_bytes_written, duplicate_download_bytes,
		       media_duration_seconds, wall_clock_seconds
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
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get metrics: %w", err)
	}
	m.FinalConcatStreamCopy = streamCopy != 0
	m.ConcatMode = concatMode
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
