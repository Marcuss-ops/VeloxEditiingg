package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/taskattempts"
)

// SQLiteTaskAttemptRepository implements taskattempts.Repository against *SQLiteStore.
type SQLiteTaskAttemptRepository struct {
	store *SQLiteStore
}

// Compile-time assertion.
var _ taskattempts.Repository = (*SQLiteTaskAttemptRepository)(nil)

// NewSQLiteTaskAttemptRepository wraps a SQLiteStore as a taskattempts.Repository.
func NewSQLiteTaskAttemptRepository(store *SQLiteStore) *SQLiteTaskAttemptRepository {
	return &SQLiteTaskAttemptRepository{store: store}
}

var attemptColumns = []string{
	"id", "task_id", "job_id", "attempt_number", "worker_id", "lease_id",
	"status", "started_at", "completed_at", "error_code", "error_message",
	"report_version", "created_at", "updated_at",
}

func scanAttempt(row interface{ Scan(...interface{}) error }) (*taskattempts.TaskAttempt, error) {
	var a taskattempts.TaskAttempt
	var startedAt, completedAt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(
		&a.ID, &a.TaskID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.LeaseID,
		&a.Status, &startedAt, &completedAt, &a.ErrorCode, &a.ErrorMessage,
		&a.ReportVersion, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}
	if createdAt != "" {
		if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
			a.CreatedAt = pt
		}
	}
	if updatedAt != "" {
		if pt, e := time.Parse(time.RFC3339, updatedAt); e == nil {
			a.UpdatedAt = pt
		}
	}
	if startedAt.Valid && startedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			a.StartedAt = &pt
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, completedAt.String); e == nil {
			a.CompletedAt = &pt
		}
	}
	return &a, nil
}

// Create inserts a new attempt in PENDING state.
func (r *SQLiteTaskAttemptRepository) Create(ctx context.Context, attempt *taskattempts.TaskAttempt) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task attempt repository: store not initialized")
	}
	if attempt.ID == "" {
		attempt.ID = uuid.NewString()
	}
	if attempt.Status == "" {
		attempt.Status = taskattempts.AttemptStatusPending
	}

	// Check for active attempt
	var count int
	err := r.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ? AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		attempt.TaskID,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("task attempt create check: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("task attempt create: %w", taskattempts.ErrActiveAttemptExists)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = r.store.db.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		attempt.ID, attempt.TaskID, attempt.JobID, attempt.AttemptNumber,
		attempt.WorkerID, attempt.LeaseID,
		string(attempt.Status), now, now,
	)
	if err != nil {
		return fmt.Errorf("task attempt create: %w", err)
	}
	return nil
}

// Get returns a single attempt by ID, or (nil, nil) on missing.
func (r *SQLiteTaskAttemptRepository) Get(ctx context.Context, id string) (*taskattempts.TaskAttempt, error) {
	if id == "" {
		return nil, fmt.Errorf("task attempt repository: empty id")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts WHERE id = ?`,
		id,
	)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get: %w", err)
	}
	return a, nil
}

// ListByTaskID returns all attempts for a task, ordered by attempt number.
func (r *SQLiteTaskAttemptRepository) ListByTaskID(ctx context.Context, taskID string) ([]taskattempts.TaskAttempt, error) {
	if taskID == "" {
		return nil, nil
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts WHERE task_id = ? ORDER BY attempt_number ASC`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("task attempt list: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.TaskAttempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			continue
		}
		results = append(results, *a)
	}
	return results, rows.Err()
}

// GetActiveAttempt returns the current non-terminal attempt for a task.
func (r *SQLiteTaskAttemptRepository) GetActiveAttempt(ctx context.Context, taskID string) (*taskattempts.TaskAttempt, error) {
	if taskID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts
		 WHERE task_id = ? AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID,
	)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get active: %w", err)
	}
	return a, nil
}

// GetByTaskIDAndWorkerAndLease returns the active attempt for the
// (task_id, worker_id, lease_id) tuple — used by the master's
// handleTaskResult identity-validation wire-fallback path (PR-02 /
// fix/canonical-attempt-identity). The canonical-attempt-id-first path
// looks up via Reader.Get(attempt_id); this method backs off when a
// legacy worker reports no canonical attempt_id (or sends the
// pre-PR-02 leaseID placeholder). Returns (nil, nil) when no active
// attempt matches.
func (r *SQLiteTaskAttemptRepository) GetByTaskIDAndWorkerAndLease(
	ctx context.Context, taskID, workerID, leaseID string,
) (*taskattempts.TaskAttempt, error) {
	if taskID == "" || workerID == "" || leaseID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, workerID, leaseID)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get by identity tuple: %w", err)
	}
	return a, nil
}

// SetStatus performs a CAS status change from → to.
func (r *SQLiteTaskAttemptRepository) SetStatus(ctx context.Context, id string, from, to taskattempts.AttemptStatus, revision int) error {
	if id == "" {
		return fmt.Errorf("task attempt repository: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, report_version = report_version + 1, updated_at = ?
		 WHERE id = ? AND status = ? AND report_version = ?`,
		string(to), now, id, string(from), revision,
	)
	if err != nil {
		return fmt.Errorf("task attempt set status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task attempt set status rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task attempt set status %s: %w", id, taskattempts.ErrStaleReport)
	}
	return nil
}

// CompleteFinal marks an attempt as terminal with worker-identity CAS tuple.
// Idempotent on already-terminal attempts.
func (r *SQLiteTaskAttemptRepository) CompleteFinal(ctx context.Context, id, workerID, leaseID string, status taskattempts.AttemptStatus, errorCode, errorMessage string, revision int) error {
	if id == "" {
		return fmt.Errorf("task attempt repository: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`,
		string(status), now, errorCode, errorMessage, now,
		id, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task attempt complete final: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task attempt complete final rows: %w", err)
	}
	if n == 0 {
		// Check if already terminal (idempotent)
		var existing string
		err := r.store.db.QueryRowContext(ctx,
			`SELECT status FROM task_attempts WHERE id = ?`, id,
		).Scan(&existing)
		if err == nil && taskattempts.AttemptStatus(existing).IsTerminal() {
			return nil
		}
		return fmt.Errorf("task attempt complete final %s: %w", id, taskattempts.ErrStaleReport)
	}
	return nil
}

// Delete hard-deletes an attempt.
func (r *SQLiteTaskAttemptRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx, `DELETE FROM task_attempts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("task attempt delete: %w", err)
	}
	return nil
}

// ── Phase Timings ──────────────────────────────────────────────────────────

// PersistPhaseTimings inserts or replaces phase timing rows for an attempt.
func (r *SQLiteTaskAttemptRepository) PersistPhaseTimings(ctx context.Context, attemptID string, timings []taskattempts.PhaseTiming) error {
	if attemptID == "" || len(timings) == 0 {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("phase timings begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, pt := range timings {
		_, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_phase_timings (attempt_id, phase, duration_ms, wall_start, wall_end)
			 VALUES (?, ?, ?, ?, ?)`,
			attemptID, pt.Phase, pt.DurationMS,
			pt.WallStart.Format(time.RFC3339), pt.WallEnd.Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("phase timing insert: %w", err)
		}
	}
	return tx.Commit()
}

// GetPhaseTimings returns all phase timings for an attempt.
func (r *SQLiteTaskAttemptRepository) GetPhaseTimings(ctx context.Context, attemptID string) ([]taskattempts.PhaseTiming, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT attempt_id, phase, duration_ms, wall_start, wall_end
		 FROM task_phase_timings WHERE attempt_id = ? ORDER BY wall_start ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("phase timings get: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.PhaseTiming
	for rows.Next() {
		var pt taskattempts.PhaseTiming
		var wallStart, wallEnd string
		if err := rows.Scan(&pt.AttemptID, &pt.Phase, &pt.DurationMS, &wallStart, &wallEnd); err != nil {
			continue
		}
		pt.WallStart, _ = time.Parse(time.RFC3339, wallStart)
		pt.WallEnd, _ = time.Parse(time.RFC3339, wallEnd)
		results = append(results, pt)
	}
	return results, rows.Err()
}

// ── Attempt Metrics ────────────────────────────────────────────────────────

// PersistMetrics inserts or replaces metrics for an attempt.
//
// Scorecard v1 / migration 054: extended column list (frames_*, ffmpeg_*,
// encode_passes, final_concat_stream_copy, concat_mode, temp_bytes_*,
// duplicate_download_bytes, media/wall_clock_seconds). All DEFAULT 0
// on the migration side so older workers that don't emit these fields
// (zero structs) still persist cleanly.
func (r *SQLiteTaskAttemptRepository) PersistMetrics(ctx context.Context, metrics taskattempts.AttemptMetrics) error {
	if metrics.AttemptID == "" {
		return nil
	}
	streamCopy := 0
	if metrics.FinalConcatStreamCopy {
		streamCopy = 1
	}
	concatMode := metrics.ConcatMode
	if concatMode == "" {
		concatMode = "n/a"
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_metrics (
			attempt_id, input_bytes, output_bytes,
			bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
			cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
			frames_decoded, frames_composited, frames_encoded,
			ffmpeg_speed_ratio, encode_passes,
			final_concat_stream_copy, concat_mode,
			temp_bytes_written, duplicate_download_bytes,
			media_duration_seconds, wall_clock_seconds
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		metrics.AttemptID, metrics.InputBytes, metrics.OutputBytes,
		metrics.BytesFromDrive, metrics.BytesFromBlobstore, metrics.BytesFromLocalCache,
		metrics.CPUTimeMS, metrics.GPUTimeMS, metrics.PeakRSSBytes, metrics.PeakVRAMBytes,
		metrics.FramesDecoded, metrics.FramesComposited, metrics.FramesEncoded,
		metrics.FFmpegSpeedRatio, metrics.EncodePasses,
		streamCopy, concatMode,
		metrics.TempBytesWritten, metrics.DuplicateDownloadBytes,
		metrics.MediaDurationSeconds, metrics.WallClockSeconds,
	)
	if err != nil {
		return fmt.Errorf("metrics persist: %w", err)
	}
	return nil
}

// PersistCacheStats hoists the worker's dotted-key cache counters into a
// typed row so the byte_hit_ratio can be computed in SQL. Idempotent
// INSERT OR REPLACE keyed by attempt_id.
func (r *SQLiteTaskAttemptRepository) PersistCacheStats(ctx context.Context, stats taskattempts.AttemptCacheStats) error {
	if stats.AttemptID == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cache_stats (
			attempt_id, cache_hits, cache_misses, cache_evictions,
			cache_corruptions, cache_bytes_used, cache_entries
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		stats.AttemptID, stats.CacheHits, stats.CacheMisses, stats.CacheEvictions,
		stats.CacheCorruptions, stats.CacheBytesUsed, stats.CacheEntries,
	)
	if err != nil {
		return fmt.Errorf("cache stats persist: %w", err)
	}
	return nil
}

// GetCacheStats returns the typed cache snapshot for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cache_hits, cache_misses, cache_evictions,
		        cache_corruptions, cache_bytes_used, cache_entries
		 FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	)
	var s taskattempts.AttemptCacheStats
	err := row.Scan(
		&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache stats get: %w", err)
	}
	return &s, nil
}

// PersistCostBasis hoists the cost-model envelope for one attempt; the
// master derives cost_per_output_minute from this row via ComputeCostBasis.
func (r *SQLiteTaskAttemptRepository) PersistCostBasis(ctx context.Context, basis taskattempts.AttemptCostBasis) error {
	if basis.AttemptID == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cost_basis (
			attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
			cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		basis.AttemptID, basis.CPUPricePerSecond, basis.StoragePricePerGB, basis.NetworkPricePerGB,
		basis.CPUTimeSecondsTotal, basis.StorageGBWritten, basis.NetworkGBEgressed, basis.OutputMinutesTotal,
	)
	if err != nil {
		return fmt.Errorf("cost basis persist: %w", err)
	}
	return nil
}

// GetCostBasis returns the typed cost envelope for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
		        cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		 FROM task_attempt_cost_basis WHERE attempt_id = ?`,
		attemptID,
	)
	var b taskattempts.AttemptCostBasis
	err := row.Scan(
		&b.AttemptID, &b.CPUPricePerSecond, &b.StoragePricePerGB, &b.NetworkPricePerGB,
		&b.CPUTimeSecondsTotal, &b.StorageGBWritten, &b.NetworkGBEgressed, &b.OutputMinutesTotal,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cost basis get: %w", err)
	}
	b.Compute()
	return &b, nil
}

// GetMetrics returns metrics for an attempt, or nil if not found.
func (r *SQLiteTaskAttemptRepository) GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, input_bytes, output_bytes,
		        bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
		        cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
		        frames_decoded, frames_composited, frames_encoded,
		        ffmpeg_speed_ratio, encode_passes,
		        final_concat_stream_copy, concat_mode,
		        temp_bytes_written, duplicate_download_bytes,
		        media_duration_seconds, wall_clock_seconds
		 FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	)
	var m taskattempts.AttemptMetrics
	var concatMode string
	var streamCopy int
	err := row.Scan(
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
		return nil, fmt.Errorf("metrics get: %w", err)
	}
	m.FinalConcatStreamCopy = streamCopy != 0
	m.ConcatMode = concatMode
	return &m, nil
}
