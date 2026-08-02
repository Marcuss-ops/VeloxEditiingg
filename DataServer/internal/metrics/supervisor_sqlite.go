// Package metrics / supervisor_sqlite.go
//
// SQLiteLabelResolver production implementation of AttemptsDataSource
// extracted from supervisor.go so the per-tick loop stays focused on
// lifecycle + supervision. Pure read-only SQL queries on the canonical
// velox schema (task_attempts + task_attempt_metrics +
// task_attempt_cache_stats + task_attempt_cost_basis +
// task_phase_timings + task_attempt_segment_timings + tasks +
// workers). No state of its own beyond the *sql.DB handle.
//
// This file keeps the resolver struct, the constructor, the label
// heuristics and the identity queries (RecentAttemptIDs / Labels /
// GetStatus). The six per-attempt metric queries (GetMetrics /
// GetCacheStats / GetCostBasis / GetPhaseTimingsDetailed /
// GetSegmentTimings / GetParallelism) live in the sibling file
// supervisor_sqlite_metrics.go.
package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/taskattempts"
)

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

// GetStatus mirrors the SQLiteTaskAttemptRepository contract. It is
// kept inline (rather than wrapping the repository struct) so the
// supervisor can compile in unit tests without a fully-wired store
// bundle — see supervisor_test.go.
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
