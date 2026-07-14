package store

import (
	"database/sql"
	"time"

	"velox-server/internal/taskgraph"
)

// SQLiteTaskRepository implements taskgraph.Repository against *SQLiteStore.
type SQLiteTaskRepository struct {
	store *SQLiteStore
}

func maxAttemptOrdinal(a, b int) int {
	if b > a {
		return b
	}
	return a
}

// Compile-time assertion.
var _ taskgraph.Repository = (*SQLiteTaskRepository)(nil)

// NewSQLiteTaskRepository wraps a SQLiteStore as a taskgraph.Repository.
func NewSQLiteTaskRepository(store *SQLiteStore) *SQLiteTaskRepository {
	return &SQLiteTaskRepository{store: store}
}

// taskColumns is the SELECT projection used by every Task read. Order
// MUST stay in sync with scanTask below. PR-2 (canonical-attempt-identity)
// added attempt_id + attempt_number; any later additions must update both
// the slice and the scanner.
var taskColumns = []string{
	"task_id", "job_id", "project_id", "render_plan_id",
	"executor_id", "executor_version", "status", "priority",
	"revision", "attempt_count", "attempt_id", "attempt_number",
	"worker_id", "lease_id",
	"ready_at", "started_at", "completed_at", "created_at", "updated_at",
}

func scanTask(row interface{ Scan(...interface{}) error }) (*taskgraph.Task, error) {
	var t taskgraph.Task
	var attemptID sql.NullString
	var readyAt, startedAt, completedAt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(
		&t.ID, &t.JobID, &t.ProjectID, &t.RenderPlanID,
		&t.ExecutorID, &t.ExecutorVersion, &t.Status, &t.Priority,
		&t.Revision, &t.AttemptCount, &attemptID, &t.AttemptNumber,
		&t.WorkerID, &t.LeaseID,
		&readyAt, &startedAt, &completedAt, &createdAt, &updatedAt,
	)
	if attemptID.Valid {
		t.AttemptID = attemptID.String
	}
	if err != nil {
		return nil, err
	}
	if readyAt.Valid && readyAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, readyAt.String); e == nil {
			t.ReadyAt = &pt
		}
	}
	if startedAt.Valid && startedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			t.StartedAt = &pt
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, completedAt.String); e == nil {
			t.CompletedAt = &pt
		}
	}
	// createdAt and updatedAt are non-nullable TIMESTAMP columns stored as
	// RFC3339 strings — must be parsed explicitly (sql.Scan cannot convert
	// a TEXT column into time.Time).
	if createdAt != "" {
		if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
			t.CreatedAt = pt
		}
	}
	if updatedAt != "" {
		if pt, e := time.Parse(time.RFC3339, updatedAt); e == nil {
			t.UpdatedAt = pt
		}
	}
	return &t, nil
}

// =====================================================================
// §9.5 invariant: Atomic Task + TaskAttempt transitions.
//
// The two-write pattern in handleTaskAccepted (Start + Create) and
// handleTaskResult (SetStatus|Fail + CompleteFinal) leaves a window
// where a process crash can leave Task terminal while the matching
// TaskAttempt is still RUNNING, OR where a Task is RUNNING with no
// active attempt at all. Audit invariant §9.5 ("Task RUNNING ⇒ Attempt
// RUNNING") demands these pairs commit together or not at all.
//
// The methods below are the SINGLE legal terminal-transition path for
// the task native dispatch. They open ONE transaction, perform both
// CAS statements, and either commit both or roll back both. Callers
// (gRPC handlers) MUST go through these methods; the original
// two-statement helpers above remain available for non-terminal
// idempotency bookkeeping but the §9.5-critical transitions are
// exclusively routed here.
// =====================================================================

// placementCandidateBatch is the default limit for ListReadyCandidates.
const placementCandidateBatch = 64

