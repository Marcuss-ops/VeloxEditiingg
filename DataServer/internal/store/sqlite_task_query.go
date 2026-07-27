package store

// sqlite_task_query.go: read-side query methods for the task repository.
// Pure SELECTs / scans — no INSERT/UPDATE/DELETE statements in here.
// Extracted from sqlite_task_repository.go so each sister file owns a
// single concern (queries vs CRUD vs lease/atomic transitions).

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/placement"

	"velox-shared/dispatchable"
)

// IsAllAttemptCommitsCommittedForTasks is the Phase 2.8 roll-up gate
// consumed by TaskReportIngestionService.maybeTransitionJob. Returns
// true iff every taskID has an attempt_commits row with status='COMMITTED'.
// Tasks with no attempt_commits row (legacy pre-Phase-2 paths or
// pre-commit-protocol workers) are treated as NOT-committed and block
// the Job's AWAITING_ARTIFACT promotion.
//
// Distinct CAST ensures the COUNT only counts rows that are uniquely
// matched per task_id; duplicates from re-declaration (UNIQUE
// task_id+attempt_id is a different layer) are still distinct here.
//
// Empty taskIDs returns false (defensive: nothing to commit).
func (r *SQLiteTaskRepository) IsAllAttemptCommitsCommittedForTasks(ctx context.Context, taskIDs []string) (bool, error) {
	if r.store == nil || r.store.db == nil {
		return false, fmt.Errorf("task repository: store not initialized")
	}
	if len(taskIDs) == 0 {
		return false, nil
	}
	placeholders := strings.Repeat(",?", len(taskIDs))[1:]
	args := make([]interface{}, len(taskIDs))
	for i, id := range taskIDs {
		args[i] = id
	}
	var committed int
	err := r.store.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT task_id) FROM attempt_commits
		  WHERE task_id IN (`+placeholders+`) AND status = 'COMMITTED'`,
		args...,
	).Scan(&committed)
	if err != nil {
		return false, fmt.Errorf("task repository: IsAllAttemptCommitsCommittedForTasks: %w", err)
	}
	return committed == len(taskIDs), nil
}

// AreDependenciesSatisfied returns true when all tasks in dependsOn
// have status SUCCEEDED. Returns true when dependsOn is empty.
// PR #4: used by TickReadiness for real dependency verification.
func (r *SQLiteTaskRepository) AreDependenciesSatisfied(ctx context.Context, dependsOn []string) (bool, error) {
	if len(dependsOn) == 0 {
		return true, nil
	}
	placeholders := strings.Repeat(",?", len(dependsOn))[1:]
	args := make([]interface{}, len(dependsOn))
	for i, id := range dependsOn {
		args[i] = id
	}
	var count int
	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM tasks
		 WHERE task_id IN (%s) AND status = 'SUCCEEDED'`,
		placeholders,
	)
	err := r.store.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("task deps check: %w", err)
	}
	return count == len(dependsOn), nil
}

// ListReadyCandidates returns lightweight task metadata rows for the
// placement matcher. Only the columns needed for placement decisions
// are projected — the shared SELECT in shared/dispatchable fetches
// a superset (incl. payload_json via LEFT JOIN task_specs), but the
// scheduler ignores Payload here. Full payloads are still loaded
// lazily on ClaimTaskForWorkerAtomic.
//
// The SQL itself lives in shared/dispatchable to keep the WHERE /
// ORDER BY contract in ONE place. The asset-cache snapshot service
// (Pass 5) consumes the same SELECT via the same shared function so
// that snapshot ordering tracks scheduler ordering exactly.
//
// limit <= 0 falls back to placementCandidateBatch (the scheduler's
// own default), deferring the canonical DefaultLimit constant in
// shared to the snapshot service only.
func (r *SQLiteTaskRepository) ListReadyCandidates(ctx context.Context, limit int) ([]placement.TaskCandidate, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if limit <= 0 {
		limit = placementCandidateBatch
	}

	jobs, err := dispatchable.ListNextDispatchableJobs(ctx, r.store.db, limit)
	if err != nil {
		return nil, fmt.Errorf("task list ready candidates: %w", err)
	}

	// Preserve the zero-row → nil-slice contract that handler_workers
	// (and the empty-candidates branch path in placement workers.go)
	// races against. A non-nil empty slice would still pass len==0 at
	// most call sites but breaks the eager nil check. Use var + append
	// rather than make([]T, 0, n) so the zero-row case stays nil.
	var candidates []placement.TaskCandidate
	for _, j := range jobs {
		candidates = append(candidates, placement.TaskCandidate{
			TaskID:               j.TaskID,
			JobID:                j.JobID,
			Revision:             j.Revision,
			Priority:             j.Priority,
			CreatedAt:            j.CreatedAt,
			Executor:             placement.NormalizeExecutorKey(j.ExecutorID, j.ExecutorVersion),
			RequiredCapabilities: j.RequiredCapabilities,
		})
	}
	return candidates, nil
}
