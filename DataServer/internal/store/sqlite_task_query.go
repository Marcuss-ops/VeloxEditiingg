package store

// sqlite_task_query.go: read-side query methods for the task repository.
// Pure SELECTs / scans — no INSERT/UPDATE/DELETE statements in here.
// Extracted from sqlite_task_repository.go so each sister file owns a
// single concern (queries vs CRUD vs lease/atomic transitions).

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/placement"
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
// are fetched — full payloads are loaded later by ClaimTaskForWorkerAtomic.
//
// Query: SELECT task_id, job_id, revision, priority, created_at,
// executor_id, executor_version FROM tasks WHERE status='READY'
// AND (worker_id=” OR worker_id IS NULL) ORDER BY priority DESC,
// created_at ASC LIMIT ?.
//
// limit <= 0 falls back to a safe default (placementCandidateBatch = 64).
func (r *SQLiteTaskRepository) ListReadyCandidates(ctx context.Context, limit int) ([]placement.TaskCandidate, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if limit <= 0 {
		limit = placementCandidateBatch
	}

	rows, err := r.store.db.QueryContext(ctx,
		`SELECT t.task_id, t.job_id, t.revision, t.priority, t.created_at,
		        t.executor_id, t.executor_version,
		        GROUP_CONCAT(tr.capability) AS required_capabilities
		 FROM tasks t
		 LEFT JOIN task_requirements tr ON tr.task_id = t.task_id
		 WHERE t.status = 'READY'
		   AND (t.worker_id = '' OR t.worker_id IS NULL)
		 GROUP BY t.task_id
		 ORDER BY t.priority DESC, t.created_at ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("task list ready candidates: %w", err)
	}
	defer rows.Close()

	var candidates []placement.TaskCandidate
	for rows.Next() {
		var (
			taskID             string
			jobID              string
			revision           int
			priority           int
			createdAt          string
			executorID         string
			executorVersion    int
			capabilitiesConcat sql.NullString
		)
		if scanErr := rows.Scan(&taskID, &jobID, &revision, &priority, &createdAt, &executorID, &executorVersion, &capabilitiesConcat); scanErr != nil {
			continue
		}

		var parsedTime time.Time
		if createdAt != "" {
			if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
				parsedTime = pt
			}
		}

		var capabilities []string
		if capabilitiesConcat.Valid && capabilitiesConcat.String != "" {
			capabilities = strings.Split(capabilitiesConcat.String, ",")
		}

		execKey := placement.NormalizeExecutorKey(executorID, executorVersion)

		candidates = append(candidates, placement.TaskCandidate{
			TaskID:               taskID,
			JobID:                jobID,
			Revision:             revision,
			Priority:             priority,
			CreatedAt:            parsedTime,
			Executor:             execKey,
			RequiredCapabilities: capabilities,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("task list ready candidates rows: %w", err)
	}

	return candidates, nil
}
