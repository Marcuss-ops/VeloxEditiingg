package store

import (
	"context"
	"fmt"

	"velox-server/internal/taskgraph"
)

var _ taskgraph.RuntimeFinalizationQueue = (*SQLiteTaskRepository)(nil)

func (r *SQLiteTaskRepository) RecoverInterruptedRuntimeFinalizations(ctx context.Context) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("runtime finalization queue: task store is not initialized")
	}
	_, err := r.store.db.ExecContext(ctx, `
		UPDATE task_specs
		SET payload_json = json_set(payload_json, '$.runtime_finalize_status', 'QUEUED')
		WHERE json_extract(payload_json, '$.runtime_finalize_status') = 'RUNNING'
		  AND task_id IN (SELECT task_id FROM tasks WHERE status IN ('PENDING', 'READY') AND (worker_id = '' OR worker_id IS NULL))`)
	if err != nil {
		return wrapDBInfrastructure("runtime finalization queue recovery", err)
	}
	return nil
}

// ListRuntimeFinalizationCandidates returns tasks whose durable FINALIZE
// request is queued or was interrupted while running. A RUNNING request is
// safe to replay because stock-window outputs are content-addressed and the
// task remains unclaimable while runtime_assets_pending is true.
func (r *SQLiteTaskRepository) ListRuntimeFinalizationCandidates(
	ctx context.Context,
	limit int,
) ([]taskgraph.RuntimeFinalizationCandidate, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("runtime finalization queue: task store is not initialized")
	}
	if limit <= 0 {
		limit = 32
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT t.task_id, t.job_id
		FROM task_specs s
		JOIN tasks t ON t.task_id = s.task_id
		WHERE json_extract(s.payload_json, '$.runtime_finalize_status') = 'QUEUED'
		  AND t.status IN ('PENDING', 'READY')
		  AND (t.worker_id = '' OR t.worker_id IS NULL)
		ORDER BY t.updated_at ASC, t.task_id ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, wrapDBInfrastructure("runtime finalization queue list", err)
	}
	defer rows.Close()

	candidates := make([]taskgraph.RuntimeFinalizationCandidate, 0, limit)
	for rows.Next() {
		var candidate taskgraph.RuntimeFinalizationCandidate
		if err := rows.Scan(&candidate.TaskID, &candidate.JobID); err != nil {
			return nil, wrapDBInfrastructure("runtime finalization queue scan", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("runtime finalization queue rows", err)
	}
	return candidates, nil
}
