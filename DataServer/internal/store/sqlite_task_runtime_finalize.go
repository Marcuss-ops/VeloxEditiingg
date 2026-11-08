package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/taskcontract"
)

var _ taskgraph.RuntimeFinalizationQueue = (*SQLiteTaskRepository)(nil)

func (r *SQLiteTaskRepository) RecoverInterruptedRuntimeFinalizations(ctx context.Context) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("runtime finalization queue: task store is not initialized")
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT s.task_id FROM task_specs s JOIN tasks t ON t.task_id=s.task_id
		WHERE json_extract(s.payload_json, '$.runtime_finalize_status') = 'RUNNING'
		  AND t.status IN ('PENDING', 'READY') AND (t.worker_id = '' OR t.worker_id IS NULL)`)
	if err != nil {
		return wrapDBInfrastructure("runtime finalization queue recovery", err)
	}
	var taskIDs []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			_ = rows.Close()
			return wrapDBInfrastructure("runtime finalization recovery scan", err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return wrapDBInfrastructure("runtime finalization recovery rows", err)
	}
	if err := rows.Close(); err != nil {
		return wrapDBInfrastructure("runtime finalization recovery close", err)
	}
	for _, taskID := range taskIDs {
		if _, err := r.store.db.ExecContext(ctx, `UPDATE task_specs SET payload_json=json_set(payload_json,'$.runtime_finalize_status',
			CASE WHEN json_type(payload_json,'$.runtime_prepare_task_ids')='array' THEN 'WAITING_PREPARATION' ELSE 'QUEUED' END)
			WHERE task_id=? AND json_extract(payload_json,'$.runtime_finalize_status')='RUNNING'`, taskID); err != nil {
			return wrapDBInfrastructure("runtime finalization recovery transition", err)
		}
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
		WHERE (
			json_extract(s.payload_json, '$.runtime_finalize_status') = 'QUEUED'
			OR (json_extract(s.payload_json, '$.runtime_finalize_status') = 'WAITING_PREPARATION'
			    AND NOT EXISTS (
					SELECT 1 FROM json_each(COALESCE(t.depends_on,'[]')) dep
					JOIN tasks prerequisite ON prerequisite.task_id=dep.value
					WHERE prerequisite.status <> 'SUCCEEDED'
				))
		)
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

// ClaimRuntimeFinalization uses a single conditional UPDATE as the queue's
// compare-and-swap. Multiple Master instances may list the same candidate,
// but only one can move its durable request from QUEUED to RUNNING.
func (r *SQLiteTaskRepository) ClaimRuntimeFinalization(ctx context.Context, taskID string) (bool, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return false, fmt.Errorf("runtime finalization queue: task store is not initialized")
	}
	if taskID == "" {
		return false, fmt.Errorf("runtime finalization queue: task_id is required")
	}
	claimed, err := r.transitionRuntimeFinalizationStatus(ctx, taskID, "QUEUED", "RUNNING")
	if err != nil || claimed {
		return claimed, err
	}
	return r.transitionRuntimeFinalizationStatus(ctx, taskID, "WAITING_PREPARATION", "RUNNING")
}

func (r *SQLiteTaskRepository) transitionRuntimeFinalizationStatus(ctx context.Context, taskID, from, to string) (bool, error) {
	conn, err := r.store.db.Conn(ctx)
	if err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition connection", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var jobID, taskStatus, payloadJSON string
	var specVersion int
	err = conn.QueryRowContext(ctx, `
		SELECT t.job_id, t.status, s.spec_version, COALESCE(s.payload_json, '{}')
		FROM tasks t JOIN task_specs s ON s.task_id=t.task_id
		WHERE t.task_id=? AND (t.worker_id='' OR t.worker_id IS NULL)`, taskID).
		Scan(&jobID, &taskStatus, &specVersion, &payloadJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, wrapDBInfrastructure("runtime finalization transition read", err)
	}
	if taskStatus != string(taskgraph.StatusPending) && taskStatus != string(taskgraph.StatusReady) {
		return false, nil
	}
	payload := make(map[string]interface{})
	if payloadJSON != "" && payloadJSON != "{}" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return false, fmt.Errorf("runtime finalization transition: decode task %q payload: %w", taskID, err)
		}
	}
	if payload["runtime_finalize_status"] != from {
		return false, nil
	}
	payload["runtime_finalize_status"] = to
	encoded, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("runtime finalization transition: encode task %q payload: %w", taskID, err)
	}
	spec := &taskcontract.TaskSpec{Version: specVersion, JobID: jobID, Payload: payload}
	var executorID string
	if err := conn.QueryRowContext(ctx, `SELECT executor_id FROM tasks WHERE task_id=?`, taskID).Scan(&executorID); err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition executor", err)
	}
	spec.ExecutorID = executorID
	hash, err := spec.SpecHash()
	if err != nil {
		return false, fmt.Errorf("runtime finalization transition: hash task %q payload: %w", taskID, err)
	}
	result, err := conn.ExecContext(ctx, `
		UPDATE task_specs SET payload_json=?, spec_hash=?
		WHERE task_id=? AND json_extract(payload_json, '$.runtime_finalize_status')=?`,
		string(encoded), hash, taskID, from)
	if err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition write", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition rows", err)
	}
	if changed != 1 {
		return false, nil
	}
	if _, err := conn.ExecContext(ctx, `UPDATE tasks SET updated_at=? WHERE task_id=?`, time.Now().UTC().Format(time.RFC3339), taskID); err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition touch", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return false, wrapDBInfrastructure("runtime finalization transition commit", err)
	}
	committed = true
	return true, nil
}
