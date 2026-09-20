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

var _ taskgraph.TaskSpecStore = (*SQLiteTaskRepository)(nil)

func (r *SQLiteTaskRepository) GetTaskSpecPayload(ctx context.Context, taskID string) (map[string]interface{}, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task spec store: not initialized")
	}
	var encoded string
	if err := r.store.db.QueryRowContext(ctx, `SELECT COALESCE(payload_json, '{}') FROM task_specs WHERE task_id = ?`, taskID).Scan(&encoded); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task spec store: task %q not found", taskID)
		}
		return nil, wrapDBInfrastructure("task spec read", err)
	}
	payload := make(map[string]interface{})
	if encoded != "" && encoded != "{}" {
		if err := json.Unmarshal([]byte(encoded), &payload); err != nil {
			return nil, fmt.Errorf("task spec store: decode task %q payload: %w", taskID, err)
		}
	}
	return payload, nil
}

// MergeTaskSpecPayload applies a top-level runtime patch to the existing
// renderer payload. The task identity is deliberately unchanged: this is the
// PREPARE -> FINALIZE transition of one job, not a second enqueue.
//
// Only PENDING and READY tasks can be finalized. Once a worker has leased the
// task, changing its immutable render input would make the attempt ambiguous.
func (r *SQLiteTaskRepository) MergeTaskSpecPayload(ctx context.Context, taskID string, patch map[string]interface{}) (map[string]interface{}, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task spec store: not initialized")
	}
	if taskID == "" {
		return nil, fmt.Errorf("task spec store: task_id is required")
	}
	if patch == nil {
		return nil, fmt.Errorf("task spec store: patch is required")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapDBInfrastructure("task spec merge begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	var jobID, executorID, status, payloadJSON string
	var specVersion int
	err = tx.QueryRowContext(ctx, `
		SELECT t.job_id, t.executor_id, t.status,
		       s.spec_version, COALESCE(s.payload_json, '{}')
		FROM tasks t JOIN task_specs s ON s.task_id = t.task_id
		WHERE t.task_id = ?`, taskID).Scan(&jobID, &executorID, &status, &specVersion, &payloadJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task spec store: task %q not found", taskID)
		}
		return nil, wrapDBInfrastructure("task spec merge read", err)
	}
	if status != string(taskgraph.StatusPending) && status != string(taskgraph.StatusReady) {
		return nil, fmt.Errorf("task spec store: task %q is %s; runtime payload is already leased", taskID, status)
	}

	payload := make(map[string]interface{})
	if payloadJSON != "" && payloadJSON != "{}" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, fmt.Errorf("task spec store: decode task %q payload: %w", taskID, err)
		}
	}
	for key, value := range patch {
		if value == nil {
			delete(payload, key)
			continue
		}
		payload[key] = value
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("task spec store: encode task %q payload: %w", taskID, err)
	}

	// task_specs historically stores the complete spec hash but only keeps the
	// renderer payload columns. Recompute the canonical hash over the updated
	// renderer-visible spec so integrity checks never point at stale bytes.
	spec := &taskcontract.TaskSpec{
		Version:    specVersion,
		JobID:      jobID,
		ExecutorID: executorID,
		Payload:    payload,
	}
	hash, err := spec.SpecHash()
	if err != nil {
		return nil, fmt.Errorf("task spec store: hash task %q payload: %w", taskID, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE task_specs SET payload_json = ?, spec_hash = ? WHERE task_id = ?`,
		string(encoded), hash, taskID)
	if err != nil {
		return nil, wrapDBInfrastructure("task spec merge write", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		if err != nil {
			return nil, wrapDBInfrastructure("task spec merge rows", err)
		}
		return nil, fmt.Errorf("task spec store: task %q disappeared during update", taskID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ? AND status IN ('PENDING','READY')`, time.Now().UTC().Format(time.RFC3339), taskID); err != nil {
		return nil, wrapDBInfrastructure("task spec merge touch task", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("task spec merge commit", err)
	}
	return payload, nil
}
