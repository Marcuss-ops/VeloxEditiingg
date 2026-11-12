package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/taskcontract"
)

var _ taskgraph.RuntimeFinalizationFailures = (*SQLiteTaskRepository)(nil)

// ReconcileFailedRuntimePreparations also repairs failures left behind by a
// restart. A failed dependency is rechecked inside the write transaction so a
// concurrently requeued preparation cannot cause an obsolete job failure.
func (r *SQLiteTaskRepository) ReconcileFailedRuntimePreparations(ctx context.Context, limit int) (int, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return 0, errors.New("runtime finalization failure: task store is not initialized")
	}
	if limit <= 0 {
		limit = 32
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT t.task_id FROM tasks t
		JOIN task_specs s ON s.task_id=t.task_id
		JOIN jobs j ON j.job_id=t.job_id
		WHERE json_extract(s.payload_json,'$.runtime_finalize_status')='WAITING_PREPARATION'
		  AND t.status IN ('PENDING','READY','CANCELLED')
		  AND COALESCE(t.worker_id,'')=''
		  AND UPPER(j.status) NOT IN ('SUCCEEDED','FAILED','CANCELLED','CANCELED')
		  AND EXISTS (SELECT 1 FROM json_each(COALESCE(t.depends_on,'[]')) dep
		    JOIN tasks child ON child.task_id=dep.value AND child.job_id=t.job_id
		    WHERE child.status IN ('FAILED','CANCELLED','TIMED_OUT'))
		ORDER BY t.updated_at,t.task_id LIMIT ?`, limit)
	if err != nil {
		return 0, fmt.Errorf("list failed runtime preparations: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan failed runtime preparation: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("read failed runtime preparations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close failed runtime preparations: %w", err)
	}
	count := 0
	for _, id := range ids {
		changed, err := r.failRuntimeFinalization(ctx, id, "PREPARATION_TASK_FAILED", "", true)
		if err != nil {
			return count, err
		}
		if changed {
			count++
		}
	}
	return count, nil
}

func (r *SQLiteTaskRepository) FailRuntimeFinalization(ctx context.Context, taskID, code, message string) error {
	_, err := r.failRuntimeFinalization(ctx, taskID, code, message, false)
	return err
}

func (r *SQLiteTaskRepository) failRuntimeFinalization(ctx context.Context, taskID, code, message string, dependency bool) (bool, error) {
	if r == nil || r.store == nil || r.store.db == nil || strings.TrimSpace(taskID) == "" {
		return false, errors.New("runtime finalization failure: initialized store and task ID are required")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// The first write obtains SQLite's write reservation before any reads.
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET revision=revision WHERE task_id=?
		AND status IN ('PENDING','READY','CANCELLED') AND COALESCE(worker_id,'')=''
		AND EXISTS (SELECT 1 FROM jobs WHERE jobs.job_id=tasks.job_id
		  AND UPPER(jobs.status) NOT IN ('SUCCEEDED','FAILED','CANCELLED','CANCELED'))`, taskID)
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure lock: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	var jobID, executor, payloadJSON, state string
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT t.job_id,t.executor_id,s.spec_version,s.payload_json,
		COALESCE(json_extract(s.payload_json,'$.runtime_finalize_status'),'')
		FROM tasks t JOIN task_specs s ON s.task_id=t.task_id WHERE t.task_id=?`, taskID).
		Scan(&jobID, &executor, &version, &payloadJSON, &state); err != nil {
		return false, fmt.Errorf("runtime finalization failure read: %w", err)
	}
	if dependency {
		if state != "WAITING_PREPARATION" {
			return false, nil
		}
		var childID, childStatus, errorCode, errorMessage string
		err := tx.QueryRowContext(ctx, `SELECT child.task_id,child.status,
			COALESCE((SELECT a.error_code FROM task_attempts a WHERE a.task_id=child.task_id ORDER BY a.attempt_number DESC LIMIT 1),''),
			COALESCE((SELECT a.error_message FROM task_attempts a WHERE a.task_id=child.task_id ORDER BY a.attempt_number DESC LIMIT 1),'')
			FROM tasks parent JOIN json_each(COALESCE(parent.depends_on,'[]')) dep
			JOIN tasks child ON child.task_id=dep.value AND child.job_id=parent.job_id
			WHERE parent.task_id=? AND child.status IN ('FAILED','CANCELLED','TIMED_OUT')
			ORDER BY child.task_id LIMIT 1`, taskID).Scan(&childID, &childStatus, &errorCode, &errorMessage)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("runtime finalization failure dependency: %w", err)
		}
		// A Master restart can encounter a failed attempt that predates the
		// atomic ingest retry path. Requeue the same known transient failures
		// under the same lock before declaring the parent terminal.
		if retryablePreparedWindowFailure(errorCode, errorMessage) {
			var workerID, leaseID, attemptID string
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(worker_id,''),COALESCE(lease_id,''),COALESCE(attempt_id,'') FROM tasks WHERE task_id=?`, childID).Scan(&workerID, &leaseID, &attemptID); err != nil {
				return false, fmt.Errorf("runtime finalization failure retry identity: %w", err)
			}
			retrying, err := schedulePreparedWindowRetry(ctx, tx, taskgraph.IngestResultCommand{
				TaskID: childID, JobID: jobID, WorkerID: workerID, LeaseID: leaseID, AttemptID: attemptID,
				TaskStatus: taskgraph.StatusFailed, ErrorCode: errorCode, ErrorMsg: errorMessage,
			}, time.Now().UTC())
			if err != nil {
				return false, err
			}
			if retrying {
				if err := tx.Commit(); err != nil {
					return false, fmt.Errorf("runtime finalization retry commit: %w", err)
				}
				return true, nil
			}
		}
		message = fmt.Sprintf("preparation task %s ended %s: %s %s", childID, childStatus, errorCode, errorMessage)
	} else if state != "RUNNING" {
		return false, nil
	}
	message = strings.TrimSpace(message)
	if len(message) > 2048 {
		message = message[:2048]
	}
	if code == "" {
		code = "RUNTIME_FINALIZE_FAILED"
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return false, fmt.Errorf("runtime finalization failure decode: %w", err)
	}
	payload["runtime_finalize_status"] = "FAILED"
	payload["runtime_finalize_error_code"] = code
	payload["runtime_finalize_error"] = message
	payload["runtime_assets_pending"] = true
	encoded, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure encode: %w", err)
	}
	specHash, err := (&taskcontract.TaskSpec{Version: version, JobID: jobID, ExecutorID: executor, Payload: payload}).SpecHash()
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure hash: %w", err)
	}
	now := nowRFC3339()
	if _, err := tx.ExecContext(ctx, `UPDATE task_specs SET payload_json=?,spec_hash=? WHERE task_id=?`, string(encoded), specHash, taskID); err != nil {
		return false, fmt.Errorf("runtime finalization failure spec: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status='FAILED',completed_at=?,updated_at=?,revision=revision+1 WHERE task_id=?`, now, now, taskID); err != nil {
		return false, fmt.Errorf("runtime finalization failure task: %w", err)
	}
	// Prevent independent, unleased preparations from being offered after the
	// parent job has failed. Already leased/running attempts retain their
	// identity and finish or expire through the normal lifecycle.
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELLED',completed_at=?,updated_at=?,revision=revision+1
		WHERE job_id=? AND task_id<>? AND status IN ('PENDING','READY')`, now, now, jobID, taskID); err != nil {
		return false, fmt.Errorf("runtime finalization failure cancel unleased tasks: %w", err)
	}
	res, err = tx.ExecContext(ctx, `UPDATE jobs SET status='FAILED',updated_at=?,revision=COALESCE(revision,0)+1,
		error_message=?,failed_at=?,failed_by='' WHERE job_id=?
		AND UPPER(status) NOT IN ('SUCCEEDED','FAILED','CANCELLED','CANCELED')`, now, message, now, jobID)
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure job: %w", err)
	}
	count, err = res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure job transition rows: %w", err)
	}
	if count != 1 {
		return false, fmt.Errorf("runtime finalization failure job transition: affected=%d", count)
	}
	dialect := sqliteDialect{store: r.store}
	if err := dialect.InsertHistoryTx(ctx, tx, jobID, "FAILED", "", "Job failed ["+code+"]: "+message); err != nil {
		return false, fmt.Errorf("runtime finalization failure history: %w", err)
	}
	if err := dialect.InsertEventTx(ctx, tx, jobID, "job_failed", map[string]interface{}{"error_code": code, "error": message}); err != nil {
		return false, fmt.Errorf("runtime finalization failure event: %w", err)
	}
	event, err := json.Marshal(map[string]interface{}{"job_id": jobID, "error_code": code, "error": message})
	if err != nil {
		return false, fmt.Errorf("runtime finalization failure outbox payload: %w", err)
	}
	if err := dialect.EmitOutboxTx(ctx, tx, "job", jobID, "JOB_FAILED", event); err != nil {
		return false, fmt.Errorf("runtime finalization failure outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("runtime finalization failure commit: %w", err)
	}
	return true, nil
}
