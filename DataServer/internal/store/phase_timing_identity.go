package store

import (
	"context"
	"database/sql"
	"fmt"
)

// phaseTimingIdentity is deliberately loaded from master-owned rows rather
// than copied from a worker phase payload. The attempt supplies the verified
// job/task/worker/session snapshot tuple; the task supplies the executor
// identity assigned by the master.
type phaseTimingIdentity struct {
	JobID            string
	TaskID           string
	WorkerID         string
	WorkerSessionID  string
	LeaseID          string
	WorkerSnapshotID string
	ExecutorID       string
	ExecutorVersion  int
}

// resolvePhaseTimingIdentity loads the canonical identity for an attempt.
// When the expected tuple is supplied, it is included in the lookup so a
// stale or cross-task report cannot stamp phase rows for another attempt.
func resolvePhaseTimingIdentity(
	ctx context.Context,
	tx *sql.Tx,
	attemptID, expectedTaskID, expectedWorkerID, expectedLeaseID string,
) (phaseTimingIdentity, error) {
	if attemptID == "" {
		return phaseTimingIdentity{}, fmt.Errorf("phase timing identity: attempt_id is required")
	}

	query := `
		SELECT a.job_id, a.task_id, a.worker_id, a.worker_session_id, a.lease_id,
		       a.worker_snapshot_id, t.executor_id, t.executor_version
		FROM task_attempts a
		JOIN tasks t ON t.task_id = a.task_id
		WHERE a.id = ?`
	args := []interface{}{attemptID}
	if expectedTaskID != "" {
		query += ` AND a.task_id = ?`
		args = append(args, expectedTaskID)
	}
	if expectedWorkerID != "" {
		query += ` AND a.worker_id = ?`
		args = append(args, expectedWorkerID)
	}
	if expectedLeaseID != "" {
		query += ` AND a.lease_id = ?`
		args = append(args, expectedLeaseID)
	}

	var identity phaseTimingIdentity
	if err := tx.QueryRowContext(ctx, query, args...).Scan(
		&identity.JobID,
		&identity.TaskID,
		&identity.WorkerID,
		&identity.WorkerSessionID,
		&identity.LeaseID,
		&identity.WorkerSnapshotID,
		&identity.ExecutorID,
		&identity.ExecutorVersion,
	); err != nil {
		if err == sql.ErrNoRows {
			return phaseTimingIdentity{}, fmt.Errorf(
				"phase timing identity: canonical attempt %q not found or tuple mismatch",
				attemptID,
			)
		}
		return phaseTimingIdentity{}, fmt.Errorf("phase timing identity lookup: %w", err)
	}
	return identity, nil
}
