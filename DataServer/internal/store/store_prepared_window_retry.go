package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/statemachine"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

// RetryFailedPreparedWindows requeues only failed video.prepare.window.v1
// tasks whose failure is a known upload error or a certification error
// followed by a successful worker image update. It keeps task specs and
// attempt history intact, and leaves the parent job RUNNING so the normal
// dispatcher can continue the existing DAG.
func (s *SQLiteStore) RetryFailedPreparedWindows(ctx context.Context, jobID string) ([]string, error) {
	if s == nil || s.db == nil || strings.TrimSpace(jobID) == "" {
		return nil, fmt.Errorf("prepared-window retry: store and job_id are required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, wrapDBInfrastructure("prepared-window retry connection", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, wrapDBInfrastructure("prepared-window retry begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	var jobStatus string
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(status,'') FROM jobs WHERE job_id=?`, jobID).Scan(&jobStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("prepared-window retry: job not found")
		}
		return nil, wrapDBInfrastructure("prepared-window retry job", err)
	}
	if jobStatus != "RUNNING" {
		return nil, fmt.Errorf("prepared-window retry: job status %s is not RUNNING", jobStatus)
	}

	rows, err := conn.QueryContext(ctx, `
		SELECT t.task_id,t.status,t.revision,t.winning_attempt_terminal_pending,
		       t.executor_id,s.executor_id,a.error_code,a.error_message,a.worker_id,a.completed_at
		FROM tasks t
		JOIN task_specs s ON s.task_id=t.task_id
		JOIN task_attempts a ON a.id=(SELECT a2.id FROM task_attempts a2 WHERE a2.task_id=t.task_id ORDER BY a2.attempt_number DESC,a2.created_at DESC LIMIT 1)
		WHERE t.job_id=? AND t.executor_id='video.prepare.window.v1' AND t.status='FAILED'
		ORDER BY t.task_id`, jobID)
	if err != nil {
		return nil, wrapDBInfrastructure("prepared-window retry candidates", err)
	}
	type candidate struct {
		id, status, taskExecutor, specExecutor, errorCode, failure, workerID, failedAt string
		revision                                                                       int
		pending                                                                        int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.status, &c.revision, &c.pending, &c.taskExecutor, &c.specExecutor, &c.errorCode, &c.failure, &c.workerID, &c.failedAt); err != nil {
			rows.Close()
			return nil, wrapDBInfrastructure("prepared-window retry scan", err)
		}
		retryableUpload := retryablePreparedWindowFailure(c.errorCode, c.failure)
		retryableCertification := c.errorCode == "WINDOW_CERTIFICATION_FAILED" && c.workerID != "" && c.failedAt != "" &&
			successfulWorkerUpdateAfterFailure(ctx, conn, c.workerID, c.failedAt)
		if c.taskExecutor != c.specExecutor || c.pending != 0 || !(retryableUpload || retryableCertification) {
			rows.Close()
			return nil, fmt.Errorf("prepared-window retry: task %s is not a known retryable failure after worker update", c.id)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, wrapDBInfrastructure("prepared-window retry rows", err)
	}
	rows.Close()
	if len(candidates) == 0 {
		return nil, fmt.Errorf("prepared-window retry: no eligible failed tasks")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if err := statemachine.DefaultRegistry().Validate(statemachine.DomainTask, c.status, string(taskgraph.StatusReady), statemachine.ActorOperator); err != nil {
			return nil, fmt.Errorf("prepared-window retry task transition %s: %w", c.id, err)
		}
		res, err := conn.ExecContext(ctx, `UPDATE tasks SET status='READY',worker_id='',lease_id='',lease_expires_at=NULL,
			attempt_id=NULL,ready_at=?,started_at=NULL,completed_at=NULL,
			winning_attempt_terminal_pending=0,winning_attempt_id=NULL,winning_attempt_committed_at=NULL,
			revision=revision+1,updated_at=?
			WHERE task_id=? AND job_id=? AND status='FAILED' AND executor_id='video.prepare.window.v1' AND revision=?`,
			now, now, c.id, jobID, c.revision)
		if err != nil {
			return nil, wrapDBInfrastructure("prepared-window retry task reset", err)
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return nil, wrapDBInfrastructure("prepared-window retry task rows", err)
		}
		if changed != 1 {
			return nil, fmt.Errorf("prepared-window retry: task %s changed concurrently", c.id)
		}
		ids = append(ids, c.id)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE jobs SET revision=COALESCE(revision,0)+1,updated_at=? WHERE job_id=? AND status='RUNNING'`, now, jobID); err != nil {
		return nil, wrapDBInfrastructure("prepared-window retry job revision", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, wrapDBInfrastructure("prepared-window retry commit", err)
	}
	committed = true
	return ids, nil
}

// These are the historical registry failures for which an explicit
// preparation retry is already supported. Other encoder and certification
// failures are not assumed to be transient from their message text.
func retryablePreparedWindowUpload(message string) bool {
	return strings.Contains(message, "prepared-window registry upload") &&
		(strings.Contains(message, "HTTP 401") || strings.Contains(message, "HTTP 422"))
}

func retryablePreparedWindowFailure(code, message string) bool {
	return code == contract.VideoPrepareWindowRetryableErrorCode || retryablePreparedWindowUpload(message)
}

func successfulWorkerUpdateAfterFailure(ctx context.Context, conn *sql.Conn, workerID, failedAt string) bool {
	var count int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fleet_operations
		WHERE worker_id=? AND op='update' AND status='SUCCEEDED' AND finished_at>?`, workerID, failedAt).Scan(&count)
	return err == nil && count > 0
}
