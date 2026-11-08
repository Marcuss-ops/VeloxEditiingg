package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/statemachine"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

var ErrAssemblyRetryNotEligible = errors.New("pipeline run is not eligible for assembly-only retry")

type AssemblyRetryResult struct {
	PipelineRunID string
	JobID         string
	TaskID        string
}

// RetryFailedAssembly atomically reactivates the existing failed assembly
// task, parent job and pipeline run. The task spec and compiled-plan bytes are
// intentionally untouched; successful dependencies remain reusable.
func (s *SQLiteStore) RetryFailedAssembly(ctx context.Context, runID, clientID string) (AssemblyRetryResult, error) {
	if s == nil || s.db == nil || strings.TrimSpace(runID) == "" {
		return AssemblyRetryResult{}, fmt.Errorf("assembly retry: store and pipeline_run_id are required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry connection", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	var runStatus, jobID string
	query := `SELECT pr.status, pr.velox_job_id FROM pipeline_runs pr WHERE pr.id=?`
	args := []interface{}{runID}
	if clientID != "" {
		query = `SELECT pr.status, pr.velox_job_id FROM pipeline_runs pr
			JOIN creator_forwardings cf ON cf.forwarding_id=pr.forwarding_id
			WHERE pr.id=? AND cf.external_client_id=?`
		args = append(args, clientID)
	}
	if err := conn.QueryRowContext(ctx, query, args...).Scan(&runStatus, &jobID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AssemblyRetryResult{}, ErrPipelineRunNoRow
		}
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry pipeline run", err)
	}
	if runStatus != string(pipelineruns.StatusFailed) || strings.TrimSpace(jobID) == "" {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}

	var jobStatus string
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(status,'') FROM jobs WHERE job_id=?`, jobID).Scan(&jobStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
		}
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry job", err)
	}
	if jobStatus != string(jobs.StatusFailed) {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainJob, string(jobs.StatusFailed), string(jobs.StatusPending), statemachine.ActorOperator); err != nil {
		return AssemblyRetryResult{}, fmt.Errorf("assembly retry job transition: %w", err)
	}

	var taskID, executorID, specExecutorID, taskStatus, dependsOnJSON, payloadJSON string
	var taskRevision int
	err = conn.QueryRowContext(ctx, `
		SELECT t.task_id,t.executor_id,s.executor_id,t.status,t.revision,COALESCE(t.depends_on,'[]'),COALESCE(s.payload_json,'{}')
		FROM tasks t JOIN task_specs s ON s.task_id=t.task_id
		WHERE t.job_id=? AND t.executor_id IN ('video.assemble.copy.v1','video.assemble.slice.v1')
		ORDER BY t.created_at DESC,t.task_id LIMIT 1`, jobID).
		Scan(&taskID, &executorID, &specExecutorID, &taskStatus, &taskRevision, &dependsOnJSON, &payloadJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
		}
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry task", err)
	}
	if taskStatus != string(taskgraph.StatusFailed) && taskStatus != string(taskgraph.StatusTimedOut) {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	if executorID != specExecutorID || (executorID != "video.assemble.copy.v1" && executorID != "video.assemble.slice.v1") {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainTask, taskStatus, string(taskgraph.StatusReady), statemachine.ActorOperator); err != nil {
		return AssemblyRetryResult{}, fmt.Errorf("assembly retry task transition: %w", err)
	}
	payload := make(map[string]interface{})
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return AssemblyRetryResult{}, fmt.Errorf("assembly retry: decode saved task spec: %w", err)
	}
	if pending, ok := payload["runtime_assets_pending"].(bool); ok && pending {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(payload)
	if err != nil || plan == nil {
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}

	var dependencies []string
	if err := json.Unmarshal([]byte(dependsOnJSON), &dependencies); err != nil {
		return AssemblyRetryResult{}, fmt.Errorf("assembly retry: decode task dependencies: %w", err)
	}
	for _, dependencyID := range dependencies {
		var dependencyStatus string
		if err := conn.QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id=? AND job_id=?`, dependencyID, jobID).Scan(&dependencyStatus); err != nil || dependencyStatus != string(taskgraph.StatusSucceeded) {
			return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := conn.ExecContext(ctx, `
		UPDATE tasks SET status='READY',worker_id='',lease_id='',lease_expires_at=NULL,
			attempt_id=NULL,ready_at=?,started_at=NULL,completed_at=NULL,
			winning_attempt_terminal_pending=0,winning_attempt_id=NULL,winning_attempt_committed_at=NULL,
			revision=revision+1,updated_at=?
		WHERE task_id=? AND status=? AND revision=?`, now, now, taskID, taskStatus, taskRevision)
	if err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry task reset", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry task reset rows", err)
		}
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	result, err = conn.ExecContext(ctx, `
		UPDATE jobs SET status='PENDING',error_message='',last_error='',last_error_at='',
			failed_at='',failed_by='',completed_at='',
			revision=COALESCE(revision,0)+1,updated_at=?
		WHERE job_id=? AND status='FAILED'`, now, jobID)
	if err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry job reset", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry job reset rows", err)
		}
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	runUpdate := `UPDATE pipeline_runs SET status=?,current_stage='ASSEMBLY_RETRY',error_code='',error_message='',failed_stage='',completed_at='',updated_at=? WHERE id=? AND status='FAILED'`
	runArgs := []interface{}{string(pipelineruns.StatusWorkerQueued), now, runID}
	if clientID != "" {
		runUpdate += ` AND EXISTS (SELECT 1 FROM creator_forwardings cf WHERE cf.forwarding_id=pipeline_runs.forwarding_id AND cf.external_client_id=?)`
		runArgs = append(runArgs, clientID)
	}
	result, err = conn.ExecContext(ctx, runUpdate, runArgs...)
	if err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry run reset", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry run reset rows", err)
		}
		return AssemblyRetryResult{}, ErrAssemblyRetryNotEligible
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return AssemblyRetryResult{}, wrapDBInfrastructure("assembly retry commit", err)
	}
	committed = true
	return AssemblyRetryResult{PipelineRunID: runID, JobID: jobID, TaskID: taskID}, nil
}
