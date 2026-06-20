package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ── MarkStepRunning ───────────────────────────────────────────────────────

func (r *SQLiteRepository) MarkStepRunning(ctx context.Context, cmd StartStep) error {
	if cmd.RunID == "" || cmd.StepID == "" {
		return fmt.Errorf("workflow: MarkStepRunRunID/stepID required")
	}
	if cmd.Attempt <= 0 {
		cmd.Attempt = 1
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := r.now().Format(time.RFC3339)

	// Idempotent: only flip READY → RUNNING. Re-running leaves a started
	// RUNNING step alone (job_id stays).
	res, err := tx.ExecContext(ctx,
		`UPDATE workflow_steps
		 SET status = 'RUNNING',
		     job_id = ?,
		     attempt = ?,
		     started_at = COALESCE(started_at, ?),
		     updated_at = ?,
		     revision = revision + 1
		 WHERE run_id = ? AND step_id = ? AND status = 'READY'`,
		cmd.JobID, cmd.Attempt, now, now, cmd.RunID, cmd.StepID,
	)
	if err != nil {
		return fmt.Errorf("workflow mark running: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Either the step is already RUNNING (no-op) or not in READY.
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}

	// Emit WORKFLOW_STEP_RUNNING to the local audit log.
	r.appendWorkflowEvent(ctx, tx, cmd.RunID, cmd.StepID, "WORKFLOW_STEP_RUNNING",
		fmt.Sprintf(`{"job_id":%q,"attempt":%d}`, cmd.JobID, cmd.Attempt))

	// Flip run PENDING → RUNNING on first step START.
	if err := r.maybeStartRunTx(ctx, tx, cmd.RunID, now); err != nil {
		return err
	}

	return tx.Commit()
}

// ── CompleteStepAndReleaseDependents ──────────────────────────────────────

func (r *SQLiteRepository) CompleteStepAndReleaseDependents(ctx context.Context, cmd CompleteStep) (*RunProgress, error) {
	if cmd.RunID == "" || cmd.StepID == "" {
		return nil, fmt.Errorf("workflow: CompleteStep RunID/StepID required")
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := r.now().Format(time.RFC3339)
	completedAt := cmd.CompletedAt
	if completedAt.IsZero() {
		completedAt = r.now()
	}
	completedAtStr := completedAt.UTC().Format(time.RFC3339)

	outputJSON := marshalJSON(cmd.Output)

	// 1. RUNNING → SUCCEEDED on the target step.
	res, err := tx.ExecContext(ctx,
		`UPDATE workflow_steps
		 SET status = 'SUCCEEDED',
		     output_json = ?,
		     completed_at = ?,
		     updated_at = ?,
		     revision = revision + 1
		 WHERE run_id = ? AND step_id = ? AND status = 'RUNNING'`,
		string(outputJSON), completedAtStr, now, cmd.RunID, cmd.StepID,
	)
	if err != nil {
		return nil, fmt.Errorf("workflow complete step: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, fmt.Errorf("workflow: step %s not in RUNNING", cmd.StepID)
	}

	// 2. Emit WORKFLOW_STEP_SUCCEEDED to workflow_events and outbox.
	r.appendWorkflowEvent(ctx, tx, cmd.RunID, cmd.StepID, "WORKFLOW_STEP_SUCCEEDED",
		fmt.Sprintf(`{"attempt":%d}`, cmd.Attempt))
	if r.outbx != nil {
		_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
			AggregateID: cmd.RunID,
			EventType:   "WORKFLOW_STEP_SUCCEEDED",
			Payload:     marshalJSON(map[string]any{"run_id": cmd.RunID, "step_id": cmd.StepID, "attempt": cmd.Attempt}),
		})
	}

	// 3. Find dependents: every step_id X such that workflow_dependencies
	//    records (run_id, X, target_step_id = cmd.StepID).
	rows, err := tx.QueryContext(ctx,
		`SELECT step_id, step_key FROM workflow_steps
		 WHERE run_id = ? AND step_id IN (
		    SELECT step_id FROM workflow_dependencies
		    WHERE run_id = ? AND depends_on_step_id = ?
		 ) AND status = 'BLOCKED'`,
		cmd.RunID, cmd.RunID, cmd.StepID)
	if err != nil {
		return nil, fmt.Errorf("workflow list dependents: %w", err)
	}
	type dep struct {
		stepID  string
		stepKey string
	}
	var dependents []dep
	for rows.Next() {
		var id, key string
		if err := rows.Scan(&id, &key); err != nil {
			rows.Close()
			return nil, err
		}
		dependents = append(dependents, dep{stepID: id, stepKey: key})
	}
	rows.Close()

	// 4. For each BLOCKED dependent, check if every predecessor is SUCCEEDED.
	var activated []string
	for _, dep := range dependents {
		ready, err := r.allPredecessorsSucceededTx(ctx, tx, cmd.RunID, dep.stepID)
		if err != nil {
			return nil, err
		}
		if !ready {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE workflow_steps
			 SET status = 'READY', updated_at = ?, revision = revision + 1
			 WHERE run_id = ? AND step_id = ? AND status = 'BLOCKED'`,
			now, cmd.RunID, dep.stepID,
		); err != nil {
			return nil, err
		}
		r.appendWorkflowEvent(ctx, tx, cmd.RunID, dep.stepID, "WORKFLOW_STEP_READY",
			fmt.Sprintf(`{"step_key":%q}`, dep.stepKey))
		if r.outbx != nil {
			_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
				AggregateID: cmd.RunID,
				EventType:   "WORKFLOW_STEP_READY",
				Payload: marshalJSON(map[string]any{
					"run_id": cmd.RunID, "step_id": dep.stepID, "step_key": dep.stepKey,
				}),
			})
		}
		activated = append(activated, dep.stepKey)
	}

	// 5. If every step is now SUCCEEDED, the run is SUCCEEDED.
	var pendingBlocked, pendingReady, pendingRunning, pendingFailed int
	if err := tx.QueryRowContext(ctx,
		`SELECT
		    SUM(CASE WHEN status = 'BLOCKED' THEN 1 ELSE 0 END),
		    SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END),
		    SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END),
		    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)
		 FROM workflow_steps WHERE run_id = ?`, cmd.RunID,
	).Scan(&pendingBlocked, &pendingReady, &pendingRunning, &pendingFailed); err != nil {
		return nil, fmt.Errorf("workflow run progress scan: %w", err)
	}

	completedRun := pendingBlocked+pendingReady+pendingRunning+pendingFailed == 0
	if completedRun {
		if err := r.completeRunTx(ctx, tx, cmd.RunID, now); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow complete step commit: %w", err)
	}

	run, err := r.GetRun(ctx, cmd.RunID)
	if err != nil {
		return nil, err
	}
	steps, err := r.ListSteps(ctx, cmd.RunID)
	if err != nil {
		return nil, err
	}
	return &RunProgress{
		Run:       *run,
		Steps:     steps,
		Activated: activated,
		Completed: completedRun,
	}, nil
}

// ── FailStep ──────────────────────────────────────────────────────────────

func (r *SQLiteRepository) FailStep(ctx context.Context, cmd FailStep) (*RunProgress, error) {
	if cmd.RunID == "" || cmd.StepID == "" {
		return nil, fmt.Errorf("workflow: FailStep RunID/StepID required")
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := r.now().Format(time.RFC3339)

	var maxAttempts, attempt int
	var status string
	if err := tx.QueryRowContext(ctx,
		`SELECT status, max_attempts, attempt FROM workflow_steps
		 WHERE run_id = ? AND step_id = ?`, cmd.RunID, cmd.StepID,
	).Scan(&status, &maxAttempts, &attempt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("workflow: step %s not found", cmd.StepID)
		}
		return nil, err
	}

	if cmd.Requeue && attempt < maxAttempts {
		res, err := tx.ExecContext(ctx,
			`UPDATE workflow_steps
			 SET status = 'READY',
			     error_code = ?,
			     error_message = ?,
			     updated_at = ?,
			     revision = revision + 1,
			     started_at = NULL,
			     completed_at = NULL
			 WHERE run_id = ? AND step_id = ?`,
			cmd.ErrorCode, cmd.ErrorMessage, now, cmd.RunID, cmd.StepID)
		if err != nil {
			return nil, err
		}
		_ = res
		r.appendWorkflowEvent(ctx, tx, cmd.RunID, cmd.StepID, "WORKFLOW_STEP_RETRY",
			fmt.Sprintf(`{"attempt":%d,"error_code":%q}`, cmd.Attempt, cmd.ErrorCode))
	} else {
		// Terminal failure.
		res, err := tx.ExecContext(ctx,
			`UPDATE workflow_steps
			 SET status = 'FAILED',
			     error_code = ?,
			     error_message = ?,
			     completed_at = ?,
			     updated_at = ?,
			     revision = revision + 1
			 WHERE run_id = ? AND step_id = ?`,
			cmd.ErrorCode, cmd.ErrorMessage, now, now, cmd.RunID, cmd.StepID,
		)
		if err != nil {
			return nil, err
		}
		_ = res
		r.appendWorkflowEvent(ctx, tx, cmd.RunID, cmd.StepID, "WORKFLOW_STEP_FAILED",
			fmt.Sprintf(`{"attempt":%d,"error_code":%q,"error_message":%q}`,
				cmd.Attempt, cmd.ErrorCode, cmd.ErrorMessage))
		if r.outbx != nil {
			_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
				AggregateID: cmd.RunID,
				EventType:   "WORKFLOW_STEP_FAILED",
				Payload: marshalJSON(map[string]any{
					"run_id": cmd.RunID, "step_id": cmd.StepID,
					"error_code": cmd.ErrorCode, "error_message": cmd.ErrorMessage,
				}),
			})
		}
		// On terminal FAILED, flip the containing run to FAILED too
		// (mirrors completeRunTx). PR 9 §Definition of Done requires the
		// failure path to also reach a terminal run state.
		var runStatus string
		if err := tx.QueryRowContext(ctx,
			`SELECT status FROM workflow_runs WHERE run_id = ?`, cmd.RunID,
		).Scan(&runStatus); err == nil && runStatus == "RUNNING" {
			if _, err := tx.ExecContext(ctx,
				`UPDATE workflow_runs
				 SET status = 'FAILED',
				     completed_at = ?,
				     updated_at = ?,
				     last_error_code = ?,
				     last_error_message = ?,
				     revision = revision + 1
				 WHERE run_id = ? AND status = 'RUNNING'`,
				now, now, cmd.ErrorCode, cmd.ErrorMessage, cmd.RunID,
			); err != nil {
				return nil, fmt.Errorf("workflow fail run: %w", err)
			}
			r.appendWorkflowEvent(ctx, tx, cmd.RunID, "", "WORKFLOW_RUN_FAILED",
				fmt.Sprintf(`{"error_code":%q,"error_message":%q}`, cmd.ErrorCode, cmd.ErrorMessage))
			if r.outbx != nil {
				_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
					AggregateID: cmd.RunID,
					EventType:   "WORKFLOW_RUN_FAILED",
					Payload: marshalJSON(map[string]any{
						"run_id":        cmd.RunID,
						"error_code":    cmd.ErrorCode,
						"error_message": cmd.ErrorMessage,
					}),
				})
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow fail step commit: %w", err)
	}
	run, err := r.GetRun(ctx, cmd.RunID)
	if err != nil {
		return nil, err
	}
	steps, err := r.ListSteps(ctx, cmd.RunID)
	if err != nil {
		return nil, err
	}
	return &RunProgress{Run: *run, Steps: steps}, nil
}
