package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ── CancelRun ─────────────────────────────────────────────────────────────

func (r *SQLiteRepository) CancelRun(ctx context.Context, runID string) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := r.now().Format(time.RFC3339)

	if _, err := tx.ExecContext(ctx,
		`UPDATE workflow_runs SET status = 'CANCELLED', completed_at = ?, updated_at = ?, revision = revision + 1
		 WHERE run_id = ? AND status NOT IN ('SUCCEEDED', 'CANCELLED')`,
		now, now, runID,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workflow_steps
		 SET status = 'CANCELLED',
		     updated_at = ?,
		     revision = revision + 1
		 WHERE run_id = ? AND status NOT IN ('SUCCEEDED', 'FAILED')`,
		now, runID,
	); err != nil {
		return err
	}
	r.appendWorkflowEvent(ctx, tx, runID, "", "WORKFLOW_RUN_CANCELLED", `{}`)
	return tx.Commit()
}

// ── ListRuns (PR 9 cutover wire-up helper) ─────────────────────────────────

func (r *SQLiteRepository) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT run_id FROM workflow_runs ORDER BY updated_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Run, 0, len(ids))
	for _, id := range ids {
		run, err := r.GetRun(ctx, id)
		if err != nil {
			return nil, err
		}
		if run != nil {
			out = append(out, *run)
		}
	}
	return out, nil
}

// ── GetStepByJobID (JOB_SUCCEEDED handler lookup) ──────────────────────────

func (r *SQLiteRepository) GetStepByJobID(ctx context.Context, jobID string) (*Step, string, error) {
	if jobID == "" {
		return nil, "", nil
	}
	row := r.db.QueryRowContext(ctx,
		`SELECT run_id, step_id FROM workflow_steps WHERE job_id = ?`, jobID)
	var runID, stepID string
	if err := row.Scan(&runID, &stepID); err != nil {
		if err == sql.ErrNoRows {
			return nil, "", nil
		}
		return nil, "", err
	}
	// Fetch full Step via ListSteps (small result set per run).
	steps, err := r.ListSteps(ctx, runID)
	if err != nil {
		return nil, "", err
	}
	for i := range steps {
		if steps[i].StepID == stepID {
			return &steps[i], runID, nil
		}
	}
	return nil, runID, nil
}

// ── Stats (legacy adapter) ────────────────────────────────────────────────

func (r *SQLiteRepository) Stats(ctx context.Context) (StatsReport, error) {
	rep := StatsReport{
		RunsByStatus:  map[RunStatus]int{},
		StepsByStatus: map[StepStatus]int{},
	}
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workflow_runs`,
	).Scan(&rep.TotalRuns); err != nil {
		return rep, err
	}
	runRows, err := r.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM workflow_runs GROUP BY status`)
	if err != nil {
		return rep, err
	}
	for runRows.Next() {
		var status string
		var n int
		if err := runRows.Scan(&status, &n); err != nil {
			runRows.Close()
			return rep, err
		}
		rep.RunsByStatus[RunStatus(status)] = n
	}
	runRows.Close()

	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workflow_steps`,
	).Scan(&rep.TotalSteps); err != nil {
		return rep, err
	}
	stepRows, err := r.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM workflow_steps GROUP BY status`)
	if err != nil {
		return rep, err
	}
	defer stepRows.Close()
	for stepRows.Next() {
		var status string
		var n int
		if err := stepRows.Scan(&status, &n); err != nil {
			return rep, err
		}
		rep.StepsByStatus[StepStatus(status)] = n
	}
	return rep, stepRows.Err()
}

// ── helpers ────────────────────────────────────────────────────────────────

// allPredecessorsSucceededTx: returns true iff every (depends_on_step_id)
// row for `stepID` in `runID` has status = 'SUCCEEDED'.
func (r *SQLiteRepository) allPredecessorsSucceededTx(ctx context.Context, tx *sql.Tx, runID, stepID string) (bool, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT ws.status FROM workflow_dependencies d
		 JOIN workflow_steps ws
		   ON ws.run_id = d.run_id AND ws.step_id = d.depends_on_step_id
		 WHERE d.run_id = ? AND d.step_id = ?`,
		runID, stepID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	got := false
	for rows.Next() {
		got = true
		var st string
		if err := rows.Scan(&st); err != nil {
			return false, err
		}
		if st != "SUCCEEDED" {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if !got {
		return false, fmt.Errorf("workflow: step %s has no predecessors but is BLOCKED", stepID)
	}
	return true, nil
}

// maybeStartRunTx flips the run PENDING → RUNNING on the first step start.
func (r *SQLiteRepository) maybeStartRunTx(ctx context.Context, tx *sql.Tx, runID, now string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE workflow_runs
		 SET status = 'RUNNING',
		     started_at = COALESCE(started_at, ?),
		     updated_at = ?,
		     revision = revision + 1
		 WHERE run_id = ? AND status = 'PENDING'`,
		now, now, runID,
	)
	return err
}

// completeRunTx flips RUNNING → SUCCEEDED and emits the WORKFLOW_RUN_SUCCEEDED event.
func (r *SQLiteRepository) completeRunTx(ctx context.Context, tx *sql.Tx, runID, now string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE workflow_runs
		 SET status = 'SUCCEEDED',
		     completed_at = ?,
		     updated_at = ?,
		     revision = revision + 1
		 WHERE run_id = ? AND status = 'RUNNING'`,
		now, now, runID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil
	}
	r.appendWorkflowEvent(ctx, tx, runID, "", "WORKFLOW_RUN_SUCCEEDED", `{}`)
	if r.outbx != nil {
		_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
			AggregateID: runID,
			EventType:   "WORKFLOW_RUN_SUCCEEDED",
			Payload:     marshalJSON(map[string]any{"run_id": runID}),
		})
	}
	return nil
}

// appendWorkflowEvent writes a row to workflow_events inside the given tx.
func (r *SQLiteRepository) appendWorkflowEvent(ctx context.Context, tx *sql.Tx, runID, stepID, eventType, payload string) {
	_, _ = tx.ExecContext(ctx,
		`INSERT INTO workflow_events (event_id, run_id, step_id, event_type, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		newWorkflowID("we"), runID, toNull(stepID), eventType, payload, r.now().Format(time.RFC3339),
	)
}
