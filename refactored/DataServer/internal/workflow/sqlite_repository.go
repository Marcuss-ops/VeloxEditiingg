// Package workflow/sqlite_repository — SQLite-backed Implementation of
// Repository (see repository.go). The implementation uses BEGIN IMMEDIATE
// for every mutating method so concurrent dispatchers do not race on the
// same step row.
//
// Per PR 9 §Implementation §2: CompleteStepAndReleaseDependents is the
// critical atomic operation:
//   * step  RUNNING → SUCCEEDED, output written
//   * for each dependent step:
//       if every predecessor is SUCCEEDED,
//         dependent status BLOCKED → READY
//   * if every step is now terminal, run SUCCEEDED
//
// Per PR 9 §Implementation §3: workflow_steps.job_id is the link to the
// jobs table; no duplicates on jobs.
package workflow

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SQLiteRepository is the production Repository implementation.
type SQLiteRepository struct {
	db    *sql.DB
	outbx OutboxWriter // optional; nil disables outbox emission
	now   func() time.Time
}

// NewSQLiteRepository returns a Repository backed by an *sql.DB.
func NewSQLiteRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db, now: func() time.Time { return time.Now().UTC() }}
}

// SetOutbox wires an outbox writer so Repository emits outbox_events
// alongside workflow_events. Idempotent.
func (r *SQLiteRepository) SetOutbox(w OutboxWriter) { r.outbx = w }

// SetClock replaces the time source (used only by tests).
func (r *SQLiteRepository) SetClock(fn func() time.Time) {
	if fn != nil {
		r.now = fn
	}
}

// ── CreateRun ─────────────────────────────────────────────────────────────

func (r *SQLiteRepository) CreateRun(ctx context.Context, spec WorkflowSpec) (*Run, error) {
	if spec.RunID == "" {
		return nil, fmt.Errorf("workflow: empty run_id")
	}
	if spec.WorkflowType == "" {
		return nil, fmt.Errorf("workflow: empty workflow_type")
	}
	if len(spec.Steps) == 0 {
		return nil, fmt.Errorf("workflow: empty steps")
	}

	// Validate step keys are unique and dependency refs are valid.
	keys := make(map[string]int, len(spec.Steps))
	for i, s := range spec.Steps {
		if s.StepKey == "" {
			return nil, fmt.Errorf("workflow: step[%d] missing step_key", i)
		}
		if _, dup := keys[s.StepKey]; dup {
			return nil, fmt.Errorf("workflow: duplicate step_key %q", s.StepKey)
		}
		keys[s.StepKey] = i
	}
	for _, s := range spec.Steps {
		for _, d := range s.DependsOnKeys {
			if _, ok := keys[d]; !ok {
				return nil, fmt.Errorf("workflow: step %q depends on unknown step %q", s.StepKey, d)
			}
		}
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := r.now().Format(time.RFC3339)
	inputJSON := marshalJSON(spec.Input)
	runStatus := RunStatusPending

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO workflow_runs (run_id, workflow_type, status, input_json, created_at, updated_at, revision)
		 VALUES (?, ?, ?, ?, ?, ?, 0)`,
		spec.RunID, spec.WorkflowType, string(runStatus), string(inputJSON), now, now,
	); err != nil {
		return nil, fmt.Errorf("workflow create run: %w", err)
	}

	stepIDs := make([]string, len(spec.Steps))
	for i, s := range spec.Steps {
		stepID := newWorkflowID("ws")
		stepIDs[i] = stepID

		// Initial status: READY if no deps, BLOCKED otherwise.
		status := StepStatusReady
		if len(s.DependsOnKeys) > 0 {
			status = StepStatusBlocked
		}
		maxAttempts := s.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}
		inputJSON := marshalJSON(s.Input)

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO workflow_steps
			 (step_id, run_id, step_key, status, max_attempts, input_json, created_at, updated_at, revision)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
			stepID, spec.RunID, s.StepKey, string(status), maxAttempts, string(inputJSON), now, now,
		); err != nil {
			return nil, fmt.Errorf("workflow create step %q: %w", s.StepKey, err)
		}
	}

	// Map step_key → step_id for dependency rows.
	keyToID := make(map[string]string, len(spec.Steps))
	for i, s := range spec.Steps {
		keyToID[s.StepKey] = stepIDs[i]
	}
	for i, s := range spec.Steps {
		for _, d := range s.DependsOnKeys {
			if _, err := tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO workflow_dependencies (run_id, step_id, depends_on_step_id)
				 VALUES (?, ?, ?)`,
				spec.RunID, stepIDs[i], keyToID[d],
			); err != nil {
				return nil, fmt.Errorf("workflow dependency %q→%q: %w", d, s.StepKey, err)
			}
		}
	}

	// Emit READY events for steps that are immediately dispatchable.
	for i, s := range spec.Steps {
		if len(s.DependsOnKeys) == 0 {
			r.appendWorkflowEvent(ctx, tx, spec.RunID, stepIDs[i], "WORKFLOW_STEP_READY",
				fmt.Sprintf(`{"step_key":%q,"run_id":%q}`, s.StepKey, spec.RunID))
		}
	}

	// Emit outbox for each ready step.
	if r.outbx != nil {
		for i, s := range spec.Steps {
			if len(s.DependsOnKeys) == 0 {
				payload := marshalJSON(map[string]any{
					"run_id":   spec.RunID,
					"step_key": s.StepKey,
				})
				_ = r.outbx.Enqueue(ctx, WorkflowOutboxEvent{
					AggregateID: spec.RunID,
					EventType:   "WORKFLOW_STEP_READY",
					Payload:     payload,
				})
				_ = i
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow create run commit: %w", err)
	}

	return r.GetRun(ctx, spec.RunID)
}

// ── GetRun / ListSteps ─────────────────────────────────────────────────────

func (r *SQLiteRepository) GetRun(ctx context.Context, runID string) (*Run, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT run_id, workflow_type, status, input_json, output_json,
		        revision, created_at, updated_at, started_at, completed_at,
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, '')
		 FROM workflow_runs WHERE run_id = ?`, runID)

	var (
		runIDOut, wType, status, inputJSON, outputJSON string
		createdAt, updatedAt                            string
		startedAt, completedAt                          sql.NullString
		errorCode, errorMessage                         string
		revision                                        int64
	)
	if err := row.Scan(&runIDOut, &wType, &status, &inputJSON, &outputJSON,
		&revision, &createdAt, &updatedAt, &startedAt, &completedAt,
		&errorCode, &errorMessage,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	run := &Run{
		RunID:           runIDOut,
		WorkflowType:    wType,
		Status:          RunStatus(status),
		Revision:        revision,
		Input:           decodeJSON(inputJSON),
		Output:          decodeJSON(outputJSON),
		LastErrorCode:   errorCode,
		LastErrorMessage: errorMessage,
	}
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		run.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
		run.UpdatedAt = t
	}
	if startedAt.Valid && startedAt.String != "" {
		if t, err := time.Parse(time.RFC3339, startedAt.String); err == nil {
			run.StartedAt = &t
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if t, err := time.Parse(time.RFC3339, completedAt.String); err == nil {
			run.CompletedAt = &t
		}
	}
	return run, nil
}

func (r *SQLiteRepository) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT step_id, run_id, step_key, COALESCE(job_id, ''), status, attempt, max_attempts,
		        input_json, output_json, revision, created_at, updated_at,
		        started_at, completed_at, COALESCE(error_code, ''), COALESCE(error_message, '')
		 FROM workflow_steps WHERE run_id = ? ORDER BY created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Step
	for rows.Next() {
		var (
			stepID, runIDOut, stepKey, jobIDStr, status string
			attempt, maxAttempts                        int
			inputJSON, outputJSON, createdAt, updatedAt string
			startedAt, completedAt                      sql.NullString
			errorCode, errorMessage                     string
			revision                                    int64
		)
		if err := rows.Scan(&stepID, &runIDOut, &stepKey, &jobIDStr, &status,
			&attempt, &maxAttempts,
			&inputJSON, &outputJSON, &revision, &createdAt, &updatedAt,
			&startedAt, &completedAt, &errorCode, &errorMessage,
		); err != nil {
			return nil, err
		}
		s := Step{
			StepID:       stepID,
			RunID:        runIDOut,
			StepKey:      stepKey,
			Status:       StepStatus(status),
			Attempt:      attempt,
			MaxAttempts:  maxAttempts,
			Input:        decodeJSON(inputJSON),
			Output:       decodeJSON(outputJSON),
			Revision:     revision,
			ErrorCode:    errorCode,
			ErrorMessage: errorMessage,
		}
		if jobIDStr != "" {
			s.JobID = &jobIDStr
		}
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			s.CreatedAt = t
		}
		if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
			s.UpdatedAt = t
		}
		if startedAt.Valid && startedAt.String != "" {
			if t, err := time.Parse(time.RFC3339, startedAt.String); err == nil {
				s.StartedAt = &t
			}
		}
		if completedAt.Valid && completedAt.String != "" {
			if t, err := time.Parse(time.RFC3339, completedAt.String); err == nil {
				s.CompletedAt = &t
			}
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

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
		if errors.Is(err, sql.ErrNoRows) {
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
						"run_id":         cmd.RunID,
						"error_code":     cmd.ErrorCode,
						"error_message":  cmd.ErrorMessage,
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
		if errors.Is(err, sql.ErrNoRows) {
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

// ── JSON helpers ──────────────────────────────────────────────────────────

func marshalJSON(v any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func decodeJSON(s string) map[string]any {
	var m map[string]any
	if s == "" {
		return map[string]any{}
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

func newWorkflowID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b[:]))
}

func toNull(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// Compile-time guarantee we satisfy the interface.
var _ Repository = (*SQLiteRepository)(nil)
