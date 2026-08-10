// Package store / jobs_repository_transitions.go
//
// Writer transitions on the shared baseJobRepository used by
// SQLiteJobRepository. Each transition is a CAS-guarded status flip
// (revision + current-status predicate) with dialect-abstracted audit
// hooks; the Dialect contract and the Reader (Get / List / Counts /
// getJob) live in jobs_repository_shared.go.

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/statemachine"
)

// ── jobs.Writer ─────────────────────────────────────────────────────────

func (b *baseJobRepository) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainJob, string(from), string(to), ""); err != nil {
		return fmt.Errorf("setstatus: %w", err)
	}
	sj, err := b.getJob(ctx, id)
	if err != nil {
		return fmt.Errorf("setstatus: get job %s: %w", id, err)
	}
	if sj == nil {
		// getJob returns (nil, nil) for a missing job. Without this
		// guard, the CAS at sj.Revision below dereferences nil and
		// crashes the request handler. Escalate as a typed error so
		// the API layer can map to 404 instead of a 500 panic.
		return fmt.Errorf("setstatus: job %s not found", id)
	}
	p := b.dialect
	now := nowStrISO()
	res, err := b.db.ExecContext(ctx,
		`UPDATE jobs
		   SET status = `+p.Placeholder(1)+`,
		       updated_at = `+p.Placeholder(2)+`,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = `+p.Placeholder(3)+`
		   AND `+p.CoalesceStatus()+` = `+p.Placeholder(4)+`
		   AND COALESCE(revision, 0) = `+p.Placeholder(5),
		string(to), now, id, string(from), sj.Revision)
	if err != nil {
		return fmt.Errorf("setstatus exec: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("setstatus %s: %w", id, p.ConflictError())
	}
	return nil
}

func (b *baseJobRepository) Fail(ctx context.Context, id, reason string) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Fail")
	}
	p := b.dialect

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fail begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotency check: reject terminal jobs.
	var current string
	row := tx.QueryRowContext(ctx,
		`SELECT `+p.CoalesceStatus()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	if err := row.Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fail %s: not found", id)
		}
		return fmt.Errorf("fail status: %w", err)
	}
	switch current {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return fmt.Errorf("fail: job %s is already terminal (%s)", id, current)
	}

	now := nowStrISO()
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'FAILED',
		       updated_at = `+p.Placeholder(1)+`,
		       revision = COALESCE(revision, 0) + 1,
		       error_message = `+p.Placeholder(2)+`,
		       failed_at = `+p.Placeholder(3)+`,
		       failed_by = ''
		 WHERE job_id = `+p.Placeholder(4)+`
		   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		now, reason, now, id)
	if err != nil {
		return fmt.Errorf("fail update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("fail %s: %w", id, p.ConflictError())
	}

	_ = p.InsertHistoryTx(ctx, tx, id, "FAILED", "" /* workerID */, "Job failed: "+reason)
	_ = p.InsertEventTx(ctx, tx, id, "job_failed", map[string]interface{}{
		"error": reason,
	})

	payload, _ := json.Marshal(map[string]interface{}{
		"job_id":     id,
		"error_code": "JOB_FAILED_GENERIC",
		"error":      reason,
	})
	// PR-EMITOUTBOX-HARDENING: outbox-not-wired ⇒ EmitOutboxTx returns
	// error; the transaction must rollback so the FAILED status flip
	// does NOT persist (callers see the error and can re-attempt after
	// wiring outbox).
	if outboxErr := p.EmitOutboxTx(ctx, tx, "job", id, "JOB_FAILED", payload); outboxErr != nil {
		return fmt.Errorf("fail %s: %w", id, outboxErr)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fail commit: %w", err)
	}
	return nil
}

// FailWithCode is the explicit-code variant of Fail. Callers that
// know the specific failure reason (e.g. "OUTBOX_NOT_WIRED",
// "TERMINAL_ALREADY", "EXTERNAL_DEPENDENCY_TIMEOUT") pass a stable
// error_code here so downstream consumers (outbox JOB_FAILED handler,
// alert/notify surface) can route on the code instead of regex-ing
// the human-readable reason string.
//
// The PR-OUTBOX-HANDLER wiring decodes the resulting payload as
// {job_id, error_code, error}; see internal/outbox/production.go
// for the canonical receiver.
//
// Status transitions: same as Fail (terminal-guard + revision CAS).
func (b *baseJobRepository) FailWithCode(ctx context.Context, id, errorCode, reason string) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in FailWithCode")
	}
	if errorCode == "" {
		errorCode = "JOB_FAILED_GENERIC"
	}
	p := b.dialect

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failwithcode begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var current string
	row := tx.QueryRowContext(ctx,
		`SELECT `+p.CoalesceStatus()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	if err := row.Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("failwithcode %s: not found", id)
		}
		return fmt.Errorf("failwithcode status: %w", err)
	}
	switch current {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return fmt.Errorf("failwithcode: job %s is already terminal (%s)", id, current)
	}

	now := nowStrISO()
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'FAILED',
		       updated_at = `+p.Placeholder(1)+`,
		       revision = COALESCE(revision, 0) + 1,
		       error_message = `+p.Placeholder(2)+`,
		       failed_at = `+p.Placeholder(3)+`,
		       failed_by = ''
		 WHERE job_id = `+p.Placeholder(4)+`
		   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		now, reason, now, id)
	if err != nil {
		return fmt.Errorf("failwithcode update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("failwithcode %s: %w", id, p.ConflictError())
	}

	_ = p.InsertHistoryTx(ctx, tx, id, "FAILED", "" /* workerID */, "Job failed ["+errorCode+"]: "+reason)
	_ = p.InsertEventTx(ctx, tx, id, "job_failed", map[string]interface{}{
		"error_code": errorCode,
		"error":      reason,
	})

	payload, _ := json.Marshal(map[string]interface{}{
		"job_id":     id,
		"error_code": errorCode,
		"error":      reason,
	})
	if outboxErr := p.EmitOutboxTx(ctx, tx, "job", id, "JOB_FAILED", payload); outboxErr != nil {
		return fmt.Errorf("failwithcode %s: %w", id, outboxErr)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failwithcode commit: %w", err)
	}
	return nil
}

func (b *baseJobRepository) Cancel(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Cancel")
	}
	p := b.dialect
	now := nowStrISO()

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cancel begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotency check.
	var current string
	row := tx.QueryRowContext(ctx,
		`SELECT `+p.CoalesceStatus()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	if err := row.Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("cancel %s: not found", id)
		}
		return fmt.Errorf("cancel status: %w", err)
	}
	switch current {
	case "CANCELLED":
		return tx.Commit()
	case "SUCCEEDED", "FAILED":
		return fmt.Errorf("cancel %s: cannot cancel terminal job (%s)", id, current)
	}

	var res sql.Result
	if revision >= 0 {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = `+p.Placeholder(1)+`,
			       revision = COALESCE(revision, 0) + 1,
			       claimed_at = '',
			       assigned_at = ''
			 WHERE job_id = `+p.Placeholder(2)+`
			   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
			   AND COALESCE(revision, 0) = `+p.Placeholder(3),
			now, id, revision)
	} else {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = `+p.Placeholder(1)+`,
			       revision = COALESCE(revision, 0) + 1,
			       claimed_at = '',
			       assigned_at = ''
			 WHERE job_id = `+p.Placeholder(2)+`
			   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			now, id)
	}
	if err != nil {
		return fmt.Errorf("cancel update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("cancel %s: %w", id, p.ConflictError())
	}

	_ = p.InsertHistoryTx(ctx, tx, id, "CANCELLED", "" /* workerID */, "Cancelled: "+reason)
	_ = p.InsertEventTx(ctx, tx, id, "job_cancelled", map[string]interface{}{"reason": reason})
	// Stop local deliveries that have not created a remote object yet. A
	// delivery with a remote_id is deliberately left in place: cancelling
	// Velox cannot delete an object already created by InstaEdit, so the
	// runner keeps the row for reconciliation and records the request.
	if _, err := tx.ExecContext(ctx, `
		UPDATE job_deliveries
		SET status = 'CANCELLED', updated_at = `+p.Placeholder(1)+`, completed_at = `+p.Placeholder(2)+`
		WHERE artifact_id IN (SELECT id FROM artifacts WHERE job_id = `+p.Placeholder(3)+`)
		  AND COALESCE(remote_id, '') = ''
		  AND status IN ('PENDING', 'CLAIMED', 'RETRY_WAIT', 'RUNNING')`, now, now, id); err != nil {
		return fmt.Errorf("cancel deliveries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE job_deliveries
		SET last_error_code = 'CANCEL_REQUESTED', updated_at = `+p.Placeholder(1)+`
		WHERE artifact_id IN (SELECT id FROM artifacts WHERE job_id = `+p.Placeholder(2)+`)
		  AND COALESCE(remote_id, '') <> ''
		  AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`, now, id); err != nil {
		return fmt.Errorf("mark remote deliveries cancel requested: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cancel commit: %w", err)
	}
	return nil
}

func (b *baseJobRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Delete")
	}
	p := b.dialect
	if _, err := b.db.ExecContext(ctx, `DELETE FROM jobs WHERE job_id = `+p.Placeholder(1), id); err != nil {
		return fmt.Errorf("delete %s: %w", id, err)
	}
	return nil
}
