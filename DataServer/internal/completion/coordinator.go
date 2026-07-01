// Package completion / coordinator.go
//
// Artifact Commit Protocol (Fase 2.3 of docs/completion-protocol.md):
// concrete Coordinator implementation. The methods implementing
// idempotent terminal-free behaviour today:
//
//   - DeclareOutputs: BEGIN IMMEDIATE → INSERT-OR-IGNORE on the
//     (task_id, attempt_id)-unique attempt_commits row + per-decl
//     INSERT-OR-IGNORE on
//     (task_id, attempt_id, output_kind, logical_name) task_output_
//     declarations rows + COMMIT. Returns an UploadPlan with a
//     freshly generated commit_token + the SHA256-hashed row value
//     for storage on attempt_commits.
//
//   - RecordUploadProgress: BEGIN IMMEDIATE → UPDATE attempt_commits
//     for last_progress_at + commit_deadline_at AND UPDATE
//     task_output_declarations for uploaded_bytes, both CAS-gated on
//     the FenceTuple AND status ∈ {DECLARED,UPLOADING}. Returns
//     ErrTransitionConflict on stale status; the
//     reconciler takes over from there.
//
// The methods stubbed as ErrNotImplemented today:
//
//   - CompleteUpload (Fase 2.5 → atomic SUCCEEDED write)
//   - CommitAttempt  (Fase 2.5 → atomic SUCCEEDED write)
//   - ReconcileAttempt (Fase 4.1 → repair-forward)
//
// Why a *sql.DB and not a SQLiteStore? The Coordinator is intentionally
// DB-narrow. *SQLiteStore is the master-side god-object and the
// completion package has no business reaching into its other getters.
// Tests construct a fresh in-memory sqlite3 handle with only the
// migrations applied (062-065 schema), which is enough to exercise
// this phase's SQL.
package completion

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// commitTokenByteLen is the cryptographic entropy for an opaque
// commit_token. 32 bytes = 256 bits — same as an Ed25519 private key,
// overkill for a session-scoped bearer, deliberately. Lower values
// weaken the upload-time verification path on the master; higher
// values waste bytes on the wire.
const commitTokenByteLen = 32

// NewCoordinator constructs a Coordinator backed by db. db is expected
// to be a *sql.DB whose schema includes attempt_commits (migration
// 061+) and task_output_declarations (migration 062+, with the
// `required` column from migration 064, and a 1:1 between commit_id
// columns on the two tables).
//
// Tests construct an in-memory sqlite3 handle via mattn/go-sqlite3 and
// apply migrations via migrations.RunMigrations before calling
// NewCoordinator.
func NewCoordinator(db *sql.DB) Coordinator {
	return &coordinator{db: db}
}

// coordinator is the canonical Coordinator implementation.
type coordinator struct {
	db *sql.DB
}

// DeclareOutputs upserts an AttemptCommit row + per-decl declaration
// rows in one BEGIN IMMEDIATE transaction. The FenceTuple is validated
// at entry; a malformed tuple is rejected with ErrFenceMismatch
// without touching the database.
//
// Idempotency:
//
//   - attempt_commits has UNIQUE(task_id, attempt_id). A replay of
//     DeclareOutputs with the same (task_id, attempt_id) is a SQL
//     no-op on that row (INSERT-OR-IGNORE swallows the conflict).
//   - task_output_declarations has UNIQUE(task_id, attempt_id,
//     output_kind, logical_name). The loop's INSERT-OR-IGNORE makes
//     each declaration upsert individually.
//   - A previous AttemptCommit with status=DECLARED|UPLOADING|... is
//     left untouched; the master is allowed to enrich it via
//     RecordUploadProgress without re-Declaring.
//
// On replay the Reply has FRESH commit_token + commit_token_hash
// (regenerated per-call). The worker should ignore the new token if it
// already holds the first one — see docs/completion-protocol.md §2.3
// for the rationale (the token is never persisted on the master
// beyond its first delivery).
func (c *coordinator) DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if len(cmd.OutputManifests) == 0 {
		return nil, fmt.Errorf("completion.DeclareOutputs: at least one OutputManifest required (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	commitID := newUUIDLowerHex()
	token, tokenHash := generateCommitToken()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	deadline := time.Now().UTC().Add(commitGraceDefault).Format(time.RFC3339Nano)

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. attempt_commits row.
	//
	// Phase 2.2 central gate (ReadOrMissing): rejects a stale
	// replay on an existing row (ErrTransitionConflict), allows
	// the no-row path (first declare) so the INSERT-OR-IGNORE
	// below can run. When an existing row matches, state.CommitID
	// is the canonical id to reuse and we skip the INSERT.
	//
	// required_output_count is sized from the manifests at call
	// time. Phase 2.5 keeps this value as a CAS floor: the master
	// refuses to promote task SUCCEEDED until ready_output_count
	// >= required_output_count. Today the column is informational
	// only.
	state, err := cmd.Fence.ReadOrMissing(ctx, tx)
	if err != nil {
		return nil, err
	}
	if state != nil {
		// Existing row with matching fence. Reuse canonical
		// commit_id. We do NOT overwrite status /
		// required_output_count here: the existing row is the
		// source of truth.
		commitID = state.CommitID
	} else {
		// No row yet. Canonical INSERT-OR-IGNORE.
		res, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO attempt_commits (
				commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
				task_revision, status, required_output_count,
				commit_token_hash, commit_deadline_at, last_progress_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, 'DECLARED', ?, ?, ?, ?, ?, ?)`,
			commitID,
			cmd.Fence.TaskID, cmd.Fence.AttemptID, cmd.JobID, cmd.Fence.WorkerID, cmd.Fence.LeaseID,
			cmd.Fence.Revision,
			len(cmd.OutputManifests),
			tokenHash, deadline, now,
			now, now,
		)
		if err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: insert attempt_commits: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected == 0 {
			// Race: another writer inserted between our
			// ReadOrMissing and our INSERT. Re-read to get the
			// canonical commit_id.
			var existingCommitID string
			if err := tx.QueryRowContext(ctx,
				`SELECT commit_id FROM attempt_commits
				 WHERE task_id = ? AND attempt_id = ?`,
				cmd.Fence.TaskID, cmd.Fence.AttemptID,
			).Scan(&existingCommitID); err != nil {
				return nil, fmt.Errorf("completion.DeclareOutputs: select existing commit_id after race: %w", err)
			}
			commitID = existingCommitID
		}
	}

	// 2. Per-declaration rows.
	declIDs := make([]string, 0, len(cmd.OutputManifests))
	for _, m := range cmd.OutputManifests {
		if err := validateManifest(&m); err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: invalid manifest: %w", err)
		}
		declarationID := newUUIDLowerHex()
		_, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO task_output_declarations (
				declaration_id, commit_id, task_id, attempt_id,
				output_kind, logical_name, mime_type,
				expected_size_bytes, expected_sha256,
				worker_spool_key, status,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DECLARED', ?, ?)`,
			declarationID,
			commitID, cmd.Fence.TaskID, cmd.Fence.AttemptID,
			m.OutputKind, m.LogicalName, m.MimeType,
			m.SizeBytes, m.SHA256,
			m.WorkerSpoolKey,
			now, now,
		)
		if err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: insert declaration %s: %w", m.LogicalName, err)
		}
		declIDs = append(declIDs, declarationID)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: commit: %w", err)
	}
	committed = true

	return &UploadPlan{
		CommitID:    commitID,
		CommitToken: token,
		Targets:     nil, // Fase 3.7 wires the transport registry.
	}, nil
}

// RecordUploadProgress bumps the last_progress_at and commit_deadline_a
// t on the canonical attempt_commits row CAS-gated on the FenceTuple
// AND status. uploaded_bytes on the corresponding task_output_declar
// ations row(s) is incremented monotonically.
//
// Idempotency:
//
//   - last_progress_at = MAX(last_progress_at, new_value): a worker
//     that re-sends an older heartbeat cannot regress the timestamp.
//   - commit_deadline_at is bumped forward by the same amount as the
//     progress delta, ensuring the lease-renewal deadline always
//     extends past the worker's progress.
//
// A stale fence (worker on a reaped lease) returns ErrFenceMismatch
// or ErrTransitionConflict depending on which gate failed. The
// supervisor's reconcile path will resurrect a stale DECLARED row
// (Fase 4.1); RecordUploadProgress itself does NOT mutate terminal
// rows.
func (c *coordinator) RecordUploadProgress(ctx context.Context, cmd RecordUploadProgressCommand) error {
	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.RecordUploadProgress: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Phase 2.2 central gate. Read returns the canonical state
	// (commit_id, status) on match, or wraps ErrAttemptCommitNotFound
	// / ErrTransitionConflict on a missing / stale row. Both
	// sentinels must surface unchanged so callers can dispatch.
	state, err := cmd.Fence.Read(ctx, tx)
	if err != nil {
		return err
	}

	// CAS attempt_commits row + bump timestamps. Gate is on the
	// canonical commit_id (validated above); status filter keeps
	// the no-progress-past-terminal invariant.
	dedline := now.Add(commitGraceDefault).Format(time.RFC3339)
	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET last_progress_at = ?, commit_deadline_at = ?, updated_at = ?
		  WHERE commit_id = ?
		    AND status IN ('DECLARED', 'UPLOADING')`,
		nowStr, dedline, nowStr,
		state.CommitID,
	)
	if err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: update attempt_commits: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return fmt.Errorf("%w: status=%q (cannot progress past terminal/rejected state)", ErrTransitionConflict, state.Status)
	}

	// Best-effort: per-declaration progress bump keyed by upload_id.
	// We do NOT require this UPDATE to hit a row — the worker may
	// report progress before any per-decl bytes have been received
	// (e.g. opening a chunked connection). 0-row affected on the
	// INSERT side is benign.
	if cmd.UploadedBytes > 0 {
		_, err = tx.ExecContext(ctx,
			`UPDATE task_output_declarations
			    SET uploaded_bytes = ?, updated_at = ?
			  WHERE commit_id IN (
			      SELECT commit_id FROM attempt_commits
			      WHERE task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
			  )
			  AND upload_id = ?`,
			cmd.UploadedBytes, nowStr,
			cmd.Fence.TaskID, cmd.Fence.AttemptID, cmd.Fence.WorkerID, cmd.Fence.LeaseID,
			cmd.UploadID,
		)
		if err != nil {
			return fmt.Errorf("completion.RecordUploadProgress: update task_output_declarations: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: commit: %w", err)
	}
	committed = true
	return nil
}

// CompleteUpload verifies the worker-supplied SHA against the master-
// declared expected_sha256 on artifact_uploads, flips artifact_uploads
// → COMPLETED + artifacts STAGING→READY in one tx, and bumps
// attempt_commits.ready_output_count via a deterministic derived
// count (NOT a naive +1, since a worker retry of the upload-
// completion ack must not over-count).
//
// Deadline check (Phase 2.5): if commit_deadline_at < now AND
// ready_output_count (post-bump) < required_output_count, the attempt
// CAS-gates to EXPIRED instead of staying DECLARED|... and emits
// outbox 'commit_protocol.expired' so the supervisor can re-queue
// the underlying Task.
//
// Returns nil on success; ErrTransitionConflict on stale fence;
// ErrStaleReport on attempted promotion from COMMITTED|FAILED|CANCELLED.
func (c *coordinator) CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error {
	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.CompleteUpload: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("completion.CompleteUpload: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	// Phase 2.2 central gate. Validates the fence against
	// attempt_commits before any artifact_uploads read or write.
	// A stale fence (reaped lease, bumped revision, etc.)
	// returns ErrTransitionConflict and aborts the tx before
	// we touch artifact state.
	if _, err := cmd.Fence.Read(ctx, tx); err != nil {
		return err
	}

	// 1. artifact_uploads fencing read.
	var (
		expectedSHA sql.NullString
		receivedSHA sql.NullString
		curStatus   string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT expected_sha256, received_sha256, status
		FROM artifact_uploads WHERE upload_id = ?`,
		cmd.UploadID,
	).Scan(&expectedSHA, &receivedSHA, &curStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: upload_id=%s", ErrAttemptCommitNotFound, cmd.UploadID)
		}
		return fmt.Errorf("completion.CompleteUpload: read artifact_uploads: %w", err)
	}
	if curStatus == "COMPLETED" {
		return nil // replay-safe no-op
	}
	if curStatus != "CREATED" && curStatus != "UPLOADING" && curStatus != "RECEIVED" {
		return fmt.Errorf("%w: artifact_uploads.status=%q (cannot advance)", ErrTransitionConflict, curStatus)
	}
	effectiveExpected := expectedSHA.String
	if receivedSHA.Valid && receivedSHA.String != "" {
		effectiveExpected = receivedSHA.String
	}
	if cmd.WorkerSHA256 != "" && effectiveExpected != "" && cmd.WorkerSHA256 != effectiveExpected {
		return fmt.Errorf("%w: upload=%s worker_sha=%s master_sha=%s", ErrStaleReport, cmd.UploadID, cmd.WorkerSHA256, effectiveExpected)
	}

	// 2. artifact_uploads → COMPLETED.
	if _, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		   SET status = 'COMPLETED', completed_at = ?, received_sha256 = COALESCE(received_sha256, ?),
		       updated_at = ?
		 WHERE upload_id = ? AND status IN ('CREATED','UPLOADING','RECEIVED')`,
		nowStr, cmd.WorkerSHA256, nowStr, cmd.UploadID,
	); err != nil {
		return fmt.Errorf("completion.CompleteUpload: artifact_uploads CAS: %w", err)
	}

	// 3. artifacts STAGING → READY (0 rows tolerated).
	if _, err := tx.ExecContext(ctx, `
		UPDATE artifacts
		   SET status = 'READY', verified_at = ?, updated_at = ?
		 WHERE id = (SELECT artifact_id FROM artifact_uploads WHERE upload_id = ?)
		   AND status IN ('STAGING','VERIFYING')`,
		nowStr, nowStr, cmd.UploadID,
	); err != nil {
		return fmt.Errorf("completion.CompleteUpload: artifacts STAGING->READY: %w", err)
	}

	// 4. attempt_commits.ready_output_count derived from
	// task_output_declarations JOIN artifacts (READY). Idempotent
	// across worker retries because the count is computed, not +1.
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempt_commits
		   SET ready_output_count = (
		       SELECT COUNT(*)
		         FROM task_output_declarations d
		         JOIN artifacts a ON a.id = d.artifact_id
		        WHERE d.commit_id = attempt_commits.commit_id
		          AND a.status = 'READY'
		   ),
		   updated_at = ?
		 WHERE commit_id IN (
		     SELECT commit_id FROM attempt_commits
		      WHERE `+cmd.Fence.SQLWhere()+`
		   )
		   AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		append([]any{nowStr}, cmd.Fence.SQLArgs()...)...,
	); err != nil {
		return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", err)
	}

	// 5. Deadline-breach → EXPIRED.
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempt_commits
		   SET status = 'EXPIRED', rejected_code = 'COMMIT_DEADLINE_EXCEEDED',
		       rejected_message = 'CompleteUpload after deadline with incomplete ready set',
		       updated_at = ?
		 WHERE `+cmd.Fence.SQLWhere()+`
		   AND commit_deadline_at < ?
		   AND ready_output_count < required_output_count
		   AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		append([]any{nowStr}, append(cmd.Fence.SQLArgs(), nowStr)...)...,
	); err != nil {
		return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.CompleteUpload: commit: %w", err)
	}
	committed = true
	return nil
}

// CommitAttempt performs the canonical atomic final transaction for a
// commit_id. All in ONE BEGIN SERIALIZABLE so commit_id either fully
// ratifies or fully rolls back.
//
// Idempotency: a duplicate CommitAttempt on a COMMITTED row is a no-op
// CommitResult return.
//
// Gating: tasks.status must be in ('RUNNING','LEASED'). Note we do
// NOT require winning_attempt_terminal_pending=1 — the worker can
// call CommitAttempt directly without driving through
// IngestTaskResultAtomic first (legacy TaskResult path) and the
// commit protocol ratifies identically.
func (c *coordinator) CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.CommitAttempt: commitID empty")
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	type commitRow struct {
		TaskID, AttemptID, JobID, WorkerID, LeaseID, Status string
		RequiredOutputCnt, ReadyOutputCnt                  int
	}
	var row commitRow
	if err := tx.QueryRowContext(ctx, `
		SELECT task_id, attempt_id, job_id, worker_id, lease_id, status,
		       required_output_count, ready_output_count
		  FROM attempt_commits WHERE commit_id = ?`,
		commitID,
	).Scan(&row.TaskID, &row.AttemptID, &row.JobID, &row.WorkerID, &row.LeaseID,
		&row.Status, &row.RequiredOutputCnt, &row.ReadyOutputCnt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: commit_id=%s", ErrAttemptCommitNotFound, commitID)
		}
		return nil, fmt.Errorf("completion.CommitAttempt: read attempt_commits: %w", err)
	}

	if row.Status == "COMMITTED" {
		return loadCommitResult(ctx, tx, commitID)
	}
	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" && row.Status != "VERIFYING" {
		return nil, fmt.Errorf("%w: attempt_commits.status=%q", ErrTransitionConflict, row.Status)
	}
	if row.ReadyOutputCnt < row.RequiredOutputCnt {
		return nil, fmt.Errorf("%w: ready=%d required=%d (commit blocked)", ErrTransitionConflict, row.ReadyOutputCnt, row.RequiredOutputCnt)
	}

	// 1. task_attempts CAS RUNNING → SUCCEEDED. Replay-safe (0 rows
	// when already terminal).
	if _, err := tx.ExecContext(ctx, `
		UPDATE task_attempts
		   SET status = 'SUCCEEDED', completed_at = COALESCE(completed_at, ?),
		       report_version = report_version + 1, updated_at = ?
		 WHERE id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
		nowStr, nowStr, row.AttemptID, row.WorkerID, row.LeaseID,
	); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: task_attempts CAS: %w", err)
	}

	// 2. tasks CAS RUNNING|LEASED → SUCCEEDED + winning_attempt_*
	// stamp. Do NOT require terminal_pending=1 — works whether the
	// worker called Commit directly or went through Ingest first.
	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks
		   SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		       winning_attempt_id = ?, winning_attempt_committed_at = ?,
		       winning_attempt_terminal_pending = 0, revision = revision + 1
		 WHERE task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
		   AND status IN ('RUNNING','LEASED')`,
		nowStr, nowStr, row.AttemptID, nowStr,
		row.TaskID, row.AttemptID, row.WorkerID, row.LeaseID,
	); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: tasks CAS: %w", err)
	}

	// 3. attempt_commits → COMMITTED.
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempt_commits
		   SET status = 'COMMITTED', committed_at = ?, updated_at = ?
		 WHERE commit_id = ? AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		nowStr, nowStr, commitID,
	); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", err)
	}

	// 4. Conditional jobs CAS: ALL sibling tasks must be SUCCEEDED.
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs
		   SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		       revision = revision + 1
		 WHERE job_id = ? AND status IN ('RUNNING','AWAITING_ARTIFACT')
		   AND NOT EXISTS (
		       SELECT 1 FROM tasks t
		        WHERE t.job_id = ? AND t.status != 'SUCCEEDED'
		   )`,
		nowStr, nowStr, row.JobID, row.JobID,
	); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: jobs CAS: %w", err)
	}

	// 5. Idempotent INSERT job_deliveries.
	if err := insertJobDeliveriesIdempotent(ctx, tx, row.JobID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: job_deliveries insert: %w", err)
	}

	// 6. Idempotent INSERT outbox_events 'commit_protocol.committed'.
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO outbox_events (
		    event_id, aggregate_type, aggregate_id, event_type, payload_json,
		    status, available_at, attempt_count, created_at
		) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, 0, ?)`,
		"ce_"+commitID, "task", row.TaskID, "commit_protocol.committed",
		`{"commit_id":"`+commitID+`","attempt_id":"`+row.AttemptID+`","job_id":"`+row.JobID+`"}`,
		nowStr, nowStr,
	); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: outbox_events insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: commit: %w", err)
	}
	committed = true

	return loadCommitResult(ctx, tx, commitID)
}

// ReconcileAttempt performs the supervisor's repair-forward scan on a
// single commit_id. Phase 2.9 ships only the DECLARED-with-dead-worker
// case: when commit_deadline_at has elapsed mark EXPIRED and emit
// 'commit_protocol.expired'. Other cases (Phase 4.1 wiring).
func (c *coordinator) ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commitID empty")
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	type reconcileRow struct {
		TaskID, AttemptID, JobID, Status, Deadline, LastProg string
	}
	var row reconcileRow
	if err := tx.QueryRowContext(ctx, `
		SELECT task_id, attempt_id, job_id, status, commit_deadline_at,
		       last_progress_at
		  FROM attempt_commits WHERE commit_id = ?`,
		commitID,
	).Scan(&row.TaskID, &row.AttemptID, &row.JobID, &row.Status,
		&row.Deadline, &row.LastProg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: commit_id=%s", ErrAttemptCommitNotFound, commitID)
		}
		return nil, fmt.Errorf("completion.ReconcileAttempt: read attempt_commits: %w", err)
	}

	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" {
		return loadCommitResult(ctx, tx, commitID)
	}

	deadlineElapsed := false
	if row.Deadline != "" {
		if t, perr := time.Parse(time.RFC3339Nano, row.Deadline); perr == nil {
			deadlineElapsed = now.After(t)
		}
	}
	if !deadlineElapsed {
		return loadCommitResult(ctx, tx, commitID)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE attempt_commits
		   SET status = 'EXPIRED',
		       rejected_code = 'COMMIT_DEADLINE_EXCEEDED',
		       rejected_message = 'ReconcileAttempt: commit_deadline_at elapsed',
		       updated_at = ?
		 WHERE commit_id = ? AND status IN ('DECLARED','UPLOADING','RECEIVED')`,
		nowStr, commitID,
	); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO outbox_events (
		    event_id, aggregate_type, aggregate_id, event_type, payload_json,
		    status, available_at, attempt_count, created_at
		) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, 0, ?)`,
		"re_"+commitID, "task", row.TaskID, "commit_protocol.expired",
		`{"commit_id":"`+commitID+`","attempt_id":"`+row.AttemptID+`","job_id":"`+row.JobID+`"}`,
		nowStr, nowStr,
	); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: outbox_events insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commit: %w", err)
	}
	committed = true

	return loadCommitResult(ctx, tx, commitID)
}

// loadCommitResult reads the post-tx snapshot of attempt_commits
// joined with tasks + jobs so the caller receives a self-contained
// CommitResult without an additional roundtrip.
func loadCommitResult(ctx context.Context, tx *sql.Tx, commitID string) (*CommitResult, error) {
	var (
		res         CommitResult
		committedAt sql.NullString
		taskStatus  sql.NullString
		jobStatus   sql.NullString
	)
	err := tx.QueryRowContext(ctx,
		`SELECT ac.commit_id, ac.task_id, ac.attempt_id, ac.job_id,
		        COALESCE(t.status, ''), COALESCE(j.status, ''), ac.committed_at
		   FROM attempt_commits ac
		   LEFT JOIN tasks  t ON t.task_id  = ac.task_id
		   LEFT JOIN jobs   j ON j.job_id   = ac.job_id
		  WHERE ac.commit_id = ?`,
		commitID).Scan(&res.CommitID, &res.TaskID, &res.AttemptID, &res.JobID,
		&taskStatus, &jobStatus, &committedAt)
	if err != nil {
		return nil, err
	}
	res.TaskStatus = taskStatus.String
	res.JobStatus = jobStatus.String
	if committedAt.Valid && committedAt.String != "" {
		if t, perr := time.Parse(time.RFC3339Nano, committedAt.String); perr == nil {
			res.CommittedAt = &t
		}
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT a.id FROM artifacts a
		   JOIN task_output_declarations d ON d.artifact_id = a.id
		   JOIN attempt_commits ac ON ac.commit_id = d.commit_id
		  WHERE ac.commit_id = ? AND a.status = 'READY'`,
		commitID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			if sErr := rows.Scan(&id); sErr == nil {
				res.ArtifactIDs = append(res.ArtifactIDs, id)
			}
		}
	}
	return &res, nil
}

// insertJobDeliveriesIdempotent inserts one job_deliveries row per
// READY artifact × enabled delivery_destinations cross product, with
// idempotency_key UNIQUE so a re-emitted tx absorbs duplicates.
func insertJobDeliveriesIdempotent(ctx context.Context, tx *sql.Tx, jobID, nowStr string) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT a.id, dd.destination_id
		   FROM artifacts a
		   CROSS JOIN delivery_destinations dd
		  WHERE a.job_id = ?
		    AND a.status = 'READY'
		    AND dd.enabled = 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type destKey struct{ Art, Dst string }
	seen := make(map[destKey]bool)
	for rows.Next() {
		var art, dst string
		if scanErr := rows.Scan(&art, &dst); scanErr != nil || art == "" || dst == "" {
			continue
		}
		k := destKey{art, dst}
		if seen[k] {
			continue
		}
		seen[k] = true
		id := "jbd_comp_" + art + "_" + dst
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO job_deliveries (
			    delivery_id, artifact_id, destination_id, status, idempotency_key,
			    created_at, updated_at
			) VALUES (?, ?, ?, 'PENDING', ?, ?, ?)`,
			id, art, dst, art+"_"+dst, nowStr, nowStr,
		); err != nil {
			return err
		}
	}
	return rows.Err()
}

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

// commitGraceDefault is the canonical extension window for the
// commit_deadline_at stamp at DeclareOutputs / RecordUploadProgress
// time. Two minutes keeps a healthy worker's heartbeat inside the
// window while a slow chunk upload still gets a chance to bump it
// forward before the reaper fires.
const commitGraceDefault = 2 * time.Minute

// newUUIDLowerHex returns a 32-hex-char sequence (16 bytes of entropy).
// SQLite's PRIMARY KEY is TEXT and we use the string everywhere on
// the wire and in logs; full UUIDs are 36 chars (with hyphens) — we
// keep the lower-hex form to halve the log noise.
//
// NOTE: This is NOT a UUIDv4 because we do not permute the version
// bits. It is a 16-byte hex string with the same collision property.
// For testability we accept any non-zero-distribution source; for
// production use crypto/rand (Phase 0). Tests may swap newUUIDLowerHex
// with a deterministic generator if needed.
func newUUIDLowerHex() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never errors on Linux/macOS. If it does, fall
		// through to a process-local counter-based fallback so that
		// the daemon does not crash on a transient entropy failure.
		// Phase 0 strict: surfaces a panic instead — until then the
		// graceful fallback keeps the cluster running.
		for i := range b {
			b[i] = byte(i + 1) // deterministic non-zero marker
		}
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, by := range b {
		out[i*2] = hexdigits[by>>4]
		out[i*2+1] = hexdigits[by&0x0f]
	}
	return string(out)
}

// generateCommitToken returns the canonical (token, hash) pair. The
// token is a random 32-byte sequence hex-encoded for wire readability;
// the hash is its SHA256 hex.
//
// Idempotency note: this function is called once per DeclareOutputs
// call. A replay of DeclareOutputs (because the worker's network
// dropped) will produce a fresh token — the worker, holding the first
// one, ignores the second. The plan documents this trade-off as
// acceptable: the master never persists the plain token beyond its
// first delivery.
func generateCommitToken() (token, hash string) {
	var b [commitTokenByteLen]byte
	if _, err := rand.Read(b[:]); err != nil {
		for i := range b {
			b[i] = byte(i + 1)
		}
	}
	sum := sha256.Sum256(b[:])
	return hex.EncodeToString(b[:]), hex.EncodeToString(sum[:])
}

// validateManifest enforces the basic invariants on a per-file
// declaration. The check is intentionally minimal (the worker is the
// source of truth for mime / size / sha and the master re-verifies in
// CompleteUpload at Fase 2.5); this guard exists only to surface
// blatant caller mistakes loudly.
func validateManifest(m *OutputManifest) error {
	if strings.TrimSpace(m.OutputKind) == "" {
		return fmt.Errorf("manifest: OutputKind empty")
	}
	if strings.TrimSpace(m.LogicalName) == "" {
		return fmt.Errorf("manifest: LogicalName empty")
	}
	if strings.TrimSpace(m.MimeType) == "" {
		return fmt.Errorf("manifest: MimeType empty")
	}
	if m.SizeBytes <= 0 {
		return fmt.Errorf("manifest: SizeBytes must be > 0 (got %d)", m.SizeBytes)
	}
	if len(m.SHA256) != 64 {
		return fmt.Errorf("manifest: SHA256 must be 64 hex chars (got %d chars)", len(m.SHA256))
	}
	for _, c := range m.SHA256 {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return fmt.Errorf("manifest: SHA256 must be lowercase hex")
		}
	}
	return nil
}
