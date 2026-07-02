// Package completion / coordinator.go
//
// Artifact Commit Protocol (Fase 2.3 of docs/completion-protocol.md):
// concrete Coordinator implementation.
//
// The Coordinator owns the *sql.Tx lifecycle (open, commit, defer
// rollback) per method call. Every per-table write is delegated to a
// typed repository on the UnitOfWork produced by the
// UnitOfWorkFactory — CompleteUpload, CommitAttempt, and
// ReconcileAttempt now contain ZERO raw SQL against attempt_commits,
// task_attempts, tasks, jobs, job_deliveries, outbox_events,
// artifact_uploads, or artifacts (Verdetto P1 #8 / #9, Blocco 3).
//
// DeclareOutputs and RecordUploadProgress stay as raw SQL because
// they own the HMAC + INSERT-OR-IGNORE dance tightly coupled to
// the FenceTuple.Read gate; folding them into the UoW would re-
// introduce HMAC-key plumbing inside the repos that does not
// belong there. Future PRs can fold them in if needed.
//
// Tx lifecycle stays in the Coordinator layer (one LevelSerializable
// tx per Coordinator method call). The repos do NOT start or
// commit transactions.
//
// CommitResult snapshot is read BEFORE tx.Commit() via
// AttemptCommitRepository.GetCommitResult so the snapshot is part
// of the same write lock — fixes the previous tx-after-commit bug
// where the snapshot was read from a closed tx and failed on
// SUBSEQUENT regenerations of the CommitResult contract.
//
// The FenceTuple.Read / ReadOrMissing central gate still lives in
// fencing.go and operates on the same tx the Coordinator opens;
// it is the canonical doubling identity across CAS predicates.
package completion

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

// CoordinatorConfig groups the inputs the Coordinator needs at
// construction time. The HMACKey is the master-side secret used as
// the HMAC-SHA256 key for the deterministic commit-token derivation
// in DeclareOutputs (P0 #6, Verdetto Blocco 2); it MUST be at least
// 32 raw bytes so HMAC-SHA256 operates with its nominal entropy.
//
// DB is the *sql.DB the Coordinator opens per-method transactions
// on. The Coordinator builds a SQLite-backed UnitOfWorkFactory
// internally from this DB (calling NewSQLiteUnitOfWorkFactory), so
// callers do not need to wire the factory themselves.
type CoordinatorConfig struct {
	DB      *sql.DB
	HMACKey []byte
}

// NewCoordinator constructs a Coordinator backed by cfg. cfg.DB is
// expected to be a *sql.DB whose schema includes attempt_commits
// (migration 061+), task_output_declarations (migration 062+),
// artifacts (migration 041+), artifact_uploads (migration 030+),
// task_attempts (migration 045+), tasks (migration 039+), jobs
// (migration 013+), delivery_destinations (migration 022+),
// job_deliveries (migration 022+), and outbox_events (migration
// 014+).
//
// cfg.HMACKey is the master-side secret used as the HMAC-SHA256 key
// for the deterministic commit-token derivation. NewCoordinator
// returns an error when the key is missing or short; the caller
// (bootstrap, recover_output) MUST refuse to start the master with a
// replayable token derivation.
//
// Tests pass an explicit 32-byte testkey via CoordinatorConfig{HMACKey: ...}.
func NewCoordinator(cfg CoordinatorConfig) (Coordinator, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("completion.NewCoordinator: cfg.DB is required")
	}
	if len(cfg.HMACKey) < 32 {
		return nil, fmt.Errorf("completion.NewCoordinator: cfg.HMACKey must be >= 32 bytes for HMAC-SHA256 nominal entropy (got %d)", len(cfg.HMACKey))
	}
	return &coordinator{
		db:         cfg.DB,
		hmacKey:    cfg.HMACKey,
		uowFactory: NewSQLiteUnitOfWorkFactory(cfg.DB),
		budget:     NewConflictBudget(DefaultConflictBudgetPolicy()),
	}, nil
}

// coordinator is the canonical Coordinator implementation.
type coordinator struct {
	db         *sql.DB
	hmacKey    []byte
	uowFactory UnitOfWorkFactory
	// budget counts consecutive ErrTransitionConflict on the
	// three canonical attempt_commits CAS paths
	// (UpdateReadyCountExhaustive + SetExpired + MarkCommitted) and
	// escalates to ErrConflictBudgetExhausted at the threshold.
	// Initialised in NewCoordinator with the default policy.
	budget *ConflictBudget
}

// recordAttemptCommitsCAS routes a CAS error from one of the three
// canonical attempt_commits CAS paths through the conflict budget.
//
// Returns the original err unchanged when the budget is under
// threshold (or no err) so the caller can surface it; returns a
// wrapped ErrConflictBudgetExhausted when the streak crossed the
// boundary so the caller can escalate to its supervisor.
//
// Calls with err == nil reset the budget counter (record-able as a
// successful Coordinator-method exit).
func (c *coordinator) recordAttemptCommitsCAS(err error) error {
	if c.budget == nil {
		return err
	}
	budgetErr := c.budget.Record(err)
	if budgetErr == nil {
		// nil from Record means either a reset (err was nil) or
		// under-threshold continuation. In both cases the caller
		// should proceed with its normal err.
		return err
	}
	return budgetErr
}

// ────────────────────────────────────────────────────────────────────────
// DeclareOutputs — OUT OF UNITOFWORK SCOPE (HMAC + INSERT-OR-IGNORE
// dance tied to FenceTuple.Read). See package-level doc.
// ────────────────────────────────────────────────────────────────────────

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
// already holds the first one.
func (c *coordinator) DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if len(cmd.OutputManifests) == 0 {
		return nil, fmt.Errorf("completion.DeclareOutputs: at least one OutputManifest required (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	// Mint a fresh candidate commit_id from crypto/rand. The canonical
	// commit_id (i.e. the value persisted in attempt_commits) is decided
	// AFTER FenceTuple.ReadOrMissing runs: if a row already exists, we
	// reuse its commit_id; otherwise we INSERT with this candidate.
	// Entropy failure is fail-closed (Verdetto P0 #7): a deterministic
	// fallback would collide commits across the cluster and break
	// UNIQUE(task_id, attempt_id) dedup.
	commitID, err := newUUIDLowerHex()
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: mint commit_id: %w", err)
	}

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

	state, err := cmd.Fence.ReadOrMissing(ctx, tx)
	if err != nil {
		return nil, err
	}
	if state != nil {
		// Replay path: reuse the canonical commit_id from the existing
		// attempt_commits row. The local candidate commitID is discarded.
		commitID = state.CommitID
	}

	// Derive commit_token + commit_token_hash AFTER canonicalization so
	// a replay produces the SAME token against the canonical commitID.
	// The previous Blocco 2 implementation derived the token BEFORE
	// ReadOrMissing (against the local candidate) which broke this
	// invariant; the regression-guard
	// TestCoordinator_DeclareOutputs_ReplayYieldsIdenticalToken caught
	// it. Sequencing here is the contract.
	token, tokenHash, err := generateDeterministicCommitToken(c, commitID, cmd.Fence)
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: derive commit_token: %w", err)
	}

	if state == nil {
		// First-call path: INSERT with commit_id + token_hash.
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
			// ReadOrMissing and our INSERT. Re-read the canonical
			// commit_id from the row the other writer inserted and
			// re-derive the determinism-bearing token so the
			// returned UploadPlan stays consistent.
			var existingCommitID string
			if err := tx.QueryRowContext(ctx,
				`SELECT commit_id FROM attempt_commits
				 WHERE task_id = ? AND attempt_id = ?`,
				cmd.Fence.TaskID, cmd.Fence.AttemptID,
			).Scan(&existingCommitID); err != nil {
				return nil, fmt.Errorf("completion.DeclareOutputs: select existing commit_id after race: %w", err)
			}
			commitID = existingCommitID
			token, tokenHash, err = generateDeterministicCommitToken(c, commitID, cmd.Fence)
			if err != nil {
				return nil, fmt.Errorf("completion.DeclareOutputs: re-derive commit_token after race: %w", err)
			}
			_ = tokenHash // canonical row already carries its own commit_token_hash
		}
	}

	declIDs := make([]string, 0, len(cmd.OutputManifests))
	for _, m := range cmd.OutputManifests {
		if err := validateManifest(&m); err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: invalid manifest: %w", err)
		}
		declarationID, derr := newUUIDLowerHex()
		if derr != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: mint declaration_id: %w", derr)
		}
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
	_ = declIDs // reserved for future transport registry wiring

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: commit: %w", err)
	}
	committed = true

	return &UploadPlan{
		CommitID:    commitID,
		CommitToken: token,
		Targets:     nil,
	}, nil
}

// ────────────────────────────────────────────────────────────────────────
// RecordUploadProgress — OUT OF UNITOFWORK SCOPE (per-declaration
// uploaded_bytes UPDATE on task_output_declarations is held by the
// heartbeat update path, not by the commit protocol). See package-
// level doc.
// ────────────────────────────────────────────────────────────────────────

// RecordUploadProgress bumps the last_progress_at and commit_deadline_at
// on the canonical attempt_commits row CAS-gated on the FenceTuple
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

	state, err := cmd.Fence.Read(ctx, tx)
	if err != nil {
		return err
	}

	repos := c.uowFactory.WithTx(tx)
	n, err := repos.AttemptCommits().UpdateProgress(ctx, state.CommitID, nowStr, now.Add(commitGraceDefault).Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: update attempt_commits: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: status=%q (cannot progress past terminal/rejected state)", ErrTransitionConflict, state.Status)
	}

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

// ────────────────────────────────────────────────────────────────────────
// CompleteUpload — UNITOFWORK-DRIVEN. Zero raw SQL.
// ────────────────────────────────────────────────────────────────────────

// CompleteUpload verifies the worker-supplied SHA against the master-
// declared expected_sha256 on artifact_uploads, flips artifact_uploads
// → COMPLETED + artifacts STAGING/VERIFYING → READY|VERIFYING in one
// tx, and bumps attempt_commits.ready_output_count via a
// deterministic derived count.
//
// Returns nil on success; ErrTransitionConflict on stale fence;
// ErrStaleReport on attempted promotion from COMMITTED|FAILED|CANCELLED
// or on a server-vs-declarative SHA mismatch (Branch D, with tx
// rollback). All per-table writes are dispatched to AttemptCommitRepository.
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

	if _, err := cmd.Fence.Read(ctx, tx); err != nil {
		return err
	}

	repos := c.uowFactory.WithTx(tx)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	// 1. artifact_uploads read for the four-branch gate.
	uploadState, err := repos.AttemptCommits().GetArtifactUploadState(ctx, cmd.UploadID)
	if err != nil {
		return err
	}
	if uploadState.Status == "COMPLETED" {
		// Verdetto P2 (Blocco 5): replay-safe no-op is a
		// successful exit — reset the budget counter so a
		// previous streak does not poison the next attempt.
		c.recordAttemptCommitsCAS(nil)
		return nil // replay-safe no-op
	}
	if uploadState.Status != "CREATED" && uploadState.Status != "UPLOADING" && uploadState.Status != "RECEIVED" {
		return fmt.Errorf("%w: artifact_uploads.status=%q (cannot advance)", ErrTransitionConflict, uploadState.Status)
	}
	effectiveExpected := uploadState.ExpectedSHA256
	if uploadState.ReceivedSHA256 != "" {
		effectiveExpected = uploadState.ReceivedSHA256
	}

	// Worker fabrication early-reject: the worker's local SHA must
	// match the canonical expected_sha256 declared earlier in
	// DeclareOutputs. This protects against a worker that
	// post-Declare rewrites its claimed hash to anything different
	// (e.g., trying to align with a forged file). The ServerSHA256
	// gate below is independent and authoritative for STAGING->READY.
	if cmd.WorkerSHA256 != "" && effectiveExpected != "" && cmd.WorkerSHA256 != effectiveExpected {
		return fmt.Errorf("%w: upload=%s worker_sha=%s master_declared=%s",
			ErrStaleReport, cmd.UploadID, cmd.WorkerSHA256, effectiveExpected)
	}

	// Verdetto P0 #5 — authoritative SHA gate.
	//
	// Four branches determined by ServerSHA256 + effectiveExpected:
	//
	//   A. ServerSHA="" AND effectiveExpected="" — no canonical reference
	//      on either side. Bytes transferred but neither side has a
	//      hash. Stay at VERIFYING.
	//   B. ServerSHA="" AND effectiveExpected!="" — declarative SHA
	//      present, master hasn't verified. Stay at VERIFYING.
	//   C. ServerSHA matches effectiveExpected (or ServerSHA!="" with no
	//      canonical reference) — master agrees with declarative.
	//      Promote artifact STAGING/VERIFYING → READY.
	//   D. ServerSHA!="" AND differs from effectiveExpected — reject
	//      with ErrStaleReport; tx rolls back.
	serverMatches := cmd.ServerSHA256 == "" || effectiveExpected == "" || cmd.ServerSHA256 == effectiveExpected
	if !serverMatches {
		return fmt.Errorf("%w: upload=%s server_sha=%s master_declared=%s",
			ErrStaleReport, cmd.UploadID, cmd.ServerSHA256, effectiveExpected)
	}

	verdict := ArtifactKeepVerifying
	if cmd.ServerSHA256 != "" && (effectiveExpected == "" || cmd.ServerSHA256 == effectiveExpected) {
		verdict = ArtifactReady
	}

	if err := repos.AttemptCommits().CompleteArtifactUpload(ctx, verdict, cmd.UploadID, cmd.ServerSHA256, nowStr); err != nil {
		return fmt.Errorf("completion.CompleteUpload: artifact CAS: %w", err)
	}

	if err := repos.AttemptCommits().UpdateReadyCountExhaustive(ctx, cmd.Fence, nowStr); err != nil {
		// Verdetto P2 (Blocco 5): route through the conflict budget.
		// Under threshold → propagate the original ErrTransitionConflict
		// unchanged. Over threshold → propagate
		// ErrConflictBudgetExhausted so the caller can escalate.
		if budgetErr := c.recordAttemptCommitsCAS(err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", err)
	}

	if err := repos.AttemptCommits().SetExpired(ctx, cmd.Fence, nowStr); err != nil {
		// Verdetto P2 (Blocco 5): same pattern as
		// UpdateReadyCountExhaustive — both are canonical
		// attempt_commits CAS paths that count toward the budget.
		if budgetErr := c.recordAttemptCommitsCAS(err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.CompleteUpload: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5): reset the conflict budget on a
	// successful CompleteUpload so a fresh streak starts next time.
	c.recordAttemptCommitsCAS(nil)
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// CommitAttempt — UNITOFWORK-DRIVEN. Zero raw SQL. The CommitResult
// snapshot is read BEFORE tx.Commit() to fix the previous
// tx-after-commit bug (Verdetto P1 #9).
// ────────────────────────────────────────────────────────────────────────

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

	repos := c.uowFactory.WithTx(tx)
	row, err := repos.AttemptCommits().Find(ctx, commitID)
	if err != nil {
		return nil, err
	}
	if row.Status == "COMMITTED" {
		// Idempotent re-call: snapshot already-COMMITTED state via
		// the SAME tx (still open), GetCommitResult is part of the
		// same write lock — no race window.
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: snapshot on idempotent re-call: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: commit (idempotent): %w", err)
		}
		committed = true
		return res, nil
	}
	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" && row.Status != "VERIFYING" {
		return nil, fmt.Errorf("%w: attempt_commits.status=%q", ErrTransitionConflict, row.Status)
	}
	if row.ReadyOutputCnt < row.RequiredOutputCnt {
		return nil, fmt.Errorf("%w: ready=%d required=%d (commit blocked)", ErrTransitionConflict, row.ReadyOutputCnt, row.RequiredOutputCnt)
	}

	if err := repos.TaskAttempts().MarkSucceeded(ctx, row.AttemptID, row.WorkerID, row.LeaseID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: task_attempts CAS: %w", err)
	}
	if err := repos.Tasks().MarkSucceeded(ctx, row.TaskID, row.AttemptID, row.WorkerID, row.LeaseID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: tasks CAS: %w", err)
	}
	if err := repos.AttemptCommits().MarkCommitted(ctx, commitID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", err)
	}
	if err := repos.Jobs().MarkSucceededIfTasksDone(ctx, row.JobID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: jobs CAS: %w", err)
	}
	if err := repos.Deliveries().InsertDeliveriesForJob(ctx, row.JobID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: job_deliveries insert: %w", err)
	}
	payloadJSON := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
	if err := repos.Outbox().InsertEvent(ctx, "ce_"+commitID, "task", row.TaskID, "commit_protocol.committed", payloadJSON, nowStr); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: outbox_events insert: %w", err)
	}

	// Snapshot CommitResult BEFORE commit (Verdetto P1 #9 / tx-after-
	// commit bug fix). The read is part of the same LevelSerializable
	// write lock so the result cannot drift from the just-written
	// SUCCEEDED state under a concurrent writer.
	res, err := repos.AttemptCommits().GetCommitResult(ctx, commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: snapshot CommitResult: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.CommitAttempt: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5): reset the conflict budget on a
	// successful CommitAttempt so a fresh streak starts next time.
	c.recordAttemptCommitsCAS(nil)
	return res, nil
}

// ────────────────────────────────────────────────────────────────────────
// ReconcileAttempt — UNITOFWORK-DRIVEN. Zero raw SQL. The CommitResult
// snapshot is read BEFORE tx.Commit() (Verdetto P1 #9).
// ────────────────────────────────────────────────────────────────────────

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

	repos := c.uowFactory.WithTx(tx)
	row, err := repos.AttemptCommits().Find(ctx, commitID)
	if err != nil {
		return nil, err
	}

	if row.Status != "DECLARED" && row.Status != "UPLOADING" && row.Status != "RECEIVED" {
		// Already terminal or bypass-able — snapshot and return.
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot on non-terminal status: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: commit (noop): %w", err)
		}
		committed = true
		return res, nil
	}

	deadlineElapsed := false
	if row.CommitDeadlineAt != "" {
		if t, perr := time.Parse(time.RFC3339Nano, row.CommitDeadlineAt); perr == nil {
			deadlineElapsed = now.After(t)
		}
	}
	if !deadlineElapsed {
		res, gerr := repos.AttemptCommits().GetCommitResult(ctx, commitID)
		if gerr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot on deadline-not-elapsed: %w", gerr)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: commit (deadline-not-elapsed): %w", err)
		}
		committed = true
		return res, nil
	}

	if err := repos.AttemptCommits().SetExpiredByID(ctx, commitID, nowStr); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", err)
	}
	payloadJSON := `{"commit_id":"` + commitID + `","attempt_id":"` + row.AttemptID + `","job_id":"` + row.JobID + `"}`
	if err := repos.Outbox().InsertEvent(ctx, "re_"+commitID, "task", row.TaskID, "commit_protocol.expired", payloadJSON, nowStr); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: outbox_events insert: %w", err)
	}

	// Snapshot CommitResult BEFORE commit (Verdetto P1 #9 / tx-after-
	// commit bug fix).
	res, err := repos.AttemptCommits().GetCommitResult(ctx, commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: snapshot CommitResult: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.ReconcileAttempt: commit: %w", err)
	}
	committed = true
	return res, nil
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
//
// Verdetto P0 #7: entropy failure is fail-closed. A previous
// `byte(i+1)` fallback was deterministic and would have collided
// commits across the cluster, breaking UNIQUE(task_id, attempt_id)
// dedup at scale. The error propagates through DeclareOutputs into
// the tx rollback path, surfacing in /ready and the supervisor.
func newUUIDLowerHex() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("completion: entropy failure for UUID (crypto/rand): %w", err)
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, by := range b {
		out[i*2] = hexdigits[by>>4]
		out[i*2+1] = hexdigits[by&0x0f]
	}
	return string(out), nil
}

// generateDeterministicCommitToken derives a replay-safe bearer
// token from the canonical (commit_id, fence) and the master-side
// HMAC key. Two calls with the same inputs return the same token.
//
// Token shape: HMAC-SHA256(key, "v1|<commitID>|<taskID>|<attemptID>|<workerID>|<leaseID>|<revision>").
// The token is the hex-encoded HMAC digest (32 bytes -> 64 hex
// chars). The hash persisted on attempt_commits.commit_token_hash
// is the SHA256 hex of the decoded token bytes.
func generateDeterministicCommitToken(c *coordinator, commitID string, fence FenceTuple) (token, hash string, err error) {
	if len(c.hmacKey) < 32 {
		return "", "", fmt.Errorf("completion: commit HMAC key not configured (must be >= 32 bytes)")
	}
	mac := hmac.New(sha256.New, c.hmacKey)
	if _, ferr := fmt.Fprintf(mac, "v1|%s|%s|%s|%s|%s|%d",
		commitID, fence.TaskID, fence.AttemptID, fence.WorkerID, fence.LeaseID, fence.Revision,
	); ferr != nil {
		return "", "", fmt.Errorf("completion: derive commit_token hmac write: %w", ferr)
	}
	sum := mac.Sum(nil)
	token = hex.EncodeToString(sum)
	hashSum := sha256.Sum256(sum)
	return token, hex.EncodeToString(hashSum[:]), nil
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

