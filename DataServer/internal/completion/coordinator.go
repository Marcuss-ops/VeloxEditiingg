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
	"database/sql"
	"fmt"
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

// SetConflictBudgetSink installs (or replaces) the Prometheus
// instrumentation point the ConflictBudget emits state-machine
// transitions to. The sink parameter MAY be nil, which clears
// any previously installed sink and returns the budget to a
// no-instrumentation state (useful for tests that want to swap
// sinks between phases).
//
// The seam is additive — bootstrap wires the metrics.Collector
// post-construct, so callers that already build a Coordinator
// without this method are unaffected. Internally the call
// delegates to (*ConflictBudget).WithMetricsSink, which is nil-
// safe and lock-guarded.
//
// Idempotent across multiple calls: replacing an existing sink
// is allowed; the new one becomes the active sink on the next
// Record/Reset.
func (c *coordinator) SetConflictBudgetSink(sink ConflictBudgetSink) {
	if c.budget == nil {
		return
	}
	c.budget.WithMetricsSink(sink)
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

// recordAttemptCommitsCAS routes a CAS error from one of the canonical
// attempt_commits CAS paths through the conflict budget under a
// per-key label (typically "commit:<commit_id>"). Verdetto P0 #4
// (Blocco 3) mandates per-key isolation so concurrent independent
// commit_ids do not aggregate into one false-positive streak.
//
// Returns the original err unchanged when the budget is under
// threshold (or no err) so the caller can surface it; returns a
// wrapped ErrConflictBudgetExhausted when the streak crossed the
// boundary so the caller can escalate to its supervisor.
//
// Calls with err == nil reset the per-key counter (recordable as a
// successful Coordinator-method exit for that specific commit).
func (c *coordinator) recordAttemptCommitsCAS(key string, err error) error {
	if c.budget == nil {
		return err
	}
	budgetErr := c.budget.Record(key, err)
	if budgetErr == nil {
		// nil from Record means either a reset (err was nil) or
		// under-threshold continuation. In both cases the caller
		// should proceed with its normal err.
		return err
	}
	return budgetErr
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

	state, err := cmd.Fence.Read(ctx, tx)
	if err != nil {
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
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): replay-safe
		// no-op is a successful exit — reset the per-commit
		// budget counter so a previous streak on THIS commit
		// does not poison the next attempt. The key uses the
		// upload_id as a stable per-replay key. (Independent
		// replays of the same upload_id share a streak by
		// design.)
		c.recordAttemptCommitsCAS("upload:"+cmd.UploadID, nil)
		return nil // replay-safe no-op
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
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): route through
		// the conflict budget under the per-commit key so concurrent
		// independent commits don't aggregate into one streak.
		// Under threshold → propagate the original ErrTransitionConflict
		// unchanged. Over threshold → propagate
		// ErrConflictBudgetExhausted so the caller can escalate.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: ready_output_count bump: %w", err)
	}

	if err := repos.AttemptCommits().SetExpired(ctx, cmd.Fence, nowStr); err != nil {
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): same pattern
		// as UpdateReadyCountExhaustive — both are canonical
		// attempt_commits CAS paths that count toward the budget.
		// Per-key: this commit's streak is independent of any
		// other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+state.CommitID, err); budgetErr != nil {
			return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", budgetErr)
		}
		return fmt.Errorf("completion.CompleteUpload: deadline-breach EXPIRED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.CompleteUpload: commit: %w", err)
	}
	committed = true
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful CompleteUpload
	// so a fresh streak starts next time for THIS commit only.
	c.recordAttemptCommitsCAS("commit:"+state.CommitID, nil)
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
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): MarkCommitted
		// is the third canonical attempt_commits CAS path. Per-key
		// isolation: this commit's streak is independent of any
		// other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, err); budgetErr != nil {
			return nil, fmt.Errorf("completion.CommitAttempt: attempt_commits CAS: %w", budgetErr)
		}
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
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful CommitAttempt.
	c.recordAttemptCommitsCAS("commit:"+commitID, nil)
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
		// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): SetExpiredByID
		// is the canonical attempt_commits CAS path on the
		// reconcile side. Per-key: this commit's streak is
		// independent of any other in-flight commit's conflicts.
		if budgetErr := c.recordAttemptCommitsCAS("commit:"+commitID, err); budgetErr != nil {
			return nil, fmt.Errorf("completion.ReconcileAttempt: attempt_commits CAS: %w", budgetErr)
		}
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
	// Verdetto P2 (Blocco 5) + P0 #4 (Blocco 3): reset the
	// per-commit conflict budget on a successful ReconcileAttempt.
	c.recordAttemptCommitsCAS("commit:"+commitID, nil)
	return res, nil
}
