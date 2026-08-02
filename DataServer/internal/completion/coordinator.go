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
//
// Layout of this package:
//   - coordinator.go — contract surface: CoordinatorConfig,
//     NewCoordinator, coordinator struct, conflict-budget routing.
//   - coordinator_upload.go — CompleteUpload (artifact CAS + ready
//     count + deadline breach).
//   - coordinator_commit.go — CommitAttempt (canonical atomic
//     final tx for a commit_id).
//   - coordinator_reconcile.go — ReconcileAttempt (supervisor
//     repair-forward scan).
package completion

import (
	"database/sql"
	"fmt"

	"velox-server/internal/store"
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
	// BlobStore is required by the production master-stream path to promote
	// a verified staging file before the commit becomes visible. It remains
	// optional for the DB-only unit-test coordinator.
	BlobStore store.BlobStore
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
		blobStore:  cfg.BlobStore,
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
	blobStore  store.BlobStore
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
