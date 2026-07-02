// Package completion / unitofwork.go
//
// Unit of Work pattern (Verdetto P1 #8 / #9, Blocco 3).
//
// The coordinator owns the *sql.Tx lifecycle (open, commit, defer
// rollback) but delegates EVERY raw SQL statement to one of the six
// repositories exposed by the UnitOfWork. The interfaces below are
// the single concrete contract; the SQLite-backed adapter lives in
// sqlite_uow.go.
//
// Design invariants:
//
//  1. No *sql.Tx in the interface signatures. Each repository holds
//     the tx as private state set at UnitOfWorkFactory.WithTx(tx)
//     construction. This keeps the Coordinator free of SQL concerns:
//     it speaks typed Go parameters only.
//
//  2. Tx lifecycle stays at the Coordinator layer (one tx per
//     Coordinator method call). The repos do not start or commit
//     transactions.
//
//  3. The CommitResult snapshot returned by CommitAttempt /
//     ReconcileAttempt is read BEFORE tx.Commit() via
//     AttemptCommitRepository.GetCommitResult; the snapshot is part
//     of the same LevelSerializable write lock so the caller
//     receives a transactional self-contained view that excludes
//     drift from concurrent jobs (Verdetto P1 #9).
//
//  4. DeclareOutputs and RecordUploadProgress do NOT use this UoW.
//     They stay raw-SQL because they own the HMAC + INSERT-OR-IGNORE
//     dance tied to the FenceTuple.Read gating; pulling them into
//     the UoW would re-introduce an HMAC-key plumbing path inside the
//     repos that does not belong there. Only CompleteUpload,
//     CommitAttempt, ReconcileAttempt are UoW-driven.
//
//  5. CompleteUpload's artifact_uploads + artifacts CAS lives on
//     AttemptCommitRepository via GetArtifactUploadState +
//     CompleteArtifactUpload. The pair of tables is owned by the
//     commit-attempt domain; adding a 7th/8th repo would split one
//     atomic decision across two interfaces. Branch D (SHA mismatch)
//     is rejected by the Coordinator upstream; the repo only sees
//     KeepVerifying or Ready verdicts.
package completion

import (
	"context"
	"database/sql"
)

// UnitOfWork bundles the six repositories sharing a single *sql.Tx.
// Returned by UnitOfWorkFactory.WithTx; the tx is held internally by
// each repo's adapter.
type UnitOfWork interface {
	AttemptCommits() AttemptCommitRepository
	TaskAttempts() TaskAttemptRepository
	Tasks() TaskRepository
	Jobs() JobFinalizationRepository
	Deliveries() DeliveryRepository
	Outbox() OutboxRepository
}

// UnitOfWorkFactory produces a UnitOfWork bound to a *sql.Tx. The
// factory holds the *sql.DB (or *SQLiteStore) needed by repos; the
// caller (Coordinator) supplies the tx on a per-method basis.
//
// The factory is the single seam between the completion package and
// the underlying DB driver. A future Postgres adapter implements the
// same interface and is wired in via the same NewCoordinator path.
type UnitOfWorkFactory interface {
	WithTx(tx *sql.Tx) UnitOfWork
}

// SQLFencer is a tiny adapter for methods that need to embed the
// canonical fence WHERE/Args inline. The FenceTuple satisfies this
// interface naturally.
type SQLFencer interface {
	SQLWhere() string
	SQLArgs() []any
}

// AttemptCommitRow is the typed projection of the attempt_commits row
// the Coordinator reads at fence-validation time. It lives in this
// package on purpose: the coordinator / repos need a typed shape but
// must NOT cycle through taskgraph / taskattempts (those packages
// already own a wider Task / TaskAttempt concept; cycling them back
// into this package's UoW would create two views of the same row).
type AttemptCommitRow struct {
	CommitID          string
	TaskID            string
	AttemptID         string
	JobID             string
	WorkerID          string
	LeaseID           string
	Status            string
	RequiredOutputCnt int
	ReadyOutputCnt    int
	// CommitDeadlineAt is the RFC3339 string of the supervisor's
	// deadline stamp. ReconcileAttempt compares it against now() to
	// detect deadline-elapsed rows. Zero-length on a fresh row.
	CommitDeadlineAt string
}

// ArtifactUploadState is the typed projection of the artifact_uploads
// row the Coordinator reads at CompleteUpload's four-branch gate.
// Mirrors only the columns CompleteUpload inspects (expected_sha256,
// received_sha256, status).
type ArtifactUploadState struct {
	UploadID       string
	ExpectedSHA256 string
	ReceivedSHA256 string
	Status         string
}

// ArtifactCompletionVerdict selects the artifact_uploads + artifacts
// CAS path CompleteUpload applies after the SHA gate computes the
// four-branch verdict. Branch D (mismatch) is rejected by the
// Coordinator upstream and does not appear here.
//
//   - KeepVerifying: artifact stays VERIFYING (no master SHA or
//     declarative SHA, or both). artifact_uploads.received_sha256
//     is preserved via COALESCE so a partial probe from a prior
//     tick's chunked handler is NOT clobbered.
//   - Ready: artifact STAGING/VERIFYING -> READY (server SHA
//     matches declarative). artifact_uploads.received_sha256 is
//     stamped with the master-derived server SHA verbatim so a
//     stale chunked-handshake probe value cannot survive a verified
//     re-CAS.
type ArtifactCompletionVerdict int

const (
	ArtifactKeepVerifying ArtifactCompletionVerdict = iota
	ArtifactReady
)

// AttemptCommitRepository owns all reads and writes on attempt_commits
// AND the artifact_uploads + artifacts pair CompleteUpload drives.
// The "fence" operations carry a SQLFencer so the Coordinator passes
// the FenceTuple through directly without ever referring to *sql.Tx.
type AttemptCommitRepository interface {
	// Find returns the attempt_commits row by commit_id. Returns
	// (nil, ErrAttemptCommitNotFound) on a missing row and
	// (nil, ErrTransitionConflict) on a stale fence mismatch (Read).
	Find(ctx context.Context, commitID string) (*AttemptCommitRow, error)

	// GetArtifactUploadState returns the ArtifactUploadState row for
	// the supplied upload_id. Returns ErrAttemptCommitNotFound on a
	// missing row (the CompleteUpload uploadID gate shares the same
	// sentinel). Called inside the same tx as CompleteArtifactUpload.
	GetArtifactUploadState(ctx context.Context, uploadID string) (*ArtifactUploadState, error)

	// CompleteArtifactUpload drives the artifact_uploads + artifacts
	// CAS pair for CompleteUpload. Branch D (SHA mismatch) is
	// rejected by the Coordinator upstream; this method only sees
	// KeepVerifying or Ready verdicts.
	//
	// For ArtifactReady the artifact_uploads.received_sha256 is
	// stamped verbatim from serverSHA; for ArtifactKeepVerifying the
	// column is preserved via COALESCE(received_sha256, serverSHA).
	// The artifact status itself moves to VERIFYING (KeepVerifying)
	// or READY (Ready).
	//
	// CAS gates are shared: artifact_uploads.status IN
	// ('CREATED','UPLOADING','RECEIVED') AND
	// artifacts.status IN ('STAGING','VERIFYING').
	CompleteArtifactUpload(ctx context.Context, verdict ArtifactCompletionVerdict, uploadID, serverSHA, nowStr string) error

	// GetCommitResult reads the post-update snapshot of
	// attempt_commits joined with tasks + jobs + artifacts so the
	// caller receives a self-contained CommitResult without an
	// additional roundtrip. Called BEFORE tx.Commit() so the
	// snapshot is part of the same LevelSerializable write lock
	// (Verdetto P1 #9 / tx-after-commit fix).
	GetCommitResult(ctx context.Context, commitID string) (*CommitResult, error)

	// UpdateProgress bumps last_progress_at + commit_deadline_at on
	// the row matching the canonical commit_id CAS-gated on
	// status IN ('DECLARED','UPLOADING'). Returns the number of
	// rows affected (0 → ErrTransitionConflict caller-side).
	UpdateProgress(ctx context.Context, commitID, nowStr, deadlineStr string) (int64, error)

	// UpdateReadyCountExhaustive recomputes ready_output_count from
	// (declarations JOIN artifacts) for the canonical fence CAS-gated
	// on status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING').
	UpdateReadyCountExhaustive(ctx context.Context, fence SQLFencer, nowStr string) error

	// SetExpired transitions an attempt to EXPIRED with a
	// COMMIT_DEADLINE_EXCEEDED reason. CAS gates on
	// status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING') and
	// (deadline elapsed AND ready<required).
	SetExpired(ctx context.Context, fence SQLFencer, nowStr string) error

	// SetExpiredByID transitions an attempt to EXPIRED by commit_id,
	// CAS-gated on status IN ('DECLARED','UPLOADING','RECEIVED').
	// Used by ReconcileAttempt where the canonical fence tuple is
	// resolved from the row itself.
	SetExpiredByID(ctx context.Context, commitID, nowStr string) error

	// MarkCommitted transitions an attempt to COMMITTED. CAS gates
	// on status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING').
	MarkCommitted(ctx context.Context, commitID, nowStr string) error
}

// TaskAttemptRepository owns all reads and writes on task_attempts.
type TaskAttemptRepository interface {
	// MarkSucceeded transitions task_attempts to SUCCEEDED CAS-gated
	// on the (attempt_id, worker_id, lease_id) tuple and status NOT
	// IN terminal states. Returns ErrTransitionConflict on stale
	// CAS; the caller decides whether to fall through to
	// ReconcileAttempt.
	MarkSucceeded(ctx context.Context, attemptID, workerID, leaseID, nowStr string) error
}

// TaskRepository owns the typed task writes the coordinator needs.
type TaskRepository interface {
	// MarkSucceeded transitions tasks to SUCCEEDED for the canonical
	// (task_id, attempt_id, worker_id, lease_id) tuple, status IN
	// ('RUNNING','LEASED'). Stamps winning_attempt_id +
	// winning_attempt_committed_at + winning_attempt_terminal_pending=0
	// + revision++.
	MarkSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, nowStr string) error
}

// JobFinalizationRepository owns the conditional jobs CAS that flips
// the job to SUCCEEDED only when all sibling tasks are SUCCEEDED.
type JobFinalizationRepository interface {
	// MarkSucceededIfTasksDone CAS-gates jobs.status='SUCCEEDED' when
	// every task in (job_id, status != 'SUCCEEDED') is empty.
	// 0 rows affected is benign when a sibling is still pending.
	MarkSucceededIfTasksDone(ctx context.Context, jobID, nowStr string) error
}

// DeliveryRepository owns the job_deliveries idempotent INSERT after
// ratification. It hides the cross-join logic between artifacts and
// delivery_destinations behind a single typed method call.
type DeliveryRepository interface {
	// InsertDeliveriesForJob computes the (artifact × destination)
	// cross product and INSERT-OR-IGNOREs each row with a
	// deterministic idempotency_key.
	InsertDeliveriesForJob(ctx context.Context, jobID, nowStr string) error
}

// OutboxRepository owns the outbox_events idempotent INSERT for
// commit-protocol.committed / .expired events.
type OutboxRepository interface {
	// InsertEvent idempotently inserts an outbox event with the
	// supplied (event_id, aggregate_type, aggregate_id, event_type,
	// payload_json) tuple. event_id is the primary-key UNIQUE; a
	// re-emitted tx absorbs the duplicate.
	InsertEvent(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, nowStr string) error
}
