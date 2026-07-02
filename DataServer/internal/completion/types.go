// Package completion / types.go
//
// Artifact Commit Protocol (Fase 2 of docs/completion-protocol.md):
// canonical types and interface for the master-side commit pipeline.
//
// Responsibility: this package owns the SINGLE entry point for every
// terminal and intermediate transition between the worker's TaskResult
// declaration and the master-side SUCCEEDED writes. ALL of these paths
// flow through Coordinator:
//
//   - happy path (worker submits, master verifies, master commits);
//   - retry / replay (network blip, worker reconnect);
//   - master restart (in-flight AttemptCommit row in DECLARED/UPLOADING
//     state on a fresh process);
//   - reconciler (weekly supervisor tick that walks the cases listed
//     in docs/completion-protocol.md §Phase 4);
//   - administrative command (velox-worker recover-output).
//
// No other code path may write tasks.tasks.status='SUCCEEDED', task_att
// .status='SUCCEEDED', jobs.status='SUCCEEDED', or attempt_commits
// .status='COMMITTED' (the scan_test guard in
// internal/artifacts/scan_test.go enforces this for jobs.status; the
// same discipline applies to the new tables once Phase 2.5 lands the
// tx writer).
//
// This file is the TYPES-ONLY half of the package. The SQL-bearing
// implementation lives in coordinator.go; the FenceTuple helper lives
// in fencing.go.
package completion

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors. Callers MUST use errors.Is to inspect — string match
// on the .Error() output is forbidden because the wording is part of
// the wire contract.
var (
	// ErrStaleReport signals that the (worker_id, lease_id, revision)
	// tuple exposed by the caller is older than the canonical row.
	// Reconciliation must NOT retry blindly on this error.
	ErrStaleReport = errors.New("completion: stale report")

	// ErrTransitionConflict signals that the CAS step was raced by
	// another writer (concurrent ClaimAttempt or another legitimate
	// completion path). Caller can re-read and decide.
	ErrTransitionConflict = errors.New("completion: transition conflict")

	// ErrFenceMismatch signals that the input FenceTuple is malformed
	// (empty strings or negative revision). This is a programmer error
	// — caller should never reach Coordinator with such an input.
	ErrFenceMismatch = errors.New("completion: fence tuple mismatch")

	// ErrAttemptCommitNotFound signals that the supplied commitID does
	// not exist in attempt_commits. Reconciliation will get this on a
	// stale worker re-declaring an Attempt good-for-EXPIRED; the
	// coordinator decides whether to resurrect the row or reject.
	ErrAttemptCommitNotFound = errors.New("completion: attempt commit not found")
)

// OutputManifest is the worker's per-file declaration. Mirrors the
// proto field OutputManifest in worker_control.proto (Fase 3.3);
// workers populate this when sending TaskOutputDeclared. Until the
// proto regen lands (Fase 1.4 deferred), the master reads it from the
// typed Go struct.
//nolint:revive
type OutputManifest struct {
	OutputKind     string `json:"output_kind"`
	LogicalName    string `json:"logical_name"`
	MimeType       string `json:"mime_type"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256         string `json:"sha256"`
	WorkerSpoolKey string `json:"worker_spool_key"`
}

// DeclareOutputsCommand is the worker's first message post-render.
// Replay-safe: calling DeclareOutputs twice with the same Fence and
// OutputManifests is a no-op on the database side (INSERT-OR-IGNORE on
// the UNIQUE(task_id, attempt_id) and
// UNIQUE(task_id, attempt_id, output_kind, logical_name) constraints).
type DeclareOutputsCommand struct {
	Fence           FenceTuple
	JobID           string
	OutputManifests []OutputManifest
}

// UploadPlan is the master's reply to DeclareOutputs.
//
// CommitID is the dedicated key the master uses to gate later upload
// / commit operations on this exact Attempt. The worker carries it on
// every subsequent commit-protocol call.
//
// CommitToken is the OPAQUE bearer token the master hands the worker
// for the upload window. The token is generated at DeclareOutputs
// time and returned ONCE; the master stores ONLY its SHA256 hash on
// the attempt_commits row. The plain token never persists on the
// master beyond this return value.
//
// Targets is the per-manifest upload target list. Empty in this phase
// because transport registration (Fase 3.7) lands later; the master
// still returns the slice shape for forward-compatibility.
type UploadPlan struct {
	CommitID    string
	CommitToken string
	Targets     []UploadTarget
}

// UploadTarget is the per-manifest upload instructions. Empty until
// Fase 3.7 wires the transport registry. The proto Definition lands
// in Fase 3.4.
type UploadTarget struct {
	DeclarationID string
	ArtifactID    string
	UploadID      string
	TransportID   string
	UploadURL     string
	ChunkSize     int64
	ExpiresAtUnix int64
}

// RecordUploadProgressCommand is the worker's heartbeat during the
// upload window. The master CASes on attempt_commits.last_progress_at
// and bumps commit_deadline_at; verifying the FenceTuple guards against
// a worker whose lease has been reaped out from under it.
type RecordUploadProgressCommand struct {
	Fence         FenceTuple
	UploadID      string
	UploadedBytes int64
}

// CompleteUploadCommand is the worker's "bytes transferred" signal in
// the same tx as the master-side SHA256 + artifact-uploads acceptance.
// Fase 2.5 implementation complete.
type CompleteUploadCommand struct {
	Fence             FenceTuple
	UploadID          string
	UploadedSizeBytes int64
	WorkerSHA256      string
}

// CommitResult describes what CommitAttempt/ReconcileAttempt produced.
// Fase 2.5-4.1: implemented — returns the post-tx snapshot of
// attempt_commits joined with tasks + jobs.
type CommitResult struct {
	CommitID    string
	TaskID      string
	AttemptID   string
	JobID       string
	TaskStatus  string
	JobStatus   string
	ArtifactIDs []string
	CommittedAt *time.Time
}

// Coordinator is the SINGLE entry point for every Artifact Commit
// Protocol transition.
type Coordinator interface {
	// DeclareOutputs is the worker's first post-render call. Idempotent.
	// Returns the UploadPlan the worker uses to drive the upload phase.
	DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error)

	// RecordUploadProgress is the worker's mid-upload heartbeat.
	// Idempotent: re-running with the same (fence, upload_id) leaves
	// the DB in a state equivalent to a single call (monotonically
	// advancing last_progress_at).
	RecordUploadProgress(ctx context.Context, cmd RecordUploadProgressCommand) error

	// CompleteUpload is the worker's "bytes transferred" signal.
	// Fase 2.5: verifies SHA256, advances artifact status, detects
	// deadline breaches, and bumps ready_output_count.
	CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error

	// CommitAttempt performs the canonical atomic SUCCEEDED write.
	// Fase 2.5: idempotent — a duplicate call on COMMITTED is a no-op.
	CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error)

	// ReconcileAttempt is the supervisor's repair-forward entry point.
	// Fase 4.1: handles DECLARED-with-dead-worker, deadline-expired,
	// and other repair cases.
	ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error)
}
