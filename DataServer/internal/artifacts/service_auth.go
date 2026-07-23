// Package artifacts / service_auth.go
//
// AuthReader is the read-only persistence surface Service.BeginUpload
// uses for job / attempt / uniqueness auth. It hides *sql.DB from
// Service so the production composition root never injects a raw
// database handle into the auth path.
//
// Method set kept narrow: BeginUpload consumes exactly three queries.
// Adding more would grow the interface beyond its scope; future
// read-only concerns should land on a purpose-built reader (e.g.
// QuarantineReader) instead of AuthReader.
//
// ErrNoRows semantics on each method:
//
//   - LoadJob: returns (nil, nil). "No row" is a soft miss — BeginUpload
//     decides whether to surface it as ErrJobNotRunning.
//   - LoadAttempt: returns ErrAttemptMismatch-wrapped error. "No
//     attempt row" is treated as an auth failure, matching the
//     pre-split behavior of loadAttempt in service.go and the rest of
//     the auth chain's "absent == failed" semantics.
//   - FindExistingReadyArtifact: returns ("", nil). Soft miss —
//     BeginUpload decides whether to surface it as
//     ErrDuplicateReadyArtifact.
package artifacts

import (
	"context"

	"velox-server/internal/store"
)

// JobState is an alias to store.JobState. The projection lives in the
// store package because the SQLite adapter is implemented there; the
// consumer-owned AuthReader interface below keeps the alias so existing
// callers and tests do not need to change.
type JobState = store.JobState

// AuthReader returns the auth-shaped projections Service requires.
// Read-only by contract — implementations MUST NOT add a write method
// without breaking the "auth is read-only" invariant.
type AuthReader interface {
	// LoadJob returns the JobState for jobID, or (nil, nil) when no
	// row matches.
	LoadJob(ctx context.Context, jobID string) (*JobState, error)
	// LoadAttempt returns (task_attempts.status, worker_id, lease_id)
	// for the (job_id, attempt_number) pair. A missing row returns a
	// ErrAttemptMismatch-wrapped error (a missing attempt is an auth
	// failure, not a soft miss).
	LoadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error)
	// FindExistingReadyArtifact returns the artifact id of any READY
	// artifact of the given kind for the given job, or "" when none.
	// Used by BeginUpload's uniqueness gate (ErrDuplicateReadyArtifact).
	FindExistingReadyArtifact(ctx context.Context, jobID, kind string) (string, error)
}
