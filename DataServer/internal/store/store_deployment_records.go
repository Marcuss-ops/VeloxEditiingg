package store

import (
	"errors"
	"time"
)

// store_deployment_records.go owns the deployment_records table model:
// the status constants, the sentinel errors, the DeploymentRecord row
// shape (mirrors the SQL columns 1:1) and the schema bootstrap. Inserts
// live in store_deployment_records_insert.go, terminal transitions in
// store_deployment_records_transition.go, and reads in
// store_deployment_records_query.go.
//
// Step 5/15 lifecycle (out of scope here, lands in Step 6): the
// Fleet Controller calls InsertDeploymentRecord on PENDING, then
// UpdateDeploymentStatus once the worker-side prepare-host.sh
// pipeline reports back via worker_events / heartbeat. Until
// Step 6 lands, only the schema + repository plumbing exist.

const (
	// DeployStatusPending is the initial state at insert. The
	// row exists, but the worker-side update has not yet
	// completed. Rows in PENDING are the dashboard's "in-flight
	// deploys" view.
	DeployStatusPending = "PENDING"

	// DeployStatusSucceeded marks a deploy where the worker's
	// heartbeat reported image_digest == target_digest AND the
	// worker emitted a matching worker_events row signalling
	// "deploy_completed".
	DeployStatusSucceeded = "SUCCEEDED"

	// DeployStatusFailed marks a deploy that did NOT promote —
	// health check failed, cosign verify failed on the worker,
	// or the worker didn't come up at all within timeout.
	DeployStatusFailed = "FAILED"

	// DeployStatusRolledBack marks a successful forward-and-roll-
	// back cascade: target_digest was attempted and failed, so
	// the worker was rolled back to previous_digest and a new
	// deployment_records row was written with is_rollback=true
	// (the row documents the rollback, not the original
	// forward deploy which already has its own row).
	DeployStatusRolledBack = "ROLLED_BACK"
)

// ErrDeploymentNotFound is returned by GetLatestDeploymentForWorker
// when no rows exist for that worker. Maps to a 404 at the API
// boundary; callers MUST distinguish "unknown worker" from
// "known worker with no deploys yet" — the messaging differs.
var ErrDeploymentNotFound = errors.New("no deployment records for worker")

// ErrDeploymentConcurrentTransition is returned by
// updateDeploymentTerminal when the fenced UPDATE matches zero rows even
// though the row was found a moment earlier in the same transaction — a
// concurrent writer moved the row between our read and our write. The row
// EXISTS (so this is NOT ErrDeploymentNotFound); the transition is refused
// rather than clobbering the other writer's terminal outcome.
var ErrDeploymentConcurrentTransition = errors.New("deployment state machine: concurrent transition")

// ErrDeploymentDigestMismatch is returned by MarkVerifiedSucceeded when the
// authenticated observed digest does not match the record's target digest.
// The transition is NOT applied: an unverified success must never advance
// last_successful_digest. The caller (UpdateExecutor) marks the row FAILED
// with error_code `digest_mismatch` and runs the rollback cascade.
var ErrDeploymentDigestMismatch = errors.New("deployment digest mismatch")

// DeploymentRecord mirrors a single row in deployment_records.
// All time fields are RFC3339 strings in the SQL row to keep
// the schema dialect-agnostic; Go-side conversion is at the
// repository boundary so callers see time.Time.
//
// IsRollback distinguishes "intentional rollback to previous_digest"
// from "forward deploy". Step 6's rollback path sets this flag
// when a previously-FAILED forward deploy triggers an automatic
// rollback. It is meaningful only with a terminal status:
//   - status=SUCCEEDED + IsRollback=false — canonical forward happy
//     path (target_digest was deployed and the worker came up).
//   - status=SUCCEEDED + IsRollback=true  — rollback itself succeeded
//     (the worker is now on previous_digest, the operator's intent).
//   - status=ROLLED_BACK + IsRollback=true — forward failed, the
//     auto-rollback transition row is logged.
//
// status=FAILED + IsRollback=true is unreachable through the
// canonical happy path; status=PENDING + IsRollback=true is
// unreachable because the Fleet Controller (Step 2) emits the
// PENDING row with IsRollback=false. Both combinations are
// syntactically allowed in the schema (the two fields are
// independent columns) but should never appear in production.
type DeploymentRecord struct {
	DeploymentID   string     `json:"deployment_id"`
	WorkerID       string     `json:"worker_id"`
	PreviousDigest string     `json:"previous_digest"`
	TargetDigest   string     `json:"target_digest"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Status         string     `json:"status"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	AppliedBy      string     `json:"applied_by"`
	IsRollback     bool       `json:"is_rollback"`
}

// CreateDeploymentRecordsTableIfNotExists is the test/dev-only
// bootstrap path. Production uses the migration runner from
// internal/store/migrations/sqlite/103_deployment_records.sql + 151
// (error_message) — this function is here so unit tests against an
// in-memory SQLite can stand up the table without a full migration
// sweep.
//
// Idempotent: safe to call repeatedly. The DDL uses CREATE TABLE
// IF NOT EXISTS so the migration runner's checksum tracking
// stays the source of truth (this function does NOT insert into
// schema_migrations).
//
// The DDL mirrors sqlite/103_deployment_records.sql as amended by
// 151_worker_deployment_state.sql (error_message) and
// 153_deployment_error_code.sql (error_code) — the columns those
// migrations ALTER in — modulo inline CHECK length > 0 on the
// digest columns which the migration file also carries — both
// layers enforce the digest non-emptiness invariant,
// defence-in-depth against raw-INSERT bugs that bypass the Go-side
// InsertDeploymentRecord validation).
func (s *SQLiteStore) CreateDeploymentRecordsTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS deployment_records (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  previous_digest TEXT CHECK (previous_digest IS NULL OR length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  applied_by TEXT NOT NULL,
  is_rollback INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_deployment_records_worker ON deployment_records(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deployment_records_status ON deployment_records(status, started_at DESC);
`
	if _, err := s.db.Exec(ddl); err != nil {
		return err
	}
	return s.CreateWorkerDeploymentStateTableIfNotExists()
}
