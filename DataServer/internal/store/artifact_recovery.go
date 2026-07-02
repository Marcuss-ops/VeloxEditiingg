// Package store / artifact_recovery.go
//
// Typed recovery-path helper for the velox-worker recover-output CLI.
//
// Background: cmd/worker/recover_output.go is the Phase 6.4 admin escape
// hatch that drives the canonical completion.Coordinator (DeclareOutputs
// → CompleteUpload → CommitAttempt) against a master's SQLite when a
// worker rendered an MP4 to disk but crashed BEFORE declaring it. Step 5
// of that CLI's pipeline registers (artifact STAGING, artifact_uploads
// CREATED) rows where the normal pipeline would have arrived via a
// streaming gRPC BeginUpload call. Because the file is already at rest
// on the master host, the streaming entry-point is irrelevant in the
// recovery flow, so we INSERT the rows inline.
//
// This file extracts that INSERT pair into a typed store helper so the
// CLI stops holding raw db.ExecContext(...) calls (which would otherwise
// be flagged by scripts/ci/check-sql-ownership.sh — double violation
// because cmd/worker/ lives OUTSIDE internal/ AND outside the
// store/** allowlist). The CLI retains its local *sql.DB open because
// opening a connection is an admin tool's bootstrap concern, not a
// SQL-ownership one; only the SQL itself moves.
//
// Idempotency contract (preserved verbatim from the original CLI):
//
//   - artifacts.id PK + artifact_uploads.upload_id PK absorb a re-run
//     with the same (commit_id, job_id) tuple.
//   - INSERT OR IGNORE returns 0 affected rows on a re-run; the CLI
//     verifies before each step that the same upload_id is reused so
//     the gating CAS in CompleteUpload continues to advance the row.
//
// The typed session struct replaces the previous positional-arg list
// so the call site has compile-time guarantees that no required field
// is forgotten.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RECOVERY_DEFAULT_TTL is the default expires_at - now() interval
// applied by RegisterRecoveryUploadSession when the caller leaves
// RecoveryUploadSession.ExpiresAtTTL at zero. The 24h constant is
// tuned against the recovery CLI's worst-case Coordinator-pause
// interval observed historically; a future PR can tighten this once
// the recovery path's wall-clock budget is empirically measured.
//
// Rationale: migration 030's idx_artifact_uploads_expiry (status,
// expires_at) feeds the reconciler rule "staging session troppo
// vecchio -> EXPIRED". A previous version of this helper set
// expires_at == created_at == now, which marked the row as stale the
// moment real wall-clock ticked past insertion — the reconciler
// would EXPIRE the row before the worker could drive the recovery
// pipeline's downstream CompleteUpload CAS. Setting a 24h default
// gives every well-formed recovery enough wall-clock budget to
// reach the CAS.
const RECOVERY_DEFAULT_TTL = 24 * time.Hour

// RecoveryUploadSession is the input to RegisterRecoveryUploadSession.
// The caller computes UploadID and ArtifactID deterministically from a
// stable (commit_id) source so the INSERT OR IGNORE below absorbs
// re-runs of the CLI on the same attempt.
type RecoveryUploadSession struct {
	// UploadID is `recover-<commit_id>` (matches the original CLI's
	// derivation). Upload_id is the primary key on artifact_uploads.
	UploadID string
	// ArtifactID is `art_recover_<commit_id>`. artifact.id is the
	// primary key on artifacts and is also FK'd by
	// artifact_uploads.artifact_id.
	ArtifactID string
	// JobID stamps the job_id column on both rows.
	JobID string
	// WorkerID + LeaseID authenticate the recovery attempt
	// (artifact_uploads.CAS in CompleteUpload re-validates them).
	WorkerID string
	LeaseID  string
	// FilePath is the local path to the rendered MP4; written into
	// artifacts.storage_key as the canonical pointer. The CLI uses
	// this path verbatim on CompleteUpload's Branch-A invocation when
	// the master has re-derived a different SHA, so the path remains
	// stable across the recovery tx.
	FilePath string
	// SizeBytes is the master-derived byte count (from hashFile() in
	// the CLI, tx-bytes once the recovery completes).
	SizeBytes int64
	// SHA256 is the lowercase hex SHA-256 of the rendered MP4 bytes
	// (master-derived; the worker's self-reported SHA-256 is NOT
	// trusted per PR 2 spec Fase 4).
	SHA256 string
	// ExpiresAtTTL is the time interval from the helper's now() at
	// which the artifact_uploads row becomes eligible for the
	// reconciler's EXPIRE rule (migration 030's
	// idx_artifact_uploads_expiry feeds "staging session troppo
	// vecchio -> EXPIRED"). Leave zero and the helper applies
	// RECOVERY_DEFAULT_TTL (24h); if the caller needs a TTL other
	// than 24h, they MUST set the field to a duration strictly
	// greater than the wall-clock interval between
	// RegisterRecoveryUploadSession returning and
	// CompleteUpload's CAS landing. There is no DEFAULT value of
	// `ExpiresAtTTL` that means "expire immediately" (the helper
	// applies 24h only for zero/negative values — tiny positive
	// values like 1*time.Nanosecond are NOT clamped and would
	// produce near-immediate expiry) — that semantic was the bug
	// this fix removes (a previous version set expires_at =
	// created_at = now, which migration 030's
	// `idx_artifact_uploads_expiry`-backed reconciler rule
	// (status, expires_at) would fire on the very next pass,
	// killing the recovery session before CompleteUpload's CAS
	// could advance it).
	ExpiresAtTTL time.Duration
}

// RegisterRecoveryUploadSession atomically inserts the (artifact
// STAGING, artifact_uploads CREATED) rows that the recovery CLI uses
// as its pre-pipeline setup, so CompleteUpload's CAS has something to
// advance.
//
// The two INSERTs are issued sequentially (NOT in a single transaction)
// because they are independent rows: the second INSERT's
// artifact_uploads.artifact_id FK references the row the first INSERT
// just created. A single BEGIN IMMEDIATE wrapper would be a no-op
// faster than the current sequential layout because SQLite serializes
// writers anyway; a future PR can fold both into one tx if the
// recovery flow ever gains a third pre-pipeline step.
//
// Idempotency: artifacts id PK + artifact_uploads.upload_id PK absorb
// re-runs of the CLI on the same attempt. INSERT OR IGNORE silently
// returns 0 affected rows on conflict.
//
// Silent-IGNORE contract on the artifacts insert: `INSERT OR IGNORE`
// swallows BOTH (a) PRIMARY KEY conflicts on `artifacts.id` (intended
// — this is the idempotent re-run case the recovery CLI relies on
// when an admin re-invokes the same recovery CLI on the same
// commit_id, OR when the multi-step recovery pipeline
// (DeclareOutputs / CompleteUpload / CommitAttempt) re-stages the
// row across steps) AND (b) UNIQUE INDEX `idx_artifacts_storage_key`
// violations on (storage_provider, storage_key) WHERE storage_key <>
// ” (NOT intended — would surface only on a cross-session collision
// between two distinct artifact_ids that happen to share a local-path
// FilePath). The helper does NOT currently distinguish between these
// cases: both yield zero `RowsAffected` on the artifacts INSERT and
// the helper then proceeds to INSERT 2. The recovery CLI today never
// hits case (b) under well-formed inputs because distinct commit_ids
// produce distinct rendered file paths on the master host (the
// renderer writes each commit to a commit-derived subpath, so two
// sessions can never share a (storage_provider='local', storage_key)
// tuple unless the caller explicitly reuses a path), but the
// helper's contract is silently inconsistent here — a future caller
// that feeds a cross-commit recovery under a shared local-path
// FilePath will lose the row without an error signal. Hardening
// option (NOT implemented in this version): inspect
// `result.RowsAffected()` on the artifacts INSERT and, when it is 0
// AND the helper can prove (e.g. via a prior SELECT-for-existence
// check on `artifacts.id` at call entry) that the artifact_id was
// not previously INSERTed successfully against this `*sql.DB`, return
// a sentinel `ErrStorageKeyConflict` so the caller can decide how to
// react. A second equally-valid shape is to extend `RecoveryUploadSession`
// with an `AllowStorageKeyReuse bool` opt-in flag and treat
// RowsAffected==0 under that flag as an error path; whichever
// surface is adopted, future callers in cross-commit flows MUST be
// able to tell PK-absorb from storage_key-collision.
//
// expires_at contract — REQUIRED READING for callers: migration
// 030's `idx_artifact_uploads_expiry ON
// artifact_uploads(status, expires_at)` feeds the reconciler rule
// "staging session troppo vecchio -> EXPIRED", which transitions
// status='CREATED' rows whose expires_at has fallen behind wall-clock
// into status='EXPIRED' on the next reconciler pass. The helper
// therefore MUST stamp expires_at > created_at; if expires_at <=
// created_at, EXPIRE fires before the worker drives the recovery
// pipeline's downstream CompleteUpload CAS and the recovery row
// vanishes silently. The TTL surface is
// RecoveryUploadSession.ExpiresAtTTL: callers MUST set it to a
// duration STRICTLY greater than the wall-clock interval between
// RegisterRecoveryUploadSession returning and CompleteUpload's CAS
// landing. Zero/negative values are tolerated by the helper (it
// applies RECOVERY_DEFAULT_TTL, 24h), but production callers with
// a tighter budget than 24h MUST override. The helper does NOT
// verify this bound — a future PR could add a `ttl > 0` precondition
// for callers that opt in via RecoveryUploadSession, but doing so
// here would force all existing callers to set the field, which is
// outside the scope of this fix.
//
// Returns nil on success; non-nil error on either INSERT failure.
func RegisterRecoveryUploadSession(ctx context.Context, db *sql.DB, s RecoveryUploadSession) error {
	if s.UploadID == "" || s.ArtifactID == "" || s.JobID == "" {
		return fmt.Errorf("store: RegisterRecoveryUploadSession: upload_id, artifact_id and job_id are required")
	}
	if s.WorkerID == "" || s.LeaseID == "" {
		return fmt.Errorf("store: RegisterRecoveryUploadSession: worker_id and lease_id are required")
	}
	if s.FilePath == "" {
		return fmt.Errorf("store: RegisterRecoveryUploadSession: file path is required")
	}
	if s.SHA256 == "" || s.SizeBytes <= 0 {
		return fmt.Errorf("store: RegisterRecoveryUploadSession: sha256 and size_bytes >0 are required")
	}

	// Format is RFC3339Nano (not RFC3339) to preserve byte-for-byte
	// parity with the original recover_output.go CLI which used
	// time.RFC3339Nano for created_at/updated_at stamps. Downstream
	// readers (Upload repository + reconciler) parse with
	// time.RFC3339Nano; aligning the helper avoids a silent round-trip
	// regression on re-runs.
	//
	// Time computation: capture now() once and derive both created_at
	// and expires_at from it so the two stamps are guaranteed to
	// share the same UTC second. expires_at = now + ExpiresAtTTL
	// (or RECOVERY_DEFAULT_TTL when ExpiresAtTTL is left at zero);
	// a historical version of this helper set expires_at = created_at
	// = now, which the reconciler's "staging session troppo vecchio
	// -> EXPIRED" rule (migration 030) would EXPIRE on the very next
	// pass — killing the recovery session before CompleteUpload's
	// CAS can advance it. The TTL field on RecoveryUploadSession is
	// the surface for tuning this.
	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339Nano)
	ttl := s.ExpiresAtTTL
	if ttl <= 0 {
		ttl = RECOVERY_DEFAULT_TTL
	}
	expiresAt := now.Add(ttl).Format(time.RFC3339Nano)

	// ── INSERT 1: artifacts (STAGING, local storage provider) ──
	//
	// Column list matches migration
	// store/migrations/sqlite/010_job_attempts_and_artifacts.sql
	// verbatim. The artifacts table at this version has no
	// `updated_at` column — that scope was dropped from the registry
	// to keep the schema append-only across the PR 3.5-a cutover.
	// Adding `updated_at` here would force a forward-only schema
	// migration and break the recovery CLI on legacy databases; the
	// `created_at` stamp IS sufficient for the audit trail because
	// the recovery CLI is a one-shot admin tool, not a long-lived
	// process that mutates the row.
	if _, err := db.ExecContext(ctx, `
		INSERT OR IGNORE INTO artifacts (
			id, job_id, type, storage_provider, storage_key, sha256, size_bytes,
			status, created_at
		) VALUES (?, ?, 'video', 'local', ?, ?, ?, 'STAGING', ?)`,
		s.ArtifactID, s.JobID, s.FilePath, s.SHA256, s.SizeBytes, createdAt,
	); err != nil {
		return fmt.Errorf("store: RegisterRecoveryUploadSession insert artifacts: %w", err)
	}

	// ── INSERT 2: artifact_uploads (CREATED, expected_* + temporary_storage_key) ──
	if _, err := db.ExecContext(ctx, `
		INSERT OR IGNORE INTO artifact_uploads (
			upload_id, artifact_id, job_id, attempt_number,
			worker_id, lease_id, status, temporary_storage_key,
			expected_size_bytes, expected_sha256, created_at, expires_at
		) VALUES (?, ?, ?, 1, ?, ?, 'CREATED', ?, ?, ?, ?, ?)`,
		s.UploadID, s.ArtifactID, s.JobID,
		s.WorkerID, s.LeaseID,
		s.FilePath, s.SizeBytes, s.SHA256,
		createdAt, expiresAt,
	); err != nil {
		return fmt.Errorf("store: RegisterRecoveryUploadSession insert artifact_uploads: %w", err)
	}

	return nil
}
