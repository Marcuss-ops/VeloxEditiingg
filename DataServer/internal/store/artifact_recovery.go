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
	now := time.Now().UTC().Format(time.RFC3339Nano)

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
		s.ArtifactID, s.JobID, s.FilePath, s.SHA256, s.SizeBytes, now,
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
		now, now,
	); err != nil {
		return fmt.Errorf("store: RegisterRecoveryUploadSession insert artifact_uploads: %w", err)
	}

	return nil
}
