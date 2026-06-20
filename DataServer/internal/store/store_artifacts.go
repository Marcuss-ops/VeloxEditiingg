// Package store / store_artifacts.go
//
// Methods backing internal/queue.ArtifactFinalizationService (PR2b).
// The artifact state machine is keyed by artifact.id (not job_id) to support
// jobs that produce multiple artifacts (e.g. audio + video + thumbnail).
//
// TransitionArtifactStatus is the Tx1 lock: STAGING → VERIFYING.
// FinalizeArtifactVerified is the Tx2 commit: VERIFYING → READY with
// verified_at + sha256 + size_bytes + mime_type + duration_ms stamped.
//
// Both run in BEGIN IMMEDIATE so concurrent finalizers cannot race on the
// same row. The application layer enforces state machine legality; SQL
// CHECK constraints are not added so legacy rows from before migration 021
// continue to load.
//
// PR 8 + PR 9 cutover: ARTIFACT_READY outbox emission goes through the
// generic outbox.Store (outbox_events table) instead of the legacy
// orchestrator_outbox table.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/outbox"
)

// ErrArtifactTransitionConflict is returned when TransitionArtifactStatus
// finds the row in a state that does not match the expected `from`. Callers
// should surface this as "already finalizing / already verified".
var ErrArtifactTransitionConflict = errors.New("artifact: transition from-state mismatch")

// ErrArtifactNotFound is returned when the artifact_id does not exist.
var ErrArtifactNotFound = errors.New("artifact: not found")

// TransitionArtifactStatus atomically moves an artifact from `from` to `to`.
// Used as Tx1 (STAGING → VERIFYING) and as the late Verifying→Quarantined
// rollback path. SQL is just the canonical atomic UPDATE with the from-state
// filter — race-free because SQLite serializes writers.
func (s *SQLiteStore) TransitionArtifactStatus(ctx context.Context, artifactID, from, to string) error {
	if artifactID == "" || from == "" || to == "" {
		return fmt.Errorf("artifact: TransitionArtifactStatus: missing required arg")
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE artifacts SET status = ? WHERE id = ? AND status = ?`,
		to, artifactID, from,
	)
	if err != nil {
		return fmt.Errorf("artifact: TransitionArtifactStatus exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// diagnostics: distinguish "wrong state" vs "no row"
		var status string
		row := s.db.QueryRowContext(ctx, `SELECT status FROM artifacts WHERE id = ?`, artifactID)
		switch err := row.Scan(&status); err {
		case sql.ErrNoRows:
			return ErrArtifactNotFound
		case nil:
			if status != from {
				return fmt.Errorf("%w: artifact=%s current=%s expected=%s target=%s",
					ErrArtifactTransitionConflict, artifactID, status, from, to)
			}
			// status matches but row still didn't update — concurrent writer stole it
			return ErrArtifactTransitionConflict
		default:
			return err
		}
	}
	return nil
}

// FinalizeArtifactVerified stamps the artifact with sha256 + size + mime +
// verified_at + duration_ms and transitions VERIFYING → READY in a single
// BEGIN IMMEDIATE transaction. Returns the post-update Artifact row.
//
// Idempotent on re-run: once a row is in READY, the WHERE filter (status =
// 'VERIFYING') matches zero rows and the function returns ErrArtifactTransitionConflict;
// callers must treat this as success (already finalized).
//
// PR 8 + PR 9: an ARTIFACT_READY outbox event is enqueued INSIDE the
// same transaction as the artifact status flip (transactional-outbox
// pattern). Aggregate_id = artifact_id so downstream consumers
// (DeliveryRunner, JobSummary projection) can recover the source
// artifact directly. Build the payload BEFORE commit so a crash between
// the state-change commit and the outbox INSERT cannot silently drop the
// event.
func (s *SQLiteStore) FinalizeArtifactVerified(ctx context.Context, artifactID, sha256 string, sizeBytes int64, mimeType string) (*Artifact, error) {
	if artifactID == "" {
		return nil, fmt.Errorf("artifact: FinalizeArtifactVerified: missing artifact_id")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx,
		`UPDATE artifacts
		 SET status = 'READY',
		     sha256 = ?, size_bytes = ?, mime_type = ?,
		     verified_at = ?
		 WHERE id = ? AND status = 'VERIFYING'`,
		sha256, sizeBytes, mimeType, now, artifactID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifact: FinalizeArtifactVerified: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var status string
		row := tx.QueryRowContext(ctx, `SELECT status FROM artifacts WHERE id = ?`, artifactID)
		switch err := row.Scan(&status); err {
		case sql.ErrNoRows:
			return nil, ErrArtifactNotFound
		case nil:
			if status == "READY" {
				// Idempotent re-run: return the current row without re-stamping it.
				if err := tx.Commit(); err != nil {
					return nil, fmt.Errorf("artifact: FinalizeArtifactVerified: idempotent commit: %w", err)
				}
				return s.GetArtifact(artifactID)
			}
			return nil, fmt.Errorf("%w: artifact=%s current=%s",
				ErrArtifactTransitionConflict, artifactID, status)
		default:
			return nil, err
		}
	}

	row := tx.QueryRowContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		        COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		        COALESCE(local_path, ''), COALESCE(sha256, ''),
		        COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		        status, COALESCE(verified_at, ''), created_at
		 FROM artifacts WHERE id = ?`, artifactID)
	var a Artifact
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &a.VerifiedAt, &a.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrArtifactNotFound
		}
		return nil, fmt.Errorf("artifact: FinalizeArtifactVerified scan: %w", err)
	}

	// Enqueue the outbox event INSIDE the same tx so commit is the
	// single atomicity boundary. emitOutbox forwards `tx` as the Executor
	// so the INSERT joins the same write tx as the UPDATE above — a
	// crash before commit means both state change and event roll back;
	// a successful commit makes both rows durably visible in one step.
	//
	// If the outbox INSERT fails (busy DB, schema drift, etc.), we
	// rollback the tx rather than commit a state change with no
	// downstream notification. That preserves the transactional outbox
	// guarantee: either both rows become visible, or neither does.
	payload := []byte(fmt.Sprintf(
		`{"artifact_id":"%s","sha256":"%s","size_bytes":%d,"mime_type":"%s","job_id":%q}`,
		a.ID, a.SHA256, a.SizeBytes, mimeType, a.JobID,
	))
	if err := s.emitOutbox(ctx, tx, outbox.InsertParams{
		AggregateType: "artifact",
		AggregateID:   a.ID,
		EventType:     "ARTIFACT_READY",
		Payload:       payload,
	}); err != nil {
		return nil, fmt.Errorf("artifact: FinalizeArtifactVerified: outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifact: FinalizeArtifactVerified: commit: %w", err)
	}

	return &a, nil
}
