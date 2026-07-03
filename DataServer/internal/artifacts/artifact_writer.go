// Package artifacts / artifact_writer.go — Blocco 4 step #5 split.
//
// artifact_writer.go owns SQL for the `artifacts` table inside the
// verified-finalization tx. The functions are unexported; external
// callers go through SQLiteFinalizationRepository.
//
// Split rationale: the previous `sqlite_finalization_repository.go`
// interleaved SQL across four tables (`artifacts`, `artifact_uploads`,
// `jobs`, `job_deliveries`) inside the same function body. The
// coordinator (sqlite_finalization_repository.go) still owns the
// tx lifecycle + the jobs.status='SUCCEEDED' write (kept inline to
// satisfy scan_test.go's allowlist), but per-table SQL moves to its
// own file:
//
//   - artifact_writer.go        (this file) — `artifacts` table
//   - upload_session_writer.go  — `artifact_uploads` table
//
// Atomicity is preserved exactly: each per-table call here runs
// inside the coordinator's *sql.Tx, joining the same tx as the jobs
// CAS + job_deliveries INSERT.
package artifacts

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// ── INPUT ROWS ──────────────────────────────────────────────────────────

// artifactStagingRow is the input to insertArtifactStagingInTx. Maps
// to the columns of the `artifacts` table that the coordinator writes
// during CreateArtifactAndUploadSession (Step A: STAGING insert).
type artifactStagingRow struct {
	ArtifactID      string
	JobID           string
	AttemptID       int64
	Kind            string
	StorageProvider string
	CreatedAt       time.Time
}

// artifactReadyFields is the input to casArtifactReadyInTx. Maps to
// the columns that the STAGING → READY CAS writes master-computed
// metadata for during FinalizeVerified (Step 3).
type artifactReadyFields struct {
	ArtifactID      string
	JobID           string
	StorageProvider string
	StorageKey      string
	SHA256Hex       string
	SizeBytes       int64
	MIMEType        string
	VerifiedAtStr   string
}

// ── IN-TX METHODS ───────────────────────────────────────────────────────

// insertArtifactStagingInTx inserts a STAGING artifacts row inside
// the coordinator's tx. Joins the same tx as the matching
// artifact_uploads row in CreateArtifactAndUploadSession so the pair
// commits or rolls back together.
func insertArtifactStagingInTx(
	ctx context.Context,
	tx *sql.Tx,
	row artifactStagingRow,
) error {
	if row.ArtifactID == "" || row.JobID == "" {
		return fmt.Errorf("artifacts: insertArtifactStagingInTx: artifact_id and job_id are required")
	}
	storageProvider := row.StorageProvider
	if storageProvider == "" {
		storageProvider = "local"
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifacts (id, job_id, attempt_id, type,
		                       storage_provider, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		row.ArtifactID, row.JobID, row.AttemptID, row.Kind,
		storageProvider, "STAGING", row.CreatedAt.UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("artifacts: insertArtifactStagingInTx: %w", err)
	}
	return nil
}

// casArtifactReadyInTx flips STAGING → READY and stamps
// master-computed metadata. Returns ErrTransitionConflict via the
// RowsAffected check when 0 rows matched (job_id/auth mismatch OR the
// row is no longer in STAGING).
func casArtifactReadyInTx(
	ctx context.Context,
	tx *sql.Tx,
	fields artifactReadyFields,
) error {
	if fields.ArtifactID == "" || fields.JobID == "" {
		return fmt.Errorf("artifacts: casArtifactReadyInTx: artifact_id and job_id are required")
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'READY',
		    storage_provider = ?,
		    storage_key = ?,
		    sha256 = ?, size_bytes = ?, mime_type = ?,
		    verified_at = ?
		WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
		fields.StorageProvider, fields.StorageKey,
		fields.SHA256Hex, fields.SizeBytes, fields.MIMEType,
		fields.VerifiedAtStr,
		fields.ArtifactID, fields.JobID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: casArtifactReadyInTx: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: artifacts affected=%d artifact=%s",
			ErrTransitionConflict, n, fields.ArtifactID)
	}
	return nil
}

// ── POST-TX PROJECTION ─────────────────────────────────────────────────

// loadArtifactProjection reads the post-commit `artifacts` row and
// returns the canonical *store.Artifact projection. Any Scan failure
// (including sql.ErrNoRows) wraps to a typed error — after a successful
// CAS UPDATE on the same id the row MUST exist; a non-found result is
// a data-integrity bug and must surface loudly, not be silently
// masked by a (nil, nil) fast-path.
//
// Lives in artifact_writer.go rather than the coordinator because the
// SELECT column list is owned by this writer — adding verified_at_full
// / retention_class / etc. happens here in exactly one place.
func loadArtifactProjection(ctx context.Context, db *sql.DB, id string) (*store.Artifact, error) {
	if id == "" {
		return nil, fmt.Errorf("artifacts: loadArtifactProjection: empty id")
	}
	row := db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, id)
	var a store.Artifact
	var verifiedAtStr string
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &verifiedAtStr, &a.CreatedAt,
	); err != nil {
		return nil, fmt.Errorf("artifacts: loadArtifactProjection: %w", err)
	}
	return &a, nil
}
