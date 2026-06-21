package store

import (
	"context"
	"fmt"
	"path/filepath"
	"time"
)

// ArtifactMaintenanceRepository is the persistence contract for artifact
// maintenance operations used by the reconciler and other background tasks.
type ArtifactMaintenanceRepository interface {
	// ListReadyLocal returns all READY artifacts with local storage provider
	// and non-empty storage_key + verified_at.
	ListReadyLocal(ctx context.Context) ([]ReadyLocalArtifact, error)
	// QuarantineArtifact flips an artifact from READY → QUARANTINED.
	// It returns ErrArtifactAlreadyQuarantined when 0 rows are affected
	// (already terminal). If the outbox event emission fails, it returns
	// ErrQuarantineStatusOnly so callers can distinguish full success from
	// status-only success.
	QuarantineArtifact(ctx context.Context, artifactID, reason string) error
	// ListStuckStaging returns the IDs of STAGING artifacts older than
	// cutoff, bounded by limit.
	ListStuckStaging(ctx context.Context, cutoff time.Time, limit int) ([]string, error)
	// MarkStuckArtifactFailed CAS-flips a STAGING artifact to FAILED.
	// Returns true when exactly 1 row was affected.
	MarkStuckArtifactFailed(ctx context.Context, id string) (bool, error)
}

// ReadyLocalArtifact is one READY row with local storage.
type ReadyLocalArtifact struct {
	ID         string
	StorageKey string
	VerifiedAt time.Time
}

// SQLiteArtifactMaintenanceRepository implements ArtifactMaintenanceRepository.
type SQLiteArtifactMaintenanceRepository struct {
	store *SQLiteStore
}

// NewSQLiteArtifactMaintenanceRepository creates a maintenance repo.
func NewSQLiteArtifactMaintenanceRepository(store *SQLiteStore) *SQLiteArtifactMaintenanceRepository {
	return &SQLiteArtifactMaintenanceRepository{store: store}
}

// Compile-time check.
var _ ArtifactMaintenanceRepository = (*SQLiteArtifactMaintenanceRepository)(nil)

// ListReadyLocal selects all READY rows with local storage provider.
func (r *SQLiteArtifactMaintenanceRepository) ListReadyLocal(ctx context.Context) ([]ReadyLocalArtifact, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT storage_key, id, COALESCE(verified_at, '')
		FROM artifacts
		WHERE status = 'READY'
		  AND storage_provider = 'local'
		  AND storage_key <> ''
		  AND verified_at IS NOT NULL AND verified_at <> ''`)
	if err != nil {
		return nil, fmt.Errorf("artifact maintenance: ListReadyLocal: %w", err)
	}
	defer rows.Close()

	var out []ReadyLocalArtifact
	for rows.Next() {
		var key, id, verifiedStr string
		if err := rows.Scan(&key, &id, &verifiedStr); err != nil {
			return nil, fmt.Errorf("artifact maintenance: ListReadyLocal scan: %w", err)
		}
		var ts time.Time
		if verifiedStr != "" {
			if t, perr := time.Parse(time.RFC3339, verifiedStr); perr == nil {
				ts = t
			}
		}
		out = append(out, ReadyLocalArtifact{
			ID:         id,
			StorageKey: filepath.ToSlash(key),
			VerifiedAt: ts,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifact maintenance: ListReadyLocal rows: %w", err)
	}
	return out, nil
}

// ErrArtifactAlreadyQuarantined is returned when the UPDATE matches 0 rows.
var ErrArtifactAlreadyQuarantined = fmt.Errorf("artifact maintenance: already terminal")

// ErrQuarantineStatusOnly is returned when status was committed but outbox
// event emission failed (best-effort).
var ErrQuarantineStatusOnly = fmt.Errorf("artifact maintenance: quarantine status committed but outbox deferred")

// QuarantineArtifact flips READY → QUARANTINED in its own tx, then emits
// an outbox event in a second tx. The two-phase pattern avoids poisoning
// the status tx if outbox_events is missing.
func (r *SQLiteArtifactMaintenanceRepository) QuarantineArtifact(ctx context.Context, artifactID, reason string) error {
	if artifactID == "" {
		return fmt.Errorf("artifact maintenance: QuarantineArtifact: empty artifactID")
	}

	// Phase 1: flip status.
	tx1, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("artifact maintenance: quarantine begin-status: %w", err)
	}
	res, err := tx1.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'QUARANTINED'
		WHERE id = ? AND status = 'READY'`, artifactID)
	if err != nil {
		_ = tx1.Rollback()
		return fmt.Errorf("artifact maintenance: quarantine UPDATE: %w", err)
	}
	affected, rerr := res.RowsAffected()
	if rerr != nil {
		_ = tx1.Rollback()
		return rerr
	}
	if affected == 0 {
		_ = tx1.Rollback()
		return ErrArtifactAlreadyQuarantined
	}
	if err := tx1.Commit(); err != nil {
		return fmt.Errorf("artifact maintenance: quarantine commit-status: %w", err)
	}

	// Phase 2: emit outbox event (best-effort).
	now := time.Now().UTC().Format(time.RFC3339)
	payload := fmt.Sprintf(`{"artifact_id":%q,"reason":%q,"detected_at":%q}`,
		artifactID, reason, now)

	tx2, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrQuarantineStatusOnly
	}
	if _, err := tx2.ExecContext(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, status, available_at, created_at)
		VALUES ('artifact', ?, 'ARTIFACT_QUARANTINED', ?, 'PENDING', ?, ?)`,
		artifactID, payload, now, now); err != nil {
		_ = tx2.Rollback()
		return ErrQuarantineStatusOnly
	}
	if err := tx2.Commit(); err != nil {
		return ErrQuarantineStatusOnly
	}
	return nil
}

// ListStuckStaging returns STAGING artifact IDs older than cutoff.
func (r *SQLiteArtifactMaintenanceRepository) ListStuckStaging(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id FROM artifacts
		WHERE status = 'STAGING'
		  AND created_at <> ''
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, cutoff.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("artifact maintenance: ListStuckStaging: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("artifact maintenance: ListStuckStaging scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifact maintenance: ListStuckStaging rows: %w", err)
	}
	return ids, nil
}

// MarkStuckArtifactFailed CAS-flips STAGING → FAILED.
func (r *SQLiteArtifactMaintenanceRepository) MarkStuckArtifactFailed(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("artifact maintenance: MarkStuckArtifactFailed: empty id")
	}
	res, err := r.store.db.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'FAILED'
		WHERE id = ? AND status = 'STAGING'`, id)
	if err != nil {
		return false, fmt.Errorf("artifact maintenance: MarkStuckArtifactFailed: %w", err)
	}
	affected, rerr := res.RowsAffected()
	if rerr != nil {
		return false, fmt.Errorf("artifact maintenance: MarkStuckArtifactFailed rows: %w", rerr)
	}
	return affected == 1, nil
}
