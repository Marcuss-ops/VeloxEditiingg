package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrArtifactAlreadyQuarantined indicates that a concurrent reconciler or
// foreground transition already moved the artifact out of READY.
var ErrArtifactAlreadyQuarantined = errors.New("store: artifact already quarantined")

// ReadyArtifact is the persistence projection used by the artifact reconciler
// to compare durable READY rows with final blobs on disk.
type ReadyArtifact struct {
	ArtifactID string
	StorageKey string
	VerifiedAt time.Time
}

// ArtifactReconcilerRepository owns the SQL used by the artifact cleanup
// orchestrator. It exposes domain-shaped projections and atomic/CAS methods;
// callers never receive *sql.Rows or *sql.Tx.
type ArtifactReconcilerRepository struct {
	db      *sql.DB
	gcStore *ArtifactGCStore
}

func NewArtifactReconcilerRepository(db *sql.DB) *ArtifactReconcilerRepository {
	if db == nil {
		panic("store: NewArtifactReconcilerRepository requires a non-nil database")
	}
	return &ArtifactReconcilerRepository{db: db, gcStore: NewArtifactGCStore(db)}
}

// NewArtifactReconcilerRepositoryFromStore binds artifact cleanup queries to
// the canonical SQLiteStore. The reconciler receives domain-shaped methods,
// never a raw database handle.
func NewArtifactReconcilerRepositoryFromStore(s *SQLiteStore) *ArtifactReconcilerRepository {
	if s == nil || s.db == nil {
		panic("store: NewArtifactReconcilerRepositoryFromStore requires a non-nil SQLiteStore")
	}
	return &ArtifactReconcilerRepository{db: s.db, gcStore: NewArtifactGCStore(s.db)}
}

// GCStore returns the typed artifact-GC gateway backed by the same database.
func (r *ArtifactReconcilerRepository) GCStore() *ArtifactGCStore { return r.gcStore }

func (r *ArtifactReconcilerRepository) ListReadyArtifacts(ctx context.Context) (map[string]ReadyArtifact, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT storage_key, id, COALESCE(verified_at, '')
		FROM artifacts
		WHERE status = 'READY'
		  AND storage_provider = 'local'
		  AND storage_key <> ''
		  AND verified_at IS NOT NULL AND verified_at <> ''`)
	if err != nil {
		return nil, fmt.Errorf("store: list ready artifacts: %w", err)
	}
	defer rows.Close()

	out := make(map[string]ReadyArtifact, 1024)
	for rows.Next() {
		var key, id, verified string
		if err := rows.Scan(&key, &id, &verified); err != nil {
			return nil, fmt.Errorf("store: list ready artifacts scan: %w", err)
		}
		var verifiedAt time.Time
		if verified != "" {
			parsed, err := time.Parse(time.RFC3339, verified)
			if err != nil {
				return nil, fmt.Errorf("store: list ready artifacts parse verified_at: %w", err)
			}
			verifiedAt = parsed
		}
		out[key] = ReadyArtifact{ArtifactID: id, StorageKey: key, VerifiedAt: verifiedAt}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list ready artifacts rows: %w", err)
	}
	return out, nil
}

func (r *ArtifactReconcilerRepository) ListStuckArtifacts(ctx context.Context, olderThan time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id FROM artifacts
		WHERE status = 'STAGING'
		  AND created_at <> ''
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, olderThan.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("store: list stuck artifacts: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: list stuck artifacts scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list stuck artifacts rows: %w", err)
	}
	return ids, nil
}

func (r *ArtifactReconcilerRepository) EnqueueArtifactGC(ctx context.Context, artifactID, reason string, eligibleAt time.Time) error {
	return r.gcStore.EnqueueArtifactGCCandidate(ctx, artifactID, reason, eligibleAt)
}

func (r *ArtifactReconcilerRepository) MarkStuckArtifactFailed(ctx context.Context, artifactID string) (bool, error) {
	res, err := r.db.ExecContext(ctx, `
		UPDATE artifacts SET status = 'FAILED'
		WHERE id = ? AND status = 'STAGING'`, artifactID)
	if err != nil {
		return false, fmt.Errorf("store: mark stuck artifact failed: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// QuarantineReadyArtifact atomically changes READY -> QUARANTINED and emits
// the outbox event in the same transaction. A missing outbox table is
// surfaced to the caller; unlike the old split implementation, the status
// and event cannot diverge silently.
func (r *ArtifactReconcilerRepository) QuarantineReadyArtifact(ctx context.Context, artifactID, reason string, now time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: quarantine begin: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE artifacts SET status = 'QUARANTINED'
		WHERE id = ? AND status = 'READY'`, artifactID)
	if err != nil {
		return fmt.Errorf("store: quarantine update: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrArtifactAlreadyQuarantined
	}

	payload, err := json.Marshal(map[string]string{
		"artifact_id": artifactID,
		"reason":      reason,
		"detected_at": now.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("store: quarantine payload: %w", err)
	}
	nowStr := now.UTC().Format(time.RFC3339)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload_json, status, available_at, created_at)
		VALUES ('artifact', ?, 'ARTIFACT_QUARANTINED', ?, 'PENDING', ?, ?)`,
		artifactID, payload, nowStr, nowStr); err != nil {
		return fmt.Errorf("store: quarantine outbox insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: quarantine commit: %w", err)
	}
	return nil
}

var _ = (*ArtifactReconcilerRepository)(nil)
