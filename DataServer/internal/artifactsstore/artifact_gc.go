package artifactsstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/storecore"
)

const (
	ArtifactGCEligible = "ELIGIBLE"
	ArtifactGCDeleting = "DELETING"
	ArtifactGCDeleted  = "DELETED"
	ArtifactGCFailed   = "FAILED"
)

// ArtifactGCCandidate is a durable deletion lease. The artifact row remains
// authoritative; this table only records why and when its bytes may be
// removed.
type ArtifactGCCandidate struct {
	ArtifactID      string
	Reason          string
	EligibleAt      time.Time
	LeaseOwner      string
	LeaseExpiresAt  *time.Time
	DeleteAttempts  int
	LastError       string
	Status          string
	StorageProvider string
	StorageKey      string
	LocalPath       string
}

// ArtifactGCStore exposes the GC methods to orchestration code that owns the
// shared database handle but not the full SQLiteStore lifecycle.
type ArtifactGCStore struct{ db *sql.DB }

func NewArtifactGCStore(db *sql.DB) *ArtifactGCStore {
	if db == nil {
		panic("artifactsstore: NewArtifactGCStore requires a non-nil database")
	}
	return &ArtifactGCStore{db: db}
}

// EnqueueArtifactGCCandidate marks an artifact eligible without deleting it.
// Repeated enqueue calls are harmless and never resurrect a completed delete.
func (g *ArtifactGCStore) EnqueueArtifactGCCandidate(ctx context.Context, artifactID, reason string, eligibleAt time.Time) error {
	if artifactID == "" || reason == "" {
		return fmt.Errorf("artifact gc: artifact_id and reason are required")
	}
	if eligibleAt.IsZero() {
		eligibleAt = time.Now().UTC()
	}
	_, err := g.db.ExecContext(ctx, `
		INSERT INTO artifact_gc_candidates (artifact_id, reason, eligible_at, status)
		VALUES (?, ?, ?, 'ELIGIBLE')
		ON CONFLICT(artifact_id) DO UPDATE SET
			reason=excluded.reason,
			eligible_at=MIN(artifact_gc_candidates.eligible_at, excluded.eligible_at),
			status=CASE WHEN artifact_gc_candidates.status IN ('DELETED')
				THEN artifact_gc_candidates.status ELSE 'ELIGIBLE' END`,
		artifactID, reason, eligibleAt.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("artifact gc enqueue: %w", err)
	}
	return nil
}

// LeaseArtifactGCCandidates claims rows so only one worker removes a file.
func (g *ArtifactGCStore) LeaseArtifactGCCandidates(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]ArtifactGCCandidate, error) {
	if owner == "" {
		return nil, fmt.Errorf("artifact gc: lease owner is required")
	}
	if lease <= 0 {
		lease = 15 * time.Minute
	}
	if limit <= 0 {
		limit = 100
	}
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("artifact gc lease begin: %w", err)
	}
	defer tx.Rollback()

	now = now.UTC()
	expires := now.Add(lease)
	rows, err := tx.QueryContext(ctx, `
		SELECT c.artifact_id, c.reason, c.eligible_at, c.delete_attempts,
		       c.last_error, c.status, c.lease_expires_at,
		       COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
		       COALESCE(a.local_path,'')
		FROM artifact_gc_candidates c
		JOIN artifacts a ON a.id = c.artifact_id
		WHERE c.eligible_at <= ?
		  AND (c.status = 'ELIGIBLE' OR (c.status = 'DELETING' AND c.lease_expires_at < ?))
		ORDER BY c.eligible_at ASC LIMIT ?`, now.Format(time.RFC3339), now.Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("artifact gc lease select: %w", err)
	}
	var candidates []ArtifactGCCandidate
	for rows.Next() {
		var c ArtifactGCCandidate
		var eligible string
		var leaseExpires sql.NullString
		if err := rows.Scan(&c.ArtifactID, &c.Reason, &eligible, &c.DeleteAttempts,
			&c.LastError, &c.Status, &leaseExpires, &c.StorageProvider, &c.StorageKey, &c.LocalPath); err != nil {
			rows.Close()
			return nil, fmt.Errorf("artifact gc lease scan: %w", err)
		}
		c.EligibleAt, _ = time.Parse(time.RFC3339, eligible)
		if leaseExpires.Valid && leaseExpires.String != "" {
			if parsed, e := time.Parse(time.RFC3339, leaseExpires.String); e == nil {
				c.LeaseExpiresAt = &parsed
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE artifact_gc_candidates SET status='DELETING', lease_owner=?, lease_expires_at=? WHERE artifact_id=? AND (status='ELIGIBLE' OR (status='DELETING' AND lease_expires_at < ?))`, owner, expires.Format(time.RFC3339), c.ArtifactID, now.Format(time.RFC3339))
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("artifact gc lease update: %w", err)
		}
		n, err := storecore.ReadRowsAffected(res, "artifact gc lease")
		if err != nil {
			rows.Close()
			return nil, err
		}
		if n == 1 {
			c.LeaseOwner, c.LeaseExpiresAt, c.Status = owner, &expires, ArtifactGCDeleting
			candidates = append(candidates, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifact gc lease rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifact gc lease commit: %w", err)
	}
	return candidates, nil
}

// CompleteArtifactGC records the result of an external file deletion. The
// DB artifact is marked DELETED only after the bytes are gone or absent.
func (g *ArtifactGCStore) CompleteArtifactGC(ctx context.Context, artifactID, owner string, deleted bool, deleteErr string) error {
	if artifactID == "" || owner == "" {
		return fmt.Errorf("artifact gc: artifact_id and owner are required")
	}
	if deleted {
		result, err := g.db.ExecContext(ctx, `UPDATE artifact_gc_candidates SET status='DELETED', lease_owner='', lease_expires_at=NULL, last_error='' WHERE artifact_id=? AND status='DELETING' AND lease_owner=?`, artifactID, owner)
		if err != nil {
			return fmt.Errorf("artifact gc complete: %w", err)
		}
		if n, err := storecore.ReadRowsAffected(result, "artifact gc complete"); err != nil {
			return err
		} else if n != 1 {
			return fmt.Errorf("artifact gc complete: lease not owned or already completed")
		}
		if _, err := g.db.ExecContext(ctx, `UPDATE artifacts SET status='DELETED' WHERE id=? AND status IN ('FAILED','QUARANTINED','DELETED')`, artifactID); err != nil {
			return fmt.Errorf("artifact gc artifact status: %w", err)
		}
		return nil
	}
	result, err := g.db.ExecContext(ctx, `UPDATE artifact_gc_candidates SET status='ELIGIBLE', lease_owner='', lease_expires_at=NULL, delete_attempts=delete_attempts+1, last_error=? WHERE artifact_id=? AND status='DELETING' AND lease_owner=?`, deleteErr, artifactID, owner)
	if err != nil {
		return fmt.Errorf("artifact gc failure: %w", err)
	}
	if n, err := storecore.ReadRowsAffected(result, "artifact gc failure"); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("artifact gc failure: lease not owned or already completed")
	}
	return nil
}
