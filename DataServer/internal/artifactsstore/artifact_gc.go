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
			status=CASE WHEN artifact_gc_candidates.status IN ('DELETED','DELETING')
				THEN artifact_gc_candidates.status ELSE 'ELIGIBLE' END`,
		artifactID, reason, eligibleAt.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("artifact gc enqueue: %w", err)
	}
	return nil
}

// EnqueueQuarantinedArtifactsForRetention makes old local quarantined blobs
// eligible for deletion using the durable ARTIFACT_QUARANTINED event as the
// quarantine timestamp. The quarantine age is evaluated before a candidate is
// created, so the normal GC lease path remains the only byte deletion path.
func (g *ArtifactGCStore) EnqueueQuarantinedArtifactsForRetention(ctx context.Context, before, eligibleAt time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := g.db.QueryContext(ctx, `
		SELECT a.id
		FROM artifacts a
		JOIN (
			SELECT aggregate_id, MIN(created_at) AS quarantined_at
			FROM outbox_events
			WHERE aggregate_type='artifact' AND event_type='ARTIFACT_QUARANTINED'
			GROUP BY aggregate_id
		) q ON q.aggregate_id=a.id
		LEFT JOIN artifact_gc_candidates c ON c.artifact_id=a.id
		WHERE a.status='QUARANTINED' AND a.storage_provider='local'
		  AND q.quarantined_at <= ?
		  AND c.artifact_id IS NULL
		ORDER BY q.quarantined_at ASC, a.id ASC LIMIT ?`,
		before.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return 0, fmt.Errorf("artifact gc list quarantined retention: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("artifact gc scan quarantined retention: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("artifact gc rows quarantined retention: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("artifact gc close quarantined retention: %w", err)
	}
	queued := 0
	for _, id := range ids {
		if err := g.EnqueueArtifactGCCandidate(ctx, id, "quarantined_retention", eligibleAt); err != nil {
			return queued, err
		}
		queued++
	}
	return queued, nil
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
	now = now.UTC()
	expires := now.Add(lease)
	nowText := now.Format(time.RFC3339)
	expiresText := expires.Format(time.RFC3339)
	rows, err := g.db.QueryContext(ctx, `
		SELECT c.artifact_id, c.reason, c.eligible_at, c.delete_attempts,
		       c.last_error, c.status, c.lease_expires_at,
	       COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
	       COALESCE(a.local_path,'')
		FROM artifact_gc_candidates c
		JOIN artifacts a ON a.id=c.artifact_id
		WHERE c.eligible_at <= ?
		  AND (c.status='ELIGIBLE' OR (c.status='DELETING' AND c.lease_expires_at < ?))
		ORDER BY c.eligible_at ASC LIMIT ?`, nowText, nowText, limit)
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
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("artifact gc lease rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("artifact gc lease close rows: %w", err)
	}
	leased := make([]ArtifactGCCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		result, err := g.db.ExecContext(ctx, `UPDATE artifact_gc_candidates
			SET status='DELETING', lease_owner=?, lease_expires_at=?
			WHERE artifact_id=?
			  AND (status='ELIGIBLE' OR (status='DELETING' AND lease_expires_at < ?))`,
			owner, expiresText, candidate.ArtifactID, nowText)
		if err != nil {
			return leased, fmt.Errorf("artifact gc lease update: %w", err)
		}
		n, err := storecore.ReadRowsAffected(result, "artifact gc lease")
		if err != nil {
			return leased, err
		}
		if n == 1 {
			candidate.LeaseOwner, candidate.LeaseExpiresAt, candidate.Status = owner, &expires, ArtifactGCDeleting
			leased = append(leased, candidate)
		}
	}
	return leased, nil
}

// CompleteArtifactGCNoObject retires a GC candidate for a failed staging
// artifact that never acquired a storage path. It deliberately preserves the
// FAILED artifact state: there were no bytes to delete and no final artifact.
func (g *ArtifactGCStore) CompleteArtifactGCNoObject(ctx context.Context, artifactID, owner string) error {
	if artifactID == "" || owner == "" {
		return fmt.Errorf("artifact gc: artifact_id and owner are required")
	}
	result, err := g.db.ExecContext(ctx, `UPDATE artifact_gc_candidates
		SET status='DELETED', lease_owner='', lease_expires_at=NULL, last_error=''
		WHERE artifact_id=? AND status='DELETING' AND lease_owner=?`, artifactID, owner)
	if err != nil {
		return fmt.Errorf("artifact gc no-object complete: %w", err)
	}
	if n, err := storecore.ReadRowsAffected(result, "artifact gc no-object complete"); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("artifact gc no-object complete: lease not owned or already completed")
	}
	return nil
}

// CompleteArtifactGC records the result of an external file deletion. The
// DB artifact is marked DELETED only after the bytes are gone or absent.
func (g *ArtifactGCStore) CompleteArtifactGC(ctx context.Context, artifactID, owner string, deleted bool, deleteErr string) error {
	return g.CompleteArtifactGCAt(ctx, artifactID, owner, deleted, deleteErr, time.Now().UTC())
}

// CompleteArtifactGCAt records a GC result and applies the caller-computed
// retry time for failures, preventing deterministic path errors from being
// retried on every reconciler tick.
func (g *ArtifactGCStore) CompleteArtifactGCAt(ctx context.Context, artifactID, owner string, deleted bool, deleteErr string, retryAt time.Time) error {
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
	if retryAt.IsZero() {
		retryAt = time.Now().UTC()
	}
	result, err := g.db.ExecContext(ctx, `UPDATE artifact_gc_candidates SET status='ELIGIBLE', eligible_at=?, lease_owner='', lease_expires_at=NULL, delete_attempts=delete_attempts+1, last_error=? WHERE artifact_id=? AND status='DELETING' AND lease_owner=?`, retryAt.UTC().Format(time.RFC3339), deleteErr, artifactID, owner)
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
