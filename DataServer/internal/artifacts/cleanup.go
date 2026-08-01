// Package artifacts / cleanup.go — the four cleanup rule passes of the
// Reconciler. Extracted from reconciler.go: expired uploads (rule 1),
// orphan final blobs + READY-without-blob quarantine (rules 2+3) and
// stuck STAGING artifacts (rule 4). Raw SQL here is covered by the
// reconciler sql-allowlist marker atop reconciler.go (baseline-ratched
// in scripts/ci/sql-baseline.txt).
package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/store"
)

// =====================================================================
// Rule 1: expired upload + staging cleanup
// =====================================================================

func (r *Reconciler) reconcileExpiredUploads(ctx context.Context) (int, error) {
	cutoff := r.clock.Now().Add(-r.config.OrphanBlobAge)
	sessions, err := r.repo.FindStuckStaging(ctx, cutoff, r.config.BatchLimit)
	if err != nil {
		return 0, fmt.Errorf("rule1: FindStuckStaging: %w", err)
	}
	if len(sessions) == 0 {
		return 0, nil
	}

	var n int
	for _, s := range sessions {
		// Defensive: only sweep sessions whose expires_at has passed
		// in case the uploadTTL on a jobs row is shorter than the spec.
		if !s.ExpiresAt.IsZero() && r.clock.Now().Before(s.ExpiresAt) {
			continue
		}

		// Best-effort: flip status. TransitionUploadStatus is CAS;
		// loser rows are skipped and re-evaluated on the next pass.
		// The repo returns store.ErrUploadStateInvalid on RowsAffected
		// mismatch; we check via errors.Is so the wrap chain works in
		// both store-direct callers (post-1/4) and the legacy
		// in-place-translation callers.
		if err := r.repo.TransitionUploadStatus(ctx, s.UploadID, s.Status, string(store.UploadExpired)); err != nil {
			if errors.Is(err, store.ErrUploadStateInvalid) || errors.Is(err, ErrUploadStateInvalid) {
				continue
			}
			log.Printf("[RECONCILER] rule1: upload %s transition failed: %v", s.UploadID, err)
			continue
		}

		// Cleanup the staging temp file. The spec says the staging file
		// is in BlobStore.StagingDir(); NopBlobStore's baseDir is used
		// instead. RemoveStaging accepts either case.
		if s.TemporaryStorageKey != "" {
			if rerr := r.blobStore.RemoveStaging(s.TemporaryStorageKey); rerr != nil {
				log.Printf("[RECONCILER] rule1: upload %s remove staging %s failed: %v",
					s.UploadID, s.TemporaryStorageKey, rerr)
			}
		}
		n++
	}
	return n, nil
}

// =====================================================================
// Rules 2 + 3: orphan final blobs + READY-without-blob QUARANTINED.
// =====================================================================

type readyEntry struct {
	artifactID string
	storageKey string
	verifiedAt time.Time
}

func (r *Reconciler) reconcileBlobs(ctx context.Context) (orphans, quarantinedWithEvent, quarantinedStatusOnly int, err error) {
	// 1. SELECT all artifacts with status='READY' and a verified_at
	//    timestamp. The map is the source-of-truth for which blob paths
	//    should exist on disk.
	dbEntries, err := r.loadReadyEntries(ctx)
	if err != nil {
		return 0, 0, 0, err
	}

	// 2. Walk FinalDir. Build the on-disk relative-path set + the
	//    modification time of each file (used by rule 2 to skip "just
	//    written" files when a FINALIZE just landed).
	diskEntries, err := r.walkFinalDir()
	if err != nil {
		return 0, 0, 0, err
	}

	oldEnoughCutoff := r.clock.Now().Add(-r.config.OrphanBlobAge)
	quarantineMin := r.config.QuarantineMinAge
	now := r.clock.Now()

	// 3. (disk - db) AND old = orphans -> rule 2: delete.
	for rel, info := range diskEntries {
		if _, foundInDB := dbEntries[rel]; foundInDB {
			continue
		}
		if info.ModTime().After(oldEnoughCutoff) {
			// Recently written; give the FINALIZE worker a chance to
			// commit the corresponding artifact row.
			continue
		}
		if rerr := r.deleteFinalFile(rel); rerr == nil {
			orphans++
		} else if !errors.Is(rerr, os.ErrNotExist) {
			log.Printf("[RECONCILER] rule2: delete orphan %s failed: %v", rel, rerr)
		}
	}

	// 4. (db - disk) AND verified_at old enough = rule 3: quarantine.
	for rel, entry := range dbEntries {
		if _, onDisk := diskEntries[rel]; onDisk {
			continue
		}
		if entry.verifiedAt.IsZero() {
			continue
		}
		if now.Sub(entry.verifiedAt) < quarantineMin {
			continue
		}
		qerr := r.quarantineArtifactTx(ctx, entry.artifactID, "blob_missing_on_disk:"+rel)
		switch {
		case qerr == nil:
			quarantinedWithEvent++
		case errors.Is(qerr, ErrArtifactAlreadyQuarantined):
			// idempotent — count neither bucket (not a failure)
			continue
		case errors.Is(qerr, ErrQuarantineStatusOnly):
			// status committed, outbox event deferred — surface as a
			// separate so dashboards can detect it without log scraping
			quarantinedStatusOnly++
		default:
			log.Printf("[RECONCILER] rule3: quarantine artifact %s failed: %v", entry.artifactID, qerr)
		}
	}

	return orphans, quarantinedWithEvent, quarantinedStatusOnly, nil
}

// loadReadyEntries selects all READY rows with a non-empty verified_at.
// No LIMIT: the in-memory map must include every READY row for the
// (disk - db) / (db - disk) diff to be meaningful.
//
// Memory bound: target installs < 1M artifacts (~10MB map). At 10M+
// READY the map would push >100MB per 15-minute cycle and a future
// iteration should paginate the SELECT with intermediate disk-set
// diffing. Within the documented target (<1M) this is acceptable.
func (r *Reconciler) loadReadyEntries(ctx context.Context) (map[string]readyEntry, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT storage_key, id, COALESCE(verified_at, '')
		FROM artifacts
		WHERE status = 'READY'
		  AND storage_provider = 'local'
		  AND storage_key <> ''
		  AND verified_at IS NOT NULL AND verified_at <> ''`)
	if err != nil {
		return nil, fmt.Errorf("rule2/3: load READY: %w", err)
	}
	defer rows.Close()

	out := make(map[string]readyEntry, 1024)
	for rows.Next() {
		var key, id, verifiedStr string
		if err := rows.Scan(&key, &id, &verifiedStr); err != nil {
			return nil, fmt.Errorf("rule2/3: scan: %w", err)
		}
		var ts time.Time
		if verifiedStr != "" {
			if t, perr := time.Parse(time.RFC3339, verifiedStr); perr == nil {
				ts = t
			}
		}
		// Normalize to forward-slashes so cross-platform path matching works.
		out[filepath.ToSlash(key)] = readyEntry{
			artifactID: id,
			storageKey: key,
			verifiedAt: ts,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rule2/3: rows: %w", err)
	}
	return out, nil
}

func (r *Reconciler) walkFinalDir() (map[string]fs.FileInfo, error) {
	finalDir := r.blobStore.FinalDir()
	if finalDir == "" {
		return map[string]fs.FileInfo{}, nil
	}
	out := make(map[string]fs.FileInfo, 1024)
	err := filepath.WalkDir(finalDir, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			return nil
		}
		// Skip leftover temp files from prior PromoteToCanonical calls.
		// The temp suffix is `.tmp.XXXXXXXX` (8 hex chars); the post-rename
		// canonical name has no `.tmp` substring. Using strings.Contains
		// (stdlib) — the inline helper was reinventing it pointlessly.
		if strings.Contains(d.Name(), ".tmp") {
			return nil
		}
		rel, rerr := filepath.Rel(finalDir, path)
		if rerr != nil {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		out[filepath.ToSlash(rel)] = info
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("rule2/3: walk FinalDir: %w", err)
	}
	return out, nil
}

func (r *Reconciler) deleteFinalFile(rel string) error {
	abs := filepath.Join(r.blobStore.FinalDir(), filepath.FromSlash(rel))
	return os.Remove(abs)
}

// =====================================================================
// Rule 4: stuck STAGING artifacts.
//
// Spec text says "FAILED/EXPIRED"; this implementation uses FAILED
// uniformly. Reasons documented inline:
//
//   - artifacts.STAGING transitions to FAILED via a single guarded
//     UPDATE (CAS) which is idempotent under retries — the spec says
//     the resolver "stops at FAILED".
//   - Artifact rows DO NOT carry the upload-session's expiry; EXPIRED
//     is reserved for upload session rows. The artifact is "failed"
//     if the corresponding upload was abandoned OR if Finalize never
//     happened for any other reason — both reduce to FAILED without
//     loss of information for downstream consumers.
//   - A future PR can introduce a status column on the artifact that
//     distinguishes "render never finished" vs "render finished but
//     never finalized" without changing this logic.
// =====================================================================

func (r *Reconciler) reconcileStuckArtifacts(ctx context.Context) (int, error) {
	cutoff := r.clock.Now().Add(-r.config.StuckArtifactAge).UTC().Format(time.RFC3339)

	rows, err := r.db.QueryContext(ctx, `
		SELECT id FROM artifacts
		WHERE status = 'STAGING'
		  AND created_at <> ''
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, cutoff, r.config.BatchLimit)
	if err != nil {
		return 0, fmt.Errorf("rule4: query stuck artifacts: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("rule4: scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rule4: rows: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var n int
	for _, id := range ids {
		// CAS: only flip if still in STAGING. Concurrent foreground
		// Finalize could have flipped this in the meantime - skip and
		// let the next pass handle any residuals.
		res, err := r.db.ExecContext(ctx, `
			UPDATE artifacts
			SET status = 'FAILED'
			WHERE id = ? AND status = 'STAGING'`, id)
		if err != nil {
			log.Printf("[RECONCILER] rule4: UPDATE artifact %s failed: %v", id, err)
			continue
		}
		affected, rerr := res.RowsAffected()
		if rerr != nil {
			log.Printf("[RECONCILER] rule4: RowsAffected artifact %s failed: %v", id, rerr)
			continue
		}
		if affected == 1 {
			if _, err := r.db.ExecContext(ctx, `
				INSERT INTO artifact_gc_candidates (artifact_id, reason, eligible_at, status)
				VALUES (?, 'stuck_staging', ?, 'ELIGIBLE')
				ON CONFLICT(artifact_id) DO NOTHING`, id, r.clock.Now().UTC().Format(time.RFC3339)); err != nil {
				log.Printf("[RECONCILER] rule4: enqueue GC candidate %s failed: %v", id, err)
			}
			n++
		}
	}
	return n, nil
}
