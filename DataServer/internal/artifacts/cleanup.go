// Package artifacts / cleanup.go — the four filesystem/application cleanup
// passes of the Reconciler. SQL persistence is owned by internal/artifactsstore.
package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/artifactsstore"
	"velox-server/internal/repository"
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
		// The repo returns artifactsstore.ErrUploadStateInvalid on RowsAffected
		// mismatch; we check via errors.Is so the wrap chain works in
		// both store-direct callers (post-1/4) and the legacy
		// in-place-translation callers.
		if err := r.repo.TransitionUploadStatus(ctx, s.UploadID, s.Status, string(repository.UploadExpired)); err != nil {
			if errors.Is(err, artifactsstore.ErrUploadStateInvalid) || errors.Is(err, ErrUploadStateInvalid) {
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
	// Load all READY artifacts through the typed store repository.
	storedEntries, err := r.artifactRepo.ListReadyArtifacts(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	dbEntries := make(map[string]readyEntry, len(storedEntries))
	for key, entry := range storedEntries {
		dbEntries[filepath.ToSlash(key)] = readyEntry{
			artifactID: entry.ArtifactID,
			storageKey: entry.StorageKey,
			verifiedAt: entry.VerifiedAt,
		}
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

// isBlobstoreTempName reports whether a FinalDir entry is a leftover temp
// file from PromoteToCanonical. The temp pattern is `<base>.tmp.<random>`
// (os.CreateTemp with pattern `*.tmp.*`); Go renders the random suffix in
// DECIMAL (runtime_rand as a uint32 — variable length, empirically pinned by
// cleanup_tmpname_test.go against the toolchain's real output). The canonical
// name is the temp name minus that suffix. Matching the marker + decimal
// suffix (instead of a bare substring) keeps legitimate artifacts whose names
// merely contain ".tmp" (e.g. render.tmp-cut.mp4) in the DB-diff set so rule
// 2 can still sweep them as orphans.
//
// Collision note: a real artifact whose name ends in `.tmp.<decimal>` is
// indistinguishable from a temp file — the `.tmp.*` namespace is owned by the
// blobstore's CreateTemp convention, so callers must not mint canonical names
// in that shape.
func isBlobstoreTempName(name string) bool {
	const marker = ".tmp."
	idx := strings.LastIndex(name, marker)
	if idx < 0 {
		return false
	}
	suffix := name[idx+len(marker):]
	if suffix == "" {
		return false
	}
	_, err := strconv.ParseUint(suffix, 10, 64)
	return err == nil
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
		// Skip leftover temp files from prior PromoteToCanonical calls;
		// see isBlobstoreTempName for the precise pattern.
		if isBlobstoreTempName(d.Name()) {
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
//     CAS transition which is idempotent under retries — the spec says
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
	cutoff := r.clock.Now().Add(-r.config.StuckArtifactAge)
	ids, err := r.artifactRepo.ListStuckArtifacts(ctx, cutoff, r.config.BatchLimit)
	if err != nil {
		return 0, fmt.Errorf("rule4: list stuck artifacts: %w", err)
	}
	var n int
	for _, id := range ids {
		changed, err := r.artifactRepo.MarkStuckArtifactFailed(ctx, id)
		if err != nil {
			log.Printf("[RECONCILER] rule4: mark artifact %s failed: %v", id, err)
			continue
		}
		if changed {
			if err := r.artifactRepo.EnqueueArtifactGC(ctx, id, "stuck_staging", r.clock.Now()); err != nil {
				log.Printf("[RECONCILER] rule4: enqueue GC candidate %s failed: %v", id, err)
			}
			n++
		}
	}
	return n, nil
}
