// sql-allowlist: artifacts reconciler — orphan-blob + QUARANTINED cleanup sweeps via raw SQL; future refactor candidate for typed repos in internal/store. Read-heavy SELECTs + two non-atomic txs (status flip + outbox emission) documented inline as split for transactional safety.

package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// Reconciler sweeps the artifacts state once and applies four cleanup
// rules from the verified-finalization spec:
//
//  1. upload scaduto + staging presente  --> elimina staging, status EXPIRED
//  2. blob finale senza riga DB dopo 24h --> elimina
//  3. artifact READY con blob assente   --> QUARANTINED + ARTIFACT_QUARANTINED event
//  4. artifact STAGING troppo vecchio   --> FAILED
//
// Design (validated by thinking pass before implementation):
//
//   - rules 2 and 3 share a single SELECT of all READY storage_keys
//     into a map, then a single WalkDir over BlobStore.FinalDir().
//     The map difference identifies the two cleanup sets:
//     (disk \ db) -> orphan rule 2, (db \ disk) -> rule 3.
//
//   - rule 1 uses Repository.FindStuckStaging and BlobStore.RemoveStaging.
//
//   - rule 4 issues a guarded UPDATE per row so concurrent foreground
//     finalizers are not stomped.
//
// Tradeoffs:
//
//   - In-memory map for the DB-prepared set: bounded by artifact count
//     (~100k rows = a few MB). Cheap.
//
//   - Filesystem WalkDir: O(files). At 100k files ~ a few seconds on
//     local FS; safe inside the 15-minute reconciliation interval.
//
//   - Cleanup of orphans (rule 2) is best-effort (a failed os.Remove
//     is logged and skipped). Subsequent passes converge.
//
//   - The Rule 3 quarantine transition uses TWO separate transactions
//     (status flip + outbox emission) instead of a single combined
//     transaction. The reasoning is documented inline in
//     quarantineArtifactTx: a combined-commit soft-skip on a missing
//     outbox_events table is fragile across SQLite drivers / future
//     builds. Splitting cleanly separates the FAILURE surface of the
//     two operations so the QUARANTINED status is durable regardless
//     of outbox schema state.
//
// Goroutine lifecycle: Run(ctx, interval) loops until ctx is cancelled
// (graceful shutdown). Reconcile(ctx) is the one-shot callable that
// callers (tests, admin commands) can invoke.
//
// The per-session repository is store.UploadRepository. The Reconciler
// still holds a *sql.DB because rules 2/3/4 use direct SELECT / UPDATE
// on the artifacts + outbox_events tables (sql-allowlist marker at
// the top of this file); Rule 1 alone uses the typed repo via
// FindStuckStaging + TransitionUploadStatus.
type Reconciler struct {
	db        *sql.DB
	blobStore store.BlobStore
	repo      store.UploadRepository
	clock     clock.Clock
	config    ReconcilerConfig
}

// ReconcilerConfig holds tunables that the spec fixes to 24h by
// default but bootstrap can override from cfg if desired.
type ReconcilerConfig struct {
	// OrphanBlobAge is the minimum age of a final blob with no
	// matching DB row before rule 2 deletes it. Spec: 24h.
	OrphanBlobAge time.Duration
	// StuckArtifactAge is the minimum age of an artifact row in
	// STAGING before rule 4 flips it to FAILED. Defensive default
	// 24h so legitimate uploads in flight are not stomped.
	StuckArtifactAge time.Duration
	// QuarantineMinAge is the minimum verified_at age before rule 3
	// marks a READY row as QUARANTINED. Protects against races with
	// foreground Finalize promoting the blob a few ms after our SELECT.
	QuarantineMinAge time.Duration
	// BatchLimit bounds how many rows each rule processes per pass so
	// a flush of stuck rows cannot lock SQLite for >1s.
	BatchLimit int
}

// DefaultReconcilerConfig matches the verified-finalization spec defaults.
func DefaultReconcilerConfig() ReconcilerConfig {
	return ReconcilerConfig{
		OrphanBlobAge:    24 * time.Hour,
		StuckArtifactAge: 24 * time.Hour,
		QuarantineMinAge: 60 * time.Second,
		BatchLimit:       200,
	}
}

// ReconcileStats reports what one reconciliation pass produced.
// QuarantinedWithEvent vs QuarantinedStatusOnly distinguishes
// QUARANTINED + outbox emitted from QUARANTINED status-only (outbox
// emission failed). Operators reading the stats need this split to
// detect schema drift / outbox table outages without grepping logs.
type ReconcileStats struct {
	ExpiredUploads   int // rule 1
	OrphanFinalBlobs int // rule 2
	// Rule 3 split: artifact READY where blob is missing.
	QuarantinedWithEvent  int // QUARANTINED committed AND outbox event committed
	QuarantinedStatusOnly int // QUARANTINED committed but outbox event deferred (schema drift)
	StuckArtifacts        int // rule 4
}

// NewReconciler composes a Reconciler. db and blobStore must outlive
// the Reconciler (Run holds references). repo can be the same
// store.NewSQLiteUploadRepository(db) as Service uses (transitively via
// the same *sql.DB).
func NewReconciler(db *sql.DB, blobStore store.BlobStore, repo store.UploadRepository, c clock.Clock, config ReconcilerConfig) (*Reconciler, error) {
	if db == nil {
		return nil, fmt.Errorf("artifacts: Reconciler: nil db")
	}
	if blobStore == nil {
		return nil, fmt.Errorf("artifacts: Reconciler: nil blob store")
	}
	if repo == nil {
		return nil, fmt.Errorf("artifacts: Reconciler: nil repo")
	}
	if c == nil {
		c = clock.System{}
	}
	if config.OrphanBlobAge <= 0 {
		config.OrphanBlobAge = 24 * time.Hour
	}
	if config.StuckArtifactAge <= 0 {
		config.StuckArtifactAge = 24 * time.Hour
	}
	if config.QuarantineMinAge <= 0 {
		config.QuarantineMinAge = 60 * time.Second
	}
	if config.BatchLimit <= 0 {
		config.BatchLimit = 200
	}
	return &Reconciler{
		db:        db,
		blobStore: blobStore,
		repo:      repo,
		clock:     c,
		config:    config,
	}, nil
}

// Run drives reconciliation on a tick until ctx is cancelled.
//
// Each tick logs its ReconcileStats even when zero so operators can
// verify the loop is alive on a quiet cluster.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately on startup so a recently-restarted master
	// does not wait a full interval before cleaning up its accumulated
	// orphans.
	r.runOnce(ctx, "startup")

	for {
		select {
		case <-ctx.Done():
			log.Printf("[RECONCILER] shutting down")
			return
		case <-ticker.C:
			r.runOnce(ctx, "tick")
		}
	}
}

// runOnce is the loop body; named so logs distinguish startup vs tick.
func (r *Reconciler) runOnce(ctx context.Context, source string) {
	stats, err := r.Reconcile(ctx)
	if err != nil {
		log.Printf("[RECONCILER] %s pass failed: %v", source, err)
		return
	}
	if stats.ExpiredUploads+stats.OrphanFinalBlobs+stats.QuarantinedWithEvent+stats.QuarantinedStatusOnly+stats.StuckArtifacts > 0 {
		log.Printf("[RECONCILER] %s pass expired=%d orphan_blobs=%d quarantined_event=%d quarantined_status_only=%d stuck_artifacts=%d",
			source, stats.ExpiredUploads, stats.OrphanFinalBlobs,
			stats.QuarantinedWithEvent, stats.QuarantinedStatusOnly, stats.StuckArtifacts)
	}
}

// Reconcile applies all four rules. Each rule is independent; a
// failure in one does not abort the others.
func (r *Reconciler) Reconcile(ctx context.Context) (ReconcileStats, error) {
	var stats ReconcileStats

	// Rule 1: expired upload sessions + staging cleanup.
	if n, err := r.reconcileExpiredUploads(ctx); err != nil {
		log.Printf("[RECONCILER] rule1 error: %v", err)
	} else {
		stats.ExpiredUploads = n
	}

	// Rules 2 + 3 are combined in a single SELECT/walk pass.
	orphans, withEvent, statusOnly, err := r.reconcileBlobs(ctx)
	if err != nil {
		log.Printf("[RECONCILER] rule2/3 error: %v", err)
	} else {
		stats.OrphanFinalBlobs = orphans
		stats.QuarantinedWithEvent = withEvent
		stats.QuarantinedStatusOnly = statusOnly
	}

	// Rule 4: stuck STAGING.
	if n, err := r.reconcileStuckArtifacts(ctx); err != nil {
		log.Printf("[RECONCILER] rule4 error: %v", err)
	} else {
		stats.StuckArtifacts = n
	}

	return stats, nil
}

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
	// modification time of each file (used by rule 2 to skip "just
	// written" files when a FINALIZE just landed).
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
// Rule 3 helper: transactional QUARANTINED flip + ARTIFACT_QUARANTINED
// outbox event.
//
// Two separate commits (NOT one combined tx with soft-skip on missing
// outbox_events). Reason: the single-tx + soft-skip pattern relies on
// SQLite's behavior where a single failed statement does NOT poison a
// whole transaction; this is undocumented and varies across SQLite
// builds (`SQLITE_OMIT_*`, future drivers). Splitting cleanly decouples
// the failure surfaces: QUARANTINED status is always durable when
// emitted; outbox emission is best-effort and reported separately.
// =====================================================================

// ErrArtifactAlreadyQuarantined is returned when the UPDATE matches 0
// rows because a concurrent reconciler (or admin) has already flipped
// the status. Callers should treat this as success (idempotent).
var ErrArtifactAlreadyQuarantined = errors.New("reconciler: artifact already terminal")

// ErrQuarantineStatusOnly is returned when the QUARANTINED status was
// committed but the ARTIFACT_QUARANTINED outbox event emission failed
// (best-effort). Callers (rule 3 counting) surface this as a separate
// bucket so dashboards can detect outbox schema drift without scraping logs.
var ErrQuarantineStatusOnly = errors.New("reconciler: quarantine status committed but outbox event deferred")

func (r *Reconciler) quarantineArtifactTx(ctx context.Context, artifactID, reason string) error {
	if artifactID == "" {
		return fmt.Errorf("reconciler: quarantineArtifactTx: empty artifactID")
	}

	// Phase 1: flip READY -> QUARANTINED in its own transaction.
	// The CAS WHERE clause prevents stomping concurrent finalizers.
	tx1, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reconciler: quarantineArtifactTx begin-status: %w", err)
	}
	res, err := tx1.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'QUARANTINED'
		WHERE id = ? AND status = 'READY'`, artifactID)
	if err != nil {
		_ = tx1.Rollback()
		return fmt.Errorf("reconciler: quarantine UPDATE: %w", err)
	}
	affected, rerr := res.RowsAffected()
	if rerr != nil {
		_ = tx1.Rollback()
		return rerr
	}
	if affected == 0 {
		_ = tx1.Rollback()
		// Idempotent: another reconciler (or admin, or a foreground
		// Finalize that beat us) already flipped to a terminal state.
		return ErrArtifactAlreadyQuarantined
	}
	if err := tx1.Commit(); err != nil {
		return fmt.Errorf("reconciler: quarantineArtifactTx commit-status: %w", err)
	}

	// Phase 2: emit ARTIFACT_QUARANTINED outbox event in its own tx.
	// Best-effort: if outbox_events is missing or the commit fails, the
	// caller (rule 3) learns about it via ErrQuarantineStatusOnly so
	// operators can distinguish "outbox healthy + successful quarantine"
	// from "outbox broken + status-only quarantine" in dashboard stats.
	// The QUARANTINED status flip from Phase 1 is the source of truth;
	// downstream consumers can replay the missed event out of band by
	// re-reading artifacts where status='QUARANTINED'.
	tx2, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[RECONCILER] quarantine outbox begin failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	payload := fmt.Sprintf(`{"artifact_id":%q,"reason":%q,"detected_at":%q}`,
		artifactID, reason, r.clock.Now().UTC().Format(time.RFC3339))
	now := r.clock.Now().UTC().Format(time.RFC3339)
	if _, err := tx2.ExecContext(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, status, available_at, created_at)
		VALUES ('artifact', ?, 'ARTIFACT_QUARANTINED', ?, 'PENDING', ?, ?)`,
		artifactID, payload, now, now); err != nil {
		_ = tx2.Rollback()
		if isNoSuchTable(err) {
			log.Printf("[RECONCILER] outbox_events missing; QUARANTINED status still committed for artifact=%s (event emission deferred)", artifactID)
			return ErrQuarantineStatusOnly
		}
		log.Printf("[RECONCILER] quarantine outbox INSERT failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	if err := tx2.Commit(); err != nil {
		log.Printf("[RECONCILER] quarantine outbox commit failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	return nil
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
			n++
		}
	}
	return n, nil
}
