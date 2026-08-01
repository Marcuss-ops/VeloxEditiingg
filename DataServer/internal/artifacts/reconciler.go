// sql-allowlist: artifacts reconciler — orphan-blob + QUARANTINED cleanup sweeps via raw SQL; future refactor candidate for typed repos in internal/store. Read-heavy SELECTs + two non-atomic txs (status flip + outbox emission) documented inline as split for transactional safety. The rule passes live in cleanup.go and the quarantine retry tx in retry.go.

package artifacts

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	gcStore   *store.ArtifactGCStore
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
	GCDeleted             int // durable candidate bytes removed
	GCFailed              int // candidate deletion deferred
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
		gcStore:   store.NewArtifactGCStore(db),
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
	if stats.ExpiredUploads+stats.OrphanFinalBlobs+stats.QuarantinedWithEvent+stats.QuarantinedStatusOnly+stats.StuckArtifacts+stats.GCDeleted+stats.GCFailed > 0 {
		log.Printf("[RECONCILER] %s pass expired=%d orphan_blobs=%d quarantined_event=%d quarantined_status_only=%d stuck_artifacts=%d gc_deleted=%d gc_failed=%d",
			source, stats.ExpiredUploads, stats.OrphanFinalBlobs,
			stats.QuarantinedWithEvent, stats.QuarantinedStatusOnly, stats.StuckArtifacts, stats.GCDeleted, stats.GCFailed)
	}
}

// Reconcile applies all four rules. Each rule is independent; a
// failure in one does not abort the others. The rule passes live in
// cleanup.go; the quarantine retry tx in retry.go.
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
	if deleted, failed, err := RunArtifactGC(ctx, r.gcStore, r.blobStore, "reconciler", r.clock.Now(), 15*time.Minute, r.config.BatchLimit); err != nil {
		log.Printf("[RECONCILER] artifact GC error: %v", err)
	} else {
		stats.GCDeleted, stats.GCFailed = deleted, failed
	}

	return stats, nil
}
