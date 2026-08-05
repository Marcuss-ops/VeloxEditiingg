package artifacts

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

type Reconciler struct {
	artifactRepo *store.ArtifactReconcilerRepository
	blobStore    store.BlobStore
	repo         store.UploadRepository
	clock        clock.Clock
	config       ReconcilerConfig
}

type ReconcilerConfig struct {
	OrphanBlobAge    time.Duration
	StuckArtifactAge time.Duration
	QuarantineMinAge time.Duration
	BatchLimit       int
}

func DefaultReconcilerConfig() ReconcilerConfig {
	return ReconcilerConfig{OrphanBlobAge: 24 * time.Hour, StuckArtifactAge: 24 * time.Hour, QuarantineMinAge: 60 * time.Second, BatchLimit: 200}
}

type ReconcileStats struct {
	ExpiredUploads        int
	OrphanFinalBlobs      int
	QuarantinedWithEvent  int
	QuarantinedStatusOnly int
	StuckArtifacts        int
	GCDeleted             int
	GCFailed              int
}

func NewReconciler(artifactRepo *store.ArtifactReconcilerRepository, blobStore store.BlobStore, repo store.UploadRepository, c clock.Clock, config ReconcilerConfig) (*Reconciler, error) {
	if artifactRepo == nil {
		return nil, fmt.Errorf("artifacts: Reconciler: nil artifact repository")
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
	return &Reconciler{artifactRepo: artifactRepo, blobStore: blobStore, repo: repo, clock: c, config: config}, nil
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
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

func (r *Reconciler) runOnce(ctx context.Context, source string) {
	stats, err := r.Reconcile(ctx)
	if err != nil {
		log.Printf("[RECONCILER] %s pass failed: %v", source, err)
		return
	}
	if stats.ExpiredUploads+stats.OrphanFinalBlobs+stats.QuarantinedWithEvent+stats.QuarantinedStatusOnly+stats.StuckArtifacts+stats.GCDeleted+stats.GCFailed > 0 {
		log.Printf("[RECONCILER] %s pass expired=%d orphan_blobs=%d quarantined_event=%d quarantined_status_only=%d stuck_artifacts=%d gc_deleted=%d gc_failed=%d", source, stats.ExpiredUploads, stats.OrphanFinalBlobs, stats.QuarantinedWithEvent, stats.QuarantinedStatusOnly, stats.StuckArtifacts, stats.GCDeleted, stats.GCFailed)
	}
}

func (r *Reconciler) Reconcile(ctx context.Context) (ReconcileStats, error) {
	var stats ReconcileStats
	if n, err := r.reconcileExpiredUploads(ctx); err != nil {
		log.Printf("[RECONCILER] rule1 error: %v", err)
	} else {
		stats.ExpiredUploads = n
	}
	orphans, withEvent, statusOnly, err := r.reconcileBlobs(ctx)
	if err != nil {
		log.Printf("[RECONCILER] rule2/3 error: %v", err)
	} else {
		stats.OrphanFinalBlobs, stats.QuarantinedWithEvent, stats.QuarantinedStatusOnly = orphans, withEvent, statusOnly
	}
	if n, err := r.reconcileStuckArtifacts(ctx); err != nil {
		log.Printf("[RECONCILER] rule4 error: %v", err)
	} else {
		stats.StuckArtifacts = n
	}
	if deleted, failed, err := RunArtifactGC(ctx, r.artifactRepo.GCStore(), r.blobStore, "reconciler", r.clock.Now(), 15*time.Minute, r.config.BatchLimit); err != nil {
		log.Printf("[RECONCILER] artifact GC error: %v", err)
	} else {
		stats.GCDeleted, stats.GCFailed = deleted, failed
	}
	return stats, nil
}
