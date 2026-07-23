package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	"velox-server/internal/outbox"
	"velox-server/internal/store"
)

// assetDeps holds the artifact pipeline components built before modules
// (Drive) are available. The AssetService and Enqueuer are built
// LATER in buildModules because they require the Drive integration
// service for typed-resolver construction.

// workflow.Repository retired: write methods are gated and the outbox
// handlers are no-op stubs. No runtime path consumes a
// workflow.Repository any more.
type assetDeps struct {
	ArtifactSvc      *artifacts.Service
	ArtifactReader   artifacts.ArtifactReader
	BlobStore        store.BlobStore
	ChunkedUploadSvc *artifacts.ChunkedUploadService
	Reconciler       *artifacts.Reconciler // mandatory — buildAssets fails fast if init fails
	OutboxRegistry   *outbox.Registry
	OutboxDispatcher *outbox.Dispatcher
}

// buildAssets creates the workflow repository, artifact pipeline,
// chunked-upload service, and outbox registry+dispatcher.
//
// The AssetService and Enqueuer are intentionally NOT built here —
// they depend on the Drive integration service which is
// created by buildModules (after the module-level Drive
// constructor runs).  Those fields are populated by buildModules
// calling wireAssetServiceAndEnqueuer below.
func buildAssets(cfg *config.Config, p *persistenceDeps, j *jobsDeps) (*assetDeps, error) {
	_ = cfg

	// ── Artifacts.Service (sole SUCCEEDED gate) ─────────────────────
	//
	// Three narrow SQLite components (artifact reader + upload-session
	// writer + finalize writer) share the same *sql.DB so the finalize
	// tx can join with concurrent updates on artifact_uploads. The
	// delivery-plan resolver is wired into the finalize writer
	// constructor (NOT method-chained) so the per-job destination set
	// is resolved inside the same tx that INSERTs job_deliveries.
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB(), cfg.Runtime.DeliveryGlobalFallback)
	uploadRepo := store.NewSQLiteUploadRepository(p.SQLite.DB())
	artifactReader := store.NewSQLiteArtifactReader(p.SQLite.DB())
	authReader := store.NewSQLiteAuthReader(p.SQLite.DB())
	uploadWriter := artifacts.NewSQLiteUploadSessionWriter(p.SQLite.DB())
	finalizeWriter := artifacts.NewSQLiteFinalizeWriter(p.SQLite.DB(), artifactReader, planResolver)
	// JobDeliveryCounter typed reader — required by NewService post
	// the VELOX_FFPROBE_VERIFY_ON_FINALIZE gate (RW-PROD-008 A4).
	// Production cannot silently run the gate without it; NewService
	// panics on nil so a bootstrap miss is loud at startup.
	deliveryCounter := store.NewSQLiteJobDeliveryCounter(p.SQLite.DB())
	artifactSvc := artifacts.NewService(
		uploadRepo,
		uploadWriter,
		finalizeWriter,
		artifactReader,
		p.BlobStore,
		authReader,
		nil, // clock.System default (production)
		deliveryCounter,
	)
	log.Printf("[BOOTSTRAP] artifacts.Service ready (single-tx SUCCEEDED gate via FinalizationWriter + DeliveryPlanResolver)")

	// ── Chunked upload service ───────────────────────────────────────
	chunkedSvc := artifacts.NewChunkedUploadService(
		artifactSvc,
		uploadRepo,
		p.BlobStore,
		p.SQLite.DB(),
	)
	log.Printf("[BOOTSTRAP] ChunkedUploadService ready (persistent chunked upload via artifact pipeline)")

	// ── Reconciler (mandatory — fail-fast if init fails) ──────────
	reconciler, recErr := artifacts.NewReconciler(
		p.SQLite.DB(),
		p.BlobStore,
		uploadRepo,
		nil, // clock.System default (production)
		artifacts.DefaultReconcilerConfig(),
	)
	if recErr != nil {
		return nil, fmt.Errorf("bootstrap: Reconciler init failed: %w — Reconciler is mandatory when artifacts are enabled", recErr)
	}
	log.Printf("[BOOTSTRAP] artifacts.Reconciler ready (mandatory — 4 rules)")

	// ── Outbox registry + dispatcher ────────────────────────────────
	// outbox.ProductionRegistry() is the canonical wiring location for
	// all outbox handlers. Today the registry is empty (the dispatcher
	// marks every emitted event as FAILED via the "no handler → MarkFailed"
	// path); once real handlers are wired, this single call point picks
	// them up with no bootstrap change. The completeness invariant is
	// asserted by internal/outbox/completeness_test.go.
	outboxRegistry := outbox.ProductionRegistry()
	outboxDispatcher := outbox.NewDispatcher(p.Outbox, outboxRegistry, outbox.Config{
		PollInterval: 750 * time.Millisecond,
		BatchSize:    32,
		LockDuration: 30 * time.Second,
		MaxAttempts:  5,
	})

	// ── Drain residual legacy outbox events ────────────────────────
	// Idempotent: only PENDING/PROCESSING events for these types
	// are marked DISCARDED_LEGACY_CUTOVER; already PROCESSED or FAILED
	// rows are left untouched. Safe to run on every restart.
	legacyTypes := []string{
		"WORKFLOW_STEP_READY",
		"WORKFLOW_STEP_SUCCEEDED",
		"WORKFLOW_RUN_SUCCEEDED",
		"WORKFLOW_RUN_FAILED",
		"WORKFLOW_STEP_FAILED",
		"WORKFLOW_STEP_RUNNING",
		"WORKFLOW_STEP_RETRY",
		"WORKFLOW_RUN_CANCELLED",
		"JOB_SUCCEEDED",
		"ARTIFACT_READY",
		"DELIVERY_CREATED",
	}
	result, drainErr := p.Outbox.DrainLegacyEvents(context.Background(), legacyTypes)
	if drainErr != nil {
		log.Printf("[BOOTSTRAP] WARNING: DrainLegacyEvents failed (non-fatal): %v", drainErr)
	} else if result.TotalDiscarded > 0 {
		log.Printf("[BOOTSTRAP] DrainLegacyEvents: discarded %d residual legacy outbox events %v",
			result.TotalDiscarded, result.ByEventType)
	} else {
		log.Printf("[BOOTSTRAP] DrainLegacyEvents: no residual legacy outbox events to drain")
	}

	return &assetDeps{
		ArtifactSvc:      artifactSvc,
		ArtifactReader:   artifactReader,
		BlobStore:        p.BlobStore,
		ChunkedUploadSvc: chunkedSvc,
		Reconciler:       reconciler,
		OutboxRegistry:   outboxRegistry,
		OutboxDispatcher: outboxDispatcher,
	}, nil
}
