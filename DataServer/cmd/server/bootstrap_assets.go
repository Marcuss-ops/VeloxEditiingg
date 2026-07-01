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
)

// assetDeps holds the artifact pipeline components built before modules
// (YouTube, Drive) are available. The AssetService and Enqueuer are built
// LATER in buildModules because they require the Drive/YouTube integration
// services for typed-resolver construction.
//
// Fase 4c: WorkflowRepo (workflow.Repository) removed — write methods are
// gated (Fase 4b) and the 4 outbox handlers are inert no-op stubs (Fase 4a).
// No runtime path consumes a workflow.Repository any more.
type assetDeps struct {
	ArtifactSvc      *artifacts.Service
	ChunkedUploadSvc *artifacts.ChunkedUploadService
	Reconciler       *artifacts.Reconciler // mandatory — buildAssets fails fast if init fails
	OutboxRegistry   *outbox.Registry
	OutboxDispatcher *outbox.Dispatcher
}

// buildAssets creates the workflow repository, artifact pipeline,
// chunked-upload service, and outbox registry+dispatcher.
//
// The AssetService and Enqueuer are intentionally NOT built here —
// they depend on the Drive/YouTube integration services which are
// created by buildModules (after the module-level YouTube/Drive
// constructors run).  Those fields are populated by buildModules
// calling wireAssetServiceAndEnqueuer below.
func buildAssets(cfg *config.Config, p *persistenceDeps, j *jobsDeps) (*assetDeps, error) {
	_ = cfg

	// ── Artifacts.Service (sole SUCCEEDED gate) ─────────────────────
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB(), cfg.Runtime.DeliveryGlobalFallback)
	finRepo := artifacts.NewSQLiteFinalizationRepository(p.SQLite.DB())
	finRepo.WithPlanResolver(planResolver)
	artifactSvc := artifacts.NewService(
		artifacts.NewSQLiteRepository(p.SQLite.DB()),
		finRepo,
		p.BlobStore,
		p.SQLite.DB(),
		nil, // clock.System default (production)
	)
	log.Printf("[BOOTSTRAP] artifacts.Service ready (single-tx SUCCEEDED gate via FinalizationRepository.FinalizeVerified + DeliveryPlanResolver)")

	// ── Chunked upload service ───────────────────────────────────────
	chunkedSvc := artifacts.NewChunkedUploadService(
		artifactSvc,
		artifacts.NewSQLiteRepository(p.SQLite.DB()),
		p.BlobStore,
		p.SQLite.DB(),
	)
	log.Printf("[BOOTSTRAP] ChunkedUploadService ready (persistent chunked upload via artifact pipeline)")

	// ── Reconciler (mandatory — fail-fast if init fails) ──────────
	reconciler, recErr := artifacts.NewReconciler(
		p.SQLite.DB(),
		p.BlobStore,
		artifacts.NewSQLiteRepository(p.SQLite.DB()),
		nil, // clock.System default (production)
		artifacts.DefaultReconcilerConfig(),
	)
	if recErr != nil {
		return nil, fmt.Errorf("bootstrap: Reconciler init failed: %w — Reconciler is mandatory when artifacts are enabled", recErr)
	}
	log.Printf("[BOOTSTRAP] artifacts.Reconciler ready (mandatory — 4 rules)")

	// ── Outbox registry + dispatcher ────────────────────────────────
	// PR 2: outbox.ProductionRegistry() is the canonical wiring location
	// for all outbox handlers. Today the registry is empty (the dispatcher
	// marks every emitted event as FAILED via the "no handler → MarkFailed"
	// path); once PR 2 wires real handlers, this single call point picks
	// them up with no bootstrap change. The completeness invariant is
	// asserted by internal/outbox/completeness_test.go.
	outboxRegistry := outbox.ProductionRegistry()
	outboxDispatcher := outbox.NewDispatcher(p.Outbox, outboxRegistry, outbox.Config{
		PollInterval: 750 * time.Millisecond,
		BatchSize:    32,
		LockDuration: 30 * time.Second,
		MaxAttempts:  5,
	})

	// ── Drain residual legacy outbox events (PR #2) ────────────────
	// Idempotent: only PENDING/PROCESSING events for these 4 types
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
		ChunkedUploadSvc: chunkedSvc,
		Reconciler:       reconciler,
		OutboxRegistry:   outboxRegistry,
		OutboxDispatcher: outboxDispatcher,
	}, nil
}
