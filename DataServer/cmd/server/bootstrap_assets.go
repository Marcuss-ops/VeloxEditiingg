package main

import (
	"fmt"
	"log"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	"velox-server/internal/outbox"
	workflowevents "velox-server/internal/services/workflow_events"
	"velox-server/internal/workflow"
)

// assetDeps holds the artifact pipeline and workflow components built
// before modules (YouTube, Drive) are available.  The AssetService and
// Enqueuer are built LATER in buildModules because they require the
// Drive/YouTube integration services for typed-resolver construction.
type assetDeps struct {
	WorkflowRepo     workflow.Repository
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

	// ── Workflow repository ──────────────────────────────────────────
	workflowRepo := workflow.NewSQLiteRepository(p.SQLite.DB())
	workflowRepo.SetOutbox(&outboxWorkflowAdapter{store: p.Outbox})

	// ── Artifacts.Service (sole SUCCEEDED gate) ─────────────────────
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB())
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
	outboxRegistry := outbox.NewRegistry()
	stepReady := workflowevents.StepReadyHandler{Wf: workflowRepo, Q: j.Repository}
	jobSucceeded := workflowevents.JobSucceededHandler{Wf: workflowRepo}
	artifactReady := workflowevents.ArtifactReadyHandler{Wf: workflowRepo}
	deliveryCreated := workflowevents.DeliveryCreatedHandler{Wf: workflowRepo}
	if err := outboxRegistry.Register(stepReady); err != nil {
		return nil, fmt.Errorf("outbox register stepReady: %w", err)
	}
	if err := outboxRegistry.Register(jobSucceeded); err != nil {
		return nil, fmt.Errorf("outbox register jobSucceeded: %w", err)
	}
	if err := outboxRegistry.Register(artifactReady); err != nil {
		return nil, fmt.Errorf("outbox register artifactReady: %w", err)
	}
	if err := outboxRegistry.Register(deliveryCreated); err != nil {
		return nil, fmt.Errorf("outbox register deliveryCreated: %w", err)
	}
	outboxDispatcher := outbox.NewDispatcher(p.Outbox, outboxRegistry, outbox.Config{
		PollInterval: 750 * time.Millisecond,
		BatchSize:    32,
		LockDuration: 30 * time.Second,
		MaxAttempts:  5,
	})

	return &assetDeps{
		WorkflowRepo:     workflowRepo,
		ArtifactSvc:      artifactSvc,
		ChunkedUploadSvc: chunkedSvc,
		Reconciler:       reconciler,
		OutboxRegistry:   outboxRegistry,
		OutboxDispatcher: outboxDispatcher,
	}, nil
}
