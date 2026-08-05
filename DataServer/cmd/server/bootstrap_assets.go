package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/completion"
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
	Completion       completion.Coordinator
	CompletionStore  completion.UploadProtocolStore
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
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB())
	uploadRepo := store.NewSQLiteUploadRepository(p.SQLite.DB())
	artifactReader := store.NewSQLiteArtifactReader(p.SQLite.DB())
	authReader := store.NewSQLiteAuthReader(p.SQLite.DB())
	uploadWriter := artifacts.NewSQLiteUploadSessionWriter(store.NewSQLiteUploadSessionWriter(p.SQLite.DB()))
	finalizeWriter := artifacts.NewSQLiteFinalizeWriter(store.NewSQLiteArtifactFinalizer(p.SQLite.DB(), planResolver))
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
	)
	log.Printf("[BOOTSTRAP] ChunkedUploadService ready (persistent chunked upload via artifact pipeline)")

	var completionCoord completion.Coordinator
	var completionStore completion.UploadProtocolStore
	if keyHex := cfg.Runtime.CommitHMACKey; keyHex != "" {
		key, decodeErr := hex.DecodeString(keyHex)
		if decodeErr != nil {
			return nil, fmt.Errorf("bootstrap: decode VELOX_COMMIT_HMAC_KEY: %w", decodeErr)
		}
		coord, coordErr := completion.NewCoordinator(completion.CoordinatorConfig{DB: p.SQLite.DB(), HMACKey: key, BlobStore: p.BlobStore})
		if coordErr != nil {
			return nil, fmt.Errorf("bootstrap: completion coordinator: %w", coordErr)
		}
		completionCoord = coord
		if bound, ok := coord.(completion.UploadProtocolStore); ok {
			completionStore = bound
		} else {
			return nil, fmt.Errorf("bootstrap: completion coordinator lacks upload protocol store")
		}
		log.Printf("[BOOTSTRAP] completion coordinator ready (typed artifact commit protocol)")
	} else {
		log.Printf("[BOOTSTRAP] completion coordinator disabled: VELOX_COMMIT_HMAC_KEY is empty")
	}

	// ── Reconciler (mandatory — fail-fast if init fails) ──────────
	reconciler, recErr := artifacts.NewReconciler(
		store.NewArtifactReconcilerRepository(p.SQLite.DB()),
		p.BlobStore,
		uploadRepo,
		nil, // clock.System default (production)
		artifacts.DefaultReconcilerConfig(),
	)
	if recErr != nil {
		return nil, fmt.Errorf("bootstrap: Reconciler init failed: %w — Reconciler is mandatory when artifacts are enabled", recErr)
	}
	log.Printf("[BOOTSTRAP] artifacts.Reconciler ready (mandatory — 4 rules)")

	// ── Outbox dispatcher ──────────────────────────────────────────
	// outbox.ProductionRegistry() is called EXACTLY ONCE at bootstrap
	// persistence (buildPersistence) so the single *Registry instance
	// is shared with subsystem registrations (buildWorkers registers
	// BundleRebuildHandler on p.OutboxRegistry). Using a fresh
	// ProductionRegistry() here would create a second
	// (otherwise-equivalent) Registry with zero handlers — the
	// dispatcher's "no handler" MarkFailed branch would fire for
	// every WORKER_BUNDLE_REBUILD_REQUESTED the workers subsystem
	// durably enqueues. The completeness invariant for
	// package-globally-registered handlers is asserted by
	// internal/outbox/completeness_test.go; subsystem registrations
	// are validated by their owning package's tests (e.g.
	// workers/bundle_rebuild_outbox_test.go).
	outboxDispatcher := outbox.NewDispatcher(p.Outbox, p.OutboxRegistry, outbox.Config{
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
		Completion:       completionCoord,
		CompletionStore:  completionStore,
		Reconciler:       reconciler,
		OutboxRegistry:   p.OutboxRegistry,
		OutboxDispatcher: outboxDispatcher,
	}, nil
}
