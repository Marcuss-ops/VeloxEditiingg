package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/app"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/deliveries"
	deliveryProviders "velox-server/internal/deliveries/providers"
	"velox-server/internal/forwarding"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/platform/clock"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// deliveryPlanResolverAdapter bridges the concrete
// *deliveries.SQLiteDeliveryPlanResolver to the enqueue.PlanResolver
// interface. The two layers (enqueue and deliveries) intentionally do not
// import each other — the enqueue package defines a minimal local
// PlanResolver contract to avoid an import cycle and to keep the
// precondition testable in isolation. The adapter is the single bridge
// at the composition root; it converts deliveries.PlanContext into the
// minimal enqueue.PlanDestination (only the fields the precondition
// needs: DestinationID, Priority, RetryBudget). Backoff and AcquiredAt
// are dropped because the enqueue precondition does not consume them.
type deliveryPlanResolverAdapter struct {
	inner *deliveries.SQLiteDeliveryPlanResolver
}

func (a *deliveryPlanResolverAdapter) ResolvePlan(ctx context.Context, jobID, artifactID string) (*enqueue.ResolvedPlan, error) {
	if a == nil || a.inner == nil {
		return nil, nil
	}
	plan, err := a.inner.ResolvePlan(ctx, jobID, artifactID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	out := &enqueue.ResolvedPlan{JobID: plan.JobID}
	for _, d := range plan.Destinations {
		out.Destinations = append(out.Destinations, enqueue.PlanDestination{
			DestinationID: d.DestinationID,
			Priority:      d.Priority,
			RetryBudget:   d.RetryBudget,
		})
	}
	return out, nil
}

// moduleDeps holds the module-level components built at bootstrap
// (YouTube, Drive, Ansible, Livestream, Frontend) plus the
// asset-level services that depend on them.
//
// PR 1: the canonical Job+Task writer (store.AtomicJobTaskCreator) is
// NOT stored on moduleDeps. buildTasks already constructs one in
// taskDeps.AtomicCreator; the only job-write caller is
// creatorflow.CreateJobWithPlan (canonical POST /api/v1/jobs) which
// threads the writer from taskDeps directly. Two separate writer
// instances pointed at the same *SQLiteStore would be a stateless
// duplicate — we share the single instance owned by buildTasks.
type moduleDeps struct {
	Registry         *app.Registry
	Health           *app.HealthModule
	YouTube          *app.YouTubeModule
	Drive            *app.DriveModule
	Ansible          *app.AnsibleModule
	AssetService     *voiceoverassets.AssetService
	Enqueuer         *enqueue.Enqueuer
	DeliveryRunner   *deliveries.DeliveryRunner
	ForwardingRunner *forwarding.CreatorForwardingRunner
}

// buildModules creates all Gin modules, the asset service (which needs
// Drive/YouTube resolvers), the enqueuer, the delivery registry +
// runner, and registers everything into an app.Registry.
//
// The returned moduleDeps carries the per-module pointers so the caller
// can wire them into the serverDeps compat path and the supervisor.
func buildModules(cfg *config.Config, p *persistenceDeps, j *jobsDeps, w *workerDeps, a *assetDeps, t *taskDeps) (*moduleDeps, error) {
	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)

	// ── YouTube module ───────────────────────────────────────────────
	ytMod, err := app.NewYouTubeModule(cfg, cfg.Runtime.DataDir, p.SQLite)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: youtube module: %w", err)
	}

	// ── Drive module ─────────────────────────────────────────────────
	driveMod, err := app.NewDriveModule(cfg, p.SQLite)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: drive module: %w", err)
	}

	// ── Asset Service (needs Drive + YouTube services) ──────────────
	voiceoverStore := voiceoverassets.NewStore(cfg.Runtime.DataDir, cfg.Runtime.MaxVoiceoverBytes, []string{cfg.Runtime.DataDir})

	// The drive module's Service() is already non-nil after NewDriveModule.
	var driveSvc voiceoverassets.DriveDownloader
	if driveMod != nil {
		if svc := driveMod.Service(); svc != nil {
			driveSvc = svc
		}
	}
	typedResolvers := voiceoverassets.NewTypedResolversFromStore(voiceoverStore, driveSvc, nil)
	assetRegistry := voiceoverassets.NewResolverRegistry(typedResolvers...)
	assetRepo := store.NewSQLiteAssetRepository(p.SQLite)
	assetSvc := voiceoverassets.NewAssetService(assetRepo, p.BlobStore, assetRegistry, clock.System{})

	// ── Enqueuer (needs atomic creator + jobs repository + asset service) ──
	// Production requires an explicit delivery plan. The same switch that
	// permits the resolver's dev fallback also relaxes enqueue-time validation,
	// so creation and finalization can never disagree about plan requirements.
	t.AtomicCreator.WithDeliveryPlanPolicy(!cfg.Runtime.DeliveryGlobalFallback)

	// PR-delivery-plan-precondition: wire the real DB-backed delivery plan
	// resolver into the Enqueuer. ResolvePlan (NOT ResolveDestinations) is
	// called before every enqueue so retry_budget can be validated and
	// propagated to job.MaxRetries upfront, eliminating the late re-resolve
	// in FinalizeVerified. The local adapter bridges the concrete deliveries
	// resolver to the enqueue.PlanResolver interface (see type above).
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB(), cfg.Runtime.DeliveryGlobalFallback)
	planAdapter := &deliveryPlanResolverAdapter{inner: planResolver}
	enqueuer := enqueue.NewEnqueuer(t.AtomicCreator, j.Repository, assetSvc, planAdapter)

	// ── Register modules ────────────────────────────────────────────
	healthMod := app.NewHealthModule()
	registry.Register(healthMod)
	registry.Register(app.NewWorkersModule(cfg, w.Registry, w.Lifecycle, w.UpdateHandler, auth, assetSvc, p.BlobStore))
	registry.Register(ytMod)
	registry.Register(driveMod)

	ansibleMod := app.NewAnsibleModule(cfg, cfg.Runtime.DataDir, auth, p.SQLite)
	registry.Register(ansibleMod)

	livestreamMod := app.NewLivestreamModule(ytMod.Service(), p.SQLite)
	registry.Register(livestreamMod)
	registry.Register(app.NewFrontendModule(cfg))

	// ── Delivery runner ─────────────────────────────────────────────
	deliveryReg := deliveries.NewRegistry()
	if ytMod != nil {
		ytProvider := deliveryProviders.NewYouTubeProvider(ytMod.Service(), p.BlobStore)
		deliveryReg.Register(ytProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: youtube")
	}
	if driveMod != nil {
		driveProvider := deliveryProviders.NewDriveProvider(driveMod.Service(), p.BlobStore)
		deliveryReg.Register(driveProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: drive")
	}

	deliveryRunner := deliveries.NewDeliveryRunner(
		deliveries.DefaultRunnerConfig(),
		deliveryReg,
		p.SQLite,
		fmt.Sprintf("delivery-runner-%d", time.Now().UnixNano()),
	)

	// ── Creator Forwarding runner ───────────────────────────────────
	var fwdRunner *forwarding.CreatorForwardingRunner
	if cfg.Render.RemoteEngineURL != "" {
		reClient := remoteengine.NewClient(remoteengine.Config{
			URL:       cfg.Render.RemoteEngineURL,
			Token:     cfg.Render.RemoteEngineToken,
			TimeoutMS: cfg.Render.RemoteEngineTimeoutMS,
			Retries:   cfg.Render.RemoteEngineRetries,
		})
		fwdRunner = forwarding.NewCreatorForwardingRunner(
			forwarding.DefaultRunnerConfig(),
			p.SQLite,
			reClient,
			enqueuer,
			fmt.Sprintf("cf-runner-%d", time.Now().UnixNano()),
		)
		log.Printf("[BOOTSTRAP] CreatorForwardingRunner initialized (remote_engine=%s)", cfg.Render.RemoteEngineURL)
	}

	return &moduleDeps{
		Registry:         registry,
		Health:           healthMod,
		YouTube:          ytMod,
		Drive:            driveMod,
		AssetService:     assetSvc,
		Enqueuer:         enqueuer,
		DeliveryRunner:   deliveryRunner,
		ForwardingRunner: fwdRunner,
	}, nil
} // Compile-time references that FORCE the compiler to keep the imports below.
// The static anchor complements the live runtime wiring of
// moduleDeps.CreatorFlowPlanWriter above so the creator-flow payload path
// surface area is reachable both at compile time (these symbols) and at
// runtime (the field populated by buildModules).
var (
	_ = creatorflow.CreateJobWithPlan // canonical Job+Task creation entry point
	_ = creatorflow.New               // constructor symmetry
	_ creatorflow.RenderPlan          // typed input contract; consumed by canonical POST /api/v1/jobs
	_ = store.NewAtomicJobTaskCreator // canonical writer (also bound dynamically above)
)
