package main

import (
	"fmt"
	"log"
	"time"

	"velox-server/internal/app"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/deliveries"
	deliveryProviders "velox-server/internal/deliveries/providers"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/pipeline"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// moduleDeps holds the module-level components built at bootstrap
// (YouTube, Drive, Ansible, Livestream, Frontend) plus the
// asset-level services that depend on them.
//
// PR-operation 01 / Fase 3: the canonical Job+Task writer
// (store.AtomicJobTaskCreator) is NOT stored on moduleDeps. buildTasks
// already constructs one in taskDeps.AtomicCreator and runServer reuses
// it for serverDeps.atomicPlanWriter. Two separate writer instances
// pointed at the same *SQLiteStore would be a stateless duplicate and
// would invite future drift between the task-tick path and the POST
// path — so we share the single instance owned by buildTasks.
type moduleDeps struct {
	Registry       *app.Registry
	Health         *app.HealthModule
	YouTube        *app.YouTubeModule
	Drive          *app.DriveModule
	Ansible        *app.AnsibleModule
	AssetService   *voiceoverassets.AssetService
	Enqueuer       *enqueue.Enqueuer
	DeliveryRunner *deliveries.DeliveryRunner
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
	pipeline.InitRemoteEngine(cfg)

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
		driveSvc = driveMod.Service()
	}
	typedResolvers := voiceoverassets.NewTypedResolversFromStore(voiceoverStore, driveSvc, nil)
	assetRegistry := voiceoverassets.NewResolverRegistry(typedResolvers...)
	assetRepo := store.NewSQLiteAssetRepository(p.SQLite)
	assetSvc := voiceoverassets.NewAssetService(assetRepo, p.BlobStore, assetRegistry, clock.System{})

	// ── Enqueuer (needs atomic creator + jobs repository + asset service) ──
	enqueuer := enqueue.NewEnqueuer(t.AtomicCreator, j.Repository, assetSvc)

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

	return &moduleDeps{
		Registry:       registry,
		Health:         healthMod,
		YouTube:        ytMod,
		Drive:          driveMod,
		AssetService:   assetSvc,
		Enqueuer:       enqueuer,
		DeliveryRunner: deliveryRunner,
	}, nil
} // Compile-time references that FORCE the compiler to keep the imports below.
// The static anchor complements the live runtime wiring of
// moduleDeps.CreatorFlowPlanWriter above so the creator-flow payload path
// surface area is reachable both at compile time (these symbols) and at
// runtime (the field populated by buildModules).
var (
	_ = creatorflow.CreateJobWithPlan // canonical Job+Task creation entry point
	_ = creatorflow.New               // constructor symmetry
	_ creatorflow.RenderPlan          // typed input contract; consumed by POST /orchestrator/jobs in Fase 3
	_ = store.NewAtomicJobTaskCreator // canonical writer (also bound dynamically above)
)
