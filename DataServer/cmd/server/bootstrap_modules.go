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
	"velox-server/internal/credentials"
	"velox-server/internal/deliveries"
	deliveryProviders "velox-server/internal/deliveries/providers"
	"velox-server/internal/forwarding"
	validationhandlers "velox-server/internal/handlers/remote/workers/validation"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/observability"
	"velox-server/internal/platform/clock"
	"velox-server/internal/remoteengine"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// workerRegistryAdapter adapts *workers.Registry to the
// observability.WorkerReader interface, converting WorkerInfo
// structs to map[string]any.
type workerRegistryAdapter struct {
	reg   *workers.Registry
	store *store.SQLiteStore
}

// sqliteJobInspectionAdapter exposes the already persisted job read models
// through observability's backend-neutral contract. It is intentionally an
// adapter at the composition root: the observability package must not import
// SQLite or leak raw database rows into its API.
type sqliteJobInspectionAdapter struct {
	store *store.SQLiteStore
}

func (a *sqliteJobInspectionAdapter) ListJobEvents(_ context.Context, jobID string, limit int) ([]observability.JobEvent, error) {
	rows, err := a.store.ListJobEvents(jobID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]observability.JobEvent, 0, len(rows))
	for _, row := range rows {
		out = append(out, observability.JobEvent{
			Timestamp: row.Timestamp,
			JobID:     row.JobID,
			Event:     row.Event,
			Payload:   observability.DecodeJobEventPayload(row.RawJSON),
		})
	}
	return out, nil
}

func (a *sqliteJobInspectionAdapter) ListArtifacts(_ context.Context, jobID string, limit int) ([]observability.ArtifactSnapshot, error) {
	rows, err := a.store.GetArtifactsByJob(jobID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]observability.ArtifactSnapshot, 0, len(rows))
	for _, row := range rows {
		out = append(out, observability.ArtifactSnapshot{
			ID: row.ID, Type: row.Type, Status: row.Status, SHA256: row.SHA256,
			SizeBytes: row.SizeBytes, DurationSeconds: row.DurationSeconds,
			MimeType: row.MimeType, CreatedAt: row.CreatedAt, VerifiedAt: row.VerifiedAt,
		})
	}
	return out, nil
}

func (a *sqliteJobInspectionAdapter) ListDeliveries(_ context.Context, jobID string) ([]observability.DeliverySnapshot, error) {
	rows, err := a.store.ListJobDeliveriesByJob(jobID)
	if err != nil {
		return nil, err
	}
	out := make([]observability.DeliverySnapshot, 0, len(rows))
	for _, row := range rows {
		out = append(out, observability.DeliverySnapshot{
			DeliveryID: row.DeliveryID, ArtifactID: row.ArtifactID,
			DestinationID: row.DestinationID, Status: row.Status,
			RemoteID: row.RemoteID, RemoteURL: row.RemoteURL,
			AttemptCount: row.AttemptCount, MaxAttempts: row.MaxAttempts,
			LastError: row.LastError, LastErrorMessage: row.LastErrorMessage,
			CompletedAt: row.CompletedAt,
		})
	}
	return out, nil
}

var _ observability.JobInspectionReader = (*sqliteJobInspectionAdapter)(nil)

func (a *workerRegistryAdapter) ListWorkers() ([]map[string]any, error) {
	if a.reg == nil {
		if a.store != nil {
			return a.store.ListWorkers()
		}
		return nil, nil
	}
	infos := a.reg.List(context.Background())
	if len(infos) == 0 && a.store != nil {
		return a.store.ListWorkers()
	}
	out := make([]map[string]any, len(infos))
	for i, info := range infos {
		targetDigest := ""
		if a.store != nil {
			if deployment, err := a.store.GetLatestDeploymentForWorker(context.Background(), info.WorkerID.String()); err == nil && deployment != nil {
				targetDigest = deployment.TargetDigest
			}
		}
		out[i] = map[string]any{
			// WorkerID is a typed identity value. Observability's map
			// boundary intentionally exposes its canonical string form so
			// downstream aggregation can index workers consistently.
			"worker_id":         string(info.WorkerID),
			"worker_name":       info.WorkerName,
			"status":            info.ConnectionStatus,
			"last_heartbeat":    info.LastHB,
			"engine_version":    info.EngineVersion,
			"code_version":      info.CodeVersion,
			"worker_class":      info.Class,
			"current_job":       info.CurrentJob,
			"connection_status": info.ConnectionStatus,
			"health":            info.Health,
			"health_state":      info.HealthState,
			"image_digest":      info.ImageDigest,
			"target_digest":     targetDigest,
			"readiness":         info.Readiness,
			"metrics":           info.Metrics,
		}
	}
	return out, nil
}

func (a *workerRegistryAdapter) GetWorker(workerID string) (map[string]any, error) {
	if a.reg == nil {
		return nil, fmt.Errorf("worker registry not available")
	}
	info := a.reg.GetWorker(context.Background(), workerID)
	if info == nil {
		return nil, nil
	}
	targetDigest := ""
	if a.store != nil {
		if deployment, err := a.store.GetLatestDeploymentForWorker(context.Background(), info.WorkerID.String()); err == nil && deployment != nil {
			targetDigest = deployment.TargetDigest
		}
	}
	return map[string]any{
		"worker_id":         string(info.WorkerID),
		"worker_name":       info.WorkerName,
		"status":            info.ConnectionStatus,
		"last_heartbeat":    info.LastHB,
		"engine_version":    info.EngineVersion,
		"code_version":      info.CodeVersion,
		"worker_class":      info.Class,
		"current_job":       info.CurrentJob,
		"connection_status": info.ConnectionStatus,
		"health":            info.Health,
		"health_state":      info.HealthState,
		"image_digest":      info.ImageDigest,
		"target_digest":     targetDigest,
		"readiness":         info.Readiness,
		"metrics":           info.Metrics,
	}, nil
}

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
// (Drive and Ansible) plus the asset-level services that depend
// on them.
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
	Drive            *app.DriveModule
	Ansible          *app.AnsibleModule
	Workers          *app.WorkersModule
	AssetService     *voiceoverassets.AssetService
	Enqueuer         *enqueue.Enqueuer
	DeliveryRunner   *deliveries.DeliveryRunner
	ForwardingRunner *forwarding.CreatorForwardingRunner
}

// buildModules creates all Gin modules, the asset service (which needs
// the Drive service), the enqueuer, the delivery registry + runner, and
// registers everything into an app.Registry.
//
// The returned moduleDeps carries the per-module pointers so the caller
// can wire them into the serverDeps compat path and the supervisor.
func buildModules(cfg *config.Config, p *persistenceDeps, j *jobsDeps, w *workerDeps, a *assetDeps, t *taskDeps) (*moduleDeps, error) {
	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)

	// ── Drive module ─────────────────────────────────────────────────
	driveMod, err := app.NewDriveModule(cfg, p.SQLite)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: drive module: %w", err)
	}

	// ── Asset Service (needs the Drive service) ──────────────
	voiceoverStore := voiceoverassets.NewStore(cfg.Runtime.DataDir, cfg.Runtime.MaxVoiceoverBytes, []string{cfg.Runtime.DataDir})
	voiceoverStore.SetRewriteDevBypass(cfg.Runtime.AssetRewriteDevBypass)

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
	// Delivery routing is explicit: there is no global destination fallback.
	t.AtomicCreator.WithDeliveryPlanPolicy(true)

	// PR-delivery-plan-precondition: wire the real DB-backed delivery plan
	// resolver into the Enqueuer. ResolvePlan (NOT ResolveDestinations) is
	// called before every enqueue so retry_budget can be validated and
	// propagated to job.MaxRetries upfront, eliminating the late re-resolve
	// in FinalizeVerified. The local adapter bridges the concrete deliveries
	// resolver to the enqueue.PlanResolver interface (see type above).
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(p.SQLite.DB())
	planAdapter := &deliveryPlanResolverAdapter{inner: planResolver}
	enqueuer := enqueue.NewEnqueuer(t.AtomicCreator, j.Repository, assetSvc, planAdapter)

	// Warn at boot if any deprecated SOCIAL_GATEWAY_* alias is still set in
	// the operator's environment. PR-15.10 retirement chain closed the
	// deprecation cycle on `main`; the canonical contract honors ONLY
	// SOCIAL_API_* env vars (see socialclient.ConfigFromEnv). Operators
	// carrying the legacy aliases in /etc/velox-server.env will see
	// ErrNotConfigured at DeliverArtifact time; this one-line WARN gives
	// them a clear rename hint at every master boot so the rename
	// surfaces BEFORE a silent delivery failure.
	for _, alias := range cfg.Compatibility.RetiredSocialAliases {
		log.Printf("[BOOTSTRAP][SOCIALCLIENT] WARN legacy alias env detected: %s is RETIRED (PR-15.10) and NOT honored — rename to %s.", alias.Env, alias.Canonical)
	}

	// Compute the socialclient.Config ONCE here so the Enqueuer validator
	// wiring AND the socialGatewayProvider below share the same
	// configuration source (no risk of two reads disagreeing on the env).
	socialClientCfg := socialclient.ConfigFromRuntime(cfg.Runtime.Social)
	if err := socialClientCfg.Validate(); err != nil {
		log.Printf("[BOOTSTRAP] socialclient config invalid: %v — provider will refuse deliveries until fixed", err)
	}

	// Wire the Social API boundary as the per-entry destination validator.
	// When SOCIAL_API_URL is unset, socialclient.Config{BaseURL=""} returns
	// ErrNotConfigured from every ValidateDestination call; the enqueue
	// pre-flight loop treats this as a HARD operational failure because
	// DeliveryRunner classifies provider-not-configured as terminal (there
	// is no retry path to consume retry_budget). Operators must configure
	// the social_repo before submitting plans with social destinations.
	// When SOCIAL_API_URL IS set, the validator hard-rejects entries with
	// 4xx responses from the social_repo and lets 5xx / rate-limit fall
	// through to the runner's retry_budget.
	socialClient := socialclient.New(socialClientCfg)
	enqueuer.WithSocialValidator(socialClient)
	log.Printf("[BOOTSTRAP] Enqueuer wired with social destination validator (%s)", socialClientCfg)

	// ── Register modules ────────────────────────────────────────────
	healthMod := app.NewHealthModule()
	registry.Register(healthMod)
	workersModule := app.NewWorkersModule(cfg, w.Registry, w.Lifecycle, w.UpdateHandler, auth, assetSvc, p.BlobStore, driveMod.Service())
	if p.SQLite != nil {
		workersModule.SetValidationHandler(validationhandlers.NewHandler(validationhandlers.NewValidationStore(p.SQLite)))
		log.Printf("[BOOTSTRAP] Worker validation repository and canonical routes wired")
		reader := api.NewSQLDBReader(p.SQLite.DB())
		if reader != nil {
			workersModule.SetMetricsHandler(api.NewMetricsHandler(reader.Metrics))
			workersModule.SetSessionsHandler(api.NewSessionsHandler(reader.Sessions))
			workersModule.SetEventsHandler(api.NewEventsHandler(reader.Events))
			log.Printf("[BOOTSTRAP] Worker metrics/sessions/events read endpoints registered")
		}
	}
	registry.Register(workersModule)
	registry.Register(driveMod)

	ansibleMod := app.NewAnsibleModule(cfg, cfg.Runtime.DataDir, auth, p.SQLite)
	registry.Register(ansibleMod)

	// ── Observability REST API ─────────────────────────────────────
	if t.Observability != nil {
		workerReader := &workerRegistryAdapter{reg: w.Registry, store: p.SQLite}
		obsSvc := t.Observability.WithJobs(j.Repository).WithWorkers(workerReader).
			WithJobInspection(&sqliteJobInspectionAdapter{store: p.SQLite})
		registry.Register(observability.NewModule(obsSvc, api.AdminAuthMiddleware(cfg)))
		log.Printf("[BOOTSTRAP] Observability REST API registered")
	}

	// ── Delivery runner ─────────────────────────────────────────────
	deliveryReg := deliveries.NewRegistry()
	if driveMod != nil {
		driveProvider := deliveryProviders.NewDriveProvider(driveMod.Service(), p.BlobStore)
		deliveryReg.Register(driveProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: drive")
	}

	// The social_gateway provider is platform-agnostic — it talks to the
	// external Social API through socialclient (HTTP). Registration is
	// always attempted; the provider itself returns ErrProviderNotConfigured
	// at DeliverArtifact time when SOCIAL_API_URL is unset, so the dev
	// experience remains a clean "destination FAILED, not silently
	// skipped" without nil-pointer risk. (The legacy SOCIAL_GATEWAY_URL
	// alias was dropped on the close-out of the deprecation cycle — see
	// CHANGELOG for the migration window.)
	// socialClientCfg is computed earlier (above the Enqueuer wiring) so
	// the validator and the provider share a single Config source.
	socialGatewayProvider := deliveryProviders.NewSocialGatewayProvider(socialClientCfg)
	deliveryReg.Register(socialGatewayProvider)
	deliveryReg.RegisterLegacyPhaseProvider(socialGatewayProvider)
	log.Printf("[BOOTSTRAP] Delivery provider registered: %s (%s)", socialGatewayProvider.Name(), socialClientCfg)
	if err := deliveryReg.ValidateCredentialContracts(); err != nil {
		return nil, fmt.Errorf("delivery provider credential contract: %w", err)
	}

	deliveryRunner := deliveries.NewDeliveryRunner(
		deliveries.DefaultRunnerConfig(),
		deliveryReg,
		p.SQLite,
		fmt.Sprintf("delivery-runner-%d", time.Now().UnixNano()),
	)
	if keys, keyErr := credentials.LoadKeyring(cfg.Runtime.Credentials); keyErr == nil {
		if vault, vaultErr := credentials.NewVault(p.SQLite, keys); vaultErr == nil {
			deliveryRunner.WithCredentialVault(vault)
			log.Printf("[BOOTSTRAP] Delivery credential vault enabled")
		}
	} else {
		log.Printf("[BOOTSTRAP] Delivery credential vault unavailable: %v", keyErr)
	}

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
		Drive:            driveMod,
		Workers:          workersModule,
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
