package main

// Composition: domain dependency wiring + supervisor registration.
// Holds appComponents, buildAppComponents, wirePostBuild, buildSupervisor.
//
// Blocco 4 step #2: extracted from bootstrap.go. The split keeps
// runServer linear (≤200 lines) while the build* orchestration +
// supervisor registration live here alongside the typed helper
// structs.

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/alertengine"
	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/deploy"
	"velox-server/internal/deploy/cosign"
	"velox-server/internal/fleet"
	"velox-server/internal/fleet/opsalerts"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	scripthandlers "velox-server/internal/handlers/server/script"
	"velox-server/internal/ingest"
	"velox-server/internal/instaeditauth"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// stubAssetResolver is the Step 12/15 minimal asset-resolver
// stub used when the production asset picker is not yet wired.
// It returns a single canned pickup URL + expectedBytes=0.
// Production wiring lands when the canonical asset picker
// (ResolveAsset's real implementation) is integrated.
type stubAssetResolver struct {
	pickupURL     string
	expectedBytes int64
}

func (s stubAssetResolver) ResolveAsset(_ context.Context, _ string) (string, int64, error) {
	return s.pickupURL, s.expectedBytes, nil
}

// deployUpdateImageValidator wraps internal/deploy.ValidateImageRef
// for the UpdateExecutor's BackendImageRefValidator surface.
// Kept inline (not promoted to a separate file) because the
// Step 9/15 wiring path needs the type-name at the composition
// root and the shape is single-purpose.
type deployUpdateImageValidator struct{}

// Validate delegates to deploy.ValidateImageRef. The error
// sentinels (ErrEmptyImageRef, ErrMobileImageRef, etc.) flow
// through unmodified so the executor's audit-dashboard grep
// remains stable.
func (deployUpdateImageValidator) Validate(ref string) error {
	return deploy.ValidateImageRef(ref)
}

// newUpdateCosignVerifier returns the production-default Cosign
// verifier for the UpdateExecutor's BackendCosignVerifierIfc
// surface. The ExternalCosignVerifier shells out to the cosign
// CLI; VELOX_SKIP_COSIGN_VERIFY=1 short-circuits via the env
// guard inside the verifier.
func newUpdateCosignVerifier() BackendCosignVerifierIfc {
	return cosign.NewExternalCosignVerifier()
}

// BackendCosignVerifierIfc is the alias type the fleet package
// declares in update_executor.go. We re-declare it here so the
// composition root doesn't have to import the fleet package's
// unexported type. (The two declarations are structurally
// identical — the fleet package's is the canonical interface;
// this satisfies it via structural typing.)
type BackendCosignVerifierIfc interface {
	Verify(ctx context.Context, ref string) error
}

// appComponents holds every dependency the master process needs at
// runtime. The split into per-file helpers means runServer itself
// stays linear (≤200 lines) while the build* calls live in the
// slice of files that already defined them. Fields are set in
// buildAppComponents in dependency order; DO NOT reorder them
// without re-reading that function.
//
// appComponents does NOT duplicate the god-shape of the obsolete
// `*serverDeps` mega-struct: every field is typed; no field exists
// for "future use". New top-level concerns grown from an existing
// field are added with explicit justification in a comment.
type appComponents struct {
	cfg         *config.Config
	persistence *persistenceDeps
	jobs        *jobsDeps
	tasks       *taskDeps
	workers     *workerDeps
	assets      *assetDeps
	modules     *moduleDeps
	fleet       *FleetDep

	// Resolver is the canonical creatorflow.Resolver. The pipeline
	// handler (sync forward path) and the CreatorForwardingRunner
	// (async poll path) share this instance so they converge on the
	// same (job_id, forwarding_id) write path.
	resolver *creatorflow.Resolver

	// instaeditVerifier validates the short-lived JWT the InstaEdit
	// BFF sends when calling the /api/v1/instaedit/* routes. Nil when
	// INSTAEDIT_CONTROL_JWT_SECRET is not configured (dev/test).
	instaeditVerifier *instaeditauth.Verifier

	// CapabilityRegistry wires artifact.commit.v1 dispatch gates.
	// Registered probes (coordinator, spool, transport) surface in
	// /ready via Readyz().
	capabilityRegistry *registry.CapabilityRegistry

	// Metrics: scorecard v1 Prometheus exporter. Nil in tests
	// means /metrics is omitted by registerMetricsRoutes.
	metricsRegistry  *velmetrics.Registry
	metricsCollector *velmetrics.Collector

	// Supervisor owns long-lived background runners. Built in
	// buildAppComponents AFTER every other dependency so a runner
	// hook into a missing dep is a structural composition bug.
	supervisor *supervisor.Supervisor

	// health is a thin alias of modules.Health. Hoisted here so
	// registerReadinessChecks does not need to reach into modules.
	health *app.HealthModule
}

// close releases owned resources. Called via defer on the returned
// *appComponents in runServer (priority: connection close, since
// every other resource leaks upward through the pool).
func (c *appComponents) close() error {
	if c == nil || c.persistence == nil || c.persistence.SQLite == nil {
		return nil
	}
	if err := c.persistence.SQLite.Close(); err != nil {
		log.Printf("[SERVER] Store close failed: %v", err)
		return err
	}
	return nil
}

// routerBundle assembles the per-route dependency sets from the
// build* return values. Kept as a method on appComponents (rather
// than free-standing) so the next time a new build* helper adds a
// per-route dep, the only place to wire it is in this method.
func (c *appComponents) routerBundle() RouterBundle {
	// Build a single shared dark editor handler for both the legacy
	// /api/darkeditor/dark_editor_v2 surface and the InstaEdit-protected
	// /api/v1/instaedit/editor surface.
	deCfg := &darkeditor.Config{
		TempDir:      filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "temp"),
		ProjectsDir:  filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "projects"),
		LogDir:       filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "logs"),
		NVIDIAAPIKey: c.cfg.NVIDIA.APIKey,
	}
	deHandler := darkeditor.NewHandler(deCfg)
	if c.persistence.SQLite != nil {
		deHandler.SetDBStore(c.persistence.SQLite)
	}

	return RouterBundle{
		Fleet: FleetRouteDeps{
			// The Handler wraps FleetDep.Controller via the
			// ControllerAudit interface seam defined in
			// internal/handlers/server/api/admin_operations_handler.go;
			// nil-tolerant below (registerFleetOperationsRoutes
			// logs+skips when handler=nil).
			Handler: c.fleet.getHandler(),
		},
		Script: ScriptRouteDeps{
			Cfg:         c.cfg,
			SQLiteStore: c.persistence.SQLite,
			Enqueuer:    c.modules.Enqueuer,
			DocCreator: func() scripthandlers.GoogleDocCreator {
				if c.modules.Drive == nil {
					return nil
				}
				return c.modules.Drive.Service()
			}(),
		},
		Pipeline: PipelineRouteDeps{
			Cfg:          c.cfg,
			Enqueuer:     c.modules.Enqueuer,
			SQLiteStore:  c.persistence.SQLite,
			JobsRepo:     c.jobs.Repository,
			CmdMgr:       c.workers.CommandManager,
			TaskReader:   c.tasks.TaskRepository,
			Resolver:     c.resolver,
			AssetService: c.modules.AssetService,
		},
		Darkeditor: DarkeditorRouteDeps{Cfg: c.cfg, SQLiteStore: c.persistence.SQLite, Handler: deHandler},
		Upload: UploadRouteDeps{
			Cfg:            c.cfg,
			WorkerTokens:   c.workers.TokenManager,
			ArtifactSvc:    c.assets.ArtifactSvc,
			ArtifactReader: c.assets.ArtifactReader,
			BlobStore:      c.assets.BlobStore,
			ChunkedHandler: workerhandlersuploads.NewChunkedUploadHandler(c.assets.ChunkedUploadSvc),
		},
		Metrics: MetricsRouteDeps{Registry: c.metricsRegistry},
		InstaEdit: InstaEditRouteDeps{
			Verifier:    c.instaeditVerifier,
			Service:     instaedithandler.NewServiceFromSQLite(c.persistence.SQLite, c.jobs.Repository, store.NewSQLiteAssetRepository(c.persistence.SQLite), c.modules.Enqueuer),
			DarkHandler: deHandler,
		},
	}
}

// buildAppComponents constructs the master process's full dependency
// graph in the canonical order. Each step fails fast so the operator
// sees the FIRST misconfiguration (rather than a confused mass of
// nil-receiver panics in supervisor startup). The returned
// appComponents is the input to startTransports +
// registerReadinessChecks + runUntilShutdown.
func buildAppComponents(cfg *config.Config) (*appComponents, error) {
	// Wire the alert sink BEFORE buildSupervisor so the
	// outbox-dispatcher's first JOB_FAILED delivery hits the real
	// sink, not the NopNotifier default. buildSupervisor registers
	// outbox-dispatcher as a ClassCritical supervisor runner, and
	// we don't want any startup-window alerts silently dropped.
	// The return value of buildAlerts is DISCARDED on purpose: the
	// wiring is a side effect of registering alert handlers with
	// the outbox dispatcher; the resulting *alertsDeps is consumed
	// internally and never read by anyone else. Storing it on
	// appComponents would create a dead field.
	if _, err := buildAlerts(); err != nil {
		return nil, fmt.Errorf("bootstrap: alerts: %w", err)
	}

	p, err := buildPersistence(cfg)
	if err != nil {
		return nil, err
	}

	j, err := buildJobs(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	t, err := buildTasks(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	if err := wirePostBuild(j, t); err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	a, err := buildAssets(cfg, p, j)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	m, err := buildModules(cfg, p, j, w, a, t)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	log.Printf(
		"[ROUTES] script dependency state: enqueuer=%t store=%t remote_engine=%t",
		m != nil && m.Enqueuer != nil,
		p != nil && p.SQLite != nil,
		cfg != nil && strings.TrimSpace(cfg.Render.RemoteEngineURL) != "",
	)
	if m == nil || m.Enqueuer == nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("server composition: script API requires a non-nil enqueuer")
	}
	if p == nil || p.SQLite == nil {
		return nil, fmt.Errorf("server composition: script API requires a non-nil sqlite store")
	}

	// PR-taskgraph-wiring: forward the canonical Resolver (built
	// from the build* return values) to the pipeline handler so
	// the handler's sync forward path and the runner's async
	// poll-and-forward path converge on the same (job_id,
	// forwarding_id). The runner picks up the same Resolver via
	// ForwardingRunner.SetResolver below.
	var resolver *creatorflow.Resolver
	if p != nil && p.SQLite != nil && m != nil && m.Enqueuer != nil {
		resolver = creatorflow.NewResolver(cfg, m.Enqueuer, p.SQLite)
	}
	if m != nil && m.ForwardingRunner != nil && resolver != nil {
		m.ForwardingRunner.SetResolver(resolver)
		log.Printf("[BOOTSTRAP] CreatorForwardingRunner wired to canonical Resolver (Blocco 5)")
	}

	// Construct the canonical capability registry here so the
	// gRPC handler's SetCapabilityRegistry call (in startTransports)
	// has a non-nil registry available. Probe registration happens
	// later in registerReadinessChecks.
	capabilityRegistry := registry.NewCapabilityRegistry()

	metricsRegistry := velmetrics.NewRegistry()
	metricsCollector := velmetrics.NewCollector(metricsRegistry)

	// Build the InstaEdit control-plane verifier when the shared
	// secret is configured. A nil verifier means the /api/v1/instaedit
	// routes are not mounted, so dev/test deployments keep working.
	var instaeditVerifier *instaeditauth.Verifier
	if secret := strings.TrimSpace(cfg.Auth.InstaeditControlJWTSecret); secret != "" {
		v, err := instaeditauth.New(secret)
		if err != nil {
			_ = p.SQLite.Close()
			return nil, fmt.Errorf("bootstrap: instaedit auth verifier: %w", err)
		}
		instaeditVerifier = v
		log.Printf("[BOOTSTRAP] InstaEdit control JWT verifier configured")
	}

	supervisor, err := buildSupervisor(a, m, j, p, w, t, metricsCollector)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	fleetDep, err := buildFleet(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("bootstrap: fleet: %w", err)
	}
	if fleetDep != nil {
		if err := registerFleetRunner(supervisor, fleetDep); err != nil {
			_ = p.SQLite.Close()
			return nil, fmt.Errorf("bootstrap: fleet supervisor: %w", err)
		}
		fleetDep.tickWiredAtBoot = true
		log.Printf("[BOOTSTRAP] FleetController wired and supervised (operation ledger tick enabled)")
	}

	// Step 6/15 fleet-operator: wire the admin worker mutations handler
	// (POST /api/v1/admin/workers/{id}/{drain,resume,quarantine}).
	// Composition order: buildFleet returns FleetDep AFTER buildModules
	// returns moduleDeps.Workers, so the FleetController and Registry
	// are both available here. The mutations handler publishes via
	// FleetController.PublishOperation and synchronously flips
	// WorkerInfo.Drain / WorkerInfo.Quarantined via the Registry so the
	// placement matcher immediately excludes the worker.
	//
	// Nil-tolerant: a partial-boot FleetController keeps the POST
	// routes un-mounted (the adminWorkersMutationsHandler nil-guard
	// in app/workers.go:RegisterRoutes handles it).
	if fleetDep != nil && fleetDep.Controller != nil && m != nil && m.Workers != nil {
		m.Workers.SetMutationsHandler(
			api.NewAdminWorkersMutationsHandler(m.Workers.Registry(), fleetDep.Controller),
		)
		log.Printf("[BOOTSTRAP] Admin workers mutations handler wired (drain/resume/quarantine; tick goroutine drives the audit lifecycle)")
	}

	// Step 10/15 fleet-operator: wire the 4-level health probe handler
	// (GET /api/v1/admin/workers/{id}/health?level=A|B|C|D; absent
	// returns the aggregated envelope over the 4 levels).
	//
	// Production wiring is partial under Step 10+:
	//   - Registry adapter (Level C) — fully functional
	//   - Deployments repo (Level B image_digest_match) — wired from
	//     Step 5/15; the remaining Level B sub-checks need SSH
	//   - SSH client (Level A + Level B docker inspect) — nil; the
	//     probes surface CheckResult{passed:false, detail:"ssh client
	//     not wired (Step 11+ dependency)"} so the operator sees
	//     the audit gap rather than 503-on-every-probe
	//   - Smoke runner (Level D) — nil; same pattern, surfaces
	//     Step 12+ follow-up
	//
	// Step 11+ wires the real SSH client (Ansible/playbook surface
	// per the user spec) and Step 12+ wires the real Level-D smoke
	// runner. Both planned in the rollout plan; Step 10/15 ships
	// the surface that the operator dashboard reaches backend
	// answers through.
	if fleetDep != nil && m != nil && m.Workers != nil {
		healthHandler := api.NewAdminWorkersHealthHandler(
			m.Workers.Registry(),
			api.HealthProbeDeps{
				SSH:         nil,      // Step 11+ — real SSH client
				Deployments: p.SQLite, // Step 5/15 — deployments repo already wired
				Registry:    &fleet.RealRegistryLevelCGater{Reg: m.Workers.Registry()},
				Smoke:       nil, // Step 12+ — real smoke runner
			},
		)
		m.Workers.SetHealthHandler(healthHandler)
		log.Printf("[BOOTSTRAP] Admin workers health handler wired (level=C fully functional; levels A,D audit-only pending Step 11+/12+; level B partial — image_digest_match wired; docker_inspect/curl/healthcheck pending Step 11+)")
	}

	// Step 12/15 fleet-operator: register the LevelDSmokeExecutor
	// for the OperationKindSmoke kind (replaces the noop default
	// from NewExecutorRegistry), AND wire the on-demand POST
	// /api/v1/admin/workers/{id}/smoke endpoint that publishes
	// these operations.
	//
	// Production wiring is partial under Step 12+:
	//   - SmokeRuns repo — wired from p.SQLite
	//   - Asset resolver — minimal stub (production wiring lands
	//     when the canonical asset picker is in place)
	//   - Lease store — wired from a registry adapter (sets
	//     WorkerInfo.Drain transient during the run)
	//   - Worker exec (BackendWorkerExec) — nil; the executor's
	//     ErrSmokeRunnerNotWired sentinel surfaces the missing
	//     dep without 503-on-every-probe
	//   - Drive uploader (BackendDriveUploader) — nil; same pattern
	//
	// Async execution: the FleetController tick goroutine
	// (Step 7+ already wired in buildFleet) processes queued
	// smoke operations. Operator dashboard polls GET
	// /api/v1/admin/operations/{id} for terminal state; the
	// smoke_runs table records the duration_ms baseline.
	if fleetDep != nil && fleetDep.Registry != nil && m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		smokeBackend := fleet.LevelDSmokeBackend{
			Worker:    fleet.NewLocalShellWorker(),
			Drive:     fleet.NewLocalFileDriveUploader(),
			Asset:     stubAssetResolver{pickupURL: "asset://e2e/smoke/canary.mp4", expectedBytes: 0},
			Lease:     fleet.NewRegistryDrainLease(m.Workers.Registry()),
			SmokeRuns: p.SQLite,
		}
		if err := fleetDep.Registry.Register(fleet.OperationKindSmoke, fleet.NewLevelDSmokeExecutor(smokeBackend)); err != nil {
			log.Printf("[BOOTSTRAP] WARN: LevelDSmokeExecutor registration failed: %v (kind=%s continues with noop fallback)", err, fleet.OperationKindSmoke)
		} else {
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor registered for kind=%s (Worker=LocalShell, Drive=LocalFile, Lease=RegistryDrain, SmokeRuns=SQLite)", fleet.OperationKindSmoke)
		}
		m.Workers.SetSmokeHandler(api.NewAdminWorkersSmokeHandler(m.Workers.Registry(), fleetDep.Controller))
		log.Printf("[BOOTSTRAP] Admin workers smoke handler wired (POST /api/v1/admin/workers/{id}/smoke; tick goroutine drives LevelDSmokeExecutor)")
	}

	// Step 13/15 fleet-operator: wire the dual GET telemetry
	// endpoints
	//   GET /api/v1/admin/workers/{id}/metrics
	//     → LATEST snapshot from worker_metrics_snapshots
	//       (migration 105). 404 when the scheduler hasn't
	//       ticked yet for the worker.
	//   GET /api/v1/admin/workers/metrics
	//     → {data, has_more, count} envelope with one row per
	//       worker (the LATEST snapshot per worker_id).
	//
	// Both endpoints serve the persisted snapshot (not real-time
	// aggregation) — the metrics-snapshot-supervisor registered
	// below in buildSupervisor writes one row per worker every
	// 5 minutes; the dashboard renders a staleness indicator
	// (snapshotted_at field) rather than per-read compute.
	//
	// Nil-tolerant: a partial-boot persists no rows; the handler
	// reads from p.SQLite directly and 404s gracefully.
	if fleetDep != nil && m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		metricsHandler := api.NewAdminWorkersMetricsAggregatorHandler(p.SQLite, 5*time.Minute)
		m.Workers.SetMetricsAggregatorHandler(metricsHandler)
		log.Printf("[BOOTSTRAP] Admin workers metrics aggregator handler wired (GET /api/v1/admin/workers/{id}/metrics + /metrics; metrics-snapshot-supervisor ticks every 5min via buildSupervisor)")
	}

	// Step 16/15 fleet-operator: wire the structured alerting
	// surface (12-rule catalog persisted to alert_events via
	// migration 107). Read paths: /api/v1/admin/workers/{id}/alerts
	// + /api/v1/admin/alerts/active + /api/v1/admin/alerts/recent.
	// All adminAuth-gated; serve the operator dashboard.
	// The actual alert EVALUATION runs async in the
	// alerts-supervisor registered below in buildSupervisor
	// (ClassRestartable, 5min tick) so HTTP remains read-only.
	//
	// Nil-tolerant via the adminWorkersAlertsHandler nil guard —
	// a misconfigured bootstrap (no SQLite store) keeps the
	// routes silently un-mounted rather than 503-on-every-request.
	if m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		alertsHandler := api.NewAdminWorkersAlertsHandler(p.SQLite)
		m.Workers.SetAlertsHandler(alertsHandler)
		log.Printf("[BOOTSTRAP] Admin workers alerts handler wired (GET /api/v1/admin/workers/{id}/alerts + /api/v1/admin/alerts/active + /recent; alerts-supervisor ticks every 5min via buildSupervisor)")
	}

	return &appComponents{
		cfg:                cfg,
		persistence:        p,
		jobs:               j,
		tasks:              t,
		workers:            w,
		assets:             a,
		modules:            m,
		fleet:              fleetDep,
		resolver:           resolver,
		capabilityRegistry: capabilityRegistry,
		metricsRegistry:    metricsRegistry,
		metricsCollector:   metricsCollector,
		instaeditVerifier:  instaeditVerifier,
		supervisor:         supervisor,
		health:             m.Health,
	}, nil
}

// FleetDep is the Step 4/15 fleet-operator dependency bundle: the
// FleetController (publish + tick + audit bridge), the constructor
// of an AdminOperationsHandler for the audit routes, and the
// ExecutorRegistry reserved for future concrete executor registration.
//
// tickWiredAtBoot records whether the controller is registered in the
// process supervisor. A false value is only valid for partial test
// composition; production boot must set it to true so published
// operations cannot remain QUEUED indefinitely.
type FleetDep struct {
	Controller      *fleet.FleetController
	Registry        *fleet.ExecutorRegistry
	tickWiredAtBoot bool
}

// getHandler returns the api.AdminOperationsHandler wrapping
// Controller, or nil if Controller is absent (e.g. feature
// disabled via the bootstrap). Returns nil-safe on a zero-value
// FleetDep so destructive reads of c.fleet.getHandler() during
// route registration do not panic.
func (f *FleetDep) getHandler() *api.AdminOperationsHandler {
	if f == nil || f.Controller == nil {
		return nil
	}
	return api.NewAdminOperationsHandler(f.Controller)
}

// buildFleet constructs the FleetController + ExecutorRegistry
// when the persistence layer is available. Returns (nil, nil) on
// a persistence-disabled boot (test fixture paths) so the router
// registers the audit route stubs but serving returns 503 via the
// handler's nil-controller guard.
//
// Step 9/15: registers the UpdateExecutor for the `update`
// operation kind, replacing the Step 4/15 noop default. Live
// dependencies (Deployments repo, Cosign verifier, Image
// validator) are wired from the persistence layer; future
// steps (7+/8+/9+) plug in the real SSH client, Docker cli
// wrapper, Smoke runner, and Drive verifier (each nil-tolerant
// today: missing dep fails the Execute call loudly rather than
// silently noops).
//
// opTimeout is bumped to 30min (overriding DefaultOpTimeout's
// 10min) so the forward+rollback cascade for an `update`
// operation has headroom for: cosign verify (30s) +
// docker pull (10min) + compose restart (2min) + container
// running check + /health/ready poll (60s) + master connect
// (30s) + Level D smoke (5min) + Drive verify (60s) +
// RB-only cascade on failure (15min slack).
func buildFleet(p *persistenceDeps) (*FleetDep, error) {
	if p == nil || p.SQLite == nil {
		return nil, nil
	}
	registry := fleet.NewExecutorRegistry()
	controller := fleet.NewFleetController(
		p.SQLite,
		registry,
		fleet.DefaultTickInterval,
		30*time.Minute, // opTimeout for forward + rollback cascade
	)

	// Step 9/15 UpdateExecutor — wires the live deps that
	// bootstrap knows about (deployments repo, cosign validator,
	// image-ref validator). SSH/Docker/Smoke/Drive are Step 7+
	// follow-ups; nil-tolerant today.
	updateBackend := fleet.UpdateBackend{
		Deployments: p.SQLite,
		Cosign:      newUpdateCosignVerifier(),
		Image:       deployUpdateImageValidator{},
	}
	registry.Register(fleet.OperationKindUpdate, fleet.NewUpdateExecutor(updateBackend))
	log.Printf("[BOOTSTRAP] UpdateExecutor registered for kind=%s (SSH/Docker/Smoke/Drive pending Step 7+/8+)", fleet.OperationKindUpdate)

	return &FleetDep{
		Controller:      controller,
		Registry:        registry,
		tickWiredAtBoot: false,
	}, nil
}

// registerFleetRunner attaches the FleetController to the already-built
// supervisor. buildSupervisor runs before buildFleet because most runners
// are module dependencies, so registration is intentionally a small
// post-build step. The supervisor owns the goroutine and graceful shutdown;
// the controller owns only its operation tick.
func registerFleetRunner(sup *supervisor.Supervisor, dep *FleetDep) error {
	if sup == nil || dep == nil || dep.Controller == nil {
		return nil
	}
	const maxRetries = 5
	return sup.Register(supervisor.Runner{
		Name:  "fleet-controller",
		Class: supervisor.ClassRestartable,
		Policy: supervisor.RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     30 * time.Second,
			RestartOnPanic: true,
		},
		Run: func(ctx context.Context) error {
			log.Printf("[BOOTSTRAP] FleetController runner started")
			return dep.Controller.Run(ctx)
		},
	})
}

// wirePostBuild connects dependencies that cross build-layer
// boundaries (jobs↔tasks). Called by both buildTestDeps (tests)
// and buildAppComponents (production) so the wiring stays canonical
// in exactly one place.
func wirePostBuild(j *jobsDeps, t *taskDeps) error {
	// fix/remove-job-lease-ops: j.SQLiteRepo (concrete
	// *SQLiteJobRepository) satisfies taskgraph.JobsRetryQuerier
	// via structural typing (Get + FailWithRetry). j.Repository
	// returns jobs.Repository which no longer has FailWithRetry
	// on the canonical interface.
	if j != nil && j.SQLiteRepo != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(j.SQLiteRepo)
	}

	// feat/task-report-ingestion: build the canonical
	// TaskReportIngestionService now that all upstream deps
	// (tasks + attempts + jobs + task_output_artifacts) are
	// constructed.
	if j != nil && j.Repository != nil && t != nil && t.TaskRepository != nil && t.OutputArtifacts != nil {
		ingestionSvc, ingErr := ingest.NewTaskReportIngestionService(
			t.TaskRepository, j.Repository, t.AttemptRepository, t.OutputArtifacts,
		)
		if ingErr != nil {
			return fmt.Errorf("bootstrap: task report ingestion service: %w", ingErr)
		}
		t.IngestionSvc = ingestionSvc
	}
	return nil
}

// buildSupervisor registers the long-lived background runners
// using the SupervisedRunner taxonomy introduced in Blocco 1:
//
//   - ClassCritical:    outbox-dispatcher, delivery-runner,
//     creator-forwarding-runner, task-lease-reaper.
//     If any dies the master is dead in the water:
//     VELOX_CRITICAL_MAX_RETRIES bounds the budget
//     (0 = infinite; positive = fail-loud after).
//   - ClassRestartable: artifact-reconciler, taskgraph dispatcher,
//     metrics-supervisor. Bounded retries with
//     backoff; after exhaustion the runner is
//     removed and the supervisor logs WARN.
//   - ClassOneShot:     manifest-generator. Run once on startup;
//     failure is non-fatal (logged WARN).
func buildSupervisor(a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps, t *taskDeps, metricsCollector *velmetrics.Collector) (*supervisor.Supervisor, error) {
	sup := supervisor.New()

	criticalMaxRetries, criticalFailAfter := criticalRetryConfigFromEnv()
	criticalPolicy := supervisor.RestartPolicy{
		MaxRetries:     criticalMaxRetries,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}
	if criticalMaxRetries > 0 {
		log.Printf("[SUPERVISOR] critical retry budget: max_retries=%d (fail-loud after that many consecutive failures); fail_after=%d (log-WARN threshold)",
			criticalMaxRetries, criticalFailAfter)
	} else {
		log.Printf("[SUPERVISOR] critical retry budget: infinite (legacy 0=infinite); fail_after=%d (log-WARN threshold)",
			criticalFailAfter)
	}
	const restartableMaxRetries = 5
	restartablePolicy := supervisor.RestartPolicy{
		MaxRetries:     restartableMaxRetries,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}

	// ── ClassCritical ────────────────────────────────────────────────
	if a.OutboxDispatcher != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "outbox-dispatcher",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
				return a.OutboxDispatcher.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register outbox-dispatcher: %w", err)
		}
	}
	if m.DeliveryRunner != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "delivery-runner",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
				return m.DeliveryRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register delivery-runner: %w", err)
		}
	}
	if m.ForwardingRunner != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "creator-forwarding-runner",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] CreatorForwardingRunner started — polling creator_forwardings")
				return m.ForwardingRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register creator-forwarding-runner: %w", err)
		}
	}
	if t.TaskLeaseReaper != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "task-lease-reaper",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				return t.TaskLeaseReaper.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register task-lease-reaper: %w", err)
		}
	}

	// ── Legacy Job-side reaper DEPRECATED log (PR-13 → PR-05 cutover) ─
	if j.Repository != nil {
		// PR-13 → PR-05 cutover: the Job-side reaper is DEPRECATED.
		// TaskLeaseReaper is the canonical master-side lease enforcer.
		// VELOX_DISABLE_JOB_REAPER is preserved for back-compat (the
		// old flag would otherwise silently break operators depending
		// on it); the entry just emits a one-line DEPRECATED log.
		if os.Getenv("VELOX_DISABLE_JOB_REAPER") == "true" {
			log.Printf("[BOOTSTRAP] DEPRECATED env=VELOX_DISABLE_JOB_REAPER=true (PR-13 superseded by PR-05 TaskLeaseReaper; flag is now a no-op, set VELOX_TASK_LEASE_REAPER_DISABLED=true at the TaskLeaseReaper runner if you need to disable on the canonical path)")
		} else {
			log.Printf("[BOOTSTRAP] note=job-side zombie reaper is DEPRECATED; TaskLeaseReaper is the canonical master-side lease enforcer")
		}
	}

	// ── ClassRestartable ─────────────────────────────────────────────
	if a.Reconciler != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "artifact-reconciler",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] artifacts.Reconciler started (4 rules: expired-uploads + staging, orphan-final-blobs, READY-no-blob QUARANTINED, stuck-STAGING; 15m tick)")
				a.Reconciler.Run(ctx, 15*time.Minute)
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register artifact-reconciler: %w", err)
		}
	}
	if t.TaskLifecycle != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "taskgraph-dispatcher",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						n, err := t.TaskLifecycle.TickReadiness(ctx, 100)
						if err != nil {
							log.Printf("[TASKGRAPH] TickReadiness error: %v", err)
							return err
						}
						if n > 0 {
							log.Printf("[TASKGRAPH] TickReadiness: %d PENDING→READY", n)
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register taskgraph-dispatcher: %w", err)
		}
	}
	// SPEC §14 follow-up: metrics-supervisor is the periodic 15s
	// tick that stamps the 4 cost-per-output-minute gauges and
	// refreshes master-health gauges (RSS, goroutines, outbox
	// pending). Nil-tolerance: collector, attempts, or outbox
	// missing ⇒ runner NOT registered (master still serves
	// /metrics but skips the supervisor projection — pre-PR-3
	// deploys without the metrics surface fall through cleanly).
	// ── Alert Engine (Step 6 / Velox Metrics Center) ────────────────
	// Evaluates 5 rules every 30s: error_rate, p95_wall_ms, worker
	// offline, disk_free, ffmpeg_speed_ratio. Logs structured alerts
	// and optionally calls Slack/Telegram webhook via env vars.
	if t.Observability != nil {
		alertDeps := alertengine.DefaultRuleDeps()
		alertDeps.Obs = t.Observability
		alertDeps.DataDir = os.Getenv("VELOX_DATA_DIR")
		alertDeps.ErrorRatePct = alertengine.EnvFloat("VELOX_ALERT_ERROR_RATE_PCT", 5.0)
		alertDeps.P95WallMs = int64(alertengine.EnvFloat("VELOX_ALERT_P95_WALL_MS", 300_000))
		alertDeps.DiskFreeGB = alertengine.EnvFloat("VELOX_ALERT_DISK_FREE_GB", 10.0)
		alertDeps.FFmpegMin = alertengine.EnvFloat("VELOX_ALERT_FFMPEG_MIN", 1.5)

		engine := alertengine.New(30*time.Second, alertengine.NewNotifierFromEnv())
		for _, r := range alertengine.MakeRules(alertDeps) {
			engine.AddRule(r)
		}
		if err := sup.Register(supervisor.Runner{
			Name:   "alert-engine",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run:    engine.Run,
		}); err != nil {
			return nil, fmt.Errorf("supervisor register alert-engine: %w", err)
		}
	}

	if metricsCollector != nil && p.SQLite != nil && p.Outbox != nil {
		labelRes := velmetrics.NewSQLiteLabelResolver(p.SQLite.DB())
		costFactors := velmetrics.LoadCostFactorsFromEnv()
		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				supv := velmetrics.NewSupervisor(metricsCollector, labelRes, p.Outbox, costFactors)
				supv.SetTick(15 * time.Second)
				supv.SetLimit(1000)
				return supv.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register metrics-supervisor: %w", err)
		}
	}

	// Step 13/15 fleet-operator: 5-minute scheduler that runs
	// fleet.ComputeAndPersistSnapshot to refresh the
	// worker_metrics_snapshots table (migration 105). Distinct
	// from the metrics-supervisor above which handles Prometheus
	// op-level gauges; this is the fleet-side 13-metric rollup
	// refresh.
	//
	// Why ClassRestartable: a failed snapshot tick should retry
	// with backoff (per restartablePolicy) so a transient SQLite
	// lock or schema-migration blip doesn't permanently stall
	// the dashboard's freshness. NEVER ClassCritical: stale
	// snapshots degrade UI quality but not fleet functionality.
	if p != nil && p.SQLite != nil {
		sqlDB := p.SQLite.DB()
		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-snapshot-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(5 * time.Minute)
				defer ticker.Stop()
				log.Printf("[FLEET-METRICS] metrics-snapshot-supervisor started (5min tick; computes 13-metric rollup per worker from worker_metric_samples + fleet_operations + smoke_runs + deployment_records)")
				persist := func() {
					ds := fleet.WorkerMetricsAggregatorDataSource{
						Store: p.SQLite,
						WorkerIDsFn: func(ctx context.Context) ([]string, error) {
							return fleet.SQLiteWorkerIDs{DB: sqlDB}.WorkerIDs(ctx)
						},
					}
					n, err := fleet.ComputeAndPersistSnapshot(ctx, ds, sqlDB, time.Now().UTC())
					if err != nil {
						log.Printf("[FLEET-METRICS] snapshot tick failed: %v", err)
						return
					}
					log.Printf("[FETCH-METRICS] ticked: persisted %d worker snapshots", n)
				}
				persist()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						persist()
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register metrics-snapshot-supervisor: %w", err)
		}
	}

	// ── ClassOneShot ─────────────────────────────────────────────────
	// Manifest auto-generation: fire-and-forget on startup. Failure
	// is non-fatal (logged WARN, always returns nil) so no restart
	// loop is needed even if the manifest endpoint is briefly
	// unreachable.
	if p != nil && p.SQLite != nil && m != nil && m.Workers != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "alerts-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				// Step 16/15 ships the engine with a nil
				// DataSource — the registry API does not yet
				// expose ListAllWorkerIDs / GetWorkerCard so the
				// real adapter lands in Step 17+ with the
				// workersreg surface update. The supervisor
				// still ticks, dedup state machine is wired,
				// alert_events table is persisted, REST
				// endpoints serve the (currently empty) table.
				engine := opsalerts.NewEngine(p.SQLite, nil)
				log.Printf("[FLEET-ALERTS] alerts-supervisor started (5min tick; 12-rule catalog per the user spec; INFO never persisted, WARNING 5min dedup, CRITICAL fires immediately; data source pending Step 17+ workersreg surface)")
				return engine.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register alerts-supervisor: %w", err)
		}
	}

	if w.UpdateHandler != nil {
		if err := sup.Register(supervisor.Runner{
			Name:  "manifest-generator",
			Class: supervisor.ClassOneShot,
			Run: func(_ context.Context) error {
				if err := w.UpdateHandler.GenerateManifestV2(); err != nil {
					log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
				}
				// Always returns nil — manifest failure is never fatal.
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register manifest-generator: %w", err)
		}
	}
	return sup, nil
}
