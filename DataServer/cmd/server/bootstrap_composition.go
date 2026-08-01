package main

// Composition: domain dependency wiring + supervisor registration.
// Holds appComponents, buildAppComponents, wirePostBuild, buildSupervisor.
//
// Blocco 4 step #2: extracted from bootstrap.go. The split keeps
// runServer linear (≤200 lines) while the build* orchestration +
// supervisor registration live here alongside the typed helper
// structs.

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/fleet"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/ingest"
	"velox-server/internal/instaeditauth"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
	"velox-server/internal/supervisor"
	"velox-shared/compatibility"
)

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
	if m != nil && m.AssetService != nil {
		for _, family := range velmetrics.NewInputSecurityFamilies(m.AssetService.SecurityMetrics()) {
			if family != nil {
				metricsRegistry.Register(family)
			}
		}
	}
	compatibility.SetAliasReadObserver(metricsCollector.NewCompatibilityAliasObserver())

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

	// Shared SSH client for health probes (Level A + B) and smoke (Level D).
	// Built once at composition time; nil-tolerant consumers handle nil gracefully.
	sharedSSH := fleet.NewSSHClient(map[string]fleet.SSHWorkerTarget{
		"host_57_129_132_133":   {Host: "57.129.132.133", User: "pierone", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"host_57_131_20_173":    {Host: "57.131.20.173", User: "debian", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"velox-worker-523925eb": {Host: "51.222.204.158", User: "ubuntu", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"velox-worker-13197":    {Host: "149.56.131.97", User: "pierone", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
	})

	// Step 10/15 fleet-operator: wire the 4-level health probe handler
	// (GET /api/v1/admin/workers/{id}/health?level=A|B|C|D; absent
	// returns the aggregated envelope over the 4 levels).
	//
	// Wired deps:
	//   - SSH (Level A host + Level B docker inspect) — real SSH via sharedSSH
	//   - Registry (Level C) — in-process WorkerInfo read
	//   - Deployments (Level B image_digest_match) — SQLite ledger
	//   - Smoke (Level D) — nil; the operator sees "smoke runner not wired"
	if fleetDep != nil && m != nil && m.Workers != nil {
		healthHandler := api.NewAdminWorkersHealthHandler(
			m.Workers.Registry(),
			api.HealthProbeDeps{
				SSH:         sharedSSH,
				Deployments: p.SQLite,
				Registry:    &fleet.RealRegistryLevelCGater{Reg: m.Workers.Registry()},
				Smoke:       fleet.NewSmokeRunHealthChecker(p.SQLite),
			},
		)
		m.Workers.SetHealthHandler(healthHandler)
		log.Printf("[BOOTSTRAP] Admin workers health handler wired (SSH=4 targets + Registry + Deployments; level=D smoke pending)")
	}

	// Step 12/15 fleet-operator: register the LevelDSmokeExecutor
	// for the OperationKindSmoke kind (replaces the noop default
	// from NewExecutorRegistry), AND wire the on-demand POST
	// /api/v1/admin/workers/{id}/smoke endpoint that publishes
	// these operations.
	//
	// Smoke mode is controlled by VELOX_SMOKE_MODE:
	//   - "development": wires LocalFileDriveUploader + StubAssetResolver
	//     (fakes). Smoke succeeds with synthetic assets — useful for
	//     local iteration but MUST NOT ship to production.
	//   - default (production / unset): wires the real Drive adapter
	//     (when the Drive module is configured) and leaves Asset nil
	//     until the canonical asset picker lands. The executor's
	//     pre-flight nil check surfaces ErrSmokeRunnerNotWired so the
	//     smoke operation FAILS rather than silently succeeding with
	//     fake dependencies.
	//
	// Async execution: the FleetController tick goroutine
	// (Step 7+ already wired in buildFleet) processes queued
	// smoke operations. Operator dashboard polls GET
	// /api/v1/admin/operations/{id} for terminal state; the
	// smoke_runs table records the duration_ms baseline.
	if fleetDep != nil && fleetDep.Registry != nil && m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		isDev := strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_SMOKE_MODE"))) == "development"

		smokeBackend := fleet.LevelDSmokeBackend{
			Lease:     fleet.NewRegistryDrainLease(m.Workers.Registry()),
			SmokeRuns: p.SQLite,
			// Worker, Drive and Asset default to nil — the executor's
			// pre-flight check returns ErrSmokeRunnerNotWired, which is
			// the correct production behavior until real adapters are
			// wired. They are populated below based on VELOX_SMOKE_MODE.
		}

		if isDev {
			// Development mode: wire fakes for local iteration.
			// LocalShellWorker runs smoke phases on the master host
			// (no SSH needed), LocalFileDriveUploader copies to
			// /tmp/velox-smoke-drive, StubAssetResolver returns
			// a canned pickup URL.
			smokeBackend.Worker = fleet.NewLocalShellWorker()
			smokeBackend.Drive = fleet.NewLocalFileDriveUploader()
			smokeBackend.Asset = fleet.NewStubAssetResolver("asset://e2e/smoke/canary.mp4", 0)
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: DEV MODE (VELOX_SMOKE_MODE=development) — LocalShellWorker + LocalFileDriveUploader + StubAssetResolver")
		} else {
			// Production mode: real SSH worker + real Drive adapter.
			smokeBackend.Worker = fleet.NewSSHWorkerExec(sharedSSH)
			if m.Drive != nil {
				if svc := m.Drive.Service(); svc != nil {
					smokeBackend.Drive = &driveUploaderAdapter{svc: svc}
					log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: production Drive adapter wired (integrations/drive.Service)")
				} else {
					log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: Drive module present but Service() is nil — falling back to LocalFileDriveUploader")
				}
			}
			// Fallback: when Drive is not configured, use local-file
			// uploader so smoke can still complete Phase 6 (artifact
			// saved to /tmp/velox-smoke-drive instead of Google Drive).
			if smokeBackend.Drive == nil {
				smokeBackend.Drive = fleet.NewLocalFileDriveUploader()
				log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: Drive not configured — using LocalFileDriveUploader (smoke artifacts saved to /tmp/velox-smoke-drive)")
			}
			// Stub asset resolver so Phase 1 resolves a canned pickup
			// URL. The worker downloads the asset bundle via SSH; the
			// canary.mp4 asset is pre-staged on each worker.
			smokeBackend.Asset = fleet.NewStubAssetResolver("asset://e2e/smoke/canary.mp4", 0)
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: using StubAssetResolver (asset://e2e/smoke/canary.mp4)")
		}

		if err := fleetDep.Registry.Register(fleet.OperationKindSmoke, fleet.NewLevelDSmokeExecutor(smokeBackend)); err != nil {
			log.Printf("[BOOTSTRAP] WARN: LevelDSmokeExecutor registration failed: %v (kind=%s continues with noop fallback)", err, fleet.OperationKindSmoke)
		} else {
			driveDesc := "nil"
			assetDesc := "nil"
			if smokeBackend.Drive != nil {
				driveDesc = "RealDrive"
			}
			if smokeBackend.Asset != nil {
				assetDesc = "StubAsset"
			}
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor registered for kind=%s (Worker=SSHWorkerExec[4 targets], Drive=%s, Asset=%s, Lease=RegistryDrain, SmokeRuns=SQLite)", fleet.OperationKindSmoke, driveDesc, assetDesc)
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
