package main

// platform/database is the canonical database abstraction (see
// docs/operations/02-repository-cleanup-and-ownership.md).
import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	velmetrics "velox-server/internal/metrics"

	"velox-server/internal/config"
	"velox-server/internal/grpcserver"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ── Sentinels ─────────────────────────────────────────────────────────────

// requireLiveWorkersEnabled is the canonical gate for the A8 opt-in.
// Encapsulated as a package-level helper so the readiness check call
// site stays readable AND so a future operator-mode (e.g. `velox fleet
// live-only`) can flip the same flag from a non-env source without
// rewriting the closure above.
func requireLiveWorkersEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_REQUIRE_LIVE_WORKERS")), "true")
}

var ErrPostgresNotYetWired = errors.New(
	"bootstrap: VELOX_DB_DRIVER=postgres is not yet wired end-to-end. " +
		"Narrow-repository adapters (jobs, artifacts) accept *database.Handle. " +
		"The remaining master modules (workers, lifecycle, ansible, youtube, drive, " +
		"livestream, registration) still depend on *SQLiteStore. See docs/architecture/ " +
		"and docs/pr/ for the per-module cutover roadmap",
)

// ── wirePostBuild (shared post-construction wiring) ──────────────────────
//
// wirePostBuild connects dependencies that cross build-layer boundaries
// (jobs↔tasks). Called by both buildTestDeps (tests) and runServer
// (production) so the wiring stays canonical in exactly one place.
func wirePostBuild(j *jobsDeps, t *taskDeps) error {
	// fix/remove-job-lease-ops: j.SQLiteRepo (concrete *SQLiteJobRepository)
	// satisfies taskgraph.JobsRetryQuerier via structural typing (Get +
	// FailWithRetry). j.Repository returns jobs.Repository which no
	// longer has FailWithRetry on the canonical interface.
	if j != nil && j.SQLiteRepo != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(j.SQLiteRepo)
	}

	// feat/task-report-ingestion: build the canonical
	// TaskReportIngestionService now that all upstream deps (tasks +
	// attempts + jobs + task_output_artifacts) are constructed.
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

// ── testServerDeps: NOT a generic dependency bag ──────────────────────────
//
// PR-ROUTER-DEPS: the legacy *serverDeps mega-struct is gone. The HTTP
// router no longer accepts a global deps blob — it consumes a typed
// RouterBundle (cmd/server/router.go). Tests that previously inspected
// `deps.cmdMgr` / `deps.workerUpdateHandler` / `deps.lifecycleSvc.Jobs()`
// now exercise the build* helpers directly OR consume the slim
// testServerDeps struct below, which contains ONLY the fields the
// test suite actually asserts on (none of these leak into the router).

// testServerDeps is the minimum-bag-of-deps returned by buildTestDeps.
// Production code never reads this; newRouter takes a RouterBundle.
// Keeping this struct test-local prevents future "let's add one more
// dep to serverDeps" temptation — every test contract must justify a
// new field here explicitly.
type testServerDeps struct {
	cmdMgr              *workersreg.CommandManager
	workerUpdateHandler *workerhandlers.WorkerUpdateHandler
	jobsRepo            jobs.Repository
	sqliteStore         *store.SQLiteStore
}

// buildTestDeps is the test-only composition root. It constructs the
// canonical dependency graph tests inspect; it does NOT touch serverDeps.
func buildTestDeps(cfg *config.Config) (*testServerDeps, error) {
	p, err := buildPersistence(cfg)
	if err != nil {
		return nil, err
	}
	j, err := buildJobs(p)
	if err != nil {
		return nil, err
	}
	t, err := buildTasks(p)
	if err != nil {
		return nil, err
	}
	if err := wirePostBuild(j, t); err != nil {
		return nil, err
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		return nil, err
	}
	// buildAssets is called for its side-effects on the persistence
	// layer (e.g. wiring artifact service back to the SQLite store)
	// even though testServerDeps does not expose anything from `a`.
	// The `_` discards the return value cleanly without an unused-var
	// compile error.
	if _, err := buildAssets(cfg, p, j); err != nil {
		return nil, err
	}

	return &testServerDeps{
		cmdMgr:              w.CommandManager,
		workerUpdateHandler: w.UpdateHandler,
		jobsRepo:            j.Repository,
		sqliteStore:         p.SQLite,
	}, nil
}

// ── runServer: the slim composition root (no serverDeps) ───────────────────

func runServer(cfg *config.Config) error {
	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	// 0. Wire the alert sink before buildSupervisor so the
	//    outbox-dispatcher's first JOB_FAILED delivery hits the real
	//    sink, not the NopNotifier default. buildSupervisor registers
	//    outbox-dispatcher as a ClassCritical supervisor runner, and
	//    we don't want any startup-window alerts silently dropped.
	if _, err := buildAlerts(); err != nil {
		return fmt.Errorf("bootstrap: alerts: %w", err)
	}

	// 1. Build domain dependencies (same shape as before).
	p, err := buildPersistence(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if p.SQLite != nil {
			if cerr := p.SQLite.Close(); cerr != nil {
				log.Printf("[SERVER] Store close failed: %v", cerr)
			}
		}
	}()

	j, err := buildJobs(p)
	if err != nil {
		return err
	}
	t, err := buildTasks(p)
	if err != nil {
		return err
	}
	if err := wirePostBuild(j, t); err != nil {
		return err
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		return err
	}
	a, err := buildAssets(cfg, p, j)
	if err != nil {
		return err
	}
	m, err := buildModules(cfg, p, j, w, a, t)
	if err != nil {
		return err
	}

	// 2. Build the per-route RouterBundle directly from the build*
	//    return values — no mega-struct in between.
	bundle := RouterBundle{
		Script: ScriptRouteDeps{
			Cfg:         cfg,
			SQLiteStore: p.SQLite,
			Enqueuer:    m.Enqueuer,
		},
		Groups:     GroupsRouteDeps{SQLiteStore: p.SQLite},
		Pipeline:   PipelineRouteDeps{Cfg: cfg, Enqueuer: m.Enqueuer, JobsRepo: j.Repository, CmdMgr: w.CommandManager},
		Darkeditor: DarkeditorRouteDeps{Cfg: cfg, SQLiteStore: p.SQLite},
		Upload: UploadRouteDeps{
			Cfg:            cfg,
			ArtifactSvc:    a.ArtifactSvc,
			ChunkedHandler: workerhandlersuploads.NewChunkedUploadHandler(a.ChunkedUploadSvc),
		},
	}

	// 3. Prometheus /metrics exporter (scorecard v1 / PR-5). Nil in
	//    tests means the route is omitted by registerMetricsRoutes.
	metricsRegistry := velmetrics.NewRegistry()
	bundle.Metrics = MetricsRouteDeps{Registry: metricsRegistry}
	metricsCollector := velmetrics.NewCollector(metricsRegistry)
	r := newRouter(cfg, bundle, m.Registry)

	// 4. HTTP server.
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("[SERVER] Velox master listening on %s", addr)

	errChan := make(chan error, 1)
	go func() {
		var err error
		if cfg.Server.TLSCertFile != "" && cfg.Server.TLSKeyFile != "" {
			log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
			err = srv.ListenAndServeTLS(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("[SERVER] Listen error: %v", err)
		}
		errChan <- err
	}()

	// 5. gRPC server.
	var grpcSrv grpcServer
	if cfg.Server.GRPCPort > 0 {
		// PR-REMOVE-LIFECYCLE: j.Repository is the canonical jobs
		// surface; the old `j.Lifecycle.Jobs()` indirection layer is gone.
		jobsRepo := j.Repository
		if jobsRepo != nil && w.CommandManager != nil {
			insecureDev := cfg.Runtime.GRPCAllowInsecureDev
			// PR-5 P0 fail-fast: refuse to start the master with insecure gRPC
			// outside the dev release channel. Production / staging MUST
			// use the TLS cert+key+CA triple. See docs/SECURITY_RUNBOOK.md
			// §5.1 for the release-channel rationale.
			if insecureDev && cfg.Runtime.ReleaseChannel != "dev" {
				log.Fatalf("[FAIL] PR-5 P0 guard: VELOX_GRPC_ALLOW_INSECURE_DEV=true on release channel =%q. Production / staging MUST use the TLS cert+key+CA triple. Set VELOX_RELEASE_CHANNEL=dev to confirm dev intent, or supply VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE and unset VELOX_GRPC_ALLOW_INSECURE_DEV.",
					cfg.Runtime.ReleaseChannel)
			}
			// RW-PROD-001 A5: an operator opt-in for hard-Reject of plaintext
			// gRPC even outside the PR-5 release-channel gate. Runs AFTER the
			// PR-5 guard so an operator who set both gets the most specific
			// fatal (the release-channel one, which mentions channel name).
			if err := enforceGRPCRequireTLS(cfg); err != nil {
				log.Fatal(err) // M2: helper now returns error (testable); caller hard-fail-loud.
			}
			if err := grpcserver.ValidateWorkerAllowlist(cfg.Workers.AllowedWorkers, insecureDev); err != nil {
				return err
			}
			// RW-PROD-001 M1 follow-up: opt-in silently ignored when GRPCPort=0.
			// Emit a loud WARN so misconfigured operators notice before deploy.
			if strings.TrimSpace(os.Getenv("VELOX_GRPC_REQUIRE_TLS")) == "true" && cfg.Server.GRPCPort == 0 {
				log.Printf("[WARN] RW-PROD-001 A5: VELOX_GRPC_REQUIRE_TLS=true but VELOX_GRPC_PORT=0; opt-in ignored, gRPC will not start. Set VELOX_GRPC_PORT>0 (e.g. 8443) to enable the TLS-only gRPC plane.")
			}
			grpcHandler := grpcserver.NewHandler(
				w.Registry, w.CommandManager, jobsRepo, t.TaskRepository, t.AttemptRepository, a.ArtifactSvc, p.SQLite,
				buildGRPCHandlerConfig(cfg, insecureDev),
			)
			// feat/task-report-ingestion: install the canonical
			// TaskReportIngestionService so handleTaskResult delegates to
			// the audit-mandated sequence (atomic close + artifact register
			// + Job roll-up) rather than re-implementing it in handler code.
			if t != nil && t.IngestionSvc != nil {
				grpcHandler.SetIngestionSvc(t.IngestionSvc)
				log.Printf("[BOOTSTRAP] installed TaskReportIngestionService on gRPC handler (feat/task-report-ingestion)")
			}
			gs, lis, gerr := grpcserver.StartGRPCServer(
				cfg.Server.GRPCPort, grpcHandler,
				cfg.Server.GRPCTLSCertFile, cfg.Server.GRPCTLSKeyFile, cfg.Server.GRPCTLSCAFile,
			)
			if gerr != nil {
				log.Printf("[SERVER] gRPC server failed to start: %v", gerr)
			} else if gs != nil {
				grpcSrv = &grpcServerWrapper{Server: gs, Listener: lis}
			}
		}
	}

	// 6. Wire readiness checks.
	if m.Health != nil {
		m.Health.AddReadinessCheck("db-ping", func() error {
			if p.SQLite == nil {
				return fmt.Errorf("SQLite store is nil")
			}
			return p.SQLite.Ping()
		})
		m.Health.AddReadinessCheck("blobstore", func() error {
			if p.BlobStore == nil {
				return fmt.Errorf("blob store is nil")
			}
			if p.BlobStore.StagingDir() == "" {
				return fmt.Errorf("blob store staging dir is empty")
			}
			return nil
		})
		m.Health.AddReadinessCheck("outbox", func() error {
			if p.Outbox == nil {
				return fmt.Errorf("outbox store is nil")
			}
			return nil
		})
		// RW-PROD-004 §3 A8: master-side readiness gate for the worker-side
		// /health/ready migration. When VELOX_REQUIRE_LIVE_WORKERS=true the
		// master refuses to mark ITSELF ready while the worker fleet is empty
		// (no live CONNECTED worker within HasAtLeastOneLiveTimeout=30s).
		//
		// Why opt-in (not unconditional): a staging cluster may run with zero
		// live workers during a scheduled drain window (e.g. a 6 AM batch
		// restart), and a production-arrived cluster that crashes its last worker
		// should still serve /api/v1/script-generation even before the next
		// worker registration round-trip completes. Operators opt in when they
		// want stricter pivots (e.g. a `velox-migrate-rollout-2026Q3` that needs
		// "fleet non-empty" as a hard-cutover precondition).
		//
		// Env-var check is repeated on every Readyz Check call: enabling/
		// disabling the gate at runtime is a one-line config push on k8s.
		if w.Registry != nil {
			m.Health.AddReadinessCheck("workers_at_least_one_live", func() error {
				if !requireLiveWorkersEnabled() {
					return nil // opt-in not active → gate satisfied
				}
				if !w.Registry.HasAtLeastOneLive(context.Background()) {
					return fmt.Errorf("VELOX_REQUIRE_LIVE_WORKERS=true but no live worker is registered within 30s")
				}
				return nil
			})
		}
	}

	// 7. Background supervisor (started in a goroutine, signals done via channel).
	supervisor, err := buildSupervisor(a, m, j, p, w, t, metricsCollector)
	if err != nil {
		return err
	}

	// PR-SUPERVISOR-TAXONOMY: gate `/ready` red when any
	// expected-to-be-running background runner has silently died.
	// ClassOneShot runners are excluded from this check (they
	// are expected to exit after completing their fire-and-forget
	// task). ClassRestartable + ClassCritical runners MUST stay
	// alive; if metrics-supervisor / taskgraph-dispatcher /
	// outbox-dispatcher ever exhaust retries and return, we want
	// the orchestrator to know within one tick. Wired HERE (after
	// buildSupervisor returns) so the closure captures `supervisor`
	// without an out-of-scope compile error.
	if m.Health != nil {
		m.Health.AddReadinessCheck("supervisor_runners", func() error {
			if supervisor == nil {
				return nil
			}
			missing := supervisor.Missing()
			if len(missing) > 0 {
				return fmt.Errorf("background supervisor: %d expected runner(s) dead: %v", len(missing), missing)
			}
			return nil
		})
	}

	// 7b. Start the supervisor goroutine.
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		if supErr := supervisor.Run(bgCtx); supErr != nil {
			// ClassCritical exhaustion → supervisor returns the
			// footgun error so k8s can restart the pod. Log loudly
			// here so a human spot-check catches it on first deploy.
			log.Printf("[SERVER] supervisor returned critical error: %v", supErr)
		}
	}()

	log.Printf("[BOOTSTRAP] Bootstrap complete — %d modules, %d background runners",
		m.Registry.Len(), supervisor.Len())
	if m.Health != nil {
		m.Health.MarkReady()
	}

	// 8. Wait for signal or error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	case <-quit:
		log.Println("[SERVER] Shutdown signal received, shutting down gracefully...")
	}

	// 9. Graceful teardown.
	bgCancel()
	log.Println("[SERVER] Background goroutines cancelling — waiting for them to exit...")

	select {
	case <-supervisorDone:
		log.Println("[SERVER] Background goroutines stopped cleanly")
	case <-time.After(15 * time.Second):
		log.Printf("[SERVER] background shutdown timed out after 15s — proceeding with teardown anyway")
	}

	shutdownGRPCServer(grpcSrv)
	shutdownHTTPServer(srv, 30*time.Second)

	log.Println("[SERVER] Server stopped")
	return nil
}

// ── buildSupervisor (re-categorised via SupervisedRunner taxonomy) ──────────
//
// PR-SUPERVISOR-TAXONOMY: every runner is registered with an explicit
// Class + RestartPolicy. Criticality guarantees:
//
//   - outbox-dispatcher / delivery-runner / task-lease-reaper — ClassCritical
//     If any of these dies the master is dead in the water, so the
//     supervisor eventually cancels its context and returns the error
//     so k8s can restart the pod.
//   - taskgraph-dispatcher / artifact-reconciler / metrics-supervisor — ClassRestartable
//     Bounded retries with backoff. After exhaustion the runner is
//     removed and the supervisor logs WARN.
//   - manifest-generator — ClassOneShot
//     Run once on startup; failure is non-fatal (logged WARN, never restarted).
func buildSupervisor(a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps, t *taskDeps, metricsCollector *velmetrics.Collector) (*BackgroundSupervisor, error) {
	sup := NewBackgroundSupervisor()

	// critical defaults: short backoff, near-infinite retry budget so
	// transient DB blips do NOT trip the fail-loud path; the operator
	// only sees a hard exit when the runner has been failing for hours.
	const criticalMaxRetries = 0 // 0 = infinite for ClassCritical
	criticalPolicy := RestartPolicy{
		MaxRetries:     criticalMaxRetries,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}
	// restartable defaults: bounded retries (~5 attempts over a few
	// minutes) before the runner is removed. The supervisor itself
	// keeps running; the operator sees a WARN log + Names() list.
	const restartableMaxRetries = 5
	restartablePolicy := RestartPolicy{
		MaxRetries:     restartableMaxRetries,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}
	// one-shot: no policy needed.

	// ── ClassCritical ────────────────────────────────────────────────
	if a.OutboxDispatcher != nil {
		if err := sup.Register(&SupervisedRunner{
			Name:   "outbox-dispatcher",
			Class:  ClassCritical,
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
		if err := sup.Register(&SupervisedRunner{
			Name:   "delivery-runner",
			Class:  ClassCritical,
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
		if err := sup.Register(&SupervisedRunner{
			Name:   "creator-forwarding-runner",
			Class:  ClassCritical,
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
		if err := sup.Register(&SupervisedRunner{
			Name:   "task-lease-reaper",
			Class:  ClassCritical,
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
		//
		// History: PR-13 introduced VELOX_DISABLE_JOB_REAPER (default off)
		// as a stop-gap while the jobs lease_expiry column went away
		// (migration 048) and the canonical lease TTL moved to tasks
		// (migration 049 + PR-05 TaskLeaseReaper). With TaskLeaseReaper
		// registered as its own ClassCritical supervisor runner above,
		// the Job-side zombie reaper is redundant. We KEEP the env-var
		// read for back-compat (operators still relying on the flag
		// would break otherwise) and emit a one-time DEPRECATED log so
		// operators know to migrate to TaskLeaseReaper.
		if os.Getenv("VELOX_DISABLE_JOB_REAPER") == "true" {
			log.Printf("[BOOTSTRAP] DEPRECATED env=VELOX_DISABLE_JOB_REAPER=true (PR-13 superseded by PR-05 TaskLeaseReaper; flag is now a no-op, set VELOX_TASK_LEASE_REAPER_DISABLED=true at the TaskLeaseReaper runner if you need to disable on the canonical path)")
		} else {
			log.Printf("[BOOTSTRAP] note=job-side zombie reaper is DEPRECATED; TaskLeaseReaper is the canonical master-side lease enforcer")
		}
	}

	// ── ClassRestartable ─────────────────────────────────────────────
	if a.Reconciler != nil {
		if err := sup.Register(&SupervisedRunner{
			Name:   "artifact-reconciler",
			Class:  ClassRestartable,
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
		if err := sup.Register(&SupervisedRunner{
			Name:   "taskgraph-dispatcher",
			Class:  ClassRestartable,
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
	// SPEC §14 follow-up: metrics-supervisor is the periodic
	// 15s tick that stamps the 4 cost-per-output-minute gauges and
	// refreshes master-health gauges (RSS, goroutines, outbox
	// pending). Nil-tolerance: collector, attempts, or outbox
	// missing ⇒ runner NOT registered (master still serves
	// /metrics but skips the supervisor projection — pre-PR-3
	// deploys without the metrics surface fall through cleanly).
	if metricsCollector != nil && p.SQLite != nil && p.Outbox != nil {
		labelRes := velmetrics.NewSQLiteLabelResolver(p.SQLite.DB())
		costFactors := velmetrics.LoadCostFactorsFromEnv()
		if err := sup.Register(&SupervisedRunner{
			Name:   "metrics-supervisor",
			Class:  ClassRestartable,
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

	// ── ClassOneShot ─────────────────────────────────────────────────
	// Manifest auto-generation: fire-and-forget on startup. Failure is
	// non-fatal (logged WARN, always returns nil) so no restart loop is
	// needed even if the manifest endpoint is briefly unreachable.
	if w.UpdateHandler != nil {
		if err := sup.Register(&SupervisedRunner{
			Name:  "manifest-generator",
			Class: ClassOneShot,
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

// Compile-time check: ensure legacy BackgroundRunner types (e.g.
// *metrics.Supervisor passed via RunnerFunc) keep working through the
// back-compat Register branch.
var _ BackgroundRunner = RunnerFunc{}
