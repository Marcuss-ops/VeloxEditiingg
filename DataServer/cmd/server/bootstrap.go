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
	"syscall"
	"time"

	velmetrics "velox-server/internal/metrics"

	"velox-server/internal/app"
	"velox-server/internal/artifacts"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	"velox-server/internal/grpcserver"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/outbox"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
	workersreg "velox-server/internal/workers"
)

// ── Legacy mega-struct kept for test + router compatibility ──────────────

type serverPaths struct {
	dataDir string
}

type serverDeps struct {
	paths               *serverPaths
	reg                 *workersreg.Registry
	workersRepo         store.WorkersRepository
	sqliteStore         *store.SQLiteStore
	workerUpdateHandler *workerhandlers.WorkerUpdateHandler
	workerLifecycle     *lifecycle.Handler
	ansibleModule       *app.AnsibleModule
	youtubeModule       *app.YouTubeModule
	driveModule         *app.DriveModule
	outboxStore         *outbox.Store
	outboxDispatcher    *outbox.Dispatcher
	deliveryRunner      *deliveries.DeliveryRunner
	blobStore           store.BlobStore
	artifactSvc         *artifacts.Service
	cmdMgr              *workersreg.CommandManager
	chunkedHandler      *workerhandlersuploads.ChunkedUploadHandler
	lifecycleSvc        *jobs.LifecycleService
	assetService        *voiceoverassets.AssetService
	enqueuer            *enqueue.Enqueuer
	taskDeps            *taskDeps

	// Scorecard v1 / PR-5: master-side Prometheus /metrics exporter.
	// Wired inside runServer when config.Server.EnableMetricsEnpoint is true;
	// nil in tests means the route is omitted.
	metricsRegistry *velmetrics.Registry
	metricsCollector *velmetrics.Collector

	// PR-operation 01 / Fase 3 — wiring for the orchestrator legacy adapter.
	// atomicPlanWriter is the AtomicJobTaskCreator that backs
	// creatorflow.CreateJobWithPlan for POST /orchestrator/jobs.
	// jobsRepo is the canonical jobs.Writer/Reader (used for pre-check +
	// list + Get). tasksRepo is the canonical taskgraph.Reader used by
	// the GET projection adapter. Initialised in buildServerDeps and
	// runServer from taskDeps + buildPersistence; nil-safe checks live in
	// newOrchestratorLegacyAdapter so the legacy POST can be staged vs.
	// the new POST without breaking the build.
	atomicPlanWriter *store.AtomicJobTaskCreator
	// jobsRepo is jobs.Reader (NOT Writer) — the orchestratorLegacyAdapter
	// only needs the canonical read surface (Get/List/Counts) plus an
	// idempotency pre-check inside creatorflow.CreateJobWithPlan. Writes
	// go through store.AtomicJobTaskCreator on atomicPlanWriter. j.Repository
	// implements both Reader and Writer, so we keep the field as jobs.Reader
	// and drop the unused Write surface from the adapter dependency graph.
	jobsRepo  jobs.Reader
	tasksRepo taskgraph.Reader
}

// ── Sentinels ─────────────────────────────────────────────────────────────

var ErrPostgresNotYetWired = errors.New(
	"bootstrap: VELOX_DB_DRIVER=postgres is not yet wired end-to-end. " +
		"Narrow-repository adapters (jobs, artifacts) accept *database.Handle. " +
		"The remaining master modules (workers, lifecycle, ansible, youtube, drive, " +
		"livestream, registration) still depend on *SQLiteStore. See docs/architecture/ " +
		"and docs/pr/ for the per-module cutover roadmap",
)

// ── buildServerDeps (compat wrapper for tests) ─────────────────────────────

func buildServerDeps(cfg *config.Config) (*serverDeps, error) {
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
	// PR-04 / fix/task-expiry-atomic-transition: wire the JobsLifecycle
	// into the TaskLifecycle so ExpireTaskLease's retry-budget lookup and
	// Job-aggregate update have context. nil-safe: omitted dependency
	// means the reaper still runs and gracefully skips the Job update.
	if j != nil && j.Lifecycle != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(j.Lifecycle.Jobs())
	}
	// feat/task-report-ingestion: build the canonical
	// TaskReportIngestionService now that all upstream deps (tasks +
	// attempts + jobs + task_output_artifacts) are constructed. The
	// service drives handleTaskResult's atomic-close + artifact-register
	// + Job roll-up sequence.
	if j != nil && j.Repository != nil && t != nil && t.TaskRepository != nil && t.OutputArtifacts != nil {
		ingestionSvc, ingErr := ingest.NewTaskReportIngestionService(
			t.TaskRepository, j.Repository, t.AttemptRepository, t.OutputArtifacts,
		)
		if ingErr != nil {
			return nil, fmt.Errorf("bootstrap: task report ingestion service: %w", ingErr)
		}
		t.IngestionSvc = ingestionSvc
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		return nil, err
	}
	a, err := buildAssets(cfg, p, j)
	if err != nil {
		return nil, err
	}
	return &serverDeps{
		paths:               &serverPaths{dataDir: cfg.Runtime.DataDir},
		reg:                 w.Registry,
		workersRepo:         w.Repository,
		sqliteStore:         p.SQLite,
		workerUpdateHandler: w.UpdateHandler,
		workerLifecycle:     w.Lifecycle,
		outboxStore:         p.Outbox,
		outboxDispatcher:    a.OutboxDispatcher,
		blobStore:           p.BlobStore,
		artifactSvc:         a.ArtifactSvc,
		cmdMgr:              w.CommandManager,
		lifecycleSvc:        j.Lifecycle,
		taskDeps:            t,
		// PR-operation 01 / Fase 3 — wire the canonical writer + canonical
		// reader surface into the orchestrator legacy adapter. Until a
		// future PR threads *tasksDeps through buildServerDeps, buildTasks
		// has already produced t.AtomicCreator and t.TaskRepository, both
		// pointing at the same *SQLiteStore.
		atomicPlanWriter: t.AtomicCreator,
		jobsRepo:         j.Repository,
		tasksRepo:        t.TaskRepository,
	}, nil
}

// ── runServer: the slim composition root ───────────────────────────────────

func runServer(cfg *config.Config) error {
	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	// 1. Build domain dependencies
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
	// PR-04 / fix/task-expiry-atomic-transition: see buildServerDeps —
	// wire jobsRepo into TaskLifecycle so the reaper can read
	// jobs.max_retries and update the Job aggregate on retries-exhausted.
	if j != nil && j.Lifecycle != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(j.Lifecycle.Jobs())
	}
	// feat/task-report-ingestion: see buildServerDeps — build the
	// canonical ingestion service after buildJobs + buildTasks return
	// (it needs j.Repository for Job roll-up).
	if j != nil && j.Repository != nil && t != nil && t.TaskRepository != nil && t.OutputArtifacts != nil {
		ingestionSvc, ingErr := ingest.NewTaskReportIngestionService(
			t.TaskRepository, j.Repository, t.AttemptRepository, t.OutputArtifacts,
		)
		if ingErr != nil {
			return fmt.Errorf("bootstrap: task report ingestion service: %w", ingErr)
		}
		t.IngestionSvc = ingestionSvc
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

	// 2. Assemble serverDeps for newRouter + gRPC compat
	deps := &serverDeps{
		paths:               &serverPaths{dataDir: cfg.Runtime.DataDir},
		reg:                 w.Registry,
		workersRepo:         w.Repository,
		sqliteStore:         p.SQLite,
		workerUpdateHandler: w.UpdateHandler,
		workerLifecycle:     w.Lifecycle,
		ansibleModule:       m.Ansible,
		youtubeModule:       m.YouTube,
		driveModule:         m.Drive,
		outboxStore:         p.Outbox,
		outboxDispatcher:    a.OutboxDispatcher,
		deliveryRunner:      m.DeliveryRunner,
		blobStore:           p.BlobStore,
		artifactSvc:         a.ArtifactSvc,
		cmdMgr:              w.CommandManager,
		chunkedHandler:      workerhandlersuploads.NewChunkedUploadHandler(a.ChunkedUploadSvc),
		lifecycleSvc:        j.Lifecycle,
		assetService:        m.AssetService,
		enqueuer:            m.Enqueuer,
		taskDeps:            t,
		// PR-operation 01 / Fase 3 — wire the canonical writer + readers
		// into the orchestrator legacy adapter. moduleDeps.CreatorFlowPlanWriter
		// is the bind we exposed in Fase 2; t.AtomicCreator is the same
		// writer as taskDeps.AtomicCreator (both wrap *SQLiteStore, no
		// state of their own).
		atomicPlanWriter: t.AtomicCreator,
		jobsRepo:         j.Repository,
		tasksRepo:        t.TaskRepository,
	}

	// 3. Build router (scorecard v1 / PR-5: registry + collector wired
	// here so /metrics mounts automatically inside newRouter).
	deps.metricsRegistry = velmetrics.NewRegistry()
	deps.metricsCollector = velmetrics.NewCollector(deps.metricsRegistry)
	r := newRouter(cfg, deps, m.Registry)

	// 4. HTTP server
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

	// 5. gRPC server
	var grpcSrv grpcServer
	if cfg.Server.GRPCPort > 0 {
		jobsRepo := j.Lifecycle.Jobs()
		if jobsRepo != nil && w.CommandManager != nil {
			insecureDev := cfg.Runtime.GRPCAllowInsecureDev
			if err := // PR-5 P0 fail-fast: refuse to start the master with insecure gRPC
			// outside the dev release channel. Production / staging MUST
			// use the TLS cert+key+CA triple. See docs/SECURITY_RUNBOOK.md
			// §5.1 for the release-channel rationale.
			if insecureDev && cfg.Runtime.ReleaseChannel != "dev" {
				log.Fatalf("[FAIL] PR-5 P0 guard: VELOX_GRPC_ALLOW_INSECURE_DEV=true on release channel =%q. Production / staging MUST use the TLS cert+key+CA triple. Set VELOX_RELEASE_CHANNEL=dev to confirm dev intent, or supply VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE and unset VELOX_GRPC_ALLOW_INSECURE_DEV.",
					cfg.Runtime.ReleaseChannel)
			}
			if err := grpcserver.ValidateWorkerAllowlist(cfg.Workers.AllowedWorkers, insecureDev); err != nil {
				return err
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

	// 6. Wire readiness checks
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
	}

	// 7. Background supervisor (started in a goroutine, signals done via channel)
	supervisor, err := buildSupervisor(a, m, j, p, w, t)
	if err != nil {
		return err
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		_ = supervisor.Run(bgCtx)
	}()

	log.Printf("[BOOTSTRAP] Bootstrap complete \u2014 %d modules, %d background runners",
		m.Registry.Len(), supervisor.Len())
	if m.Health != nil {
		m.Health.MarkReady()
	}

	// 8. Wait for signal or error
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

	// 9. Graceful teardown
	bgCancel()
	log.Println("[SERVER] Background goroutines cancelling \u2014 waiting for them to exit...")

	select {
	case <-supervisorDone:
		log.Println("[SERVER] Background goroutines stopped cleanly")
	case <-time.After(15 * time.Second):
		log.Printf("[SERVER] background shutdown timed out after 15s \u2014 proceeding with teardown anyway")
	}

	shutdownGRPCServer(grpcSrv)
	shutdownHTTPServer(srv, 30*time.Second)

	log.Println("[SERVER] Server stopped")
	return nil
}

// buildSupervisor registers all background runners, including the
// manifest auto-generation as a one-shot fire-and-forget runner.
func buildSupervisor(a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps, t *taskDeps) (*BackgroundSupervisor, error) {
	sup := NewBackgroundSupervisor()

	if a.OutboxDispatcher != nil {
		if err := sup.Register(RunnerFunc{
			name: "outbox-dispatcher",
			fn: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] Outbox dispatcher started \u2014 polling outbox_events")
				return a.OutboxDispatcher.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register outbox-dispatcher: %w", err)
		}
	}
	if m.DeliveryRunner != nil {
		if err := sup.Register(RunnerFunc{
			name: "delivery-runner",
			fn: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] DeliveryRunner started \u2014 polling PENDING job_deliveries")
				return m.DeliveryRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register delivery-runner: %w", err)
		}
	}
	if j.Lifecycle != nil {
		// PR-13 \u2192 PR-05 cutover: the Job-side reaper is DEPRECATED.
		//
		// History: PR-13 introduced VELOX_DISABLE_JOB_REAPER (default off)
		// as a stop-gap while the jobs lease_expiry column went away
		// (migration 048) and the canonical lease TTL moved to tasks
		// (migration 049 + PR-05 TaskLeaseReaper). With TaskLeaseReaper
		// registering as a separate supervisor runner (see below), the
		// Job-side zombie reaper is redundant. We KEEP the env-var read
		// and the supervisor runner for now (back-compat: removing
		// either would break operators still relying on the flag) but
		// emit a one-time DEPRECATED log on each boot so operators know
		// to migrate to TaskLeaseReaper.
		if os.Getenv("VELOX_DISABLE_JOB_REAPER") == "true" {
			log.Printf("[BOOTSTRAP] DEPRECATED env=VELOX_DISABLE_JOB_REAPER=true (PR-13 superseded by PR-05 TaskLeaseReaper; flag is now a no-op, set VELOX_TASK_LEASE_REAPER_DISABLED=true at the TaskLeaseReaper runner if you need to disable on the canonical path)")
		} else {
			log.Printf("[BOOTSTRAP] note=job-side zombie reaper is DEPRECATED; TaskLeaseReaper is the canonical master-side lease enforcer")
		}
	}
	if a.Reconciler != nil {
		if err := sup.Register(RunnerFunc{
			name: "artifact-reconciler",
			fn: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] artifacts.Reconciler started (4 rules: expired-uploads + staging, orphan-final-blobs, READY-no-blob QUARANTINED, stuck-STAGING; 15m tick)")
				a.Reconciler.Run(ctx, 15*time.Minute)
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register artifact-reconciler: %w", err)
		}
	}
	if t.TaskLifecycle != nil {
		if err := sup.Register(RunnerFunc{
			name: "taskgraph-dispatcher",
			fn: func(ctx context.Context) error {
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
						} else if n > 0 {
							log.Printf("[TASKGRAPH] TickReadiness: %d PENDING\u2192READY", n)
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register taskgraph-dispatcher: %w", err)
		}
	}
	// PR-05 follow-up: TaskLeaseReaper is now its own supervisor runner
	// (independent ticker, independent log prefix) so its cadence is
	// decoupled from the readiness dispatcher. 30 s default matches
	// the master-side defaultTaskLeaseTTL (30 min) so a freshly
	// claimed task waits at most TTL+30s before being reaped if its
	// worker crashes mid-flight, within the audit \u00a7P0.4 budget.
	if t.TaskLeaseReaper != nil {
		if err := sup.Register(RunnerFunc{
			name: "task-lease-reaper",
			fn: func(ctx context.Context) error {
				return t.TaskLeaseReaper.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register task-lease-reaper: %w", err)
		}
	}

	// Manifest auto-generation: one-shot fire-and-forget runner.
	// Runs once on startup and exits; the supervisor treats it as
	// a regular runner so it benefits from the same lifecycle.
	if w.UpdateHandler != nil {
		if err := sup.Register(RunnerFunc{
			name: "manifest-generator",
			fn: func(_ context.Context) error {
				if err := w.UpdateHandler.GenerateManifestV2(); err != nil {
					log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
				}
				// Always returns nil \u2014 manifest failure is never fatal.
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register manifest-generator: %w", err)
		}
	}
	return sup, nil
}
