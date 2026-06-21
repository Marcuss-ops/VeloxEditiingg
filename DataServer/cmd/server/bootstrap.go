package main

// TODO(platform/database): the package velox-server/internal/platform/database
// was dropped by the Phase 1 architecture refactor (eliminate queue facade
// / remove dark_editor / make BlobStore mandatory) without propagating
// downstream adaptations. Three consumers (this file,
// internal/store/sqlite.go, and bootstrap_postgres_dispatch_test.go) still
// depend on its symbols (Open / Config / Handle / Driver / DriverSQLite /
// DriverPostgres). The DataServer build is intentionally broken until a
// platform/database restoration PR lands — see
// docs/architecture/OWNERSHIP.md for the platform-cutover roadmap.
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

	"velox-server/internal/app"
	"velox-server/internal/artifacts"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	"velox-server/internal/grpcserver"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/outbox"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"velox-server/internal/workflow"
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
	workflowRepo        workflow.Repository
	outboxStore         *outbox.Store
	outboxDispatcher    *outbox.Dispatcher
	deliveryRunner      *deliveries.DeliveryRunner	blobStore           store.BlobStore
	artifactSvc         *artifacts.Service
	cmdMgr              *workersreg.CommandManager
	chunkedHandler      *workerhandlersuploads.ChunkedUploadHandler
	lifecycleSvc        *queue.LifecycleService
	assetService        *voiceoverassets.AssetService
	enqueuer            *enqueue.Enqueuer
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
	j, err := buildJobs(cfg, p)
	if err != nil {
		return nil, err
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
		workflowRepo:        a.WorkflowRepo,
		outboxStore:         p.Outbox,
		outboxDispatcher:    a.OutboxDispatcher,
		blobStore:           p.BlobStore,
		artifactSvc:         a.ArtifactSvc,
		cmdMgr:              w.CommandManager,
		lifecycleSvc:        j.Lifecycle,
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

	j, err := buildJobs(cfg, p)
	if err != nil {
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
	m, err := buildModules(cfg, p, j, w, a)
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
		workflowRepo:        a.WorkflowRepo,
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
	}

	// 3. Build router
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
			if err := grpcserver.ValidateWorkerAllowlist(cfg.Workers.AllowedWorkers, insecureDev); err != nil {
				return err
			}
			grpcHandler := grpcserver.NewHandler(
				w.Registry, w.CommandManager, jobsRepo, a.ArtifactSvc, p.SQLite,
				buildGRPCHandlerConfig(cfg, insecureDev),
			)
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
	supervisor, err := buildSupervisor(a, m, j, p, w)
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

	log.Printf("[BOOTSTRAP] Bootstrap complete — %d modules, %d background runners",
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

// buildSupervisor registers all background runners, including the
// manifest auto-generation as a one-shot fire-and-forget runner.
func buildSupervisor(a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps) (*BackgroundSupervisor, error) {
	sup := NewBackgroundSupervisor()

	if a.OutboxDispatcher != nil {
		if err := sup.Register(RunnerFunc{
			name: "outbox-dispatcher",
			fn: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
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
				log.Printf("[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
				return m.DeliveryRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register delivery-runner: %w", err)
		}
	}
	if j.Lifecycle != nil {
		if err := sup.Register(RunnerFunc{
			name: "zombie-reaper",
			fn: func(ctx context.Context) error {
				ticker := time.NewTicker(60 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						results, err := j.Lifecycle.RequeueExpiredLeases(ctx, 100)
						if err != nil {
							log.Printf("[ZOMBIE] requeue error: %v", err)
						} else if len(results) > 0 {
							log.Printf("[ZOMBIE] requeued %d stuck jobs", len(results))
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register zombie-reaper: %w", err)
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
				// Always returns nil — manifest failure is never fatal.
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register manifest-generator: %w", err)
		}
	}
	return sup, nil
}
