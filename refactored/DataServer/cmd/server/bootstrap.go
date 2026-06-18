package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/artifacts"
	"velox-server/internal/audit"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	deliveryProviders "velox-server/internal/deliveries/providers"
	"velox-server/internal/grpcserver"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	handlersoutbox "velox-server/internal/handlers/outbox"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/pipeline"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/modules/ansible"
	"velox-server/internal/modules/drive"
	"velox-server/internal/modules/frontend"
	"velox-server/internal/modules/health"
	"velox-server/internal/modules/livestream"
	"velox-server/internal/modules/workers"
	"velox-server/internal/modules/youtube"
	"velox-server/internal/outbox"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"velox-server/internal/workflow"
	voiceoverassets "velox-server/internal/assets"

	"google.golang.org/grpc"
)

type serverPaths struct {
	dataDir string
}

type serverDeps struct {
	paths               *serverPaths
	fileQ               *queue.FileQueue
	reg                 *workersreg.Registry
	workersRepo         store.WorkersRepository
	sqliteStore         *store.SQLiteStore
	workerUpdateHandler *workerhandlers.WorkerUpdateHandler
	workerLifecycle     *lifecycle.Handler
	ansibleModule       *ansible.Module
	youtubeModule       *youtube.Module
	driveModule         *drive.Module
	workflowRepo        workflow.Repository
	outboxStore         *outbox.Store
	outboxDispatcher    *outbox.Dispatcher
	deliveryRunner      *deliveries.DeliveryRunner
	blobStore           store.BlobStore

	// PR 2 (chunk 4): artifacts.Service is the single-tx, master-computed-
	// hash gate for ArtifactUploaded. Bootstrap owns it (the only place
	// outside grpcserver that holds a reference); the handler can READ
	// artifacts/artifacts.Service via deps.artifactSvc but cannot bypass
	// it. Closely parallels the lifecyclePR3 / artifactGate pattern below:
	// bootstrap is the composition root, never anything else.
	artifactSvc *artifacts.Service

	// ── PR 3 composition ──────────────────────────────────────────────────
	//
	// lifecyclePR3 is the slim transactional LifecycleService.
	// artifactGate is the ONLY path to SUCCEEDED — held by the bootstrap
	// as a "private port" that handlers cannot reach directly.
	// jobRepo is reused because the gate stores a JobRepository reference
	// (not the lifecycle wrapper), matching the artifact_success_gate
	// spec in PR 3 section "Il completamento SUCCEEDED non deve essere
	// pubblico per gli handler".
	lifecyclePR3 *queue.LifecycleService
	artifactGate *queue.ArtifactSuccessGate

	assetService *voiceoverassets.AssetService
}

// grpcServer is an interface satisfied by *grpc.Server for lifecycle management.
type grpcServer interface {
	GracefulStop()
	Stop()
}

// grpcServerWrapper wraps a gRPC server with its listener for lifecycle management.
type grpcServerWrapper struct {
	Server   *grpc.Server
	Listener net.Listener
}

func (w *grpcServerWrapper) GracefulStop() { w.Server.GracefulStop() }
func (w *grpcServerWrapper) Stop()         { w.Server.Stop() }

func configureTrustedProxies(r *gin.Engine) {
	if err := r.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
		log.Printf("bootstrap: SetTrustedProxies failed: %v", err)
	}
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Request-ID") == "" {
			c.Writer.Header().Set("X-Request-ID", fmt.Sprintf("%d", time.Now().UnixNano()))
		}
		c.Next()
	}
}

func accessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Printf("%s %s %d %s", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))
	}
}

func addGzipHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Vary", "Accept-Encoding")
		c.Next()
	}
}

// outboxWorkflowAdapter adapts *outbox.Store to the workflow.OutboxWriter
// interface by mapping WorkflowOutboxEvent fields to outbox.InsertParams.
type outboxWorkflowAdapter struct {
	store *outbox.Store
}

func (a *outboxWorkflowAdapter) Enqueue(ctx context.Context, ev workflow.WorkflowOutboxEvent) error {
	_, err := a.store.Insert(ctx, nil, outbox.InsertParams{
		AggregateType: "workflow",
		AggregateID:   ev.AggregateID,
		EventType:     ev.EventType,
		Payload:       ev.Payload,
	})
	return err
}

func buildServerDeps(cfg *config.Config) (*serverDeps, error) {
	if cfg == nil {
		cfg = config.FromEnv()
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sqliteStore, err := store.NewSQLiteStore(cfg.Database.DBPath)
	if err != nil {
		return nil, err
	}

	if cfg.DataDir != "" {
		if results, err := sqliteStore.ImportLegacyJSON(cfg.DataDir); err != nil {
			log.Printf("[BOOTSTRAP] Legacy JSON import error (non-fatal): %v", err)
		} else {
			for _, r := range results {
				if r.Status == "imported" {
					log.Printf("[BOOTSTRAP] Migrated: %s (%d records)", r.Source.Name, r.Imported)
				}
			}
		}
	}

	reg := workersreg.New(sqliteStore)
	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from SQLite", revokedCount)
	}
	workersRepo := store.NewSQLiteWorkersRepository(sqliteStore)
	cmdMgr := workersreg.NewCommandManager(sqliteStore)
	tokenMgr := workersreg.NewTokenManager(sqliteStore)
	workerUpdateHandler := workerhandlers.NewWorkerUpdateHandler(cfg, reg, cmdMgr, tokenMgr, cfg.Runtime.DataDir)
	workerLifecycle := lifecycle.NewHandler(cfg, reg, sqliteStore)

	jobRepo := store.NewSQLiteJobRepository(sqliteStore)
	lcService, err := queue.NewLegacyLifecycleService(jobRepo, sqliteStore)
	if err != nil {
		return nil, err
	}

	// ── PR 3 composition root ──────────────────────────────────────────────
	//
	// lcPR3 is the slim transactional LifecycleService. It owns the
	// JobRepository + a RealClock — and NOTHING ELSE. SUCCEEDED is
	// unreachable from this struct.
	//
	// artifactGate is the ONLY path to SUCCEEDED. It holds the secret
	// JobRepository reference; bootstrap is the only component with a
	// pointer to it. Handlers cannot reach it.
	lcPR3, err := queue.NewLifecycleService(jobRepo, queue.RealClock{})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: PR 3 lifecycle service: %w", err)
	}
	artifactGate := queue.NewArtifactSuccessGate(jobRepo)
	artifactGate.SetAuditHook(func(jobID, artifactID string, revision int) {
		log.Printf("[ARTIFACT-GATE] promoted job %s artifact=%s revision=%d → SUCCEEDED", jobID, artifactID, revision)
	})

	querySvc := queue.NewQueryService(sqliteStore)

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		DBStore:    sqliteStore,
		MaxRetries: cfg.Workers.MaxJobAttempts,
	}, lcService, querySvc)
	if err != nil {
		return nil, err
	}

	outboxStore := outbox.NewStore(sqliteStore.DB())

	workflowRepo := workflow.NewSQLiteRepository(sqliteStore.DB())
	workflowRepo.SetOutbox(&outboxWorkflowAdapter{store: outboxStore})

	var blobStore store.BlobStore
	localBS, bsErr := store.NewLocalBlobStore(cfg.Runtime.StagingDir, cfg.Runtime.StorageDir)
	if bsErr != nil {
		log.Printf("[BOOTSTRAP] BlobStore init warning: %v -- using nop blob store", bsErr)
		blobStore = store.NewNopBlobStore(cfg.Runtime.DataDir)
	} else {
		blobStore = localBS
	}
	log.Printf("[BOOTSTRAP] BlobStore ready: staging=%s storage=%s", blobStore.StagingDir(), blobStore.FinalDir())

	// PR 2 (chunk 4): artifacts.Service. The same *sql.DB that
	// store.SQLiteStore uses for migrations/journaling is passed in so
	// FinalizeArtifactAndCompleteJob can join its multi-table tx with the
	// artifact_uploads UPDATE. Clock is nil → defaults to realClock{}
	// inside NewService.
	//
	// The repository uses SQLiteRepository against the same connection —
	// SQLite serializes writers so concurrent uploads on the same job_id
	// are race-free at the SQL layer. The state-machine legality (which
	// status transitions are allowed) lives in Service not in the repo.
	artifactSvc := artifacts.NewService(
		artifacts.NewSQLiteRepository(sqliteStore.DB()),
		blobStore,
		sqliteStore.DB(),
		nil, // RealClock default
	)
	log.Printf("[BOOTSTRAP] artifacts.Service ready (single-tx, master-computed-hash gate for ArtifactUploaded)")

	outboxRegistry := outbox.NewRegistry()
	stepReady := handlersoutbox.StepReadyHandler{Wf: workflowRepo, Q: fileQ}
	jobSucceeded := handlersoutbox.JobSucceededHandler{Wf: workflowRepo}
	artifactReady := handlersoutbox.ArtifactReadyHandler{Wf: workflowRepo}
	deliveryCreated := handlersoutbox.DeliveryCreatedHandler{Wf: workflowRepo}
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
	outboxDispatcher := outbox.NewDispatcher(outboxStore, outboxRegistry, outbox.Config{
		PollInterval: 750 * time.Millisecond,
		BatchSize:    32,
		LockDuration: 30 * time.Second,
		MaxAttempts:  5,
	})

	return &serverDeps{
		paths:               &serverPaths{dataDir: cfg.Runtime.DataDir},
		fileQ:               fileQ,
		reg:                 reg,
		workersRepo:         workersRepo,
		sqliteStore:         sqliteStore,
		workerUpdateHandler: workerUpdateHandler,
		workerLifecycle:     workerLifecycle,
		workflowRepo:        workflowRepo,
		outboxStore:         outboxStore,
		outboxDispatcher:    outboxDispatcher,
		blobStore:           blobStore,
		artifactSvc:         artifactSvc,
		lifecyclePR3:        lcPR3,
		artifactGate:        artifactGate,
	}, nil
}

func runServer(cfg *config.Config) error {
	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	deps, err := buildServerDeps(cfg)
	if err != nil {
		return err
	}

	runDuadDBBootCheck(deps, cfg)

	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)
	pipeline.InitRemoteEngine(cfg)

	registry.Register(health.New())
	registry.Register(workers.New(cfg, deps.reg, deps.workerLifecycle, deps.workerUpdateHandler, auth))
	ytMod := youtube.New(cfg, deps.paths.dataDir, deps.sqliteStore)
	deps.youtubeModule = ytMod
	registry.Register(ytMod)
	driveMod := drive.New(cfg)
	deps.driveModule = driveMod
	registry.Register(driveMod)

	maxVoiceoverBytes := int64(256 * 1024 * 1024)
	if raw := strings.TrimSpace(os.Getenv("VELOX_MAX_VOICEOVER_BYTES")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			maxVoiceoverBytes = parsed
		}
	}
	if driveMod != nil && deps.sqliteStore != nil {
		driveMod.WithSQLiteStore(deps.sqliteStore)
	}
	voiceoverBridge := voiceoverassets.NewService(cfg.DataDir, []string{cfg.DataDir}, maxVoiceoverBytes, driveMod.Service())
	enqueue.SetVoiceoverAssetService(voiceoverBridge)

	// Generic asset registry (PR 6): content-addressed, multi-role asset store.
	assetRepo := store.NewSQLiteAssetRepository(deps.sqliteStore)
	typedResolvers := voiceoverassets.NewTypedResolversFromStore(voiceoverBridge.Store(), driveMod.Service(), nil)
	assetRegistry := voiceoverassets.NewResolverRegistry(typedResolvers...)
	deps.assetService = voiceoverassets.NewAssetService(assetRepo, deps.blobStore, assetRegistry, nil)
	ansibleMod := ansible.New(cfg, deps.paths.dataDir, auth, deps.sqliteStore)
	deps.ansibleModule = ansibleMod
	registry.Register(ansibleMod)
	livestreamMod := livestream.New(ytMod.Service, deps.sqliteStore)
	registry.Register(livestreamMod)
	registry.Register(frontend.New(cfg))

	deliveryReg := deliveries.NewRegistry()
	if ytMod != nil && ytMod.Service() != nil {
		ytProvider := deliveryProviders.NewYouTubeProvider(ytMod.Service(), deps.blobStore)
		deliveryReg.Register(ytProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: youtube")
	}
	if driveMod != nil && driveMod.Service() != nil {
		driveProvider := deliveryProviders.NewDriveProvider(driveMod.Service(), deps.blobStore)
		deliveryReg.Register(driveProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: drive")
	}

	deliveryRunner := deliveries.NewDeliveryRunner(
		deliveries.DefaultRunnerConfig(),
		deliveryReg,
		deps.sqliteStore,
		fmt.Sprintf("delivery-runner-%d", time.Now().UnixNano()),
	)
	deps.deliveryRunner = deliveryRunner

	r := newRouter(cfg, deps, registry)

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("[SERVER] Velox master listening on %s", addr)

	var grpcSrv grpcServer
	if cfg.Server.GRPCPort > 0 {
		lcSvc := deps.fileQ.LifecycleService()
		if lcSvc == nil {
			log.Printf("[SERVER] gRPC disabled: fileQ.LifecycleService is nil (deps.fileQ=%v)", deps.fileQ)
		} else {
			transitionSvc := queue.NewTransitionService(lcSvc)
			cmdMgr := workersreg.NewCommandManager(deps.sqliteStore)
			tokenMgr := workersreg.NewTokenManager(deps.sqliteStore)
			// PR 2 (chunk 4): the gRPC artifact handler no longer trusts
			// worker-declared path/size/sha. Wiring flows bootstrap's
			// already-built artifacts.Service (the *artifacts.Service that
			// owns artifact_uploads + canonical-key promotion + single-tx
			// CAS) into grpcserver.NewHandler; the handler rejects nil at
			// handleArtifactUploaded time as a defense-in-depth check.
			grpcHandlerConfig := &grpcserver.HandlerConfig{
				PushMode: cfg.Server.GRPCPushMode,
			}
			grpcHandler := grpcserver.NewHandler(
				deps.reg, cmdMgr, tokenMgr, lcSvc, transitionSvc, artifactSvc, deps.sqliteStore, grpcHandlerConfig,
			)

			grpcServer, lis, err := grpcserver.StartGRPCServer(
				cfg.Server.GRPCPort, grpcHandler,
				cfg.Server.GRPCTLSCertFile, cfg.Server.GRPCTLSKeyFile, cfg.Server.GRPCTLSCAFile,
			)
			if err != nil {
				log.Printf("[SERVER] gRPC server failed to start: %v", err)
			} else if grpcServer != nil {
				grpcSrv = &grpcServerWrapper{Server: grpcServer, Listener: lis}
			}
		}
	}

	if deps.workerUpdateHandler != nil {
		go func() {
			if err := deps.workerUpdateHandler.GenerateManifestV2(); err != nil {
				log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
			}
		}()
	}

	if deps.outboxDispatcher != nil {
		go func() {
			log.Printf("[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
			if err := deps.outboxDispatcher.Run(context.Background()); err != nil {
				log.Printf("[BOOTSTRAP] Outbox dispatcher exited: %v", err)
			}
		}()
	}

	if deps.deliveryRunner != nil {
		go func() {
			log.Printf("[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
			if err := deps.deliveryRunner.Run(context.Background()); err != nil {
				log.Printf("[BOOTSTRAP] DeliveryRunner exited: %v", err)
			}
		}()
	}

	if deps.fileQ != nil {
		go func() {
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				n, err := deps.fileQ.RequeueZombieJobs(context.Background(), 30*time.Minute)
				if err != nil {
					log.Printf("[ZOMBIE] requeue error: %v", err)
				} else if n > 0 {
					log.Printf("[ZOMBIE] requeued %d stuck jobs", n)
				}
			}
		}()
	}

	if deps.blobStore != nil && deps.artifactSvc != nil {
		rec, recErr := artifacts.NewReconciler(
			deps.sqliteStore.DB(),
			deps.blobStore,
			artifacts.NewSQLiteRepository(deps.sqliteStore.DB()),
			nil, // RealClock default
			artifacts.DefaultReconcilerConfig(),
		)
		if recErr != nil {
			log.Printf("[BOOTSTRAP] Reconciler init failed: %v -- continuing without it", recErr)
		} else {
			recCtx, recCancel := context.WithCancel(context.Background())
			go rec.Run(recCtx, 15*time.Minute)
			defer recCancel()
			log.Printf("[BOOTSTRAP] artifacts.Reconciler started (4 rules: expired-uploads + staging, orphan-final-blobs, READY-no-blob QUARANTINED, stuck-STAGING; 15m tick)")
		}
	}

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if grpcSrv != nil {
		grpcSrv.GracefulStop()
		log.Println("[SERVER] gRPC server stopped")
	}

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[SERVER] Graceful shutdown failed: %v", err)
		return err
	}

	if deps.sqliteStore != nil {
		if err := deps.sqliteStore.Close(); err != nil {
			log.Printf("[SERVER] Store close failed: %v", err)
		}
	}

	log.Println("[SERVER] Server stopped")
	return nil
}

func runDuadDBBootCheck(deps *serverDeps, cfg *config.Config) {
	log.Printf("[BOOTSTRAP] NOTE: dual-DB boot check is a no-op stub (PR9 cutover)")
}


func runDataLayerAudit(cfg *config.Config) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}

	secretsDir := filepath.Join(dataDir, "secrets")
	auditor := audit.NewDataLayerAuditor(dataDir, secretsDir)

	auditor.AllowLegacy("drive/drive_links.json")
	auditor.AllowLegacy("drive/drive_master_folders_list.json")
	auditor.AllowLegacy("jobs/multi_step_jobs.json")
	auditor.AllowLegacy("jobs/dead_letter_queue.json")
	auditor.AllowLegacy("analytics/analytics_cache.json")

	result := auditor.Audit()

	if !result.Passed {
		log.Printf("[AUDIT] Data layer audit FAILED with %d errors", len(result.Errors))
		for _, e := range result.Errors {
			log.Printf("[AUDIT] ERROR: %s", e)
		}
		return result.FailOnError()
	}

	if len(result.Warnings) > 0 {
		log.Printf("[AUDIT] Data layer audit passed with %d warnings", len(result.Warnings))
		for _, w := range result.Warnings {
			log.Printf("[AUDIT] WARNING: %s", w)
		}
	} else {
		log.Printf("[AUDIT] Data layer audit PASSED")
	}

	return nil
}
