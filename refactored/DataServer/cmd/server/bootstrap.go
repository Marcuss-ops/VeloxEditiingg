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
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
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
	// it. Closely parallels the lifecyclePR3 composition pattern below:
	// bootstrap is the composition root, never anything else.
	artifactSvc *artifacts.Service

	// PR chunked: ChunkedUploadService wraps artifactSvc with persistent
	// chunk tracking so resumable chunked uploads survive master restarts.
	chunkedHandler *workerhandlersuploads.ChunkedUploadHandler

	// ── Lifecycle ──────────────────────────────────────────────────────────
	//
	// lifecycleSvc is the sole transactional LifecycleService used by
	// FileQueue, gRPC, HTTP handlers, reaper, and workflow. SUCCEEDED is
	// reachable only through artifacts.Service.FinalizeArtifactAndCompleteJob
	// which performs jobs CAS + artifacts CAS + outbox in a single tx.
	lifecycleSvc  *queue.LifecycleService

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

	// ── Lifecycle composition root ─────────────────────────────────────────
	//
	// lifecycleSvc is the sole transactional LifecycleService. It owns the
	// JobRepository + a RealClock — and NOTHING ELSE. SUCCEEDED is
	// reachable only through artifacts.Service.FinalizeArtifactAndCompleteJob.
	lifecycleSvc, err := queue.NewLifecycleService(jobRepo, queue.RealClock{})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: lifecycle service: %w", err)
	}

	querySvc := queue.NewQueryService(sqliteStore)

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		DBStore:    sqliteStore,
		MaxRetries: cfg.Workers.MaxJobAttempts,
	}, lifecycleSvc, querySvc)
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

	// PR 3.5-a: artifacts.Service is the only component that flips
	// jobs.status='SUCCEEDED'. It owns the FinalizationRepository
	// (atomic-tx SUCCEEDED write + atomic-tx artifacts+artifact_uploads
	// insert) and the old Repository (upload-session CRUD). The same
	// *sql.DB is shared so the finalization tx can join with the
	// concurrent update on artifact_uploads (step 7 of FinalizeVerified).
	//
	// PR delivery plans: the FinalizationRepository receives a
	// DeliveryPlanResolver so FinalizeVerified resolves per-job
	// delivery destinations instead of querying all globally enabled ones.
	planResolver := deliveries.NewSQLiteDeliveryPlanResolver(sqliteStore.DB())
	finRepo := artifacts.NewSQLiteFinalizationRepository(sqliteStore.DB())
	finRepo.WithPlanResolver(planResolver)
	artifactSvc := artifacts.NewService(
		artifacts.NewSQLiteRepository(sqliteStore.DB()),
		finRepo,
		blobStore,
		sqliteStore.DB(),
		nil, // RealClock default
	)
	log.Printf("[BOOTSTRAP] artifacts.Service ready (single-tx SUCCEEDED gate via FinalizationRepository.FinalizeVerified + DeliveryPlanResolver)")

	// PR chunked: ChunkedUploadService wraps artifactSvc with persistent
	// chunk tracking. Uses the same Repository and BlobStore as the
	// artifact pipeline.
	chunkedSvc := artifacts.NewChunkedUploadService(
		artifactSvc,
		artifacts.NewSQLiteRepository(sqliteStore.DB()),
		blobStore,
		sqliteStore.DB(),
	)
	chunkedHandler := workerhandlersuploads.NewChunkedUploadHandler(chunkedSvc)
	log.Printf("[BOOTSTRAP] ChunkedUploadService ready (persistent chunked upload via artifact pipeline)")

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
		chunkedHandler:      chunkedHandler,
		lifecycleSvc:        lifecycleSvc,
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

	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)
	pipeline.InitRemoteEngine(cfg)

	registry.Register(health.New())
	registry.Register(workers.New(cfg, deps.reg, deps.workerLifecycle, deps.workerUpdateHandler, auth, deps.assetService, deps.blobStore))
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

	// ── Asset Registry (PR 6) ─────────────────────────────────────────────
	//
	// Replaces the old voiceover bridge (voiceoverassets.NewService).
	// The new AssetService uses content-addressed storage via BlobStore + DB
	// and provides RewriteVoiceoverPayload for the enqueue flow.
	voiceoverStore := voiceoverassets.NewStore(cfg.Runtime.DataDir, maxVoiceoverBytes, []string{cfg.Runtime.DataDir})
	typedResolvers := voiceoverassets.NewTypedResolversFromStore(voiceoverStore, driveMod.Service(), nil)
	assetRegistry := voiceoverassets.NewResolverRegistry(typedResolvers...)
	assetRepo := store.NewSQLiteAssetRepository(deps.sqliteStore)
	deps.assetService = voiceoverassets.NewAssetService(assetRepo, deps.blobStore, assetRegistry, nil)

	// Wire the new AssetService into the enqueue flow (replaces the old
	// voiceover bridge's RewriteVoiceoverPayload).
	enqueue.SetVoiceoverAssetService(deps.assetService)
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
		lcSvc := deps.lifecycleSvc
		if lcSvc == nil {
			log.Printf("[SERVER] gRPC disabled: lifecycleSvc is nil")
		} else {
			cmdMgr := workersreg.NewCommandManager(deps.sqliteStore)
			tokenMgr := workersreg.NewTokenManager(deps.sqliteStore)
			grpcHandlerConfig := &grpcserver.HandlerConfig{
				PushMode: cfg.Server.GRPCPushMode,
			}
			grpcHandler := grpcserver.NewHandler(
				deps.reg, cmdMgr, tokenMgr, lcSvc, deps.artifactSvc, deps.sqliteStore, grpcHandlerConfig,
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

	if deps.lifecycleSvc != nil {
		go func() {
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				results, err := deps.lifecycleSvc.RequeueExpiredLeases(context.Background(), 100)
				if err != nil {
					log.Printf("[ZOMBIE] requeue error: %v", err)
				} else if len(results) > 0 {
					log.Printf("[ZOMBIE] requeued %d stuck jobs", len(results))
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

func runDataLayerAudit(cfg *config.Config) error {
	dataDir := cfg.Runtime.DataDir
	if dataDir == "" {
		dataDir = "."
	}

	secretsDir := filepath.Join(dataDir, "secrets")
	auditor := audit.NewDataLayerAuditor(dataDir, secretsDir)

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
