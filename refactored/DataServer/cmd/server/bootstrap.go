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
	voiceoverassets "velox-server/internal/assets"
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
}

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

	// PR 8: outbox.Store wraps the same SQLite handle so CompleteJobTx
	// (write JOB_SUCCEEDED) and FinalizeArtifactVerified (write
	// ARTIFACT_READY) emit through the generic outbox pipeline rather
	// than the legacy orchestrator_outbox.
	outboxStore := outbox.NewStore(sqliteStore.DB())
	sqliteStore.SetOutbox(outboxStore)

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		DBStore:    sqliteStore,
		MaxRetries: cfg.Workers.MaxJobAttempts,
	})
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
	updateMgr := workersreg.NewUpdateManager()
	tokenMgr := workersreg.NewTokenManager(sqliteStore)
	workerUpdateHandler := workerhandlers.NewWorkerUpdateHandler(cfg, reg, cmdMgr, updateMgr, tokenMgr, cfg.Runtime.DataDir)
	workerLifecycle := lifecycle.NewHandler(cfg, reg, sqliteStore, cfg.Runtime.DataDir)

	jobRepo := store.NewSQLiteJobRepository(sqliteStore)
	fileQ.SetJobRepository(jobRepo)

	// PR 9: workflow.Repository replaces the legacy *queue.Orchestrator.
	// WorkflowSpec -> workflow_runs/workflow_steps (PR 8 schema).
	workflowRepo := workflow.NewSQLiteRepository(sqliteStore.DB())
	workflowRepo.SetOutbox(outboxStore)

	var blobStore store.BlobStore
	var bsErr error
	blobStore, bsErr = store.NewLocalBlobStore(cfg.Runtime.StagingDir, cfg.Runtime.StorageDir)
	if bsErr != nil {
		log.Printf("[BOOTSTRAP] BlobStore init warning: %v — using nop blob store", bsErr)
		blobStore = store.NewNopBlobStore(cfg.Runtime.DataDir)
	}
	log.Printf("[BOOTSTRAP] BlobStore ready: staging=%s storage=%s", blobStore.StagingDir(), blobStore.FinalDir())

	// PR 8: outbox.Registry is the single mapping event_type -> handler.
	// Build AFTER fileQ so workflowStepReadyHandler can submit jobs.
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
	}, nil
}

func runServer(cfg *config.Config) error {
	deps, err := buildServerDeps(cfg)
	if err != nil {
		return err
	}

	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)

	pipeline.InitRemoteEngine(cfg)

	registry.Register(health.New())
	registry.Register(workers.New(cfg, deps.reg, deps.workerLifecycle, deps.workerUpdateH, auth))
	ytMod := youtube.New(cfg, deps.paths.dataDir, deps.sqliteStore)
	deps.youtubeModule = ytMod
	registry.Register(ytMod)
	driveMod := drive.New(cfg, deps.sqliteStore)
	deps.driveModule = driveMod
	if driveMod != nil {
		registry.Register(driveMod)
	}
	maxVoiceoverBytes := int64(256 * 1024 * 1024)
	if raw := strings.TrimSpace(os.Getenv("VELOX_MAX_VOICEOVER_BYTES")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			maxVoiceoverBytes = parsed
		}
	}
	voiceoverBridge := voiceoverassets.NewService(cfg.Runtime.DataDir, []string{cfg.Runtime.DataDir}, maxVoiceoverBytes, driveMod.Service())
	enqueue.SetVoiceoverAssetService(voiceoverBridge)
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
		transitionSvc := queue.NewTransitionService(deps.sqliteStore)
		cmdMgr := workersreg.NewCommandManager(deps.sqliteStore)
		tokenMgr := workersreg.NewTokenManager(deps.sqliteStore)

		grpcHandlerConfig := &grpcserver.HandlerConfig{
			ShadowMode: cfg.Server.GRPCShadowMode,
			PushMode:   cfg.Server.GRPCPushMode,
		}
		grpcHandler := grpcserver.NewHandler(
			deps.reg, cmdMgr, tokenMgr, transitionSvc, deps.sqliteStore, grpcHandlerConfig,
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

	if deps.workerUpdateHandler != nil {
		go func() {
			if err := deps.workerUpdateHandler.GenerateManifestV2(); err != nil {
				log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
			}
		}()
	}

	// PR 9 cutover: outbox dispatcher replaces the legacy Orchestrator loop.
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

	if deps.blobStore != nil {
		go func() {
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				count, err := reconcileStaging(deps.sqliteStore, deps.blobStore)
				if err != nil {
					log.Printf("[STAGING] reconciler error: %v", err)
				} else if count > 0 {
					log.Printf("[STAGING] reconciler removed %d orphaned files", count)
				}
			}
		}()
		log.Printf("[BOOTSTRAP] Staging reconciler started — cleanup orphaned files every 15m")
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

type grpcServer interface {
	GracefulStop()
}

type grpcServerWrapper struct {
	*grpc.Server
	Listener net.Listener
}

// reconcileStaging scans the staging directory for files that are orphaned
// (no matching artifact row) or whose artifact has been in STAGING/QUARANTINED
// for more than 1 hour. Removes the orphaned files from disk.
func reconcileStaging(dbStore *store.SQLiteStore, blobStore store.BlobStore) (int, error) {
	stagingDir := blobStore.StagingDir()
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read staging dir: %w", err)
	}

	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	removed := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(stagingDir, entry.Name())

		info, err := entry.Info()
		if err != nil {
			continue
		}
		if time.Since(info.ModTime()) < 1*time.Hour {
			continue
		}

		var status string
		err = dbStore.DB().QueryRowContext(ctx,
			`SELECT status FROM artifacts WHERE storage_key = ? OR local_path = ? LIMIT 1`,
			path, path,
		).Scan(&status)
		if err != nil {
			if rmErr := os.Remove(path); rmErr == nil {
				removed++
				log.Printf("[STAGING] Removed orphaned file: %s", path)
			}
			continue
		}

		if status == "STAGING" || status == "QUARANTINED" {
			var createdAt string
			_ = dbStore.DB().QueryRowContext(ctx,
				`SELECT created_at FROM artifacts WHERE (storage_key = ? OR local_path = ?) AND status = ?`,
				path, path, status,
			).Scan(&createdAt)
			if createdAt != "" && createdAt < cutoff {
				if rmErr := os.Remove(path); rmErr == nil {
					removed++
					log.Printf("[STAGING] Removed stuck artifact file (%s): %s", status, path)
				}
			}
		}
	}
	return removed, nil
}

func runDataLayerAudit(cfg *config.Config) error {
	dataDir := cfg.Runtime.DataDir
	if dataDir == "" {
		dataDir = "."
	}

	secretsDir := filepath.Join(dataDir, "secrets")
	auditor := audit.NewDataLayerAuditor(dataDir, secretsDir, cfg.Database.DBPath)

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
