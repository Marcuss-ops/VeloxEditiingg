package main

import (
	"context"
	"fmt"
	"log"
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
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
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
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
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
	workerUpdateHandler *workersapi.WorkerUpdateHandler
	workerLifecycle     *lifecycle.Handler
	ansibleModule       *ansible.Module
	youtubeModule       *youtube.Module
	driveModule         *drive.Module
	orchestrator        *queue.Orchestrator
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

	if err := os.MkdirAll(filepath.Dir(cfg.DBDSN), 0o755); err != nil {
		return nil, err
	}

	sqliteStore, err := store.NewSQLiteStore(cfg.DBDSN)
	if err != nil {
		return nil, err
	}

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		DBStore:    sqliteStore,
		MaxRetries: cfg.MaxJobAttempts,
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

	// Worker Update Handler (bundle download, manifest, etc.)
	cmdMgr := workersreg.NewCommandManager()
	updateMgr := workersreg.NewUpdateManager()
	tokenMgr := workersreg.NewTokenManager()
	workerUpdateHandler := workersapi.NewWorkerUpdateHandler(cfg, reg, cmdMgr, updateMgr, tokenMgr, cfg.DataDir)
	workerLifecycle := lifecycle.NewHandler(cfg, reg, cfg.DataDir)

	// Create orchestrator for multi-step job pipelines
	orch, err := queue.NewOrchestrator(nil, fileQ, sqliteStore)
	if err != nil {
		return nil, fmt.Errorf("orchestrator init: %w", err)
	}

	return &serverDeps{
		paths:               &serverPaths{dataDir: cfg.DataDir},
		fileQ:               fileQ,
		reg:                 reg,
		workersRepo:         workersRepo,
		sqliteStore:         sqliteStore,
		workerUpdateHandler: workerUpdateHandler,
		workerLifecycle:     workerLifecycle,
		orchestrator:        orch,
	}, nil
}

func runServer(cfg *config.Config) error {
	deps, err := buildServerDeps(cfg)
	if err != nil {
		return err
	}

	// Run data layer audit AFTER database init
	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	// Boot-time dual-DB check: compare the runtime velox.db against every
	// well-known source candidate (../data/velox.db, .velox/data/velox.db,
	// worker_runtime/velox.db, ...). Catches the "two DB copies" race that
	// historically caused YouTube groups to disappear after deploys when
	// the runtime DBDSN pointed at a stale or shared file. Same-path hits
	// are always reported; staleness check is gated by
	// VELOX_DB_STALE_HOURS (default 24h; 0 disables the staleness check
	// while keeping same-path detection).
	runDuadDBBootCheck(deps, cfg)

	// Boot-time OAuth-token consolidation: REMOVED. The runtime path is
	// SQLite-only (S6 verdict) and no server component reads from
	// <DataDir>/secrets/youtube/tokens/*.json on boot. The legacy
	// migrate CLI and the OAuth JSON consolidator have been removed
	// entirely; fresh installs are expected to land credentials
	// directly via the canonical OAuth callback.

	registry := app.NewRegistry()
	auth := api.AdminAuthMiddleware(cfg)

	// Init pipeline remote engine (connects to external script generation service)
	pipeline.InitRemoteEngine(cfg)

	// Register all modules
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
	voiceoverBridge := voiceoverassets.NewService(cfg.DataDir, []string{cfg.DataDir}, maxVoiceoverBytes, driveMod.Service())
	enqueue.SetVoiceoverAssetService(voiceoverBridge)
	ansibleMod := ansible.New(cfg, deps.paths.dataDir, auth, deps.sqliteStore)
	deps.ansibleModule = ansibleMod
	registry.Register(ansibleMod)
	livestreamMod := livestream.New(ytMod.Service, deps.sqliteStore)
	registry.Register(livestreamMod)
	registry.Register(frontend.New(cfg))

	// Create gin engine with middleware
	r := newRouter(cfg, deps, registry)

	addr := fmt.Sprintf(":%d", cfg.MasterPort)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("[SERVER] Velox master listening on %s", addr)

	// Auto-generate manifest_v2.json at startup
	if deps.workerUpdateHandler != nil {
		go func() {
			if err := deps.workerUpdateHandler.GenerateManifestV2(); err != nil {
				log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
			}
		}()
	}

	// Start orchestrator for multi-step job pipelines
	if deps.orchestrator != nil {
		go deps.orchestrator.Start(context.Background())
		log.Printf("[BOOTSTRAP] Orchestrator started — polling multi-step jobs")
	}

	// Zombie job reaper: requeue jobs with expired leases or stuck too long
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

	// Start server in a goroutine so shutdown can be handled gracefully
	errChan := make(chan error, 1)
	go func() {
		var err error
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", cfg.TLSCertFile, cfg.TLSKeyFile)
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("[SERVER] Listen error: %v", err)
		}
		errChan <- err
	}()

	// Wait for interrupt signal or startup failure
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
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[SERVER] Graceful shutdown failed: %v", err)
		return err
	}

	// Close database before exit
	if deps.sqliteStore != nil {
		if err := deps.sqliteStore.Close(); err != nil {
			log.Printf("[SERVER] Store close failed: %v", err)
		}
	}

	log.Println("[SERVER] Server stopped")
	return nil
}

// runDuadDBBootCheck (legacy spelling kept for greppability) compares the
// runtime velox.db against every well-known source candidate and logs a
// WARN for any same-path hit or source-newer-than-runtime lag. Non-fatal:
// the operator can decide whether to flip DBDSN, restore from backup, or
// accept the state. Threshold is configurable via VELOX_DB_STALE_HOURS;
// default 24h. Set to 0 to disable the staleness check.
func runDuadDBBootCheck(deps *serverDeps, cfg *config.Config) {
	if deps == nil || cfg == nil {
		return
	}
	staleHours := 24
	if v := strings.TrimSpace(os.Getenv("VELOX_DB_STALE_HOURS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			staleHours = n
		}
	}
	threshold := time.Duration(staleHours) * time.Hour
	runtimeDBPath := ""
	if deps.sqliteStore != nil {
		runtimeDBPath = deps.sqliteStore.Path()
	}
	if runtimeDBPath == "" {
		log.Printf("[DUAL-DB] runtime velox.db path could not be resolved; skipping boot check")
		return
	}
	report := audit.CheckDualDatabase(cfg.DataDir, runtimeDBPath, threshold)
	if len(report.Warnings) == 0 {
		log.Printf("[DUAL-DB] OK: runtime velox.db (%s) clear of source candidates (checked %d, threshold=%s)",
			report.RuntimePath, len(report.Sources), threshold)
		return
	}
	for _, w := range report.Warnings {
		log.Printf("[DUAL-DB] WARNING: %s", w)
	}
	log.Printf("[DUAL-DB] runtime=%s checked=%d same_path=%v source_newer=%v lag_hours=%.2f",
		report.RuntimePath, len(report.Sources), report.SamePathHit,
		report.SourceNewerThanRuntime, report.LagHours)
}

// runDataLayerAudit checks for legacy JSON files and data layer integrity.
// Returns error if critical issues are found (hard block).
// Warnings are logged but don't block startup.
func runDataLayerAudit(cfg *config.Config) error {
	dataDir := cfg.DataDir
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
