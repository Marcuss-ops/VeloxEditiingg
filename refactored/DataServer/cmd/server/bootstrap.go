package main

import (
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/modules/ansible"
	"velox-server/internal/modules/drive"
	"velox-server/internal/modules/frontend"
	"velox-server/internal/modules/health"
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
	redisQ              *queue.Queue
	streamsQ            *queue.StreamsQueue
	reg                 *workersreg.Registry
	workersRepo         store.WorkersRepository
	sqliteStore         *store.SQLiteStore
	workerUpdateHandler *workersapi.WorkerUpdateHandler
	workerLifecycle     *workersapi.WorkerLifecycle
	ansibleModule       *ansible.Module
}

func configureTrustedProxies(r *gin.Engine) {
	if err := r.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
		log.Printf("bootstrap: SetTrustedProxies failed: %v", err)
	}
}

func adminAuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersreg.IsLocalRequestIP(c.ClientIP()) {
			c.Next()
			return
		}

		expected := strings.TrimSpace(cfg.AdminToken)
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "admin token required for remote access",
			})
			return
		}

		token := workersreg.ExtractBearerToken(
			c.GetHeader("Authorization"),
			c.GetHeader("X-Admin-Token"),
			c.Query("token"),
		)
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid admin token",
			})
			return
		}

		c.Next()
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

	reg := workersreg.NewWithPersistence(nil, false, sqliteStore, cfg.DataDir)

	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from %s", revokedCount, cfg.DataDir)
	}

	workersRepo := store.NewSQLiteWorkersRepository(sqliteStore)

	// Worker Update Handler (bundle download, manifest, etc.)
	cmdMgr := workersreg.NewCommandManager()
	updateMgr := workersreg.NewUpdateManager()
	tokenMgr := workersreg.NewTokenManager()
	workerUpdateHandler := workersapi.NewWorkerUpdateHandler(cfg, reg, cmdMgr, updateMgr, tokenMgr, cfg.DataDir)
	workerLifecycle := workersapi.NewWorkerLifecycle(cfg, reg, cfg.DataDir)

	return &serverDeps{
		paths:               &serverPaths{dataDir: cfg.DataDir},
		fileQ:               fileQ,
		reg:                 reg,
		workersRepo:         workersRepo,
		sqliteStore:         sqliteStore,
		workerUpdateHandler: workerUpdateHandler,
		workerLifecycle:     workerLifecycle,
	}, nil
}

func runServer(cfg *config.Config) error {
	deps, err := buildServerDeps(cfg)
	if err != nil {
		return err
	}

	registry := app.NewRegistry()
	auth := adminAuthMiddleware(cfg)

	// Register all modules
	registry.Register(health.New())
	registry.Register(workers.New(cfg, deps.reg, deps.workerLifecycle, deps.workerUpdateHandler, auth))
	registry.Register(youtube.New(cfg, deps.paths.dataDir, deps.sqliteStore, auth))
	registry.Register(drive.New(cfg))
	ansibleMod := ansible.New(cfg, deps.paths.dataDir, auth)
	deps.ansibleModule = ansibleMod
	registry.Register(ansibleMod)
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

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", cfg.TLSCertFile, cfg.TLSKeyFile)
		return srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	}

	return srv.ListenAndServe()
}
