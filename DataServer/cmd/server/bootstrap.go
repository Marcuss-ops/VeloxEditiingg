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
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	workersapi "velox-server/internal/handlers/remote/workers"
	jobapi "velox-server/internal/handlers/server/jobs"
	"velox-server/internal/handlers/server/youtube"
	"velox-server/internal/queue"
	jobservice "velox-server/internal/services/jobs"
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
	jobAPI              *jobapi.JobAPI
	jobSubmitHandler    *jobapi.JobSubmissionHandler
	workersRepo         store.WorkersRepository
	sqliteStore         *store.SQLiteStore
	workerUpdateHandler *workersapi.WorkerUpdateHandler
	workerLifecycle     *workersapi.WorkerLifecycle
	ansibleHandlers     *remoteansible.AnsibleHandlers
	youtubeHandlers     *youtube.YouTubeHandlers
	youtubeManager      *youtube.YouTubeManager
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

	// Log loaded revoked workers
	revokedCount := 0
	for _, id := range reg.ListRevoked() {
		_ = id
		revokedCount++
	}
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from %s", revokedCount, cfg.DataDir)
	}

	jobsRepo := store.NewSQLiteJobsRepository(sqliteStore)
	workersRepo := store.NewSQLiteWorkersRepository(sqliteStore)
	jobService := jobservice.NewService(cfg, fileQ, nil, jobsRepo, nil, reg)
	jobAPI := jobapi.NewJobAPI(cfg, fileQ, nil, jobService)
	jobSubmitHandler := jobapi.NewJobSubmissionHandler(cfg, fileQ)
	workerLifecycle := workersapi.NewWorkerLifecycle(cfg, reg, cfg.DataDir)

	// ── Worker Update Handler (bundle download, manifest, etc.) ─────
	cmdMgr := workersreg.NewCommandManager()
	updateMgr := workersreg.NewUpdateManager()
	tokenMgr := workersreg.NewTokenManager()
	workerUpdateHandler := workersapi.NewWorkerUpdateHandler(cfg, reg, cmdMgr, updateMgr, tokenMgr, cfg.DataDir)

	// ── Ansible (playbooks per worker remoti) ───────────────────────
	var ansibleHandlers *remoteansible.AnsibleHandlers
	if err := os.MkdirAll(cfg.PlaybookDir, 0755); err != nil {
		log.Printf("[BOOTSTRAP] Cannot create ansible playbook dir %s: %v", cfg.PlaybookDir, err)
	} else {
		ansibleManager := remoteansible.NewAnsibleRunManager(cfg.PlaybookDir, cfg.DataDir)
		computerMgr := remoteansible.NewAnsibleComputerManager(cfg.DataDir)
		if err := computerMgr.LoadComputers(); err != nil {
			log.Printf("[BOOTSTRAP] Failed to load ansible computers: %v", err)
		}

		ah := remoteansible.NewAnsibleHandlers(ansibleManager)
		ah.SetComputerManager(computerMgr, cfg.DataDir)
		ah.SetMasterURL(cfg.MasterURL)
		ansibleHandlers = ah

		if ansibleManager.Ready() {
			log.Printf("[BOOTSTRAP] Ansible handlers initialized (playbooks: %s)", cfg.PlaybookDir)
		} else {
			log.Printf("[BOOTSTRAP] ansible-playbook not found in PATH - Ansible features disabled (install with: apt install ansible)")
		}
	}

	return &serverDeps{
		paths:               &serverPaths{dataDir: cfg.DataDir},
		fileQ:               fileQ,
		reg:                 reg,
		jobAPI:              jobAPI,
		jobSubmitHandler:    jobSubmitHandler,
		workersRepo:         workersRepo,
		sqliteStore:         sqliteStore,
		workerUpdateHandler: workerUpdateHandler,
		workerLifecycle:     workerLifecycle,
		ansibleHandlers:     ansibleHandlers,
	}, nil
}

func runServer(cfg *config.Config) error {
	deps, err := buildServerDeps(cfg)
	if err != nil {
		return err
	}

	r := newRouter(cfg, deps)
	addr := fmt.Sprintf(":%d", cfg.MasterPort)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("[SERVER] Velox master listening on %s", addr)

	// Use TLS if cert and key are configured
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", cfg.TLSCertFile, cfg.TLSKeyFile)
		return srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	}

	return srv.ListenAndServe()
}
