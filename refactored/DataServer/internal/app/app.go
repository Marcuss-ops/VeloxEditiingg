package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Paths holds filesystem paths used by the server.
type Paths struct {
	DataDir string
}

// Deps holds all server dependencies.
type Deps struct {
	Paths    *Paths
	FileQ    *queue.FileQueue
	RedisQ   *queue.Queue
	StreamsQ *queue.StreamsQueue
	Reg      *workersreg.Registry
	SQLStore *store.SQLiteStore
}

// App is the main application that manages modules and server lifecycle.
type App struct {
	cfg      *config.Config
	registry *Registry
	deps     *Deps
}

// New creates a new application instance.
func New(cfg *config.Config) *App {
	return &App{
		cfg:      cfg,
		registry: NewRegistry(),
	}
}

// Registry returns the module registry.
func (a *App) Registry() *Registry {
	return a.registry
}

// Deps returns the server dependencies.
func (a *App) Deps() *Deps {
	return a.deps
}

// Config returns the server config.
func (a *App) Config() *config.Config {
	return a.cfg
}

// BuildDeps initializes all server dependencies.
func (a *App) BuildDeps() error {
	if a.cfg == nil {
		a.cfg = config.FromEnv()
	}

	if err := os.MkdirAll(filepath.Dir(a.cfg.DBDSN), 0o755); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}

	sqliteStore, err := store.NewSQLiteStore(a.cfg.DBDSN)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}

	// Import legacy JSON data into SQLite (idempotent, checksum-protected)
	if a.cfg.DataDir != "" {
		if results, err := sqliteStore.ImportLegacyJSON(a.cfg.DataDir); err != nil {
			log.Printf("[APP] Legacy JSON import error (non-fatal): %v", err)
		} else {
			for _, r := range results {
				if r.Status == "imported" {
					log.Printf("[APP] Migrated: %s (%d records)", r.Source.Name, r.Imported)
				}
			}
		}
	}

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		DBStore:    sqliteStore,
		MaxRetries: a.cfg.MaxJobAttempts,
	})
	if err != nil {
		return fmt.Errorf("create file queue: %w", err)
	}

	reg := workersreg.New(nil, false, sqliteStore)

	// Log loaded revoked workers
	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[APP] Loaded %d revoked workers from SQLite", revokedCount)
	}

	a.deps = &Deps{
		Paths:    &Paths{DataDir: a.cfg.DataDir},
		FileQ:    fileQ,
		Reg:      reg,
		SQLStore: sqliteStore,
	}

	return nil
}

// BuildRouter creates the gin engine and registers all routes.
func (a *App) BuildRouter() *gin.Engine {
	// Engine: in release mode skip request logging to avoid log flood.
	var r *gin.Engine
	if os.Getenv("GIN_MODE") == "release" {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
	} else {
		r = gin.Default()
	}

	// Configure trusted proxies
	if err := r.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
		log.Printf("[APP] SetTrustedProxies failed: %v", err)
	}

	// Dark editor API rewrite middleware
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/") && !strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/youtube") {
			c.Request.URL.Path = strings.Replace(c.Request.URL.Path, "/dark_editor_v2/api/v1/", "/api/v1/", 1)
		}
		c.Next()
	})

	// Global middleware
	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// Register routes from all modules
	a.registry.RegisterRoutes(r)

	return r
}

// Run starts the server.
func (a *App) Run() error {
	if err := a.BuildDeps(); err != nil {
		return err
	}

	r := a.BuildRouter()

	addr := fmt.Sprintf(":%d", a.cfg.MasterPort)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("[SERVER] Velox master listening on %s", addr)

	// Start all modules
	ctx := context.Background()
	if err := a.registry.Start(ctx); err != nil {
		return fmt.Errorf("start modules: %w", err)
	}

	// Use TLS if cert and key are configured
	if a.cfg.TLSCertFile != "" && a.cfg.TLSKeyFile != "" {
		log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", a.cfg.TLSCertFile, a.cfg.TLSKeyFile)
		return srv.ListenAndServeTLS(a.cfg.TLSCertFile, a.cfg.TLSKeyFile)
	}

	return srv.ListenAndServe()
}

// Middleware functions (moved from bootstrap.go)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if strings.HasPrefix(origin, "http://localhost:3000") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3000") ||
			strings.HasPrefix(origin, "http://localhost:3001") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3001") {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Max-Age", "86400")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
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
