package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/artifacts"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/audit"
	"velox-server/internal/config"
	"velox-server/internal/deliveries"
	deliveryProviders "velox-server/internal/deliveries/providers"
	"velox-server/internal/grpcserver"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/pipeline"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/outbox"
	"velox-server/internal/platform/clock"
	"velox-server/internal/platform/database"
	"velox-server/internal/queue"
	workflowevents "velox-server/internal/services/workflow_events"
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
	ansibleModule       *app.AnsibleModule
	youtubeModule       *app.YouTubeModule
	driveModule         *app.DriveModule
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

	// PR15.3: cmdMgr is the SINGLE CommandManager instance shared by both
	// the HTTP WorkerUpdateHandler (line ~212) and the gRPC handler
	// (used in runServer). Pre-fix, two instances were created on the same
	// SQLiteStore, racing on the worker_commands table.
	cmdMgr *workersreg.CommandManager

	// PR chunked: ChunkedUploadService wraps artifactSvc with persistent
	// chunk tracking so resumable chunked uploads survive master restarts.
	chunkedHandler *workerhandlersuploads.ChunkedUploadHandler

	// ── Lifecycle ──────────────────────────────────────────────────────────
	//
	// lifecycleSvc is the sole transactional LifecycleService used by
	// FileQueue, gRPC, HTTP handlers, reaper, and workflow. SUCCEEDED is
	// reachable only through artifacts.Service.FinalizeArtifactAndCompleteJob
	// which performs jobs CAS + artifacts CAS + outbox in a single tx.
	lifecycleSvc *queue.LifecycleService

	assetService *voiceoverassets.AssetService

	// PR15.7a: enqueuer owns the queue + voiceover rewrite so the script
	// and creator paths no longer touch any package-level global. Built
	// once at composition root and threaded through DI.
	enqueuer *enqueue.Enqueuer
}

// ErrPostgresNotYetWired is the sentinel error returned by buildServerDeps
// when VELOX_DB_DRIVER=postgres is selected but the per-module *SQLiteStore
// god-object cutover is still in flight. It is the documented invariant
// of the Postgres dispatch path: the dispatch switch lands on the
// DriverPostgres case (proves platform/database.Open worked end-to-end),
// then errors out cleanly without leaking the open *sql.DB or
// silently falling back to SQLite. Test code asserts this with
// errors.Is(err, ErrPostgresNotYetWired); operators see the full
// message at startup and a developer should prepend context via fmt.Errorf
// if a higher-up handler wraps the error.
//
// The message references the cutover roadmap so operators searching
// for "VELOX_DB_DRIVER=postgres" hit the docs that explain the
// per-module narrow-Repository migration.
var ErrPostgresNotYetWired = errors.New(
	"bootstrap: VELOX_DB_DRIVER=postgres is not yet wired end-to-end. " +
		"Narrow-repository adapters (jobs, artifacts) accept *database.Handle. " +
		"The remaining master modules (workers, lifecycle, ansible, youtube, drive, " +
		"livestream, registration) still depend on *SQLiteStore. See docs/architecture/ " +
		"and docs/pr/ for the per-module cutover roadmap",
)

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

// parseInsecureDevFlag parses the raw value of the
// VELOX_GRPC_ALLOW_INSECURE_DEV environment variable and returns true
// only if the operator explicitly opted in to plaintext gRPC transport
// for local development.
//
// Strict parsing: only the literal string "true" (case-sensitive, with
// no surrounding whitespace) returns true. Empty, "1", "True", "TRUE",
// "yes", and any typo alias returns false. This is intentional — the
// insecure-transport bypass is a security-relevant footgun and we do
// not want a permissive parser silently weakening the production
// security model because someone typed "True" instead of "true".
//
// Extracted from the runServer composition root so this step is
// independently unit-testable; the wiring test in
// bootstrap_grpconfig_test.go (TestBuildGRPCHandlerConfig_AllowInsecureFromEnv)
// asserts that the env value flows through this function into
// HandlerConfig.AllowInsecure, not just that the bool propagates.
func parseInsecureDevFlag(envVal string) bool {
	return envVal == "true"
}

// buildGRPCHandlerConfig constructs the gRPC HandlerConfig used by
// grpcserver.NewHandler. It is extracted from the runServer composition
// root so that the propagation of VELOX_GRPC_ALLOW_INSECURE_DEV →
// HandlerConfig.AllowInsecure (plus PushMode / AllowedWorkers) is
// directly unit-testable without standing up DBs, blob stores and the
// asset registry.
//
// Regression: prior versions of bootstrap.go constructed HandlerConfig
// inline and silently dropped AllowInsecure. The handler therefore kept
// refusing insecure gRPC streams even when VELOX_GRPC_ALLOW_INSECURE_DEV
// was set, because h.config.AllowInsecure stayed false. The companion
// regression test in bootstrap_grpconfig_test.go asserts AllowInsecure
// tracks insecureDev so future wiring drift is caught at unit-test
// speed instead of during local dev bring-up.
func buildGRPCHandlerConfig(cfg *config.Config, insecureDev bool) *grpcserver.HandlerConfig {
	return &grpcserver.HandlerConfig{
		PushMode:       cfg.Server.GRPCPushMode,
		AllowInsecure:  insecureDev,
		AllowedWorkers: cfg.Workers.AllowedWorkers,
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

// databaseConfigFromConfig translates config.DatabaseConfig (the env-
// loaded struct consumed by services) into platform/database.Config
// (the runtime struct consumed by Open). The translation is mostly
// structural but the Driver string-vs-typed-value distinction needs
// the explicit conversion; platform/database.Driver("") is treated as
// DriverSQLite at Open-time so empty-string-from-env still routes to
// the historical SQLite path.
func databaseConfigFromConfig(dcfg config.DatabaseConfig) database.Config {
	return database.Config{
		Driver:          database.Driver(strings.ToLower(strings.TrimSpace(dcfg.Driver))),
		SQLitePath:      dcfg.DBPath,
		URL:             dcfg.URL,
		MaxOpenConns:    dcfg.MaxOpenConns,
		MaxIdleConns:    dcfg.MaxIdleConns,
		ConnMaxLifetime: dcfg.ConnMaxLifetime,
	}
}

// schemaModeLabel is the human-readable counterpart to
// cfg.Database.MigrateOnStart, surfaced in the [BOOTSTRAP] log line
// that advertises whether the master owns the schema at boot. The
// label is intentionally short so log lines stay scannable; the
// migrate_on_start= component of the same log line carries the raw
// bool for tooling pipelines.
func schemaModeLabel(migrateOnStart bool) string {
	if migrateOnStart {
		return "master-owned (forward, migrations+post-adjustments run on boot)"
	}
	return "forward-only (external tool owns schema; master skips migrations+post-adjustments)"
}

func buildServerDeps(cfg *config.Config) (*serverDeps, error) {
	if cfg == nil {
		cfg = config.FromEnv()
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// platform/database.Open is the canonical entry point for both
	// SQLite and Postgres backends. Production no longer constructs
	// a *sql.DB directly via NewSQLiteStore; we resolve VELOX_DB_DRIVER
	// into a Handle and dispatch on Handle.Driver.
	openCtx, openCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer openCancel()
	handle, err := database.Open(openCtx, databaseConfigFromConfig(cfg.Database))
	if err != nil {
		return nil, fmt.Errorf("bootstrap: open database: %w", err)
	}

	var sqliteStore *store.SQLiteStore
	switch handle.Driver {
	case database.DriverSQLite:
		// MigrateOnStart is ORTHOGONAL to driver dispatch:
		//   VELOX_DB_DRIVER picks the SQL backend (sqlite here)
		//   VELOX_DB_MIGRATE_ON_START picks whether the master owns
		//     schema state at boot (true) or an external tool does (false).
		// When false, an external migration tool (Atlas / goose /
		// sql-migrate / Ansible-deployed schema) is authoritative and
		// NewSQLiteStoreFromHandle skips both migrations.RunMigrations
		// AND postMigrationAdjustments, surfacing the currently-applied
		// version via a one-line INFO log so operators running an
		// external tool can confirm what the master booted against.
		sqliteStore, err = store.NewSQLiteStoreFromHandle(handle, cfg.Database.DBPath, cfg.Database.MigrateOnStart)
		if err != nil {
			_ = handle.DB.Close()
			return nil, fmt.Errorf("bootstrap: build SQLite store: %w", err)
		}
		log.Printf("[BOOTSTRAP] sqlite path=%s schema_mode=%s (driver=%s, migrate_on_start=%t)",
			cfg.Database.DBPath, schemaModeLabel(cfg.Database.MigrateOnStart),
			database.Driver(strings.ToLower(strings.TrimSpace(cfg.Database.Driver))),
			cfg.Database.MigrateOnStart)
	case database.DriverPostgres:
		// Postgres wiring lands incrementally via narrow-Repository
		// adapters (jobs, artifacts already done). The remaining
		// master god-object modules (workers, lifecycle, ansible,
		// youtube, drive, livestream, registration) depend on
		// *SQLiteStore and have NOT yet completed the narrow-repo
		// cutover. Boot fails-fast here rather than silently falling
		// back to SQLite which would corrupt operator expectations.
		// See docs/architecture/ and docs/pr/ for the per-module
		// narrow-repo cutover roadmap.
		_ = handle.DB.Close()
		return nil, ErrPostgresNotYetWired
	default:
		_ = handle.DB.Close()
		return nil, fmt.Errorf("bootstrap: unsupported driver %q returned by platform/database.Open", handle.Driver)
	}

	reg := workersreg.New(handle)
	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from DB", revokedCount)
	}
	workersRepo := store.NewSQLiteWorkersRepository(sqliteStore)
	cmdMgr := workersreg.NewCommandManager(handle)
	tokenMgr := workersreg.NewTokenManager(handle)
	workerUpdateHandler := workerhandlers.NewWorkerUpdateHandler(cfg, reg, cmdMgr, tokenMgr, cfg.Runtime.DataDir)
	workerLifecycle := lifecycle.NewHandler(cfg, reg, cmdMgr, tokenMgr)

	jobRepo := store.NewSQLiteJobRepository(sqliteStore)

	// ── Lifecycle composition root ─────────────────────────────────────────
	//
	// lifecycleSvc is the sole transactional LifecycleService. It holds
	// a jobs.Repository backed by *SQLiteJobRepository (PR15.5: adapter
	// inlined — the concrete type directly satisfies jobs.Repository).
	// SUCCEEDED is reachable only through artifacts.Service.FinalizeArtifactAndCompleteJob.
	//
	// PR15.5: jobsRepository is the *SQLiteJobRepository itself (no wrapper).
	jobsRepository := store.NewJobsRepository(jobRepo)
	lifecycleSvc, err := queue.NewLifecycleService(jobsRepository, clock.System{})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: lifecycle service: %w", err)
	}

	querySvc := queue.NewQueryService(jobsRepository)

	fileQ, err := queue.NewFileQueue(&queue.FileQueueConfig{
		MaxRetries: cfg.Workers.MaxJobAttempts,
	}, lifecycleSvc, querySvc)
	if err != nil {
		return nil, err
	}

	outboxStore := outbox.NewStore(sqliteStore.DB())

	workflowRepo := workflow.NewSQLiteRepository(sqliteStore.DB())
	workflowRepo.SetOutbox(&outboxWorkflowAdapter{store: outboxStore})

	var blobStore store.BlobStore
	fsBS, bsErr := store.NewFilesystemBlobStore(cfg.Runtime.StagingDir, cfg.Runtime.StorageDir)
	if bsErr != nil {
		log.Printf("[BOOTSTRAP] BlobStore init warning: %v -- using nop blob store", bsErr)
		blobStore = store.NewNopBlobStore(cfg.Runtime.DataDir)
	} else {
		blobStore = fsBS
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
		nil, // clock.System default (production)
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
	stepReady := workflowevents.StepReadyHandler{Wf: workflowRepo, Q: fileQ}
	jobSucceeded := workflowevents.JobSucceededHandler{Wf: workflowRepo}
	artifactReady := workflowevents.ArtifactReadyHandler{Wf: workflowRepo}
	deliveryCreated := workflowevents.DeliveryCreatedHandler{Wf: workflowRepo}
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
		// PR15.3: surface the singleton CommandManager so runServer's
		// gRPC section can reuse it instead of constructing a second
		// instance on the same SQLiteStore (the original race bug).
		cmdMgr:              cmdMgr,
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

	// PR15.7a: hand the singleton Enqueuer to the pipeline package so
	// the forwarding path can use it instead of touching any global.
	pipeline.InitPipelineEnqueuer(deps.enqueuer)

	// PR15.1: NewYouTubeModule now returns error and eagerly builds the
	// integration service so deps (delivery providers) can read Service()
	// BEFORE any routes are registered.
	ytMod, err := app.NewYouTubeModule(cfg, deps.paths.dataDir, deps.sqliteStore)
	if err != nil {
		return fmt.Errorf("bootstrap: youtube module: %w", err)
	}
	deps.youtubeModule = ytMod
	// PR15.1: Drive module takes sqliteStore directly via constructor;
	// the WithSQLiteStore setter was removed.
	driveMod, err := app.NewDriveModule(cfg, deps.sqliteStore)
	if err != nil {
		return fmt.Errorf("bootstrap: drive module: %w", err)
	}
	deps.driveModule = driveMod

	maxVoiceoverBytes := int64(256 * 1024 * 1024)
	if raw := strings.TrimSpace(os.Getenv("VELOX_MAX_VOICEOVER_BYTES")); raw != "" {
		if parsed, perr := strconv.ParseInt(raw, 10, 64); perr == nil && parsed > 0 {
			maxVoiceoverBytes = parsed
		}
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
	deps.assetService = voiceoverassets.NewAssetService(assetRepo, deps.blobStore, assetRegistry, clock.System{})

	// PR15.7a: build the *enqueue.Enqueuer singleton that owns the queue
	// + voiceover rewrite. Replaces both the package-level voiceover global
	// AND the legacy SetVoiceoverAssetService hook. Threaded through DI
	// to script/{handler,RegisterRoutes} and creatorflow.Service.
	deps.enqueuer = enqueue.NewEnqueuer(deps.fileQ, deps.assetService)
	registry.Register(app.NewHealthModule())
	registry.Register(app.NewWorkersModule(cfg, deps.reg, deps.workerLifecycle, deps.workerUpdateHandler, auth, deps.assetService, deps.blobStore))
	registry.Register(ytMod)
	registry.Register(driveMod)
	ansibleMod := app.NewAnsibleModule(cfg, deps.paths.dataDir, auth, deps.sqliteStore)
	deps.ansibleModule = ansibleMod
	registry.Register(ansibleMod)
	// PR15.1: NewLivestreamModule now takes a concrete *integrationsYoutube.Service
	// instead of a lazy func() indirection. ytMod.Service() is now non-nil
	// after NewYouTubeModule returns (eager build).
	livestreamMod := app.NewLivestreamModule(ytMod.Service(), deps.sqliteStore)
	registry.Register(livestreamMod)
	registry.Register(app.NewFrontendModule(cfg))

	// PR15.1: ytMod.Service() and driveMod.Service() are now ALWAYS non-nil
	// after construction (constructor returns error if init fails). The
	// inner `... != nil` checks are removed; if ytMod/driveMod is nil,
	// that's a logic error from buildServerDeps.
	deliveryReg := deliveries.NewRegistry()
	if ytMod != nil {
		ytProvider := deliveryProviders.NewYouTubeProvider(ytMod.Service(), deps.blobStore)
		deliveryReg.Register(ytProvider)
		log.Printf("[BOOTSTRAP] Delivery provider registered: youtube")
	}
	if driveMod != nil {
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
			// PR15.3: reuse the singleton CommandManager built in
			// buildServerDeps. If it's nil here, buildServerDeps lost
			// the wiring — a fatal logic error, not a recoverable
			// condition. We refuse to silently construct a second one
			// (that would re-introduce the race condition this PR
			// set out to eliminate).
			cmdMgr := deps.cmdMgr
			if cmdMgr == nil {
				log.Fatalf("[FATAL] bootstrap invariant broken: buildServerDeps did not wire cmdMgr")
			}

			insecureDev := parseInsecureDevFlag(os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV"))

			// P0: fail-fast if VELOX_ALLOWED_WORKERS is empty in production.
			// An empty allowlist silently admits any worker, which is
			// acceptable in dev but a security gap in production.
			//
			// The non-empty / non-wildcard / unique allowlist rules are
			// enforced upstream by Config.Validate() which calls
			// ValidateProductionWorkers. This block is defense-in-depth
			// for the gRPC layer only.
			if err := grpcserver.ValidateWorkerAllowlist(cfg.Workers.AllowedWorkers, insecureDev); err != nil {
				log.Printf("[BOOTSTRAP] gRPC worker allowlist validation FAILED: %v", err)
				// The HTTP server hasn't started yet at this point — srv is
				// just an allocated struct. Returning the error causes
				// runServer() to bail out before starting any listener.
				return err
			}

			grpcHandlerConfig := buildGRPCHandlerConfig(cfg, insecureDev)
			grpcHandler := grpcserver.NewHandler(
				deps.reg, cmdMgr, lcSvc, deps.artifactSvc, deps.sqliteStore, grpcHandlerConfig,
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

	// ── Background goroutine context ─────────────────────────────────────
	//
	// All background goroutines (outbox dispatcher, delivery runner, zombie
	// reaper, artifacts reconciler) share a single context that is cancelled
	// BEFORE the HTTP/gRPC servers are stopped. bgWG tracks each goroutine
	// so the teardown sequence can Wait() with a bounded timeout — without
	// this, a slow runner could hold the process past systemd TimeoutStopSec
	// and force a SIGKILL while the SQLite store is being closed.
	var bgWG sync.WaitGroup
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	if deps.outboxDispatcher != nil {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			log.Printf("[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
			if err := deps.outboxDispatcher.Run(bgCtx); err != nil {
				log.Printf("[BOOTSTRAP] Outbox dispatcher exited: %v", err)
			}
		}()
	}

	if deps.deliveryRunner != nil {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			log.Printf("[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
			if err := deps.deliveryRunner.Run(bgCtx); err != nil {
				log.Printf("[BOOTSTRAP] DeliveryRunner exited: %v", err)
			}
		}()
	}

	if deps.lifecycleSvc != nil {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-bgCtx.Done():
					return
				case <-ticker.C:
					// Pass bgCtx (NOT context.Background) so an in-flight
					// RequeueExpiredLeases is interrupted by the same
					// cancellation that broke us out of the loop above —
					// otherwise it can run to completion even after
					// bgCancel() and squeeze the teardown window.
					results, err := deps.lifecycleSvc.RequeueExpiredLeases(bgCtx, 100)
					if err != nil {
						log.Printf("[ZOMBIE] requeue error: %v", err)
					} else if len(results) > 0 {
						log.Printf("[ZOMBIE] requeued %d stuck jobs", len(results))
					}
				}
			}
		}()
	}

	if deps.blobStore != nil && deps.artifactSvc != nil {
		rec, recErr := artifacts.NewReconciler(
			deps.sqliteStore.DB(),
			deps.blobStore,
			artifacts.NewSQLiteRepository(deps.sqliteStore.DB()),
			nil, // clock.System default (production)
			artifacts.DefaultReconcilerConfig(),
		)
		if recErr != nil {
			log.Printf("[BOOTSTRAP] Reconciler init failed: %v -- continuing without it", recErr)
		} else {
			// Reconciler shares the same bgCtx as the rest of the bg
			// runners — a separate recCtx/recCancel leaked past bgCancel()
			// and could keep writing to the DB after sqliteStore.Close().
			bgWG.Add(1)
			go func() {
				defer bgWG.Done()
				log.Printf("[BOOTSTRAP] artifacts.Reconciler started (4 rules: expired-uploads + staging, orphan-final-blobs, READY-no-blob QUARANTINED, stuck-STAGING; 15m tick)")
				rec.Run(bgCtx, 15*time.Minute)
			}()
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

	// Cancel the background context first so goroutines stop before
	// we tear down the servers (prevents them from touching closed DBs).
	bgCancel()
	log.Println("[SERVER] Background goroutines cancelling — waiting for them to exit...")

	// Bound the wait so a misbehaving runner cannot block the teardown
	// indefinitely (systemd TimeoutStopSec is 60s; budget 15s here so we
	// still have ~45s for grpcSrv.Stop() and srv.Shutdown()).
	done := make(chan struct{})
	go func() {
		bgWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Println("[SERVER] Background goroutines stopped cleanly")
	case <-time.After(15 * time.Second):
		log.Printf("[SERVER] background shutdown timed out after 15s — proceeding with teardown anyway")
	}

	if grpcSrv != nil {
		// Use Stop() instead of GracefulStop() — workers hold open
		// bidirectional streams (heartbeats) that prevent GracefulStop
		// from ever returning, causing systemd to SIGKILL after 30s.
		// Stop() immediately closes all connections and returns.
		grpcSrv.Stop()
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
	auditor := audit.NewDataLayerAuditorWithDriver(
		dataDir,
		secretsDir,
		strings.ToLower(strings.TrimSpace(cfg.Database.Driver)),
		cfg.Database.DBPath,
	)

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
