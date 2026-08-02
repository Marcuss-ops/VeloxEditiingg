package main

// Route dependency composition for the master HTTP surface.

import (
	"path/filepath"

	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/darkeditor"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	scripthandlers "velox-server/internal/handlers/server/script"
	"velox-server/internal/store"
)

// routerBundle assembles the per-route dependency sets from the
// build* return values. Kept as a method on appComponents (rather
// than free-standing) so the next time a new build* helper adds a
// per-route dep, the only place to wire it is in this method.
func (c *appComponents) routerBundle() RouterBundle {
	// Build a single shared dark editor handler for both the legacy
	// /api/darkeditor/dark_editor_v2 surface and the InstaEdit-protected
	// /api/v1/instaedit/editor surface.
	deCfg := &darkeditor.Config{
		TempDir:      filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "temp"),
		ProjectsDir:  filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "projects"),
		LogDir:       filepath.Join(c.cfg.Runtime.DataDir, "dark_editor", "logs"),
		NVIDIAAPIKey: c.cfg.NVIDIA.APIKey,
	}
	deHandler := darkeditor.NewHandler(deCfg)
	if c.persistence.SQLite != nil {
		deHandler.SetDBStore(c.persistence.SQLite)
	}

	return RouterBundle{
		Fleet: FleetRouteDeps{
			// The Handler wraps FleetDep.Controller via the
			// ControllerAudit interface seam defined in
			// internal/handlers/server/api/admin_operations_handler.go;
			// nil-tolerant below (registerFleetOperationsRoutes
			// logs+skips when handler=nil).
			Handler: c.fleet.getHandler(),
		},
		Script: ScriptRouteDeps{
			Cfg:         c.cfg,
			SQLiteStore: c.persistence.SQLite,
			Enqueuer:    c.modules.Enqueuer,
			DocCreator: func() scripthandlers.GoogleDocCreator {
				if c.modules.Drive == nil {
					return nil
				}
				return c.modules.Drive.Service()
			}(),
		},
		Pipeline: PipelineRouteDeps{
			Cfg:          c.cfg,
			Enqueuer:     c.modules.Enqueuer,
			SQLiteStore:  c.persistence.SQLite,
			JobsRepo:     c.jobs.Repository,
			CmdMgr:       c.workers.CommandManager,
			TaskReader:   c.tasks.TaskRepository,
			Resolver:     c.resolver,
			AssetService: c.modules.AssetService,
		},
		Darkeditor: DarkeditorRouteDeps{Cfg: c.cfg, SQLiteStore: c.persistence.SQLite, Handler: deHandler},
		Upload: UploadRouteDeps{
			Cfg:            c.cfg,
			WorkerTokens:   c.workers.TokenManager,
			ArtifactSvc:    c.assets.ArtifactSvc,
			ArtifactReader: c.assets.ArtifactReader,
			BlobStore:      c.assets.BlobStore,
			ChunkedHandler: workerhandlersuploads.NewChunkedUploadHandler(c.assets.ChunkedUploadSvc),
		},
		Metrics: MetricsRouteDeps{
			Registry:      c.metricsRegistry,
			BenchmarkRuns: store.NewSQLitePerformanceRepository(c.persistence.SQLite),
		},
		InstaEdit: InstaEditRouteDeps{
			Verifier:      c.instaeditVerifier,
			Service:       instaedithandler.NewServiceFromSQLite(c.persistence.SQLite, c.jobs.Repository, store.NewSQLiteAssetRepository(c.persistence.SQLite), c.modules.Enqueuer, c.resolver),
			DarkHandler:   deHandler,
			WebhookSecret: c.cfg.Auth.VeloxWebhookSecret,
		},
	}
}
