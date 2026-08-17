package main

// Route dependency composition for the master HTTP surface.

import (
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	scripthandlers "velox-server/internal/handlers/server/script"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/store"
)

// routerBundle assembles the per-route dependency sets from the
// build* return values. Kept as a method on appComponents (rather
// than free-standing) so the next time a new build* helper adds a
// per-route dep, the only place to wire it is in this method.
func (c *appComponents) routerBundle() RouterBundle {
	chunkedHandler := workerhandlersuploads.NewChunkedUploadHandler(c.assets.ChunkedUploadSvc)
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
			Resolver:    c.resolver,
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
		Upload: UploadRouteDeps{
			Cfg:                     c.cfg,
			SQLiteStore:             c.persistence.SQLite,
			WorkerTokens:            c.workers.TokenManager,
			ArtifactSvc:             c.assets.ArtifactSvc,
			ArtifactReader:          c.assets.ArtifactReader,
			BlobStore:               c.assets.BlobStore,
			ChunkedHandler:          chunkedHandler,
			CompletionTokenVerifier: c.assets.CompletionStore,
		},
		Metrics: MetricsRouteDeps{
			Registry:      c.metricsRegistry,
			BenchmarkRuns: store.NewSQLitePerformanceRepository(c.persistence.SQLite),
			// Phase 6 route-usage counting: the collector implements the
			// HTTPRouteUsageSink contract and stamps every request's
			// (surface, route template) onto velox_master_http_route_requests_total.
			RouteUsage: c.metricsCollector,
		},
		InstaEdit: InstaEditRouteDeps{
			Verifier: c.instaeditVerifier,
			Service: instaedithandler.NewServiceFromSQLite(
				c.persistence.SQLite, c.jobs.Repository,
				store.NewSQLiteAssetRepository(c.persistence.SQLite),
				c.modules.Enqueuer, c.resolver,
			).WithIntakeSourceRecorder(velmetrics.NewIntakeSourceSink()),
			WebhookSecret: c.cfg.Auth.VeloxWebhookSecret,
		},
	}
}
