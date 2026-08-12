package main

import (
	"fmt"
	"log"

	"velox-server/internal/config"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// workerDeps holds the worker-layer components built at bootstrap.
type workerDeps struct {
	Registry       *workersreg.Registry
	Repository     store.WorkersRepository
	CommandManager *workersreg.CommandManager
	TokenManager   *workersreg.TokenManager
	UpdateHandler  *workerhandlers.WorkerUpdateHandler
	Lifecycle      *lifecycle.Handler
}

// buildWorkers creates the worker registry, command/token managers,
// and the HTTP handler pair (update + lifecycle).
//
// The CommandManager is a SINGLETON shared between the HTTP
// WorkerUpdateHandler and the gRPC handler — constructing two
// instances on the same SQLiteStore races on worker_commands.
//
// Subsystem outbox handler wiring: buildWorkers is the canonical
// registration point for handler types whose owning package is
// workers/* but whose event_type lives in the outbox. We register
// BundleRebuildHandler on p.OutboxRegistry so the dispatcher (built
// by buildAssets against the same p.OutboxRegistry) sees the
// handler. Subsystem-registered handlers are NOT in
// outbox.KnownEventTypes — see bundle_rebuild_outbox.go for the
// layering rationale — but the round-trip integrity is asserted by
// workers/bundle_rebuild_outbox_test.go.
func buildWorkers(cfg *config.Config, p *persistenceDeps) (*workerDeps, error) {
	reg, err := workersreg.NewWithError(p.SQLite)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: load worker registry: %w", err)
	}
	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from DB", revokedCount)
	}

	workersRepo := store.NewSQLiteWorkersRepository(p.SQLite)
	cmdMgr := workersreg.NewCommandManager(p.SQLite)
	tokenMgr := workersreg.NewTokenManager(p.SQLite)

	// BundleRebuildHandler is wired into the canonical
	// outbox.ProductionRegistry() at workers package init() via
	// outbox.RegisterHandlerFactory. BuildPersistence already
	// triggered the production-registry cache build, so by the
	// time we get here the handler is in p.OutboxRegistry without
	// any work from this layer. Sanity-check the assumption —
	// writing the handler here would now PANIC on duplicate
	// registration.
	if p.OutboxRegistry == nil {
		return nil, fmt.Errorf("bootstrap: buildWorkers: OutboxRegistry missing on persistenceDeps — composition wiring bug")
	}

	updateHandler := workerhandlers.NewWorkerUpdateHandler(cfg, reg, cmdMgr, tokenMgr, cfg.Runtime.DataDir, p.Outbox)
	workerLifecycle := lifecycle.NewHandler(cfg, reg, p.SQLite)

	return &workerDeps{
		Registry:       reg,
		Repository:     workersRepo,
		CommandManager: cmdMgr,
		TokenManager:   tokenMgr,
		UpdateHandler:  updateHandler,
		Lifecycle:      workerLifecycle,
	}, nil
}
