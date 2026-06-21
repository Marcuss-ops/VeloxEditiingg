package main

import (
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
func buildWorkers(cfg *config.Config, p *persistenceDeps) (*workerDeps, error) {
	reg := workersreg.New(p.SQLite)
	revokedCount := len(reg.ListRevoked())
	if revokedCount > 0 {
		log.Printf("[BOOTSTRAP] Loaded %d revoked workers from DB", revokedCount)
	}

	workersRepo := store.NewSQLiteWorkersRepository(p.SQLite)
	cmdMgr := workersreg.NewCommandManager(p.SQLite)
	tokenMgr := workersreg.NewTokenManager(p.SQLite)
	updateHandler := workerhandlers.NewWorkerUpdateHandler(cfg, reg, cmdMgr, tokenMgr, cfg.Runtime.DataDir)
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
