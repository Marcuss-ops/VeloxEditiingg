package main

// Test-only composition helpers. Kept in a _test.go file so they
// compile ONLY for `go test`, NOT for `go build`. Production code
// in this package never reads testServerDeps; newRouter takes a
// RouterBundle. Keeping this struct test-local prevents future
// "let's add one more dep" temptation — every test contract must
// justify a new field here explicitly.
//
// Blocco 4 step #2: extracted from bootstrap.go.

import (
	"velox-server/internal/config"
	workerhandlers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type testServerDeps struct {
	cmdMgr              *workersreg.CommandManager
	workerUpdateHandler *workerhandlers.WorkerUpdateHandler
	jobsRepo            jobs.Repository
	sqliteStore         *store.SQLiteStore
}

// buildTestDeps is the test-only composition root. It constructs the
// canonical dependency graph tests inspect; it does NOT touch the
// production appComponents struct or the transportBundle.
func buildTestDeps(cfg *config.Config) (*testServerDeps, error) {
	p, err := buildPersistence(cfg)
	if err != nil {
		return nil, err
	}
	j, err := buildJobs(p)
	if err != nil {
		return nil, err
	}
	t, err := buildTasks(p)
	if err != nil {
		return nil, err
	}
	if err := wirePostBuild(j, t); err != nil {
		return nil, err
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		return nil, err
	}
	// buildAssets is called for its side-effects on the persistence
	// layer (e.g. wiring artifact service back to the SQLite store)
	// even though testServerDeps does not expose anything from `a`.
	// The `_` discards the return value cleanly without an
	// unused-var compile error.
	if _, err := buildAssets(cfg, p, j); err != nil {
		return nil, err
	}

	return &testServerDeps{
		cmdMgr:              w.CommandManager,
		workerUpdateHandler: w.UpdateHandler,
		jobsRepo:            j.Repository,
		sqliteStore:         p.SQLite,
	}, nil
}
