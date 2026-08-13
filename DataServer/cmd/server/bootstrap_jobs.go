package main

import (
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// jobsDeps holds the job-related components built at bootstrap.
//
// PR-REMOVE-LIFECYCLE: the legacy *jobs.LifecycleService wrapper has
// been deleted. The canonical domain surface is jobs.Repository
// (Reader + Writer + PR3 methods); callers that previously went
// through `lifecycleSvc.Jobs()` now use Repository directly.
type jobsDeps struct {
	// Repository is the canonical jobs.Repository (backed by SQLiteJobRepository).
	Repository jobs.Repository
	// SQLiteRepo is the concrete *SQLiteJobRepository that still carries
	// Get + Fail as concrete methods. Used by wirePostBuild to adapt onto
	// taskgraph.LifecycleService.SetJobsRepo via taskgraphJobsRetryQuerier,
	// which projects the narrow taskgraph.JobRetryView (MaxRetries +
	// terminal state) so taskgraph never imports the jobs package.
	SQLiteRepo *store.SQLiteJobRepository
}

// buildJobs constructs the canonical jobs.Repository from the
// persistence layer. SUCCEEDED is reachable ONLY through
// artifacts.Service.FinalizeArtifactAndCompleteJob (no LifecycleService
// indirection layer needed).
func buildJobs(p *persistenceDeps) (*jobsDeps, error) {
	jobRepo := store.NewSQLiteJobRepository(p.SQLite)
	jobsRepository := store.NewJobsRepository(jobRepo)

	return &jobsDeps{
		Repository: jobsRepository,
		SQLiteRepo: jobRepo,
	}, nil
}
