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
	// FailWithRetry as a concrete method (removed from jobs.Repository).
	// Used by wirePostBuild to pass to taskgraph.LifecycleService.SetJobsRepo
	// which needs the narrower taskgraph.JobsRetryQuerier (Get + FailWithRetry).
	// Go structural typing: *SQLiteJobRepository satisfies JobsRetryQuerier
	// because it embeds baseJobRepository which has both Get and FailWithRetry.
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
