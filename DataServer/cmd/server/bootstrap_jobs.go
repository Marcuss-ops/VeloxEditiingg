package main

import (
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// jobsDeps holds the job-related components built at bootstrap.
type jobsDeps struct {
	// Repository is the canonical jobs.Repository (backed by SQLiteJobRepository).
	Repository jobs.Repository
	// Lifecycle owns transactional status transitions with CAS gating.
	Lifecycle *jobs.LifecycleService
	// SQLiteRepo is the concrete *SQLiteJobRepository that still carries
	// FailWithRetry as a concrete method (removed from jobs.Repository).
	// Used by wirePostBuild to pass to taskgraph.LifecycleService.SetJobsRepo
	// which needs the narrower taskgraph.JobsRetryQuerier (Get + FailWithRetry).
	// Go structural typing: *SQLiteJobRepository satisfies JobsRetryQuerier
	// because it embeds baseJobRepository which has both Get and FailWithRetry.
	SQLiteRepo *store.SQLiteJobRepository
}

// buildJobs creates the JobRepository and LifecycleService from the
// persistence layer. The LifecycleService is the SOLE transactional
// entry point for job status transitions; SUCCEEDED is reachable ONLY
// through artifacts.Service.FinalizeArtifactAndCompleteJob.
func buildJobs(p *persistenceDeps) (*jobsDeps, error) {
	jobRepo := store.NewSQLiteJobRepository(p.SQLite)
	jobsRepository := store.NewJobsRepository(jobRepo)

	lifecycleSvc, err := jobs.NewLifecycleService(jobsRepository, clock.System{})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: lifecycle service: %w", err)
	}

	return &jobsDeps{
		Repository: jobsRepository,
		Lifecycle:  lifecycleSvc,
		SQLiteRepo: jobRepo,
	}, nil
}
