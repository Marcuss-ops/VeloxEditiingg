package main

import (
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/platform/clock"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// jobsDeps holds the job-related components built at bootstrap.
type jobsDeps struct {
	// Repository is the canonical jobs.Repository (backed by SQLiteJobRepository).
	Repository jobs.Repository
	// Lifecycle owns transactional status transitions with CAS gating.
	Lifecycle *queue.LifecycleService
}

// buildJobs creates the JobRepository and LifecycleService from the
// persistence layer. The LifecycleService is the SOLE transactional
// entry point for job status transitions; SUCCEEDED is reachable ONLY
// through artifacts.Service.FinalizeArtifactAndCompleteJob.
func buildJobs(p *persistenceDeps) (*jobsDeps, error) {
	jobRepo := store.NewSQLiteJobRepository(p.SQLite)
	jobsRepository := store.NewJobsRepository(jobRepo)

	lifecycleSvc, err := queue.NewLifecycleService(jobsRepository, clock.System{})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: lifecycle service: %w", err)
	}

	return &jobsDeps{
		Repository: jobsRepository,
		Lifecycle:  lifecycleSvc,
	}, nil
}
