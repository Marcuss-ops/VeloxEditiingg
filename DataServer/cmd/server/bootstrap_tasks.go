package main

import (
	"fmt"

	"velox-server/internal/ingest"
	"velox-server/internal/observability"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

// taskDeps holds the task-related components built at bootstrap.
type taskDeps struct {
	// TaskRepository is the canonical taskgraph.Repository (backed by SQLiteTaskRepository).
	TaskRepository taskgraph.Repository
	// TaskLifecycle owns transactional task status transitions.
	TaskLifecycle *taskgraph.LifecycleService
	// TaskLeaseReaper is the master-side lease enforcement runner (PR-05
	// follow-up). Constructed here so buildSupervisor can register it as
	// a dedicated supervisor tick (independent of taskgraph-dispatcher).
	TaskLeaseReaper *taskgraph.TaskLeaseReaper
	// AttemptRepository is the canonical taskattempts.Repository.
	AttemptRepository taskattempts.Repository
	// AtomicCreator provides store-level transaction coordinator for Job+Task creation.
	AtomicCreator *store.AtomicJobTaskCreator
	// Observability is the read-only aggregation service.
	Observability *observability.Service
	// IngestionSvc is the canonical TaskReportIngestionService (lives in
	// internal/taskingestion to break the taskattempts↔taskgraph import
	// cycle — see taskingestion/service.go). Wired into the gRPC Handler
	// via SetIngestionSvc in runServer / buildServerDeps after both
	// buildJobs and buildTasks have returned the upstream dependencies.
	IngestionSvc *ingest.TaskReportIngestionService
	// OutputArtifacts is the canonical taskoutput_artifacts.Repository
	// (used by IngestionSvc; exposed for observability tooling).
	OutputArtifacts taskoutput_artifacts.Repository
}

// buildTasks creates the task repositories, lifecycle service, atomic creator,
// reaper, and observability service from the persistence layer.
//
// TaskLeaseReaper is now built from the canonical LifecycleService (which owns the per-candidate atomic
// reap + retry budget query + post-commit Job aggregate update). The
// LifecycleService is wired with jobsRepo via SetJobsRepo in buildServerDeps
// and runServer AFTER this function returns (buildJobs already produced
// j.Lifecycle, so the dependency ordering is preserved).
//
// feat/task-report-ingestion: also constructs the task_output_artifacts
// repository here so the IngestionSvc has its persistence target before
// the gRPC handler wires it via SetIngestionSvc.
func buildTasks(p *persistenceDeps) (*taskDeps, error) {
	taskRepo := store.NewSQLiteTaskRepository(p.SQLite)
	attemptRepo := store.NewSQLiteTaskAttemptRepository(p.SQLite)
	atomicCreator := store.NewAtomicJobTaskCreator(p.SQLite)
	outputArtRepo := store.NewSQLiteTaskOutputArtifactsRepository(p.SQLite)

	taskLifecycle, err := taskgraph.NewLifecycleService(taskRepo)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: task lifecycle service: %w", err)
	}

	taskLeaseReaper := taskgraph.NewTaskLeaseReaper(taskLifecycle)

	obsSvc, err := observability.NewService(taskRepo, attemptRepo)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: observability service: %w", err)
	}

	return &taskDeps{
		TaskRepository:    taskRepo,
		TaskLifecycle:     taskLifecycle,
		TaskLeaseReaper:   taskLeaseReaper,
		AttemptRepository: attemptRepo,
		AtomicCreator:     atomicCreator,
		Observability:     obsSvc,
		OutputArtifacts:   outputArtRepo,
		// IngestionSvc is filled by buildServerDeps + runServer after
		// buildJobs returns (it requires j.Repository for the Job roll-up
		// step in TaskReportIngestionService.maybeTransitionJob).
	}, nil
}
