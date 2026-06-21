package main

import (
	"fmt"

	"velox-server/internal/observability"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// taskDeps holds the task-related components built at bootstrap.
type taskDeps struct {
	// TaskRepository is the canonical taskgraph.Repository (backed by SQLiteTaskRepository).
	TaskRepository taskgraph.Repository
	// TaskLifecycle owns transactional task status transitions.
	TaskLifecycle *taskgraph.LifecycleService
	// AttemptRepository is the canonical taskattempts.Repository.
	AttemptRepository taskattempts.Repository
	// AtomicCreator provides store-level transaction coordinator for Job+Task creation.
	AtomicCreator *store.AtomicJobTaskCreator
	// Observability is the read-only aggregation service.
	Observability *observability.Service
}

// buildTasks creates the task repositories, lifecycle service, atomic creator,
// and observability service from the persistence layer.
func buildTasks(p *persistenceDeps) (*taskDeps, error) {
	taskRepo := store.NewSQLiteTaskRepository(p.SQLite)
	attemptRepo := store.NewSQLiteTaskAttemptRepository(p.SQLite)
	atomicCreator := store.NewAtomicJobTaskCreator(p.SQLite)

	taskLifecycle, err := taskgraph.NewLifecycleService(taskRepo)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: task lifecycle service: %w", err)
	}

	obsSvc, err := observability.NewService(taskRepo, attemptRepo)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: observability service: %w", err)
	}

	return &taskDeps{
		TaskRepository:    taskRepo,
		TaskLifecycle:     taskLifecycle,
		AttemptRepository: attemptRepo,
		AtomicCreator:     atomicCreator,
		Observability:     obsSvc,
	}, nil
}
