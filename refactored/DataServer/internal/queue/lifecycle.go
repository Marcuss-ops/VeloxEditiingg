// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"

	"velox-server/internal/store"
)

// LifecycleService validates and executes job status transitions.
//
// The transactional shape uses {repo, clock} exclusively. All mutation
// methods route to the corresponding PR3 methods on JobRepository, which
// perform UPDATE + history + event + outbox in a single BEGIN/COMMIT.
type LifecycleService struct {
	repo  store.JobRepository
	clock Clock
}

// Validate checks whether a transition from one status to another is allowed.
func (l *LifecycleService) Validate(from, to JobStatus) error {
	if !isValidJobStatusTransition(from, to) {
		return fmt.Errorf("invalid transition: %s → %s", from, to)
	}
	return nil
}

// RecordRenderFinished records that a worker has completed rendering.
// The job stays in RUNNING — no status transition occurs. Delegates to
// JobRepository.PR3RecordRenderFinished for atomic verification of
// (worker_id, lease_id, revision) and attempt status update.
func (l *LifecycleService) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	return l.repo.PR3RecordRenderFinished(ctx, cmd)
}
