// Package completion owns the orchestration and domain decisions for the
// Artifact Commit Protocol. Persistence contracts live in internal/repository;
// the SQLite implementation, SQL, transaction lifecycle, and row projections
// live in internal/store.
package completion

import (
	"errors"
	"fmt"

	"velox-server/internal/repository"
)

const commitTokenByteLen = 32

type CoordinatorConfig struct {
	Store     repository.CompletionStore
	HMACKey   []byte
	BlobStore repository.BlobStore
}

func NewCoordinator(cfg CoordinatorConfig) (Coordinator, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("completion.NewCoordinator: cfg.Store is required")
	}
	if len(cfg.HMACKey) < commitTokenByteLen {
		return nil, fmt.Errorf("completion.NewCoordinator: cfg.HMACKey must be >= 32 bytes for HMAC-SHA256 nominal entropy (got %d)", len(cfg.HMACKey))
	}
	return &coordinator{
		store:     cfg.Store,
		hmacKey:   cfg.HMACKey,
		blobStore: cfg.BlobStore,
		budget:    NewConflictBudget(DefaultConflictBudgetPolicy()),
	}, nil
}

func (c *coordinator) SetConflictBudgetSink(sink ConflictBudgetSink) {
	if c.budget != nil {
		c.budget.WithMetricsSink(sink)
	}
}

type coordinator struct {
	store     repository.CompletionStore
	hmacKey   []byte
	blobStore repository.BlobStore
	budget    *ConflictBudget
}

func (c *coordinator) recordAttemptCommitsCAS(key string, err error) error {
	if c.budget == nil {
		return err
	}
	budgetErr := c.budget.Record(key, err)
	if budgetErr == nil {
		return err
	}
	return budgetErr
}

func mapStoreCompletionError(err error) error {
	if err == nil {
		return nil
	}
	if errorsIsStoreTransition(err) {
		return fmt.Errorf("%w: %w", ErrTransitionConflict, err)
	}
	if errorsIsStoreNotFound(err) {
		return fmt.Errorf("%w: %w", ErrAttemptCommitNotFound, err)
	}
	return err
}

func errorsIsStoreTransition(err error) bool {
	return err != nil && errors.Is(err, repository.ErrCompletionTransitionConflict)
}

func errorsIsStoreNotFound(err error) bool {
	return err != nil && errors.Is(err, repository.ErrCompletionAttemptNotFound)
}

func completionResultFromStore(in *repository.CompletionCommitResult) *CommitResult {
	if in == nil {
		return nil
	}
	return &CommitResult{CommitID: in.CommitID, TaskID: in.TaskID, AttemptID: in.AttemptID, JobID: in.JobID, TaskStatus: in.TaskStatus, JobStatus: in.JobStatus, ArtifactIDs: in.ArtifactIDs, CommittedAt: in.CommittedAt}
}
