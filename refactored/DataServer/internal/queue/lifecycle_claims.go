package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// ClaimNextJob atomically claims the next pending job for a worker.
func (l *LifecycleService) ClaimNextJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	result, err := l.jobRepo.ClaimNext(ctx, store.ClaimParams{
		WorkerID:        workerID,
		AllowedJobTypes: allowedJobTypes,
		Now:             time.Now().UTC(),
	})
	if err != nil {
		if errors.Is(err, store.ErrNoClaimableJob) {
			return nil, nil
		}
		return nil, fmt.Errorf("job repository claim: %w", err)
	}
	if result == nil || result.JobID == "" {
		return nil, nil
	}
	m, err := l.eventStore.GetJob(ctx, result.JobID)
	if err != nil {
		return nil, fmt.Errorf("post-claim job fetch: %w", err)
	}
	return MapToJob(m), nil
}
