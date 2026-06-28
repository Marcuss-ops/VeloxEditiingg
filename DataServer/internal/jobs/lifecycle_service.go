// Package jobs lifecycle_service.go — LifecycleService manages job
// queries and orchestration via the canonical jobs.Repository domain surface.
package jobs

import (
	"context"
	"fmt"

	"velox-server/internal/platform/clock"
)

// LifecycleService validates and executes job status transitions.
type LifecycleService struct {
	jobsRepo Repository // canonical domain surface (PR15.5: sole write surface)
	clock    clock.Clock
}

// NewLifecycleService constructs the transactional LifecycleService.
// jobsRepo is the canonical jobs.Repository (Reader + Writer + PR3 methods).
func NewLifecycleService(jobsRepo Repository, c clock.Clock) (*LifecycleService, error) {
	if jobsRepo == nil {
		return nil, fmt.Errorf("jobs.Repository is required")
	}
	if c == nil {
		return nil, fmt.Errorf("clock is required")
	}
	return &LifecycleService{jobsRepo: jobsRepo, clock: c}, nil
}

// ── Accessors ──────────────────────────────────────────────────────────────

// Jobs exposes the canonical jobs.Repository (Reader + Writer) for
// callers that need domain-level read/write operations.
func (l *LifecycleService) Jobs() Repository { return l.jobsRepo }

// Clock returns the clock the service uses for time stamping.
func (l *LifecycleService) Clock() clock.Clock { return l.clock }

// ── Queries ────────────────────────────────────────────────────────────────

// GetJobsByStatus returns all jobs with a given status via jobs.Reader.List.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status Status) ([]*QueueItem, error) {
	domainJobs, err := l.jobsRepo.List(ctx, Filter{
		Statuses: []Status{Status(status)},
		Limit:    1000,
	})
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*QueueItem, 0, len(domainJobs))
	for _, j := range domainJobs {
		// Build a minimal QueueItem from the canonical jobs.Job.
		job := &QueueItem{
			JobID:       j.ID,
			Status:      Status(j.Status),
			VideoName:   j.VideoName,
			ProjectID:   j.ProjectID,
			CreatedAt:   j.CreatedAt,
			UpdatedAt:   j.UpdatedAt,
			StartedAt:   j.StartedAt,
			CompletedAt: j.CompletedAt,
			MaxRetries:  j.MaxRetries,
		}
		result = append(result, job)
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID via jobs.Reader.List.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	pending, err := l.jobsRepo.List(ctx, Filter{
		Statuses: []Status{StatusPending},
		Limit:    1,
	})
	if err != nil {
		return "", err
	}
	if len(pending) == 0 {
		return "", nil
	}
	return pending[0].ID, nil
}


