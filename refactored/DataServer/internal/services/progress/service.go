// Package progress provides job progress tracking via the job_progress table.
package progress

import (
	"context"
	"time"

	"velox-server/internal/store"
)

// Service provides job progress tracking.
// Uses the job_progress table (snapshot per job, upserted on each update).
type Service struct {
	dbStore *store.SQLiteStore
}

// NewService creates a new ProgressService.
func NewService(dbStore *store.SQLiteStore) *Service {
	return &Service{dbStore: dbStore}
}

// Record writes a progress snapshot for a job (upsert by job_id).
func (s *Service) Record(ctx context.Context, jobID string, attemptNumber int, percent float64, stage, message string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	return s.dbStore.UpsertJobProgress(ctx, jobID, attemptNumber,
		percent, stage, 0, 0, message, now)
}

// Get returns the current progress for a job (if any).
func (s *Service) Get(ctx context.Context, jobID string) (*store.ProgressSnapshot, error) {
	raw, err := s.dbStore.GetJobProgress(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
