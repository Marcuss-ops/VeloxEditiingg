package instaedit

import (
	"context"
	"fmt"
)

// ListJobs returns the jobs visible to the workspace.
func (s *Service) ListJobs(ctx context.Context, workspaceID int64, limit int) ([]jobResponse, error) {
	rows, err := s.jobs.ListJobsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	resp := make([]jobResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, mapJob(row, workspaceID))
	}
	return resp, nil
}

// GetJob returns a job together with its deliveries.
func (s *Service) GetJob(ctx context.Context, workspaceID int64, jobID string) (*jobDetailResponse, error) {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	deliveries, err := s.loadDeliveries(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return &jobDetailResponse{
		Job:        mapJobWithDeliveries(row, workspaceID, deliveries),
		Deliveries: deliveries,
	}, nil
}

// GetJobDeliveries returns only the deliveries for a job.
func (s *Service) GetJobDeliveries(ctx context.Context, workspaceID int64, jobID string) ([]deliveryResponse, error) {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	return s.loadDeliveries(ctx, jobID)
}

// loadDeliveries loads and maps the deliveries for a job.
