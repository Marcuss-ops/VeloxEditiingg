package instaedit

import (
	"context"
	"fmt"
)

// ListWorkers returns workers visible to the workspace.
func (s *Service) ListWorkers(ctx context.Context, workspaceID int64) ([]workerResponse, error) {
	rows, err := s.workers.ListWorkersByWorkspace(workspaceID)
	if err != nil {
		return nil, err
	}
	resp := make([]workerResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, mapWorker(row, workspaceID))
	}
	return resp, nil
}

// GetWorker returns a single worker snapshot.
func (s *Service) GetWorker(ctx context.Context, workspaceID int64, workerID string) (*workerResponse, error) {
	row, err := s.workers.GetWorkerByWorkspace(workerID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: worker %s", ErrNotFound, workerID)
	}
	w := mapWorker(row, workspaceID)
	return &w, nil
}
