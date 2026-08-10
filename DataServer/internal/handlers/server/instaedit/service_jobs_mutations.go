package instaedit

import (
	"context"
	"fmt"
)

// CancelJob cancels a job after verifying workspace ownership.
func (s *Service) CancelJob(ctx context.Context, workspaceID int64, jobID string) error {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	return s.jobs.Cancel(ctx, jobID, "cancelled via InstaEdit BFF", 0)
}
