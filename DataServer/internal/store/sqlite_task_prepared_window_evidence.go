package store

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// ListSuccessfulSegmentTimings exposes durable preparation evidence through
// the canonical task repository used by the runtime FINALIZE supervisor.
func (r *SQLiteTaskRepository) ListSuccessfulSegmentTimings(ctx context.Context, taskIDs []string) ([]taskattempts.SegmentTiming, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("prepared-window evidence: task store is not initialized")
	}
	return NewSQLiteTaskAttemptRepository(r.store).ListSuccessfulSegmentTimings(ctx, taskIDs)
}

var _ taskattempts.SuccessfulSegmentTimingReader = (*SQLiteTaskRepository)(nil)
