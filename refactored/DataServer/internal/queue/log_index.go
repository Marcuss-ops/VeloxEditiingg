// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"time"

	"velox-server/internal/store"
)

// LogIndex provides indexing and search capabilities for job logs
type LogIndex struct {
	dbStore *store.SQLiteStore
}

// NewLogIndex creates a new log index
func NewLogIndex(dbStore *store.SQLiteStore) *LogIndex {
	return &LogIndex{
		dbStore: dbStore,
	}
}

// GetJobLogs retrieves logs for a job with optional limit
func (li *LogIndex) GetJobLogs(ctx context.Context, jobID string, limit int) ([]map[string]interface{}, error) {
	logs, err := li.dbStore.GetJobLogs(jobID, limit)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0, len(logs))
	for _, log := range logs {
		result = append(result, log)
	}

	return result, nil
}

// AddLogEntry adds a log entry to a job
func (li *LogIndex) AddLogEntry(ctx context.Context, jobID, message, workerID string, isError bool) error {
	return li.dbStore.AddJobLog(jobID, message, workerID, isError)
}

// CleanupOldLogs removes logs older than the specified age
// This is a placeholder - actual implementation would depend on schema support
func (li *LogIndex) CleanupOldLogs(ctx context.Context, maxAge time.Duration) error {
	// For now, this is a no-op since SQLite schema doesn't have a dedicated cleanup
	// In a full implementation, you'd add a timestamp column to job_logs and clean up
	return nil
}
