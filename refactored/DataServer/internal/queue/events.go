package queue

import (
	"encoding/json"
	"sync"
	"time"

	"velox-server/internal/store"
)

// EventLogger logs job events to SQLite.
type EventLogger struct {
	mu      sync.Mutex
	dbStore *store.SQLiteStore
}

// JobEvent represents a job event entry
type JobEvent struct {
	Timestamp string                 `json:"timestamp"`
	JobID     string                 `json:"job_id"`
	Event     string                 `json:"event"`
	Extra     map[string]interface{} `json:",inline"`
}

// NewEventLogger creates a new event logger.
func NewEventLogger(filePath string, dbStore *store.SQLiteStore) (*EventLogger, error) {
	return &EventLogger{
		dbStore: dbStore,
	}, nil
}

// LogEvent logs a job event to SQLite.
func (l *EventLogger) LogEvent(jobID, eventType string, extra map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339) + "Z"

	event := map[string]interface{}{
		"timestamp": now,
		"job_id":    jobID,
		"event":     eventType,
	}

	for k, v := range extra {
		event[k] = v
	}

	data, err := json.Marshal(event)
	if err != nil {
		return
	}

	if l.dbStore != nil {
		l.dbStore.InsertJobEvent(now, jobID, eventType, string(data))
	}
}

// LogEventFunc returns a function compatible with FileQueue's SetEventLogger
func (l *EventLogger) LogEventFunc() func(jobID, eventType string, extra map[string]interface{}) {
	return l.LogEvent
}

// Close is a no-op for the SQLite-only event logger.
func (l *EventLogger) Close() error {
	return nil
}

// GetRecentEvents returns recent events from SQLite.
func (l *EventLogger) GetRecentEvents(jobID string, limit int) ([]map[string]interface{}, error) {
	if l.dbStore != nil {
		events, err := l.dbStore.ListJobEvents(jobID, limit)
		if err == nil && len(events) > 0 {
			return events, nil
		}
	}
	return nil, nil
}
