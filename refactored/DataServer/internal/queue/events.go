package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"velox-server/internal/store"
)

// EventLogger logs job events to SQLite (primary) and JSONL file (backup)
type EventLogger struct {
	filePath string
	mu       sync.Mutex
	file     *os.File
	dbStore  *store.SQLiteStore
}

// JobEvent represents a job event entry
type JobEvent struct {
	Timestamp string                 `json:"timestamp"`
	JobID     string                 `json:"job_id"`
	Event     string                 `json:"event"`
	Extra     map[string]interface{} `json:",inline"`
}

// NewEventLogger creates a new event logger
func NewEventLogger(filePath string, dbStore *store.SQLiteStore) (*EventLogger, error) {
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create events directory: %w", err)
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open events file: %w", err)
	}

	return &EventLogger{
		filePath: filePath,
		file:     file,
		dbStore:  dbStore,
	}, nil
}

// LogEvent logs a job event to SQLite (primary) and JSONL (backup)
func (l *EventLogger) LogEvent(jobID, eventType string, extra map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339) + "Z"

	event := map[string]interface{}{
		"timestamp": now,
		"job_id":    jobID,
		"event":     eventType,
	}

	// Add extra fields
	for k, v := range extra {
		event[k] = v
	}

	data, err := json.Marshal(event)
	if err != nil {
		return
	}

	// SQLite is the source of truth
	if l.dbStore != nil {
		if err := l.dbStore.InsertJobEvent(now, jobID, eventType, string(data)); err != nil {
			// Log to file as fallback
			l.file.Write(data)
			l.file.Write([]byte("\n"))
		}
		return
	}

	// Fallback: JSONL file
	l.file.Write(data)
	l.file.Write([]byte("\n"))
}

// LogEventFunc returns a function compatible with FileQueue's SetEventLogger
func (l *EventLogger) LogEventFunc() func(jobID, eventType string, extra map[string]interface{}) {
	return l.LogEvent
}

// Close closes the event logger
func (l *EventLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// GetRecentEvents returns recent events from SQLite (primary) or JSONL (fallback)
func (l *EventLogger) GetRecentEvents(jobID string, limit int) ([]map[string]interface{}, error) {
	// SQLite is the source of truth
	if l.dbStore != nil {
		events, err := l.dbStore.ListJobEvents(jobID, limit)
		if err == nil && len(events) > 0 {
			return events, nil
		}
	}

	// Fallback: JSONL file
	l.mu.Lock()
	defer l.mu.Unlock()

	// Close current handle to read fresh
	l.file.Close()
	l.file, _ = os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0644)

	data, err := os.ReadFile(l.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Reopen for writing
	l.file.Close()
	l.file, _ = os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	var events []map[string]interface{}
	lines := splitLines(string(data))

	// Iterate in reverse to get most recent first
	for i := len(lines) - 1; i >= 0 && len(events) < limit; i-- {
		line := lines[i]
		if line == "" {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		// Filter by job_id if specified
		if jobID != "" {
			if jid, _ := event["job_id"].(string); jid != jobID {
				continue
			}
		}

		events = append(events, event)
	}

	return events, nil
}

// splitLines splits a string into lines
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
