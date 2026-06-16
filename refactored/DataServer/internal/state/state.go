package state

import (
	"sync"
)

// Global state for master server controls.
//

type MasterState struct {
	mu sync.RWMutex

	// Job control
	NewJobsPaused    bool
	SchedulingPaused bool

	// Runtime info
	StartTime      int64
	APIRequestsLog []APIRequestEntry
	APIRequestsMu  sync.RWMutex
	APIRequestsMax int
}

type APIRequestEntry struct {
	Timestamp  string `json:"timestamp"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	DurationMs int    `json:"duration_ms"`
	ErrorType  string `json:"error_type,omitempty"`
}

// Global instance
var Global = &MasterState{
	StartTime:      0,
	APIRequestsMax: 1000,
}

// GetStartTime returns the server start time
func (s *MasterState) GetStartTime() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.StartTime
}

// PauseNewJobs sets the pause flag for new job submissions
func (s *MasterState) PauseNewJobs(pause bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.NewJobsPaused = pause
}

// IsNewJobsPaused returns whether new jobs are paused
func (s *MasterState) IsNewJobsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.NewJobsPaused
}

// PauseScheduling sets the pause flag for job scheduling to workers
func (s *MasterState) PauseScheduling(pause bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SchedulingPaused = pause
}

// IsSchedulingPaused returns whether scheduling is paused
func (s *MasterState) IsSchedulingPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.SchedulingPaused
}

// GetState returns all state for API responses
func (s *MasterState) GetState() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]interface{}{
		"new_jobs_paused":   s.NewJobsPaused,
		"scheduling_paused": s.SchedulingPaused,
		"start_time":        s.StartTime,
	}
}

// LogAPIRequest adds an entry to the API requests log
func (s *MasterState) LogAPIRequest(entry APIRequestEntry) {
	s.APIRequestsMu.Lock()
	defer s.APIRequestsMu.Unlock()
	s.APIRequestsLog = append(s.APIRequestsLog, entry)
	if len(s.APIRequestsLog) > s.APIRequestsMax {
		s.APIRequestsLog = s.APIRequestsLog[len(s.APIRequestsLog)-s.APIRequestsMax:]
	}
}

// GetAPIRequestsLog returns recent API requests
func (s *MasterState) GetAPIRequestsLog(limit int) []APIRequestEntry {
	s.APIRequestsMu.RLock()
	defer s.APIRequestsMu.RUnlock()
	if limit <= 0 || limit > len(s.APIRequestsLog) {
		limit = len(s.APIRequestsLog)
	}
	// Return last N entries
	start := len(s.APIRequestsLog) - limit
	if start < 0 {
		start = 0
	}
	return s.APIRequestsLog[start:]
}
