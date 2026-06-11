// Package api provides HTTP client for communicating with the Velox Master server.
package api

// API event names for structured logging.
const (
	EventAPIRequest  = "API_REQUEST"
	EventAPIRetry    = "API_RETRY"
	EventAPIError    = "API_ERROR"
	EventAPISuccess  = "API_SUCCESS"
	EventAPIFallback = "API_FALLBACK"
)

// WorkerInfo represents worker identification sent to the master.
type WorkerInfo struct {
	WorkerID     string          `json:"worker_id"`
	WorkerName   string          `json:"worker_name"`
	Capabilities map[string]bool `json:"capabilities"`
	Hostname     string          `json:"hostname"`
	IP           string          `json:"ip"`
	Version      string          `json:"version"`
}

// JobRequest represents a request to get a job from the master.
type JobRequest struct {
	WorkerID string `json:"worker_id"`
}

// Job represents a job returned by the master.
type Job struct {
	JobID       string                 `json:"job_id"`
	JobRunID    string                 `json:"job_run_id"`
	JobType     string                 `json:"job_type"`
	Priority    int                    `json:"priority"`
	Parameters  map[string]interface{} `json:"parameters"`
	CreatedAt   string                 `json:"created_at"`
	TimeoutSecs int                    `json:"timeout_secs"`
}

// JobResult represents the result of a job execution.
type JobResult struct {
	JobID     string                 `json:"job_id"`
	JobRunID  string                 `json:"job_run_id"`
	WorkerID  string                 `json:"worker_id"`
	Status    string                 `json:"status"`
	Output    map[string]interface{} `json:"output"`
	Error     string                 `json:"error,omitempty"`
	StartTime string                 `json:"start_time"`
	EndTime   string                 `json:"end_time"`
}

// HeartbeatPayload represents a heartbeat message.
type HeartbeatPayload struct {
	WorkerID   string                 `json:"worker_id"`
	WorkerName string                 `json:"worker_name,omitempty"`
	Status     string                 `json:"status"`
	JobID      string                 `json:"job_id,omitempty"`
	CurrentJob string                 `json:"current_job,omitempty"`
	Extra      map[string]interface{} `json:"extra,omitempty"`
}

// APIResponse represents a generic API response.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// WorkerCommand represents a command from the master to the worker.
type WorkerCommand struct {
	Command   string                 `json:"command"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}
