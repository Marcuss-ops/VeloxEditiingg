package workers

import "time"

// LifecycleConfig holds configuration for lifecycle management
type LifecycleConfig struct {
	HealthCheckInterval    time.Duration `json:"health_check_interval"`
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout"`
	GracefulShutdownTime   time.Duration `json:"graceful_shutdown_time"`
	PreStopTimeout         time.Duration `json:"pre_stop_timeout"`
	MaxConcurrentShutdowns int           `json:"max_concurrent_shutdowns"`
	EnableAutoRecovery     bool          `json:"enable_auto_recovery"`
	EnableProactiveHealth  bool          `json:"enable_proactive_health"`
}

// DefaultLifecycleConfig returns sensible defaults
func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		HealthCheckInterval:    30 * time.Second,
		HeartbeatTimeout:       2 * time.Minute,
		GracefulShutdownTime:   5 * time.Minute,
		PreStopTimeout:         30 * time.Second,
		MaxConcurrentShutdowns: 5,
		EnableAutoRecovery:     true,
		EnableProactiveHealth:  true,
	}
}

// ShutdownState tracks the state of a graceful shutdown
type ShutdownState struct {
	WorkerID      string        `json:"worker_id"`
	InitiatedAt   time.Time     `json:"initiated_at"`
	Phase         string        `json:"phase"` // "requested", "draining", "stopping", "complete"
	ActiveJobs    []string      `json:"active_jobs"`
	Reason        string        `json:"reason"`
	LastHeartbeat time.Time     `json:"last_heartbeat"`
	Timeout       time.Duration `json:"timeout"`
	Completed     bool          `json:"completed"`
	Failed        bool          `json:"failed"`
	Error         string        `json:"error,omitempty"`
}

// WorkerHealth tracks worker health metrics
type WorkerHealth struct {
	WorkerID            string        `json:"worker_id"`
	LastHeartbeat       time.Time     `json:"last_heartbeat"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	HealthScore         float64       `json:"health_score"` // 0-1, 1 = healthy
	RecentErrors        []string      `json:"recent_errors,omitempty"`
	LastError           time.Time     `json:"last_error,omitempty"`
	JobsCompleted       int64         `json:"jobs_completed"`
	JobsFailed          int64         `json:"jobs_failed"`
	AvgJobDuration      time.Duration `json:"avg_job_duration"`
	Status              string        `json:"status"` // "healthy", "degraded", "unhealthy", "offline"
	LastChecked         time.Time     `json:"last_checked"`
}

// WorkerAlert represents an alert about a worker
type WorkerAlert struct {
	Type      string                 `json:"type"` // "health_degraded", "offline", "timeout", "error"
	WorkerID  string                 `json:"worker_id"`
	Severity  string                 `json:"severity"` // "info", "warning", "critical"
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// WorkerCommand represents a command to send to a worker
type WorkerCommand struct {
	Command   string                 `json:"command"` // "drain", "stop", "restart", "ping"
	WorkerID  string                 `json:"worker_id"`
	Timestamp time.Time              `json:"timestamp"`
	Params    map[string]interface{} `json:"params,omitempty"`
}
