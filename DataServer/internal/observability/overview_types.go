package observability

// ── Aggregate Observability Queries ──────────────────────────────────────

// OverviewResult is the aggregate system health snapshot returned by Overview().
type OverviewResult struct {
	JobsCompleted24h int64        `json:"jobs_completed_24h"`
	JobsFailed24h    int64        `json:"jobs_failed_24h"`
	ErrorRate        float64      `json:"error_rate"`
	P95RenderMS      int64        `json:"p95_render_ms"`
	ActiveWorkers    int          `json:"active_workers"`
	QueueDepth       int          `json:"queue_depth"`
	TopSlowPhases    []PhaseStat  `json:"top_slow_phases"`
	TopSlowWorkers   []WorkerStat `json:"top_slow_workers"`
	TopErrors        []ErrorStat  `json:"top_errors"`
}

// PhaseStat is a single phase aggregate for the overview.
type PhaseStat struct {
	Phase   string `json:"phase"`
	AvgMS   int64  `json:"avg_ms"`
	P95MS   int64  `json:"p95_ms"`
	Samples int    `json:"samples"`
}

// WorkerStat is a single worker aggregate for the overview.
type WorkerStat struct {
	WorkerID  string  `json:"worker_id"`
	JobCount  int     `json:"job_count"`
	AvgMS     int64   `json:"avg_ms"`
	P95MS     int64   `json:"p95_ms"`
	ErrorRate float64 `json:"error_rate"`
}

// ErrorStat is a single error aggregate.
type ErrorStat struct {
	ErrorCode string `json:"error_code"`
	Count     int    `json:"count"`
}

// WorkerPerformance is the per-worker performance summary.
type WorkerPerformance struct {
	WorkerID      string  `json:"worker_id"`
	WorkerName    string  `json:"worker_name"`
	Status        string  `json:"status"`
	JobCount      int     `json:"job_count"`
	SuccessRate   float64 `json:"success_rate"`
	AvgMS         int64   `json:"avg_ms"`
	P95MS         int64   `json:"p95_ms"`
	LastHeartbeat string  `json:"last_heartbeat"`
}

// PhaseTrendResult is the phase timing trend data.
type PhaseTrendResult struct {
	Phase       string               `json:"phase"`
	AvgMS       int64                `json:"avg_ms"`
	P95MS       int64                `json:"p95_ms"`
	Samples     int                  `json:"samples"`
	Trend       string               `json:"trend"`
	DailyPoints []PhaseTrendDayPoint `json:"daily_points,omitempty"`
}

// PhaseTrendDayPoint is a single day's aggregate for phase trends.
type PhaseTrendDayPoint struct {
	Date    string `json:"date"`
	AvgMS   int64  `json:"avg_ms"`
	P95MS   int64  `json:"p95_ms"`
	Samples int    `json:"samples"`
}
