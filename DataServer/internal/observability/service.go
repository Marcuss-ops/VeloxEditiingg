// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"fmt"
	"sort"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TaskReader is the read surface the observability service depends on.
type TaskReader interface {
	Get(ctx context.Context, id string) (*taskgraph.Task, error)
	GetByJobID(ctx context.Context, jobID string) (*taskgraph.Task, error)
	List(ctx context.Context, filter taskgraph.Filter) ([]taskgraph.Task, error)
}

// AttemptReader provides attempt queries for aggregation.
type AttemptReader interface {
	Get(ctx context.Context, id string) (*taskattempts.TaskAttempt, error)
	ListByTaskID(ctx context.Context, taskID string) ([]taskattempts.TaskAttempt, error)
	GetPhaseTimings(ctx context.Context, attemptID string) ([]taskattempts.PhaseTiming, error)
	GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error)
}

// JobReader provides job queries for observability aggregates.
type JobReader interface {
	Get(ctx context.Context, id string) (*jobs.Job, error)
	List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error)
	Counts(ctx context.Context) (jobs.Counts, error)
}

// WorkerReader provides worker queries for observability.
type WorkerReader interface {
	ListWorkers() ([]map[string]any, error)
	GetWorker(workerID string) (map[string]any, error)
}

// VersionMetricsReader provides per-version metric queries for
// regression comparison. Implemented by the store layer on
// task_attempts + task_attempt_metrics.
type VersionMetricsReader interface {
	// ListMetricsByGitSHA returns metric snapshots for all attempts
	// with the given git_sha. Returns an empty slice when no attempts
	// match (not an error).
	ListMetricsByGitSHA(ctx context.Context, gitSHA string) ([]VersionMetricSnapshot, error)
}

// VersionMetricSnapshot is a single attempt's metric values indexed
// by catalog metric name, for regression comparison.
type VersionMetricSnapshot struct {
	AttemptID      string             `json:"attempt_id"`
	WorkerID       string             `json:"worker_id"`
	ExecutorID     string             `json:"executor_id"`
	Metrics        map[string]float64 `json:"metrics"`
}

// ExecutionSummary is the aggregated execution diagnostics for a single task.
type ExecutionSummary struct {
	TaskID              string           `json:"task_id"`
	JobID               string           `json:"job_id"`
	TaskStatus          taskgraph.Status `json:"task_status"`
	AttemptCount        int              `json:"attempt_count"`
	TotalWallTimeMS     int64            `json:"total_wall_time_ms"`
	PhaseTotals         map[string]int64 `json:"phase_totals"`
	TotalInputBytes     int64            `json:"total_input_bytes"`
	TotalOutputBytes    int64            `json:"total_output_bytes"`
	BytesFromDrive      int64            `json:"bytes_from_drive"`
	BytesFromBlobstore  int64            `json:"bytes_from_blobstore"`
	BytesFromLocalCache int64            `json:"bytes_from_local_cache"`
	CPUTimeMS           int64            `json:"cpu_time_ms"`
	GPUTimeMS           int64            `json:"gpu_time_ms"`
	PeakRSSBytes        int64            `json:"peak_rss_bytes"`
	PeakVRAMBytes       int64            `json:"peak_vram_bytes"`
	Retries             int              `json:"retries"`
	Attempts            []AttemptSummary `json:"attempts"`
}

// AttemptSummary is the aggregated diagnostics for a single attempt.
type AttemptSummary struct {
	AttemptID      string                       `json:"attempt_id"`
	AttemptNumber  int                          `json:"attempt_number"`
	Status         taskattempts.AttemptStatus   `json:"status"`
	WorkerID       string                       `json:"worker_id"`
	DurationMS     int64                        `json:"duration_ms"`
	PhaseBreakdown map[string]int64             `json:"phase_breakdown"`
	Metrics        *taskattempts.AttemptMetrics `json:"metrics,omitempty"`
}

// Service is the read-only observability aggregation service.
type Service struct {
	tasks          TaskReader
	attempts       AttemptReader
	jobs           JobReader
	workers        WorkerReader
	versionMetrics VersionMetricsReader
}

// NewService constructs the observability aggregation service.
// Jobs and workers readers are optional (nil-safe) for backward compatibility
// with existing callers that only need task/attempt summarization.
func NewService(tasks TaskReader, attempts AttemptReader) (*Service, error) {
	if tasks == nil {
		return nil, fmt.Errorf("observability: task reader is required")
	}
	if attempts == nil {
		return nil, fmt.Errorf("observability: attempt reader is required")
	}
	return &Service{tasks: tasks, attempts: attempts}, nil
}

// WithJobs sets the job reader for aggregate queries (Overview).
func (s *Service) WithJobs(r JobReader) *Service { s.jobs = r; return s }

// WithWorkers sets the worker reader for worker queries.
func (s *Service) WithWorkers(r WorkerReader) *Service { s.workers = r; return s }

// WithVersionMetrics sets the version metrics reader for regression comparison.
func (s *Service) WithVersionMetrics(r VersionMetricsReader) *Service { s.versionMetrics = r; return s }

// SummarizeTask returns the aggregated execution diagnostics for a task.
func (s *Service) SummarizeTask(ctx context.Context, taskID string) (*ExecutionSummary, error) {
	task, err := s.tasks.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize: %w", err)
	}
	if task == nil {
		return nil, fmt.Errorf("observability summarize: task %s not found", taskID)
	}

	attempts, err := s.attempts.ListByTaskID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize attempts: %w", err)
	}

	summary := &ExecutionSummary{
		TaskID:       task.ID,
		JobID:        task.JobID,
		TaskStatus:   task.Status,
		AttemptCount: task.AttemptCount,
		PhaseTotals:  make(map[string]int64),
	}

	var firstStart *time.Time
	var lastEnd *time.Time

	for _, a := range attempts {
		as := AttemptSummary{
			AttemptID:      a.ID,
			AttemptNumber:  a.AttemptNumber,
			Status:         a.Status,
			WorkerID:       a.WorkerID,
			PhaseBreakdown: make(map[string]int64),
		}

		// Phase timings
		timings, err := s.attempts.GetPhaseTimings(ctx, a.ID)
		if err == nil {
			var totalDur int64
			for _, pt := range timings {
				as.PhaseBreakdown[pt.Phase] += pt.DurationMS
				summary.PhaseTotals[pt.Phase] += pt.DurationMS
				totalDur += pt.DurationMS
			}
			as.DurationMS = totalDur

			if len(timings) > 0 {
				start := timings[0].WallStart
				end := timings[len(timings)-1].WallEnd
				if firstStart == nil || start.Before(*firstStart) {
					firstStart = &start
				}
				if lastEnd == nil || end.After(*lastEnd) {
					lastEnd = &end
				}
			}
		}

		// Metrics
		metrics, err := s.attempts.GetMetrics(ctx, a.ID)
		if err == nil && metrics != nil {
			as.Metrics = metrics
			summary.TotalInputBytes += metrics.InputBytes
			summary.TotalOutputBytes += metrics.OutputBytes
			summary.BytesFromDrive += metrics.BytesFromDrive
			summary.BytesFromBlobstore += metrics.BytesFromBlobstore
			summary.BytesFromLocalCache += metrics.BytesFromLocalCache
			summary.CPUTimeMS += metrics.CPUTimeMS
			summary.GPUTimeMS += metrics.GPUTimeMS
			if metrics.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = metrics.PeakRSSBytes
			}
			if metrics.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = metrics.PeakVRAMBytes
			}
		}

		summary.Attempts = append(summary.Attempts, as)
	}

	if firstStart != nil && lastEnd != nil {
		summary.TotalWallTimeMS = lastEnd.Sub(*firstStart).Milliseconds()
	}
	if task.AttemptCount > 1 {
		summary.Retries = task.AttemptCount - 1
	}

	return summary, nil
}

// SummarizeJob returns the aggregated diagnostics for the task owning a job.
func (s *Service) SummarizeJob(ctx context.Context, jobID string) (*ExecutionSummary, error) {
	task, err := s.tasks.GetByJobID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("observability: no task for job %s", jobID)
	}
	return s.SummarizeTask(ctx, task.ID)
}

// ── Aggregate Observability Queries ──────────────────────────────────────

// OverviewResult is the aggregate system health snapshot returned by Overview().
type OverviewResult struct {
	JobsCompleted24h  int64   `json:"jobs_completed_24h"`
	JobsFailed24h     int64   `json:"jobs_failed_24h"`
	ErrorRate         float64 `json:"error_rate"`
	P95RenderMS       int64   `json:"p95_render_ms"`
	ActiveWorkers     int     `json:"active_workers"`
	QueueDepth        int     `json:"queue_depth"`
	TopSlowPhases     []PhaseStat   `json:"top_slow_phases"`
	TopSlowWorkers    []WorkerStat  `json:"top_slow_workers"`
	TopErrors         []ErrorStat   `json:"top_errors"`
}

// PhaseStat is a single phase aggregate for the overview.
type PhaseStat struct {
	Phase    string `json:"phase"`
	AvgMS    int64  `json:"avg_ms"`
	P95MS    int64  `json:"p95_ms"`
	Samples  int    `json:"samples"`
}

// WorkerStat is a single worker aggregate for the overview.
type WorkerStat struct {
	WorkerID     string  `json:"worker_id"`
	JobCount     int     `json:"job_count"`
	AvgMS        int64   `json:"avg_ms"`
	P95MS        int64   `json:"p95_ms"`
	ErrorRate    float64 `json:"error_rate"`
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
	Date    string  `json:"date"`
	AvgMS   int64   `json:"avg_ms"`
	P95MS   int64   `json:"p95_ms"`
	Samples int     `json:"samples"`
}

// Overview returns the aggregate system health snapshot.
// Reads job counts, recent attempts for timing, and worker counts.
func (s *Service) Overview(ctx context.Context) (*OverviewResult, error) {
	result := &OverviewResult{}

	// Job counts.
	if s.jobs != nil {
		counts, err := s.jobs.Counts(ctx)
		if err == nil {
			result.JobsCompleted24h = counts[jobs.StatusAwaitingArtifact] + counts[jobs.StatusSucceeded]
			result.JobsFailed24h = counts[jobs.StatusFailed] + counts[jobs.StatusCancelled]
			total := result.JobsCompleted24h + result.JobsFailed24h
			if total > 0 {
				result.ErrorRate = float64(result.JobsFailed24h) / float64(total) * 100
			}
		}

		// Queue depth = pending + running jobs.
		result.QueueDepth = int(counts[jobs.StatusPending] + counts[jobs.StatusRunning])
	}

	// Worker count.
	if s.workers != nil {
		workers, err := s.workers.ListWorkers()
		if err == nil {
			result.ActiveWorkers = len(workers)
			// Build worker stats from worker registry data.
			for _, w := range workers {
				status, _ := w["status"].(string)
				if status == "online" || status == "idle" || status == "busy" {
					continue
				}
			}
		}
	}

	// Phase stats: scan recent attempts for timing data.
	phaseDurations := make(map[string][]int64)
	workerDurations := make(map[string][]int64)
	workerJobCounts := make(map[string]int)
	errorCounts := make(map[string]int)

	recentTasks, err := s.tasks.List(ctx, taskgraph.Filter{Limit: 200})
	if err == nil {
		for _, task := range recentTasks {
			attempts, aErr := s.attempts.ListByTaskID(ctx, task.ID)
			if aErr != nil {
				continue
			}
			for _, a := range attempts {
				if a.WorkerID != "" {
					workerJobCounts[a.WorkerID]++
				}
				if a.Status == taskattempts.AttemptStatusFailed && a.ErrorCode != "" {
					errorCounts[a.ErrorCode]++
				}
				timings, tErr := s.attempts.GetPhaseTimings(ctx, a.ID)
				if tErr != nil {
					continue
				}
				var totalDur int64
				for _, pt := range timings {
					phaseDurations[pt.Phase] = append(phaseDurations[pt.Phase], pt.DurationMS)
					totalDur += pt.DurationMS
				}
				if totalDur > 0 && a.WorkerID != "" {
					workerDurations[a.WorkerID] = append(workerDurations[a.WorkerID], totalDur)
				}
			}
		}
	}

	// Compute phase stats.
	for phase, durations := range phaseDurations {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		avg := avgInt64(durations)
		p95 := percentileInt64(durations, 0.95)
		result.TopSlowPhases = append(result.TopSlowPhases, PhaseStat{
			Phase: phase, AvgMS: avg, P95MS: p95, Samples: len(durations),
		})
	}
	sort.Slice(result.TopSlowPhases, func(i, j int) bool {
		return result.TopSlowPhases[i].AvgMS > result.TopSlowPhases[j].AvgMS
	})
	if len(result.TopSlowPhases) > 5 {
		result.TopSlowPhases = result.TopSlowPhases[:5]
	}

	// Compute worker stats.
	for wid, durations := range workerDurations {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		avg := avgInt64(durations)
		p95 := percentileInt64(durations, 0.95)
		jobCount := workerJobCounts[wid]
		errRate := 0.0
		ws := WorkerStat{WorkerID: wid, JobCount: jobCount, AvgMS: avg, P95MS: p95, ErrorRate: errRate}
		result.TopSlowWorkers = append(result.TopSlowWorkers, ws)
	}
	sort.Slice(result.TopSlowWorkers, func(i, j int) bool {
		return result.TopSlowWorkers[i].AvgMS > result.TopSlowWorkers[j].AvgMS
	})
	if len(result.TopSlowWorkers) > 5 {
		result.TopSlowWorkers = result.TopSlowWorkers[:5]
	}

	// Compute error stats.
	for code, count := range errorCounts {
		result.TopErrors = append(result.TopErrors, ErrorStat{ErrorCode: code, Count: count})
	}
	sort.Slice(result.TopErrors, func(i, j int) bool {
		return result.TopErrors[i].Count > result.TopErrors[j].Count
	})
	if len(result.TopErrors) > 5 {
		result.TopErrors = result.TopErrors[:5]
	}

	// Compute p95 render time.
	var allDurations []int64
	for _, ds := range workerDurations {
		allDurations = append(allDurations, ds...)
	}
	sort.Slice(allDurations, func(i, j int) bool { return allDurations[i] < allDurations[j] })
	result.P95RenderMS = percentileInt64(allDurations, 0.95)

	return result, nil
}

// ListWorkers returns per-worker performance summaries.
func (s *Service) ListWorkers(ctx context.Context) ([]WorkerPerformance, error) {
	if s.workers == nil {
		return nil, fmt.Errorf("observability: worker reader not configured")
	}
	rawWorkers, err := s.workers.ListWorkers()
	if err != nil {
		return nil, fmt.Errorf("observability: list workers: %w", err)
	}

	// Build worker job counts from tasks.
	workerJobs := make(map[string]int)
	workerDurations := make(map[string][]int64)
	workerSuccesses := make(map[string]int)
	workerFailures := make(map[string]int)

	recentTasks, tErr := s.tasks.List(ctx, taskgraph.Filter{Limit: 500})
	if tErr == nil {
		for _, task := range recentTasks {
			attempts, aErr := s.attempts.ListByTaskID(ctx, task.ID)
			if aErr != nil {
				continue
			}
			for _, a := range attempts {
				if a.WorkerID == "" {
					continue
				}
				workerJobs[a.WorkerID]++
				if a.Status == taskattempts.AttemptStatusSucceeded {
					workerSuccesses[a.WorkerID]++
				} else if a.Status == taskattempts.AttemptStatusFailed {
					workerFailures[a.WorkerID]++
				}
				timings, ptErr := s.attempts.GetPhaseTimings(ctx, a.ID)
				if ptErr != nil {
					continue
				}
				var totalDur int64
				for _, pt := range timings {
					totalDur += pt.DurationMS
				}
				if totalDur > 0 {
					workerDurations[a.WorkerID] = append(workerDurations[a.WorkerID], totalDur)
				}
			}
		}
	}

	var result []WorkerPerformance
	for _, raw := range rawWorkers {
		wid, _ := raw["worker_id"].(string)
		if wid == "" {
			continue
		}
		wp := WorkerPerformance{
			WorkerID: wid,
		}
		if name, ok := raw["worker_name"].(string); ok {
			wp.WorkerName = name
		}
		if status, ok := raw["status"].(string); ok {
			wp.Status = status
		}
		if hb, ok := raw["last_heartbeat"].(string); ok {
			wp.LastHeartbeat = hb
		}
		wp.JobCount = workerJobs[wid]
		total := workerSuccesses[wid] + workerFailures[wid]
		if total > 0 {
			wp.SuccessRate = float64(workerSuccesses[wid]) / float64(total) * 100
		}
		durations := workerDurations[wid]
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		wp.AvgMS = avgInt64(durations)
		wp.P95MS = percentileInt64(durations, 0.95)
		result = append(result, wp)
	}

	return result, nil
}

// PhaseTrends returns phase timing aggregates, optionally filtered by executor.
func (s *Service) PhaseTrends(ctx context.Context, phase string, executor string) (*PhaseTrendResult, error) {
	if phase == "" {
		return nil, fmt.Errorf("observability: phase parameter is required")
	}

	result := &PhaseTrendResult{Phase: phase, Trend: "stable"}
	var allDurations []int64

	recentTasks, err := s.tasks.List(ctx, taskgraph.Filter{Limit: 500})
	if err != nil {
		return nil, fmt.Errorf("observability: list tasks: %w", err)
	}

	for _, task := range recentTasks {
		if executor != "" && task.ExecutorID != executor {
			continue
		}
		attempts, aErr := s.attempts.ListByTaskID(ctx, task.ID)
		if aErr != nil {
			continue
		}
		for _, a := range attempts {
			timings, tErr := s.attempts.GetPhaseTimings(ctx, a.ID)
			if tErr != nil {
				continue
			}
			for _, pt := range timings {
				if pt.Phase == phase {
					allDurations = append(allDurations, pt.DurationMS)
				}
			}
		}
	}

	sort.Slice(allDurations, func(i, j int) bool { return allDurations[i] < allDurations[j] })
	result.Samples = len(allDurations)
	result.AvgMS = avgInt64(allDurations)
	result.P95MS = percentileInt64(allDurations, 0.95)
	result.DailyPoints = buildDailyPoints(allDurations)

	return result, nil
}

// ── Scalar Metric Aggregation ───────────────────────────────────────────

// ScalarMetricResult is the aggregated result for a single scalar metric
// sampled from recent attempt_metrics rows.
type ScalarMetricResult struct {
	Name    string  `json:"name"`
	Avg     float64 `json:"avg"`
	P95     float64 `json:"p95"`
	Samples int     `json:"samples"`
}

// RecentScalarMetric reads recent attempt_metrics rows and extracts a
// named scalar field (e.g. "ffmpeg_speed_ratio"). Supported names:
// ffmpeg_speed_ratio, cache_byte_hit_ratio, duplicate_download_ratio,
// temp_storage_amplification, render_speed_ratio.
func (s *Service) RecentScalarMetric(ctx context.Context, metricName string) (*ScalarMetricResult, error) {
	recentTasks, err := s.tasks.List(ctx, taskgraph.Filter{Limit: 500})
	if err != nil {
		return nil, fmt.Errorf("observability: list tasks: %w", err)
	}

	var values []float64
	for _, task := range recentTasks {
		attempts, aErr := s.attempts.ListByTaskID(ctx, task.ID)
		if aErr != nil {
			continue
		}
		for _, a := range attempts {
			metrics, mErr := s.attempts.GetMetrics(ctx, a.ID)
			if mErr != nil || metrics == nil {
				continue
			}
			v, ok := extractScalarMetric(metrics, metricName)
			if !ok || v == 0 {
				continue
			}
			values = append(values, v)
		}
	}

	sort.Float64s(values)
	result := &ScalarMetricResult{Name: metricName, Samples: len(values)}
	if len(values) > 0 {
		result.Avg = avgFloat64(values)
		result.P95 = percentileFloat64(values, 0.95)
	}
	return result, nil
}

func extractScalarMetric(m *taskattempts.AttemptMetrics, name string) (float64, bool) {
	switch name {
	case "ffmpeg_speed_ratio":
		return m.FFmpegSpeedRatio, true
	case "cache_byte_hit_ratio":
		return m.CacheByteHitRatio(), true
	case "duplicate_download_ratio":
		return m.DuplicateDownloadRatio(), true
	case "temp_storage_amplification":
		return m.TempStorageAmplification(), true
	case "render_speed_ratio":
		return m.RenderSpeedRatio(), true
	default:
		return 0, false
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────

func avgFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentileFloat64(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avgInt64(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	var sum int64
	for _, v := range vals {
		sum += v
	}
	return sum / int64(len(vals))
}

func percentileInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func buildDailyPoints(durations []int64) []PhaseTrendDayPoint {
	if len(durations) == 0 {
		return nil
	}
	// Return a single aggregate point for now; daily rollups will
	// be populated when the daily_metric_rollups table lands (Step 2).
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return []PhaseTrendDayPoint{
		{
			Date:    time.Now().UTC().Format("2006-01-02"),
			AvgMS:   avgInt64(durations),
			P95MS:   percentileInt64(durations, 0.95),
			Samples: len(durations),
		},
	}
}
