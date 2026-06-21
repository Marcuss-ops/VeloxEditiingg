// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"fmt"
	"time"

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
	tasks    TaskReader
	attempts AttemptReader
}

// NewService constructs the observability aggregation service.
func NewService(tasks TaskReader, attempts AttemptReader) (*Service, error) {
	if tasks == nil {
		return nil, fmt.Errorf("observability: task reader is required")
	}
	if attempts == nil {
		return nil, fmt.Errorf("observability: attempt reader is required")
	}
	return &Service{tasks: tasks, attempts: attempts}, nil
}

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
