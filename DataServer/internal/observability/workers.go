// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"fmt"
	"sort"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

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
