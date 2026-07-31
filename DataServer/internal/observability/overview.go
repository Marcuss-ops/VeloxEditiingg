// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"sort"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

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
