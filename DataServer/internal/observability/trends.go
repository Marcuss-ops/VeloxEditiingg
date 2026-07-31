// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"fmt"
	"sort"
	"time"

	"velox-server/internal/taskgraph"
)

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
