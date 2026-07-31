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
// temp_storage_amplification, render_speed_ratio, render_factor,
// encode_ms_per_output_minute, cpu_ms_per_output_minute,
// download_throughput, cache_hit_ratio.
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
			cs, csErr := s.attempts.GetCacheStats(ctx, a.ID)
			if csErr != nil || cs == nil {
				cs = &taskattempts.AttemptCacheStats{}
			}
			v, ok := extractScalarMetric(metrics, *cs, metricName)
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

func extractScalarMetric(m *taskattempts.AttemptMetrics, cache taskattempts.AttemptCacheStats, name string) (float64, bool) {
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
	case "render_factor":
		return m.RenderFactor(), true
	case "encode_ms_per_output_minute":
		return m.EncodeMsPerOutputMinute(), true
	case "cpu_ms_per_output_minute":
		return m.CpuMsPerOutputMinute(), true
	case "download_throughput":
		return m.DownloadThroughputBytesPerSec(), true
	case "cache_hit_ratio":
		return cache.CacheHitRatio(), true
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
