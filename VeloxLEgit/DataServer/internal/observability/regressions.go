// Package observability / regressions.go
//
// Version correlation (Step 4 / Velox Metrics Center): regression
// comparison between two git SHAs by comparing task_attempt_metrics
// grouped by version.
package observability

import (
	"context"
	"fmt"
	"math"
	"sort"
)

// RegressionComparison holds the metric comparison between two git SHAs.
type RegressionComparison struct {
	BeforeSHA     string       `json:"before_sha"`
	AfterSHA      string       `json:"after_sha"`
	BeforeSamples int          `json:"before_samples"`
	AfterSamples  int          `json:"after_samples"`
	Metrics       []MetricDiff `json:"metrics"`
}

// MetricDiff is the comparison of a single metric between two versions.
type MetricDiff struct {
	MetricName string  `json:"metric_name"`
	BeforeAvg  float64 `json:"before_avg"`
	AfterAvg   float64 `json:"after_avg"`
	BeforeP95  float64 `json:"before_p95"`
	AfterP95   float64 `json:"after_p95"`
	// DeltaPct is the percentage change: positive = regression (slower/more),
	// negative = improvement. Computed as (after - before) / before * 100.
	DeltaPct float64 `json:"delta_pct"`
	// Conclusion is a human-readable label: "regression", "improvement", "neutral".
	Conclusion string `json:"conclusion"`
}

// CompareVersions compares task_attempt_metrics between two git SHAs.
// Returns a RegressionComparison with per-metric diffs. Threshold
// defines the minimum DeltaPct to classify as regression/improvement
// (e.g. 5.0 means a 5% change is the bar).
//
// Requires WithVersionMetrics() to have been called during wiring;
// returns an error when the versionMetrics reader is nil.
func (s *Service) CompareVersions(ctx context.Context, beforeSHA, afterSHA string, thresholdPct float64) (*RegressionComparison, error) {
	if beforeSHA == "" || afterSHA == "" {
		return nil, fmt.Errorf("observability: CompareVersions requires both before and after SHA")
	}
	if s.versionMetrics == nil {
		return nil, fmt.Errorf("observability: version metrics reader not configured (call WithVersionMetrics during wiring)")
	}

	beforeSnaps, err := s.versionMetrics.ListMetricsByGitSHA(ctx, beforeSHA)
	if err != nil {
		return nil, fmt.Errorf("observability: list metrics for before SHA %s: %w", beforeSHA, err)
	}
	afterSnaps, err := s.versionMetrics.ListMetricsByGitSHA(ctx, afterSHA)
	if err != nil {
		return nil, fmt.Errorf("observability: list metrics for after SHA %s: %w", afterSHA, err)
	}

	result := &RegressionComparison{
		BeforeSHA:     beforeSHA,
		AfterSHA:      afterSHA,
		BeforeSamples: len(beforeSnaps),
		AfterSamples:  len(afterSnaps),
	}

	if len(beforeSnaps) == 0 && len(afterSnaps) == 0 {
		return result, nil
	}

	// Collect per-metric values from both groups.
	beforeVals := make(map[string][]float64)
	afterVals := make(map[string][]float64)
	allMetrics := make(map[string]bool)

	for _, snap := range beforeSnaps {
		for name, val := range snap.Metrics {
			beforeVals[name] = append(beforeVals[name], val)
			allMetrics[name] = true
		}
	}
	for _, snap := range afterSnaps {
		for name, val := range snap.Metrics {
			afterVals[name] = append(afterVals[name], val)
			allMetrics[name] = true
		}
	}

	for name := range allMetrics {
		before := beforeVals[name]
		after := afterVals[name]

		sort.Float64s(before)
		sort.Float64s(after)

		bAvg := avgFloat(before)
		aAvg := avgFloat(after)
		bP95 := percentileFloat(before, 0.95)
		aP95 := percentileFloat(after, 0.95)

		var deltaPct float64
		if bAvg > 0 {
			deltaPct = (aAvg - bAvg) / bAvg * 100
		}

		conclusion := "neutral"
		if thresholdPct > 0 && math.Abs(deltaPct) >= thresholdPct {
			if deltaPct > 0 {
				conclusion = "regression"
			} else {
				conclusion = "improvement"
			}
		}

		result.Metrics = append(result.Metrics, MetricDiff{
			MetricName: name,
			BeforeAvg:  bAvg,
			AfterAvg:   aAvg,
			BeforeP95:  bP95,
			AfterP95:   aP95,
			DeltaPct:   deltaPct,
			Conclusion: conclusion,
		})
	}

	// Sort by absolute delta (most changed first).
	sort.Slice(result.Metrics, func(i, j int) bool {
		return math.Abs(result.Metrics[i].DeltaPct) > math.Abs(result.Metrics[j].DeltaPct)
	})

	return result, nil
}

func avgFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentileFloat(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
