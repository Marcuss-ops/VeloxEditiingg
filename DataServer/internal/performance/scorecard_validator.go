// scorecard_validator.go compares the CapacityScorecard's predicted sweet spot
// against the actual benchmark-observed sweet spot and recommends threshold
// adjustments when the prediction is systematically off.

package performance

import (
	"fmt"
	"math"
	"strings"
)

// Default safety thresholds for slot computation. These mirror the
// workers package defaults but are defined here to avoid a circular import.
var defaultThresholds = struct {
	RAMSafety     float64
	DiskSafety    float64
	NetworkSafety float64
}{
	RAMSafety:     0.75,
	DiskSafety:    0.75,
	NetworkSafety: 0.80,
}

// ScorecardPrediction is what the CapacityScorecard predicted for this worker.
type ScorecardPrediction struct {
	RenderSlots int
	SweetSpot   int // derived: min(renderSlots, prefetchSlots, publisherSlots)
}

// ValidationResult compares prediction vs observation.
type ValidationResult struct {
	PredictedSweetSpot int
	ObservedSweetSpot  int
	Accuracy           string // "exact", "within_1", "off_high", "off_low"
	// Threshold tuning suggestions derived from the gap.
	SuggestedRAMSafety     float64
	SuggestedDiskSafety    float64
	SuggestedNetworkSafety float64
	Rationale              string
}

// ValidateScorecard compares the scorecard's predicted capacity against
// the actual benchmark sweet spot and produces threshold tuning suggestions.
//
// The logic:
//   - If prediction == observation → "exact" → no threshold change needed
//   - If prediction == observation ± 1 → "within_1" → minor noise, no change
//   - If prediction > observation (predicted too many slots) → the thresholds
//     are too permissive → tighten them
//   - If prediction < observation (predicted too few slots) → the thresholds
//     are too conservative → loosen them
func ValidateScorecard(prediction ScorecardPrediction, result *ConcurrentBenchmarkResult) ValidationResult {
	if result == nil || len(result.Levels) == 0 {
		return ValidationResult{
			PredictedSweetSpot:     prediction.SweetSpot,
			SuggestedRAMSafety:     defaultThresholds.RAMSafety,
			SuggestedDiskSafety:    defaultThresholds.DiskSafety,
			SuggestedNetworkSafety: defaultThresholds.NetworkSafety,
			Rationale:              "insufficient benchmark data",
		}
	}

	vr := ValidationResult{
		PredictedSweetSpot:     prediction.SweetSpot,
		ObservedSweetSpot:      result.SweetSpot,
		SuggestedRAMSafety:     defaultThresholds.RAMSafety,
		SuggestedDiskSafety:    defaultThresholds.DiskSafety,
		SuggestedNetworkSafety: defaultThresholds.NetworkSafety,
	}

	// Classify accuracy
	diff := prediction.SweetSpot - result.SweetSpot
	switch {
	case diff == 0:
		vr.Accuracy = "exact"
	case diff == 1 || diff == -1:
		vr.Accuracy = "within_1"
	case diff > 0:
		vr.Accuracy = "off_high" // predicted too many
	case diff < 0:
		vr.Accuracy = "off_low" // predicted too few
	}

	// Build rationale
	var reasons []string

	// Determine which dimension is the bottleneck from the benchmark gains
	limiting := result.LimitingFactor

	// Tune thresholds based on the prediction gap
	if diff > 0 {
		// Predicted too many slots → thresholds are too permissive
		// Tighten the limiting resource's threshold by 5% per slot overshoot
		adjustment := float64(diff) * 0.05
		reasons = append(reasons, fmt.Sprintf("predicted %d slots but observed sweet_spot=%d (overpredicted by %d)",
			prediction.SweetSpot, result.SweetSpot, diff))

		switch strings.ToUpper(limiting) {
		case "RAM":
			vr.SuggestedRAMSafety = math.Max(0.40, defaultThresholds.RAMSafety-adjustment)
			reasons = append(reasons, fmt.Sprintf("tightening RAM safety from %.0f%% to %.0f%%",
				defaultThresholds.RAMSafety*100, vr.SuggestedRAMSafety*100))
		case "CPU":
			// CPU doesn't have a safety fraction; reduce effective cores heuristic
			reasons = append(reasons, "CPU bottleneck detected — consider reducing effective_cpu_cores heuristic")
		case "NVME":
			vr.SuggestedDiskSafety = math.Max(0.40, defaultThresholds.DiskSafety-adjustment)
			reasons = append(reasons, fmt.Sprintf("tightening disk safety from %.0f%% to %.0f%%",
				defaultThresholds.DiskSafety*100, vr.SuggestedDiskSafety*100))
		case "NETWORK":
			vr.SuggestedNetworkSafety = math.Max(0.50, defaultThresholds.NetworkSafety-adjustment)
			reasons = append(reasons, fmt.Sprintf("tightening network safety from %.0f%% to %.0f%%",
				defaultThresholds.NetworkSafety*100, vr.SuggestedNetworkSafety*100))
		default:
			// Unknown limiting factor — tighten all proportionally
			vr.SuggestedRAMSafety = math.Max(0.40, defaultThresholds.RAMSafety-adjustment)
			vr.SuggestedDiskSafety = math.Max(0.40, defaultThresholds.DiskSafety-adjustment)
			vr.SuggestedNetworkSafety = math.Max(0.50, defaultThresholds.NetworkSafety-adjustment)
			reasons = append(reasons, "unknown limiting factor — tightening all thresholds proportionally")
		}
	} else if diff < 0 {
		// Predicted too few slots → thresholds are too conservative
		// Loosen the limiting resource's threshold by 3% per slot undercount
		adjustment := float64(-diff) * 0.03
		reasons = append(reasons, fmt.Sprintf("predicted %d slots but observed sweet_spot=%d (underpredicted by %d)",
			prediction.SweetSpot, result.SweetSpot, -diff))

		switch strings.ToUpper(limiting) {
		case "RAM":
			vr.SuggestedRAMSafety = math.Min(0.90, defaultThresholds.RAMSafety+adjustment)
			reasons = append(reasons, fmt.Sprintf("loosening RAM safety from %.0f%% to %.0f%%",
				defaultThresholds.RAMSafety*100, vr.SuggestedRAMSafety*100))
		case "NVME":
			vr.SuggestedDiskSafety = math.Min(0.90, defaultThresholds.DiskSafety+adjustment)
			reasons = append(reasons, fmt.Sprintf("loosening disk safety from %.0f%% to %.0f%%",
				defaultThresholds.DiskSafety*100, vr.SuggestedDiskSafety*100))
		case "NETWORK":
			vr.SuggestedNetworkSafety = math.Min(0.95, defaultThresholds.NetworkSafety+adjustment)
			reasons = append(reasons, fmt.Sprintf("loosening network safety from %.0f%% to %.0f%%",
				defaultThresholds.NetworkSafety*100, vr.SuggestedNetworkSafety*100))
		default:
			vr.SuggestedRAMSafety = math.Min(0.90, defaultThresholds.RAMSafety+adjustment)
			vr.SuggestedDiskSafety = math.Min(0.90, defaultThresholds.DiskSafety+adjustment)
			vr.SuggestedNetworkSafety = math.Min(0.95, defaultThresholds.NetworkSafety+adjustment)
			reasons = append(reasons, "unknown limiting factor — loosening all thresholds proportionally")
		}
	} else {
		reasons = append(reasons, "prediction matches observation — no threshold adjustment needed")
	}

	// Add observed throughput curve info
	if len(result.Gains) > 0 {
		var curve []string
		for _, g := range result.Gains {
			marker := "✗"
			if g.IsEfficient {
				marker = "✓"
			}
			curve = append(curve, fmt.Sprintf("%d→%d: %+.1f%% %s", g.FromLevel, g.ToLevel, g.GainPercent, marker))
		}
		reasons = append(reasons, "throughput curve: "+strings.Join(curve, ", "))
	}

	vr.Rationale = strings.Join(reasons, "; ")
	return vr
}

// AggregateTuningRecommendations merges multiple per-worker validation results
// into fleet-wide threshold recommendations using a weighted median approach.
// Workers with more benchmark runs get higher weight.
// weightedValue pairs a float64 metric with an integer weight for
// computing weighted medians across workers with different benchmark counts.
type weightedValue struct {
	value  float64
	weight int
}

func AggregateTuningRecommendations(results []ValidationResult, runCounts map[string]int) (ramSafety, diskSafety, networkSafety float64, rationale string) {
	if len(results) == 0 {
		return defaultThresholds.RAMSafety,
			defaultThresholds.DiskSafety,
			defaultThresholds.NetworkSafety,
			"no benchmark data available"
	}

	var ramValues, diskValues, netValues []weightedValue
	totalWeight := 0

	for i, r := range results {
		workerID := fmt.Sprintf("worker_%d", i)
		weight := 1
		if wc, ok := runCounts[workerID]; ok && wc > 0 {
			weight = wc
		}
		totalWeight += weight
		ramValues = append(ramValues, weightedValue{r.SuggestedRAMSafety, weight})
		diskValues = append(diskValues, weightedValue{r.SuggestedDiskSafety, weight})
		netValues = append(netValues, weightedValue{r.SuggestedNetworkSafety, weight})
	}

	// Weighted median for each threshold
	ramSafety = weightedMedian(ramValues)
	diskSafety = weightedMedian(diskValues)
	networkSafety = weightedMedian(netValues)

	// Clamp to safe ranges
	ramSafety = clamp(ramSafety, 0.40, 0.90)
	diskSafety = clamp(diskSafety, 0.40, 0.90)
	networkSafety = clamp(networkSafety, 0.50, 0.95)

	rationale = fmt.Sprintf("aggregated from %d worker benchmark results (total weight=%d)", len(results), totalWeight)
	return
}

func weightedMedian(values []weightedValue) float64 {
	if len(values) == 0 {
		return 0.5
	}
	// Sort by value
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j].value < values[i].value {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
	totalWeight := 0
	for _, v := range values {
		totalWeight += v.weight
	}
	halfWeight := totalWeight / 2
	cumulative := 0
	for _, v := range values {
		cumulative += v.weight
		if cumulative >= halfWeight {
			return v.value
		}
	}
	return values[len(values)-1].value
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
