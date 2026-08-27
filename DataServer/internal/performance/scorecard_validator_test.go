package performance

import (
	"testing"
)

func TestValidateScorecard_ExactMatch(t *testing.T) {
	prediction := ScorecardPrediction{RenderSlots: 3, SweetSpot: 3}
	result := &ConcurrentBenchmarkResult{
		SweetSpot:      3,
		LimitingFactor: "NETWORK",
		Levels:         []ConcurrencyLevelResult{{Level: 1}, {Level: 2}, {Level: 3}},
	}
	vr := ValidateScorecard(prediction, result)
	if vr.Accuracy != "exact" {
		t.Fatalf("accuracy = %q, want exact", vr.Accuracy)
	}
	// No change to defaults when prediction matches
	if vr.SuggestedRAMSafety != 0.75 {
		t.Fatalf("ram_safety = %.2f, want 0.75", vr.SuggestedRAMSafety)
	}
}

func TestValidateScorecard_WithinOne(t *testing.T) {
	prediction := ScorecardPrediction{RenderSlots: 3, SweetSpot: 3}
	result := &ConcurrentBenchmarkResult{
		SweetSpot:      2,
		LimitingFactor: "NETWORK",
		Levels:         []ConcurrencyLevelResult{{Level: 1}, {Level: 2}},
	}
	vr := ValidateScorecard(prediction, result)
	if vr.Accuracy != "within_1" {
		t.Fatalf("accuracy = %q, want within_1", vr.Accuracy)
	}
}

func TestValidateScorecard_OffHigh_TightensThresholds(t *testing.T) {
	prediction := ScorecardPrediction{RenderSlots: 4, SweetSpot: 4}
	result := &ConcurrentBenchmarkResult{
		SweetSpot:      2,
		LimitingFactor: "NETWORK",
		Levels:         []ConcurrencyLevelResult{{Level: 1}, {Level: 2}},
		Gains: []ThroughputGain{
			{FromLevel: 1, ToLevel: 2, GainPercent: 80, IsEfficient: true},
			{FromLevel: 2, ToLevel: 3, GainPercent: 5, IsEfficient: false},
			{FromLevel: 3, ToLevel: 4, GainPercent: 2, IsEfficient: false},
		},
	}
	vr := ValidateScorecard(prediction, result)
	if vr.Accuracy != "off_high" {
		t.Fatalf("accuracy = %q, want off_high", vr.Accuracy)
	}
	// Network bottleneck → tighten network safety only
	if vr.SuggestedNetworkSafety >= 0.80 {
		t.Fatalf("network_safety = %.2f, should be < 0.80 (tightened)", vr.SuggestedNetworkSafety)
	}
	// RAM is not the bottleneck — should remain at default
	if vr.SuggestedRAMSafety != 0.75 {
		t.Fatalf("ram_safety = %.2f, should be 0.75 (unchanged)", vr.SuggestedRAMSafety)
	}
}

func TestValidateScorecard_OffLow_LoosensThresholds(t *testing.T) {
	prediction := ScorecardPrediction{RenderSlots: 1, SweetSpot: 1}
	result := &ConcurrentBenchmarkResult{
		SweetSpot:      3,
		LimitingFactor: "RAM",
		Levels:         []ConcurrencyLevelResult{{Level: 1}, {Level: 2}, {Level: 3}},
		Gains: []ThroughputGain{
			{FromLevel: 1, ToLevel: 2, GainPercent: 90, IsEfficient: true},
			{FromLevel: 2, ToLevel: 3, GainPercent: 45, IsEfficient: true},
			{FromLevel: 3, ToLevel: 4, GainPercent: 5, IsEfficient: false},
		},
	}
	vr := ValidateScorecard(prediction, result)
	if vr.Accuracy != "off_low" {
		t.Fatalf("accuracy = %q, want off_low", vr.Accuracy)
	}
	// RAM bottleneck → loosen RAM safety
	if vr.SuggestedRAMSafety <= 0.75 {
		t.Fatalf("ram_safety = %.2f, should be > 0.75 (loosened)", vr.SuggestedRAMSafety)
	}
}

func TestValidateScorecard_NilResult(t *testing.T) {
	prediction := ScorecardPrediction{RenderSlots: 3, SweetSpot: 3}
	vr := ValidateScorecard(prediction, nil)
	if vr.Rationale != "insufficient benchmark data" {
		t.Fatalf("rationale = %q, want insufficient benchmark data", vr.Rationale)
	}
}

func TestValidateScorecard_ThresholdsClamped(t *testing.T) {
	// Very large overprediction should not go below minimum thresholds
	prediction := ScorecardPrediction{RenderSlots: 10, SweetSpot: 10}
	result := &ConcurrentBenchmarkResult{
		SweetSpot:      1,
		LimitingFactor: "NETWORK",
		Levels:         []ConcurrencyLevelResult{{Level: 1}},
	}
	vr := ValidateScorecard(prediction, result)
	if vr.SuggestedNetworkSafety < 0.50 {
		t.Fatalf("network_safety = %.2f, should be >= 0.50 (clamped)", vr.SuggestedNetworkSafety)
	}
	if vr.SuggestedRAMSafety < 0.40 {
		t.Fatalf("ram_safety = %.2f, should be >= 0.40 (clamped)", vr.SuggestedRAMSafety)
	}
}

func TestAggregateTuningRecommendations_Empty(t *testing.T) {
	ram, disk, net, rationale := AggregateTuningRecommendations(nil, nil)
	if ram != 0.75 || disk != 0.75 || net != 0.80 {
		t.Fatalf("defaults = %.2f/%.2f/%.2f, want 0.75/0.75/0.80", ram, disk, net)
	}
	if rationale != "no benchmark data available" {
		t.Fatalf("rationale = %q", rationale)
	}
}

func TestAggregateTuningRecommendations_Median(t *testing.T) {
	results := []ValidationResult{
		{SuggestedRAMSafety: 0.70, SuggestedDiskSafety: 0.70, SuggestedNetworkSafety: 0.75},
		{SuggestedRAMSafety: 0.80, SuggestedDiskSafety: 0.80, SuggestedNetworkSafety: 0.85},
		{SuggestedRAMSafety: 0.75, SuggestedDiskSafety: 0.75, SuggestedNetworkSafety: 0.80},
	}
	ram, disk, net, _ := AggregateTuningRecommendations(results, nil)
	// Weighted median of [0.70, 0.75, 0.80] with equal weights = 0.75
	if ram < 0.70 || ram > 0.80 {
		t.Fatalf("ram = %.2f, want ~0.75", ram)
	}
	if disk < 0.70 || disk > 0.80 {
		t.Fatalf("disk = %.2f, want ~0.75", disk)
	}
	if net < 0.75 || net > 0.85 {
		t.Fatalf("net = %.2f, want ~0.80", net)
	}
}
