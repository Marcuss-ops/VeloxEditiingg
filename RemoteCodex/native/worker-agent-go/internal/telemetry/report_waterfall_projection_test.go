package telemetry

import (
	"testing"
	"time"
)

func TestPhaseBreakdownFromWaterfall_NilEmpty(t *testing.T) {
	if got := PhaseBreakdownFromWaterfall(nil); got != nil {
		t.Fatalf("nil stages = %v, want nil", got)
	}
	if got := PhaseBreakdownFromWaterfall([]WaterfallStage{}); got != nil {
		t.Fatalf("empty stages = %v, want nil", got)
	}
}

func TestPhaseBreakdownFromWaterfall_AllStages(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "wait_before_assets", StartedAt: base, CompletedAt: base.Add(1 * time.Second), DurationMS: 1000},
		{Name: "asset_resolve", StartedAt: base.Add(1 * time.Second), CompletedAt: base.Add(3 * time.Second), DurationMS: 2000},
		{Name: "render", StartedAt: base.Add(3 * time.Second), CompletedAt: base.Add(10 * time.Second), DurationMS: 7000},
		{Name: "finalize", StartedAt: base.Add(10 * time.Second), CompletedAt: base.Add(11 * time.Second), DurationMS: 1000},
		{Name: "upload", StartedAt: base.Add(11 * time.Second), CompletedAt: base.Add(13 * time.Second), DurationMS: 2000},
		{Name: "commit_wait", StartedAt: base.Add(13 * time.Second), CompletedAt: base.Add(14 * time.Second), DurationMS: 1000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	// expect 5 phases: asset(3s), render(7s), finalize(1s), upload(2s), commit(1s)
	if len(got) != 5 {
		t.Fatalf("len(phases) = %d, want 5", len(got))
	}

	expected := []struct {
		name  string
		ms    int64
		pct   float64
	}{
		{"asset", 3000, 21.43},    // 3/14
		{"render", 7000, 50.00},   // 7/14
		{"finalize", 1000, 7.14},  // 1/14
		{"upload", 2000, 14.29},   // 2/14
		{"commit", 1000, 7.14},    // 1/14
	}

	for i, exp := range expected {
		if got[i].Name != exp.name {
			t.Errorf("phase[%d].Name = %q, want %q", i, got[i].Name, exp.name)
		}
		if got[i].DurationMs != exp.ms {
			t.Errorf("phase[%d].DurationMs = %d, want %d", i, got[i].DurationMs, exp.ms)
		}
		if got[i].Percent != exp.pct {
			t.Errorf("phase[%d].Percent = %.2f, want %.2f", i, got[i].Percent, exp.pct)
		}
	}

	// No unclassified: stages are contiguous.
	for _, p := range got {
		if p.Name == "unclassified" {
			t.Error("unexpected unclassified phase with contiguous stages")
		}
	}
}

func TestPhaseBreakdownFromWaterfall_GapProducesUnclassified(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "render", StartedAt: base, CompletedAt: base.Add(5 * time.Second), DurationMS: 5000},
		// 3-second gap
		{Name: "upload", StartedAt: base.Add(8 * time.Second), CompletedAt: base.Add(10 * time.Second), DurationMS: 2000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	// expect 3 phases: render(5s), upload(2s), unclassified(3s)
	if len(got) != 3 {
		t.Fatalf("len(phases) = %d, want 3", len(got))
	}

	if got[0].Name != "render" || got[0].DurationMs != 5000 {
		t.Errorf("phase[0] = %q/%d, want render/5000", got[0].Name, got[0].DurationMs)
	}
	if got[1].Name != "upload" || got[1].DurationMs != 2000 {
		t.Errorf("phase[1] = %q/%d, want upload/2000", got[1].Name, got[1].DurationMs)
	}
	if got[2].Name != "unclassified" || got[2].DurationMs != 3000 {
		t.Errorf("phase[2] = %q/%d, want unclassified/3000", got[2].Name, got[2].DurationMs)
	}

	// Percentages: 5/10=50%, 2/10=20%, 3/10=30%
	if got[0].Percent != 50.0 {
		t.Errorf("render percent = %.2f, want 50.00", got[0].Percent)
	}
	if got[1].Percent != 20.0 {
		t.Errorf("upload percent = %.2f, want 20.00", got[1].Percent)
	}
	if got[2].Percent != 30.0 {
		t.Errorf("unclassified percent = %.2f, want 30.00", got[2].Percent)
	}
}

func TestPhaseBreakdownFromWaterfall_MultipleStagesSamePhase(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "wait_before_assets", StartedAt: base, CompletedAt: base.Add(1 * time.Second), DurationMS: 1000},
		{Name: "asset_resolve", StartedAt: base.Add(1 * time.Second), CompletedAt: base.Add(4 * time.Second), DurationMS: 3000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	// Both map to "asset" → merged into one entry.
	if len(got) != 1 {
		t.Fatalf("len(phases) = %d, want 1 (merged asset)", len(got))
	}
	if got[0].Name != "asset" {
		t.Errorf("phase[0].Name = %q, want asset", got[0].Name)
	}
	if got[0].DurationMs != 4000 {
		t.Errorf("phase[0].DurationMs = %d, want 4000 (1s+3s)", got[0].DurationMs)
	}
	if got[0].Percent != 100.0 {
		t.Errorf("phase[0].Percent = %.2f, want 100.00", got[0].Percent)
	}
}

func TestPhaseBreakdownFromWaterfall_SingleStage(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "render", StartedAt: base, CompletedAt: base.Add(10 * time.Second), DurationMS: 10000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	if len(got) != 1 {
		t.Fatalf("len(phases) = %d, want 1", len(got))
	}
	if got[0].Name != "render" || got[0].DurationMs != 10000 || got[0].Percent != 100.0 {
		t.Errorf("phase[0] = %+v, want render/10000/100%%", got[0])
	}
}

func TestPhaseBreakdownFromWaterfall_UnknownStageIgnored(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "unknown_future_stage", StartedAt: base, CompletedAt: base.Add(5 * time.Second), DurationMS: 5000},
		{Name: "render", StartedAt: base.Add(5 * time.Second), CompletedAt: base.Add(10 * time.Second), DurationMS: 5000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	// Unknown stages are ignored; unclassified covers their time.
	if len(got) != 2 {
		t.Fatalf("len(phases) = %d, want 2", len(got))
	}
	if got[0].Name != "render" || got[0].DurationMs != 5000 {
		t.Errorf("phase[0] = %q/%d, want render/5000", got[0].Name, got[0].DurationMs)
	}
	if got[1].Name != "unclassified" || got[1].DurationMs != 5000 {
		t.Errorf("phase[1] = %q/%d, want unclassified/5000", got[1].Name, got[1].DurationMs)
	}
}

func TestPhaseBreakdownFromWaterfall_PercentagesSumTo100(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stages := []WaterfallStage{
		{Name: "wait_before_assets", StartedAt: base, CompletedAt: base.Add(1 * time.Second), DurationMS: 1000},
		{Name: "asset_resolve", StartedAt: base.Add(1 * time.Second), CompletedAt: base.Add(2 * time.Second), DurationMS: 1000},
		{Name: "render", StartedAt: base.Add(2 * time.Second), CompletedAt: base.Add(5 * time.Second), DurationMS: 3000},
		{Name: "finalize", StartedAt: base.Add(5 * time.Second), CompletedAt: base.Add(6 * time.Second), DurationMS: 1000},
		{Name: "upload", StartedAt: base.Add(6 * time.Second), CompletedAt: base.Add(8 * time.Second), DurationMS: 2000},
		{Name: "commit_wait", StartedAt: base.Add(8 * time.Second), CompletedAt: base.Add(9 * time.Second), DurationMS: 1000},
	}

	got := PhaseBreakdownFromWaterfall(stages)

	var totalPct float64
	for _, p := range got {
		totalPct += p.Percent
	}
	// Allow small floating-point rounding.
	if totalPct < 99.9 || totalPct > 100.1 {
		t.Errorf("total percent = %.2f, want ~100.00", totalPct)
	}
}
