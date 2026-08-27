package performance

import (
	"context"
	"testing"
	"time"
)

// mockRenderRunner is a test double that returns predictable render results.
type mockRenderRunner struct {
	renderTimeMS int64
	peakRAMBytes int64
}

func (m *mockRenderRunner) Render(ctx context.Context, req BenchmarkRenderRequest) (BenchmarkRenderResult, error) {
	// Simulate render time
	select {
	case <-ctx.Done():
		return BenchmarkRenderResult{}, ctx.Err()
	case <-time.After(time.Duration(m.renderTimeMS) * time.Millisecond):
	}

	return BenchmarkRenderResult{
		Receipt: &BenchmarkRenderReceipt{
			PeakRAMBytes: m.peakRAMBytes,
			RenderWallMS: m.renderTimeMS,
			UploadWallMS: m.renderTimeMS / 2, // upload is half of render
			WallMS:       m.renderTimeMS,
		},
		ArtifactSHA256: "test-sha256",
	}, nil
}

func TestRunConcurrentBenchmark_BasicFlow(t *testing.T) {
	renderer := &mockRenderRunner{
		renderTimeMS: 50,                // fast renders for testing
		peakRAMBytes: 1024 * 1024 * 500, // 500 MB
	}

	config := ConcurrentBenchmarkConfig{
		FixtureID:             "test-fixture",
		MaxConcurrency:        3,
		RunsPerLevel:          2,
		CooldownBetweenLevels: 10 * time.Millisecond, // fast cooldown for tests
		CacheMode:             CacheModeCold,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := RunConcurrentBenchmark(
		ctx,
		renderer,
		BenchmarkFixture{ID: "test-fixture"},
		config,
		"test-worker",
	)

	if err != nil {
		t.Fatalf("RunConcurrentBenchmark failed: %v", err)
	}

	if result == nil {
		t.Fatal("result is nil")
	}

	if result.WorkerID != "test-worker" {
		t.Errorf("WorkerID = %q, want test-worker", result.WorkerID)
	}

	if len(result.Levels) != 3 {
		t.Errorf("Levels = %d, want 3", len(result.Levels))
	}

	// Verify each level has results
	for _, level := range result.Levels {
		if level.SuccessfulRuns != 2 {
			t.Errorf("Level %d: SuccessfulRuns = %d, want 2", level.Level, level.SuccessfulRuns)
		}
		if level.Throughput <= 0 {
			t.Errorf("Level %d: Throughput = %f, want > 0", level.Level, level.Throughput)
		}
	}

	// Verify gains are computed
	if len(result.Gains) != 2 {
		t.Errorf("Gains = %d, want 2", len(result.Gains))
	}

	// Sweet spot should be determined
	if result.SweetSpot < 1 {
		t.Errorf("SweetSpot = %d, want >= 1", result.SweetSpot)
	}

	// Summary should be non-empty
	if result.Summary == "" {
		t.Error("Summary is empty")
	}
}

func TestRunConcurrentBenchmark_ContextCanceled(t *testing.T) {
	renderer := &mockRenderRunner{
		renderTimeMS: 1000, // slow renders
		peakRAMBytes: 1024 * 1024 * 500,
	}

	config := ConcurrentBenchmarkConfig{
		FixtureID:             "test-fixture",
		MaxConcurrency:        4,
		RunsPerLevel:          2,
		CooldownBetweenLevels: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately
	cancel()

	_, err := RunConcurrentBenchmark(
		ctx,
		renderer,
		BenchmarkFixture{ID: "test-fixture"},
		config,
		"test-worker",
	)

	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestRunConcurrentBenchmark_DefaultsApplied(t *testing.T) {
	renderer := &mockRenderRunner{
		renderTimeMS: 10,
		peakRAMBytes: 1024 * 1024 * 100,
	}

	// Zero config should get defaults, but override cooldown for test speed
	config := ConcurrentBenchmarkConfig{
		CooldownBetweenLevels: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := RunConcurrentBenchmark(
		ctx,
		renderer,
		BenchmarkFixture{ID: "test-fixture"},
		config,
		"test-worker",
	)

	if err != nil {
		t.Fatalf("RunConcurrentBenchmark failed: %v", err)
	}

	// Should have 4 levels (default MaxConcurrency)
	if len(result.Levels) != 4 {
		t.Errorf("Levels = %d, want 4 (default MaxConcurrency)", len(result.Levels))
	}
}

func TestComputeThroughputGains(t *testing.T) {
	levels := []ConcurrencyLevelResult{
		{Level: 1, Throughput: 1.0},
		{Level: 2, Throughput: 1.95}, // 95% gain
		{Level: 3, Throughput: 2.75}, // 41% gain
		{Level: 4, Throughput: 2.9},  // 5% gain (diminishing)
	}

	gains := computeThroughputGains(levels)

	if len(gains) != 3 {
		t.Fatalf("Gains = %d, want 3", len(gains))
	}

	// First gain: 1→2 should be efficient (95% > 10%)
	if !gains[0].IsEfficient {
		t.Error("Gain 1→2 should be efficient")
	}

	// Second gain: 2→3 should be efficient (41% > 10%)
	if !gains[1].IsEfficient {
		t.Error("Gain 2→3 should be efficient")
	}

	// Third gain: 3→4 should NOT be efficient (5% < 10%)
	if gains[2].IsEfficient {
		t.Error("Gain 3→4 should NOT be efficient")
	}
}

func TestDetermineSweetSpot(t *testing.T) {
	levels := []ConcurrencyLevelResult{
		{Level: 1, Throughput: 1.0},
		{Level: 2, Throughput: 1.95},
		{Level: 3, Throughput: 2.75},
		{Level: 4, Throughput: 2.9},
	}

	gains := computeThroughputGains(levels)
	sweetSpot, _ := determineSweetSpot(levels, gains)

	// Sweet spot should be 3 (last efficient gain is 2→3)
	if sweetSpot != 3 {
		t.Errorf("SweetSpot = %d, want 3", sweetSpot)
	}
}

func TestDetermineSweetSpot_NoGains(t *testing.T) {
	levels := []ConcurrencyLevelResult{
		{Level: 1, Throughput: 1.0},
	}

	gains := computeThroughputGains(levels)
	sweetSpot, _ := determineSweetSpot(levels, gains)

	if sweetSpot != 1 {
		t.Errorf("SweetSpot = %d, want 1", sweetSpot)
	}
}

func TestPercentile(t *testing.T) {
	sorted := []int64{100, 200, 300, 400, 500}

	p50 := percentile(sorted, 50)
	if p50 != 300 {
		t.Errorf("P50 = %f, want 300", p50)
	}

	p95 := percentile(sorted, 95)
	if p95 != 500 {
		t.Errorf("P95 = %f, want 500", p95)
	}

	p100 := percentile(sorted, 100)
	if p100 != 500 {
		t.Errorf("P100 = %f, want 500", p100)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
