package observability

import (
	"context"
	"math"
	"testing"
	"time"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

func TestService_SummarizeJob(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-1"] = &taskgraph.Task{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, AttemptCount: 1}

	result, err := svc.SummarizeJob(context.Background(), "J-1")
	if err != nil {
		t.Fatalf("SummarizeJob() error: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}
	if result.TaskID != "T-1" {
		t.Errorf("TaskID = %q, want \"T-1\"", result.TaskID)
	}
}
func TestService_SummarizeTask_RollsUpDownloadVolumeInCacheSummary(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-cache-dl"] = &taskgraph.Task{ID: "T-cache-dl", JobID: "J-cache-dl", Status: taskgraph.StatusSucceeded, AttemptCount: 1}
	attempts.attempts["T-cache-dl"] = []taskattempts.TaskAttempt{{
		ID: "A-cache-dl", TaskID: "T-cache-dl", JobID: "J-cache-dl", AttemptNumber: 1,
		WorkerID: "worker-01", Status: taskattempts.AttemptStatusSucceeded,
	}}
	attempts.cacheStats = map[string]*taskattempts.AttemptCacheStats{
		"A-cache-dl": {
			AttemptID:          "A-cache-dl",
			CacheHits:          10,
			CacheMisses:        2,
			CacheLookups:       12,
			CacheDownloadCount: 2,
			CacheDownloadBytes: 2 * 524288,
		},
	}

	result, err := svc.SummarizeTask(context.Background(), "T-cache-dl")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.Cache.DownloadCount != 2 || result.Cache.DownloadBytes != 2*524288 {
		t.Fatalf("CacheSummary download volume = %d/%d; want 2/%d (migration 147 columns must roll up)", result.Cache.DownloadCount, result.Cache.DownloadBytes, 2*524288)
	}
	if result.Cache.Lookups != 12 || result.Cache.HitRatio != 10.0/12.0 {
		t.Fatalf("CacheSummary lookups/hit_ratio = %d/%.3f; want 12/%.3f", result.Cache.Lookups, result.Cache.HitRatio, 10.0/12.0)
	}
}
func TestService_SummarizeJob_NotFound(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	_, err := svc.SummarizeJob(context.Background(), "nonexistent")
	if err == nil {
		t.Error("SummarizeJob() should return error for nonexistent job")
	}
}
func TestService_RecentScalarMetric_DerivedMetrics(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-1"] = &taskgraph.Task{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, AttemptCount: 1}
	attempts.metrics["A-1"] = &taskattempts.AttemptMetrics{
		AttemptID:             "A-1",
		WallClockSeconds:      30,
		MediaDurationSeconds:  60,
		EngineSegmentBuildMs:  3000,
		CPUTimeMS:             60000,
		EngineAssetDownloadMs: 2000,
		BytesFromBlobstore:    4_000_000,
		BytesFromDrive:        2_000_000,
	}

	result, err := svc.RecentScalarMetric(context.Background(), "render_factor")
	if err != nil {
		t.Fatalf("RecentScalarMetric(render_factor) error: %v", err)
	}
	if result.Samples != 1 {
		t.Fatalf("Samples = %d, want 1", result.Samples)
	}
	// 30 / 60 = 0.5
	if math.Abs(result.Avg-0.5) > 1e-9 {
		t.Errorf("Avg = %v, want 0.5", result.Avg)
	}

	// 60s media = 1 output minute; 3000ms encode -> 3000 ms/min.
	result, err = svc.RecentScalarMetric(context.Background(), "encode_ms_per_output_minute")
	if err != nil {
		t.Fatalf("RecentScalarMetric(encode_ms_per_output_minute) error: %v", err)
	}
	if math.Abs(result.Avg-3000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 3000", result.Avg)
	}

	// 60s media = 1 output minute; 60000ms cpu -> 60000 ms/min.
	result, err = svc.RecentScalarMetric(context.Background(), "cpu_ms_per_output_minute")
	if err != nil {
		t.Fatalf("RecentScalarMetric(cpu_ms_per_output_minute) error: %v", err)
	}
	if math.Abs(result.Avg-60000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 60000", result.Avg)
	}

	// 6_000_000 bytes / 2s = 3_000_000 bytes/sec.
	result, err = svc.RecentScalarMetric(context.Background(), "download_throughput")
	if err != nil {
		t.Fatalf("RecentScalarMetric(download_throughput) error: %v", err)
	}
	if math.Abs(result.Avg-3_000_000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 3000000", result.Avg)
	}

	// cache_hit_ratio requires cache stats.
	attempts.cacheStats = map[string]*taskattempts.AttemptCacheStats{
		"A-1": {CacheHits: 75, CacheMisses: 25},
	}
	result, err = svc.RecentScalarMetric(context.Background(), "cache_hit_ratio")
	if err != nil {
		t.Fatalf("RecentScalarMetric(cache_hit_ratio) error: %v", err)
	}
	if result.Samples != 1 {
		t.Fatalf("Samples = %d, want 1", result.Samples)
	}
	if math.Abs(result.Avg-0.75) > 1e-9 {
		t.Errorf("Avg = %v, want 0.75", result.Avg)
	}
}
func TestRollupPhaseTimings(t *testing.T) {
	mk := func(dur int64, phase string, start, end time.Time) taskattempts.PhaseTiming {
		return taskattempts.PhaseTiming{AttemptID: "A", Phase: phase, DurationMS: dur, WallStart: start, WallEnd: end}
	}
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		timings     []taskattempts.PhaseTiming
		wantDur     int64
		wantTotals  map[string]int64
		wantSnaps   int
		wantBreakdn map[string]int64
	}{
		{
			name:        "empty",
			timings:     nil,
			wantDur:     0,
			wantTotals:  map[string]int64{},
			wantSnaps:   0,
			wantBreakdn: map[string]int64{},
		},
		{
			name: "wall bounds derive duration",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", base, base.Add(10*time.Second)),
				mk(50, "encode", base.Add(2*time.Second), base.Add(14*time.Second)),
			},
			wantDur:     14000,
			wantTotals:  map[string]int64{"render": 100, "encode": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "encode": 50},
		},
		{
			name: "out-of-order bounds still span min/max",
			timings: []taskattempts.PhaseTiming{
				mk(30, "encode", base.Add(20*time.Second), base.Add(25*time.Second)),
				mk(30, "render", base, base.Add(10*time.Second)),
			},
			wantDur:     25000,
			wantTotals:  map[string]int64{"encode": 30, "render": 30},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"encode": 30, "render": 30},
		},
		{
			name: "no wall bounds falls back to sum",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", time.Time{}, time.Time{}),
				mk(50, "encode", time.Time{}, time.Time{}),
			},
			wantDur:     150,
			wantTotals:  map[string]int64{"render": 100, "encode": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "encode": 50},
		},
		{
			name: "partial bounds ignore zero timestamps",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", base, base.Add(10*time.Second)),
				mk(50, "quality", time.Time{}, time.Time{}),
			},
			wantDur:     10000,
			wantTotals:  map[string]int64{"render": 100, "quality": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "quality": 50},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := &AttemptSummary{PhaseBreakdown: make(map[string]int64)}
			summary := &ExecutionSummary{PhaseTotals: make(map[string]int64)}
			got := rollupPhaseTimings(tt.timings, as, summary)
			if got != tt.wantDur {
				t.Fatalf("duration = %d, want %d", got, tt.wantDur)
			}
			if len(summary.PhaseTimings) != tt.wantSnaps {
				t.Fatalf("PhaseTimings len = %d, want %d", len(summary.PhaseTimings), tt.wantSnaps)
			}
			for phase, want := range tt.wantTotals {
				if summary.PhaseTotals[phase] != want {
					t.Errorf("PhaseTotals[%q] = %d, want %d", phase, summary.PhaseTotals[phase], want)
				}
				if as.PhaseBreakdown[phase] != want {
					t.Errorf("PhaseBreakdown[%q] = %d, want %d", phase, as.PhaseBreakdown[phase], want)
				}
			}
		})
	}
}
func TestMergeWallBounds(t *testing.T) {
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	// Starts nil and gets seeded from the first non-zero row.
	firstStart, lastEnd := mergeWallBounds([]taskattempts.PhaseTiming{
		{AttemptID: "A", WallStart: base.Add(5 * time.Second), WallEnd: base.Add(15 * time.Second)},
		{AttemptID: "A", WallStart: base, WallEnd: base.Add(20 * time.Second)},
	}, nil, nil)
	if firstStart == nil || lastEnd == nil {
		t.Fatal("expected non-nil bounds after seeding")
	}
	if !firstStart.Equal(base) || !lastEnd.Equal(base.Add(20*time.Second)) {
		t.Fatalf("bounds = %v..%v, want %v..%v", firstStart, lastEnd, base, base.Add(20*time.Second))
	}

	// Zero timestamps never replace a valid bound.
	firstStart, lastEnd = mergeWallBounds([]taskattempts.PhaseTiming{
		{AttemptID: "A", WallStart: time.Time{}, WallEnd: time.Time{}},
	}, firstStart, lastEnd)
	if !firstStart.Equal(base) || !lastEnd.Equal(base.Add(20*time.Second)) {
		t.Fatalf("zero timestamps replaced valid bounds: %v..%v", firstStart, lastEnd)
	}
}
