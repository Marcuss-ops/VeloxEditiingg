package taskattempts

import (
	"math"
	"testing"
)

func TestAttemptMetrics_RenderFactor(t *testing.T) {
	cases := []struct {
		name     string
		wall     float64
		media    float64
		expected float64
	}{
		{"half realtime", 30, 60, 0.5},
		{"realtime", 60, 60, 1},
		{"twice realtime", 120, 60, 2},
		{"zero wall", 0, 60, 0},
		{"zero media", 60, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := AttemptMetrics{WallClockSeconds: tc.wall, MediaDurationSeconds: tc.media}
			got := m.RenderFactor()
			if math.Abs(got-tc.expected) > 1e-9 {
				t.Fatalf("RenderFactor() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestAttemptMetrics_EncodeMsPerOutputMinute(t *testing.T) {
	// 120s media = 2 output minutes; 6000ms encode -> 3000 ms/min.
	m := AttemptMetrics{MediaDurationSeconds: 120, EngineSegmentBuildMs: 6000}
	if got, want := m.EncodeMsPerOutputMinute(), 3000.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("EncodeMsPerOutputMinute() = %v, want %v", got, want)
	}

	// Zero output duration returns 0.
	m = AttemptMetrics{MediaDurationSeconds: 0, EngineSegmentBuildMs: 6000}
	if got := m.EncodeMsPerOutputMinute(); got != 0 {
		t.Fatalf("expected 0 with zero media duration, got %v", got)
	}

	// Zero engine build time returns 0.
	m = AttemptMetrics{MediaDurationSeconds: 120, EngineSegmentBuildMs: 0}
	if got := m.EncodeMsPerOutputMinute(); got != 0 {
		t.Fatalf("expected 0 with zero engine build time, got %v", got)
	}
}

func TestAttemptMetrics_CpuMsPerOutputMinute(t *testing.T) {
	// 120s media = 2 output minutes; 120000ms cpu -> 60000 ms/min.
	m := AttemptMetrics{MediaDurationSeconds: 120, CPUTimeMS: 120000}
	if got, want := m.CpuMsPerOutputMinute(), 60000.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("CpuMsPerOutputMinute() = %v, want %v", got, want)
	}

	m = AttemptMetrics{MediaDurationSeconds: 0, CPUTimeMS: 120000}
	if got := m.CpuMsPerOutputMinute(); got != 0 {
		t.Fatalf("expected 0 with zero media duration, got %v", got)
	}
}

func TestAttemptMetrics_DownloadThroughputBytesPerSec(t *testing.T) {
	// 5s download; 10_000_000 bytes -> 2_000_000 bytes/sec.
	m := AttemptMetrics{
		EngineAssetDownloadMs: 5000,
		BytesFromBlobstore:    7_000_000,
		BytesFromDrive:        3_000_000,
	}
	if got, want := m.DownloadThroughputBytesPerSec(), 2_000_000.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("DownloadThroughputBytesPerSec() = %v, want %v", got, want)
	}

	// Zero download time returns 0.
	m = AttemptMetrics{EngineAssetDownloadMs: 0, BytesFromBlobstore: 1_000_000}
	if got := m.DownloadThroughputBytesPerSec(); got != 0 {
		t.Fatalf("expected 0 with zero download time, got %v", got)
	}

	// Zero downloaded bytes returns 0.
	m = AttemptMetrics{EngineAssetDownloadMs: 5000}
	if got := m.DownloadThroughputBytesPerSec(); got != 0 {
		t.Fatalf("expected 0 with zero downloaded bytes, got %v", got)
	}
}

func TestAttemptCacheStats_CacheHitRatio(t *testing.T) {
	cases := []struct {
		name     string
		hits     int64
		misses   int64
		expected float64
	}{
		{"50/50", 50, 50, 0.5},
		{"all hits", 100, 0, 1},
		{"all misses", 0, 100, 0},
		{"no activity", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := AttemptCacheStats{CacheHits: tc.hits, CacheMisses: tc.misses}
			got := cs.CacheHitRatio()
			if math.Abs(got-tc.expected) > 1e-9 {
				t.Fatalf("CacheHitRatio() = %v, want %v", got, tc.expected)
			}
		})
	}
}
