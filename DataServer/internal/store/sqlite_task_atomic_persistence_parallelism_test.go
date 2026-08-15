package store

import (
	"math"
	"testing"

	"velox-server/internal/taskattempts"
)

func seg(dur, start, end float64, slot int, status string) taskattempts.SegmentTiming {
	return taskattempts.SegmentTiming{
		DurationMS:       dur,
		StartedOffsetMS:  start,
		FinishedOffsetMS: end,
		WorkerSlot:       slot,
		Status:           status,
		FfmpegThreads:    4,
	}
}

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

// TestComputeParallelism_Empty returns the zero aggregate with only
// CalculatedAt populated.
func TestComputeParallelism_Empty(t *testing.T) {
	p := computeParallelism(nil, taskattempts.AttemptMetrics{})
	if p.SerialWorkMS != 0 || p.UnionBusyMS != 0 || p.PeakConcurrency != 0 {
		t.Fatalf("empty input must produce zero aggregates: %+v", p)
	}
	if p.CalculatedAt == "" {
		t.Fatalf("CalculatedAt must be set even for empty input")
	}
}

// TestComputeParallelism_SingleSegment covers the serial fallback for a
// single segment with zero offsets (end = accumulated serial work).
func TestComputeParallelism_SingleSegment(t *testing.T) {
	p := computeParallelism(
		[]taskattempts.SegmentTiming{seg(100, 0, 0, 0, "ok")},
		taskattempts.AttemptMetrics{LogicalCPUCount: 8, EffectiveCPUCount: 8},
	)
	assertFloat(t, "SerialWorkMS", p.SerialWorkMS, 100)
	assertFloat(t, "RenderWindowMS", p.RenderWindowMS, 100)
	assertFloat(t, "UnionBusyMS", p.UnionBusyMS, 100)
	assertFloat(t, "OverlapMS", p.OverlapMS, 0)
	assertFloat(t, "IdleGapMS", p.IdleGapMS, 0)
	if p.PeakConcurrency != 1 {
		t.Errorf("PeakConcurrency = %d, want 1", p.PeakConcurrency)
	}
	assertFloat(t, "AverageConcurrency", p.AverageConcurrency, 1)
	assertFloat(t, "SpeedupVsSerial", p.SpeedupVsSerial, 1)
	assertFloat(t, "ParallelEfficiency", p.ParallelEfficiency, 1)
	if p.ConfiguredSegmentWorkers != 1 {
		t.Errorf("ConfiguredSegmentWorkers = %d, want 1", p.ConfiguredSegmentWorkers)
	}
	if p.FFmpegThreadsPerSegment != 4 {
		t.Errorf("FFmpegThreadsPerSegment = %d, want 4", p.FFmpegThreadsPerSegment)
	}
	if p.LogicalCPUCount != 8 || p.CPUBudget != 8 {
		t.Errorf("cpu counts = %d/%d, want 8/8", p.LogicalCPUCount, p.CPUBudget)
	}
	assertFloat(t, "CPUOversubscription", p.CPUOversubscription, 0.5)
	if p.ParallelStrategy != "serial_segments" {
		t.Errorf("ParallelStrategy = %q, want serial_segments", p.ParallelStrategy)
	}
}

// TestComputeParallelism_OverlappingSegments covers the sweep-line union
// busy / overlap / peak concurrency derivation.
func TestComputeParallelism_OverlappingSegments(t *testing.T) {
	p := computeParallelism(
		[]taskattempts.SegmentTiming{
			seg(100, 0, 100, 1, "ok"),
			seg(100, 50, 150, 2, "ok"),
		},
		taskattempts.AttemptMetrics{LogicalCPUCount: 8, EffectiveCPUCount: 8},
	)
	assertFloat(t, "SerialWorkMS", p.SerialWorkMS, 200)
	assertFloat(t, "RenderWindowMS", p.RenderWindowMS, 150)
	assertFloat(t, "UnionBusyMS", p.UnionBusyMS, 150)
	assertFloat(t, "OverlapMS", p.OverlapMS, 50)
	assertFloat(t, "IdleGapMS", p.IdleGapMS, 0)
	if p.PeakConcurrency != 2 {
		t.Errorf("PeakConcurrency = %d, want 2", p.PeakConcurrency)
	}
	assertFloat(t, "AverageConcurrency", p.AverageConcurrency, 200.0/150.0)
	assertFloat(t, "ParallelEfficiency", p.ParallelEfficiency, (200.0/150.0)/2.0)
	if p.ConfiguredSegmentWorkers != 2 {
		t.Errorf("ConfiguredSegmentWorkers = %d, want 2", p.ConfiguredSegmentWorkers)
	}
	if p.ParallelStrategy != "concurrent_segments" {
		t.Errorf("ParallelStrategy = %q, want concurrent_segments", p.ParallelStrategy)
	}
}

// TestComputeParallelism_NonOverlappingSegments covers the idle gap and the
// serial strategy when segments do not overlap.
func TestComputeParallelism_NonOverlappingSegments(t *testing.T) {
	p := computeParallelism(
		[]taskattempts.SegmentTiming{
			seg(50, 0, 50, 1, "ok"),
			seg(50, 100, 150, 1, "ok"),
		},
		taskattempts.AttemptMetrics{LogicalCPUCount: 8, EffectiveCPUCount: 8},
	)
	assertFloat(t, "SerialWorkMS", p.SerialWorkMS, 100)
	assertFloat(t, "RenderWindowMS", p.RenderWindowMS, 150)
	assertFloat(t, "UnionBusyMS", p.UnionBusyMS, 100)
	assertFloat(t, "OverlapMS", p.OverlapMS, 0)
	assertFloat(t, "IdleGapMS", p.IdleGapMS, 50)
	if p.PeakConcurrency != 1 {
		t.Errorf("PeakConcurrency = %d, want 1", p.PeakConcurrency)
	}
	assertFloat(t, "SpeedupVsSerial", p.SpeedupVsSerial, 100.0/150.0)
	if p.ParallelStrategy != "serial_segments" {
		t.Errorf("ParallelStrategy = %q, want serial_segments", p.ParallelStrategy)
	}
}

// TestComputeParallelism_FiltersInvalidSegments verifies non-ok and
// non-positive-duration segments are dropped before aggregation.
func TestComputeParallelism_FiltersInvalidSegments(t *testing.T) {
	p := computeParallelism(
		[]taskattempts.SegmentTiming{
			seg(100, 0, 100, 1, "ok"),
			seg(999, 0, 999, 1, "failed"),
			seg(0, 0, 0, 1, "ok"), // zero duration
		},
		taskattempts.AttemptMetrics{LogicalCPUCount: 8, EffectiveCPUCount: 8},
	)
	assertFloat(t, "SerialWorkMS", p.SerialWorkMS, 100)
	assertFloat(t, "UnionBusyMS", p.UnionBusyMS, 100)
	if p.PeakConcurrency != 1 {
		t.Errorf("PeakConcurrency = %d, want 1", p.PeakConcurrency)
	}
}

// TestComputeParallelism_CPUFallback covers the pre-099 approximation when
// the worker emitted no CPU-capacity telemetry.
func TestComputeParallelism_CPUFallback(t *testing.T) {
	p := computeParallelism(
		[]taskattempts.SegmentTiming{seg(100, 0, 0, 0, "ok")},
		taskattempts.AttemptMetrics{}, // all CPU fields zero
	)
	if p.LogicalCPUCount != 0 {
		t.Errorf("LogicalCPUCount = %d, want 0 (ActiveWorkersAtStart fallback is 0)", p.LogicalCPUCount)
	}
	if p.CPUBudget != 1 {
		t.Errorf("CPUBudget = %d, want 1 (clamped minimum)", p.CPUBudget)
	}
	assertFloat(t, "CPUOversubscription", p.CPUOversubscription, 4.0) // (1 worker * 4 threads) / 1
}
