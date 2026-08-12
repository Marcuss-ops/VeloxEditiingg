package taskrunner

import (
	"testing"
	"time"
)

func TestTaskExecutionReportLegacyMetricsBoundary(t *testing.T) {
	report := &TaskExecutionReport{}
	if report.HasLegacyMetrics() {
		t.Fatal("zero report unexpectedly has legacy metrics")
	}
	if _, ok := report.LegacyMetric("missing"); ok {
		t.Fatal("missing legacy metric was reported as present")
	}

	report.SetLegacyMetric("legacy.key", int64(7))
	if !report.HasLegacyMetrics() {
		t.Fatal("legacy metric write was not visible")
	}
	got, ok := report.LegacyMetric("legacy.key")
	if !ok || got != int64(7) {
		t.Fatalf("legacy metric = %#v/%v, want int64(7)/true", got, ok)
	}
	if report.RawMetrics != nil {
		t.Fatal("legacy compatibility write unexpectedly created RawMetrics")
	}
}

func TestTaskExecutionReportRenderDurationUsesExecutePhase(t *testing.T) {
	start := time.Unix(100, 0)
	report := TaskExecutionReport{
		Metrics: map[string]interface{}{"pipeline.render_ms": int64(99)},
		PhaseMarkers: []PhaseMarker{
			{Name: PhaseExecute, StartedAt: start, CompletedAt: start.Add(42 * time.Millisecond), Status: "ok"},
			{Name: PhaseReport, StartedAt: start, CompletedAt: start.Add(time.Second), Status: "ok"},
		},
	}
	got, ok := report.RenderDuration()
	if !ok || got != 42*time.Millisecond {
		t.Fatalf("render duration = %v/%v, want 42ms/true", got, ok)
	}
}

func TestTaskExecutionReportRenderDurationMissingExecutePhase(t *testing.T) {
	report := TaskExecutionReport{PhaseMarkers: []PhaseMarker{{Name: PhaseReport, Status: "ok"}}}
	if got, ok := report.RenderDuration(); ok || got != 0 {
		t.Fatalf("missing execute duration = %v/%v, want 0/false", got, ok)
	}
}
