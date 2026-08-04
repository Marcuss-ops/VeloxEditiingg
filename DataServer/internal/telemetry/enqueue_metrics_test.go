package telemetry

import (
	"context"
	"runtime"
	"testing"
)

func TestEnqueueMetricsRecordsPhaseJSONAndResolverObservations(t *testing.T) {
	ctx, metrics := WithEnqueueMetrics(context.Background(), false)

	finish := BeginEnqueuePhase(ctx, "normalize_scene_video_payload")
	RecordEnqueueJSONMarshal(ctx)
	RecordEnqueueJSONUnmarshal(ctx)
	RecordEnqueueResolverQuery(ctx, "plans")
	RecordEnqueueResolverQuery(ctx, "plans")
	finish()

	snapshot := metrics.Snapshot()
	if snapshot.JSONMarshalCount != 1 || snapshot.JSONUnmarshalCount != 1 {
		t.Fatalf("JSON counts = %d/%d, want 1/1", snapshot.JSONMarshalCount, snapshot.JSONUnmarshalCount)
	}
	if snapshot.ResolverQueries != 2 || snapshot.ResolverPlanQueries != 2 {
		t.Fatalf("resolver counts = total %d plans %d, want 2/2", snapshot.ResolverQueries, snapshot.ResolverPlanQueries)
	}
	if snapshot.PhaseDuration["normalize_scene_video_payload"] <= 0 {
		t.Fatal("phase duration was not recorded")
	}
	if snapshot.AllocationMeasurementOn {
		t.Fatal("allocation measurement unexpectedly enabled")
	}
}

func TestEnqueueMetricsAllocationMeasurementIsOptIn(t *testing.T) {
	ctx, metrics := WithEnqueueMetrics(context.Background(), true)
	finish := BeginEnqueuePhase(ctx, "project_worker_payload")
	buffer := make([]byte, 128<<10)
	buffer[0] = 1
	runtime.KeepAlive(buffer)
	finish()

	snapshot := metrics.Snapshot()
	if !snapshot.AllocationMeasurementOn {
		t.Fatal("allocation measurement should be enabled")
	}
	allocated, ok := snapshot.PhaseAllocBytes["project_worker_payload"]
	if !ok || allocated == 0 {
		t.Fatalf("allocation observation = %d, want positive measured delta", allocated)
	}
}

func TestEnsureEnqueueMetricsReusesContextAccumulator(t *testing.T) {
	ctx, first := WithEnqueueMetrics(context.Background(), false)
	ctx, second := EnsureEnqueueMetrics(ctx)
	if first != second {
		t.Fatal("EnsureEnqueueMetrics replaced an existing accumulator")
	}
	if ctx == nil {
		t.Fatal("EnsureEnqueueMetrics returned nil context")
	}
}
