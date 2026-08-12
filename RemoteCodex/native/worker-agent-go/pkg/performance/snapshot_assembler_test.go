package performance

import (
	"strings"
	"testing"
	"time"

	attempttelemetry "velox-worker-agent/internal/telemetry"
)

func TestAssembleFromSnapshot_MapsCanonicalFacts(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Identity: attempttelemetry.AttemptIdentity{JobID: "job-1", AttemptID: "attempt-1", WorkerID: "worker-1"},
		Resources: attempttelemetry.TypedExecutionMetrics{
			CpuTimeMs:        2000,
			PeakRssBytes:     4096,
			DiskReadBytes:    1000,
			DiskWriteBytes:   500,
			WallClockSeconds: 10,
		},
		Process: attempttelemetry.ProcessFacts{EngineSpawnCount: 1},
		Media:   attempttelemetry.MediaFacts{BytesOut: 42, FramesOut: 500},
		WallMs:  10000,
	}
	receipt := AssembleFromSnapshot(snapshot)

	if receipt.Identity.JobID != "job-1" || receipt.Identity.AttemptID != "attempt-1" {
		t.Fatalf("identity = %+v", receipt.Identity)
	}
	if receipt.Timing.WallMs != 10000 {
		t.Fatalf("wall_ms = %d, want 10000", receipt.Timing.WallMs)
	}
	if receipt.CPU.CPUTotalMs != 2000 || receipt.CPU.CPUWallRatio != 0.2 {
		t.Fatalf("cpu = %+v, want total:2000 ratio:0.2", receipt.CPU)
	}
	if receipt.IO.TotalBytesRead != 1000 || receipt.IO.TotalBytesWritten != 500 {
		t.Fatalf("io = %+v", receipt.IO)
	}
	if receipt.Memory.PeakRSSBytes != 4096 {
		t.Fatalf("memory = %+v", receipt.Memory)
	}
	if receipt.Process.EngineSpawnCount != 1 {
		t.Fatalf("process = %+v", receipt.Process)
	}
	if receipt.Media.Frames != 500 || receipt.Media.OutputBytes != 42 {
		t.Fatalf("media = %+v", receipt.Media)
	}
	// Derived KPIs come from the single Deriver: cpu_wall_ratio = 2000/10000.
	if receipt.Derived.CPUWallRatio != 0.2 {
		t.Fatalf("derived cpu_wall_ratio = %v, want 0.2", receipt.Derived.CPUWallRatio)
	}
}

func TestAssembleFromSnapshot_PhasesClassifiedFromCatalog(t *testing.T) {
	now := time.Now().UTC()
	snapshot := &attempttelemetry.AttemptSnapshot{
		WallMs: 1000,
		Events: []attempttelemetry.RecordedPhase{
			{Component: "engine", Action: "render", Phase: "render", StartedAt: now, CompletedAt: now, DurationMS: 900, Status: attempttelemetry.StatusOK},
			{Component: "engine.video", Action: "decode", Phase: "decode", StartedAt: now, CompletedAt: now, DurationMS: 700, Status: attempttelemetry.StatusOK},
			// Unregistered event: must be quarantined (empty TimingMode).
			{Component: "mystery", Action: "thing", StartedAt: now, CompletedAt: now, DurationMS: 100, Status: attempttelemetry.StatusOK},
		},
	}
	receipt := AssembleFromSnapshot(snapshot)
	if len(receipt.Phases) != 3 {
		t.Fatalf("phases=%d, want 3", len(receipt.Phases))
	}
	var exclusiveMS, childMS, unclassified int64
	for _, phase := range receipt.Phases {
		switch string(phase.TimingMode) {
		case "exclusive":
			exclusiveMS += phase.DurationMS
		case "span_child":
			childMS += phase.DurationMS
		default:
			unclassified += phase.DurationMS
		}
	}
	if exclusiveMS != 900 {
		t.Fatalf("exclusive sum=%d, want 900 (only the top-level render phase)", exclusiveMS)
	}
	if childMS != 700 || unclassified != 100 {
		t.Fatalf("child=%d unclassified=%d, want 700/100 (never summed into accounted)", childMS, unclassified)
	}
	// accounted_ratio = 900/1000 — span children never double-count.
	if receipt.Derived.AccountedRatio != 0.9 {
		t.Fatalf("accounted_ratio=%v, want 0.9", receipt.Derived.AccountedRatio)
	}
}

func TestAssembleFromSnapshot_NilIsZeroReceipt(t *testing.T) {
	receipt := AssembleFromSnapshot(nil)
	if receipt == nil || receipt.Version != PerformanceReceiptVersionV1 {
		t.Fatalf("nil snapshot must yield a stamped empty receipt, got %+v", receipt)
	}
}

func TestAssembleFromSnapshot_JSONRoundTrips(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Identity: attempttelemetry.AttemptIdentity{AttemptID: "rt"},
		WallMs:   500,
		Resources: attempttelemetry.TypedExecutionMetrics{
			CpuTimeMs:     250,
			DiskReadBytes: 10,
		},
	}
	data, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	if !strings.Contains(string(data), `"cpu_wall_ratio"`) {
		t.Fatalf("derived section missing: %s", data)
	}
}
