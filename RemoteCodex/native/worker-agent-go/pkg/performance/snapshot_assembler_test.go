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

func TestAssembleFromSnapshot_CoverageDistinguishesUnavailableFromZero(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Resources: attempttelemetry.RawExecutionMetrics{
			TelemetryCoverageJSON: `{"cpu":true,"memory":false,"disk":true,"network":false,"cgroup":true,"process_tree":true}`,
			TelemetryCPUSource:    "cgroup_v2",
			TelemetryComplete:     false,
		},
	}
	receipt := AssembleFromSnapshot(snapshot)
	if receipt.Coverage == nil {
		t.Fatal("coverage projection is nil")
	}
	if !receipt.Coverage.CPU || receipt.Coverage.Memory || !receipt.Coverage.Disk || receipt.Coverage.Network {
		t.Fatalf("coverage = %+v", *receipt.Coverage)
	}
	if receipt.Memory.PeakRSSBytes != 0 || receipt.IO.TotalBytesRead != 0 {
		t.Fatalf("unavailable resource facts must remain zero: memory=%+v io=%+v", receipt.Memory, receipt.IO)
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

// TestSnapshotAssembler_ExclusivePhasesAccountWall pins the
// unaccounted_ms < 5% target for a realistic per-job journal: the C++
// engine.render span (catalog exclusive), the runner's compile/finalize
// spans (phase-taxonomy exclusive) sum to >= 95% of the attempt wall,
// while span children (engine.video.decode) and unclassified rows are
// never summed. This is the mechanism that makes the copy-only receipt
// explain the wall clock instead of reporting unaccounted == wall.
func TestSnapshotAssembler_ExclusivePhasesAccountWall(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Identity: attempttelemetry.AttemptIdentity{JobID: "job-1", AttemptID: "attempt-1"},
		WallMs:   2000,
		Events: []attempttelemetry.RecordedPhase{
			// engine.render — attempt-scoped catalog event, exclusive:
			// the whole native render span.
			{Origin: attempttelemetry.OriginEngine, Scope: attempttelemetry.ScopeAttempt,
				Component: "engine", Action: "render", Phase: "render", DurationMS: 1800,
				Status: attempttelemetry.StatusOK},
			// runner.compile — phase-taxonomy exclusive (attempt scope).
			{Origin: attempttelemetry.OriginWorker, Scope: attempttelemetry.ScopeAttempt,
				Component: "runner", Action: "compile", Phase: "compile", DurationMS: 120,
				Status: attempttelemetry.StatusOK},
			// runner.finalize — phase-taxonomy exclusive (attempt scope).
			{Origin: attempttelemetry.OriginWorker, Scope: attempttelemetry.ScopeAttempt,
				Component: "runner", Action: "finalize", Phase: "finalize", DurationMS: 30,
				Status: attempttelemetry.StatusOK},
			// engine.video.decode — span child, MUST NOT be summed
			// (parallel instances would double-count against the wall).
			{Origin: attempttelemetry.OriginEngine, Scope: attempttelemetry.ScopeSegment,
				Component: "engine.video", Action: "decode", Phase: "decode", DurationMS: 700,
				Status: attempttelemetry.StatusOK},
			// Unclassified row (phase "validate" is not in the taxonomy
			// and runner.validate is not a catalog event) — quarantined
			// from accounted_ratio by design.
			{Origin: attempttelemetry.OriginWorker, Scope: attempttelemetry.ScopeAttempt,
				Component: "runner", Action: "validate", Phase: "validate", DurationMS: 50,
				Status: attempttelemetry.StatusOK},
		},
	}

	receipt := AssembleFromSnapshot(snapshot)
	if len(receipt.Phases) != 5 {
		t.Fatalf("phases = %d, want 5", len(receipt.Phases))
	}
	// The exclusive sum is 1800+120+30 = 1950 of a 2000 ms wall.
	if receipt.Derived.AccountedRatio <= 0.95 {
		t.Fatalf("accounted_ratio = %v, want > 0.95 (unaccounted_ms=%d)",
			receipt.Derived.AccountedRatio, receipt.Derived.UnaccountedMS)
	}
	if receipt.Derived.UnaccountedMS > 100 { // 5%% of 2000
		t.Fatalf("unaccounted_ms = %d, want <= 100 (< 5%% of wall)", receipt.Derived.UnaccountedMS)
	}
}

// TestSnapshotAssembler_ProjectsEngineUsageFacts pins the engine-declared
// process facts (sidecar process_counters -> worker.engine.usage ->
// ProcessCollector) into the receipt's process / cpu / scheduling
// sections, keeping the copy-only zero-spawn invariant visible per job.
func TestSnapshotAssembler_ProjectsEngineUsageFacts(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		WallMs: 5000,
		Process: attempttelemetry.ProcessFacts{
			EngineSpawnCount:                 1,
			EngineExternalSpawnCount:         2,
			EngineFfmpegSpawnCount:           1,
			EngineFfprobeSpawnCount:          1,
			EngineCPUUserMs:                  1420,
			EngineCPUSystemMs:                310,
			EngineVoluntaryContextSwitches:   841,
			EngineInvoluntaryContextSwitches: 23,
			EngineMinorPageFaults:            4021,
			EngineMajorPageFaults:            0,
		},
	}

	receipt := AssembleFromSnapshot(snapshot)
	if receipt.Process.EngineSpawnCount != 1 || receipt.Process.EngineExternalSpawnCount != 2 ||
		receipt.Process.EngineFfmpegSpawnCount != 1 || receipt.Process.EngineFfprobeSpawnCount != 1 {
		t.Fatalf("process section = %+v", receipt.Process)
	}
	if receipt.CPU.EngineCPUUserMs != 1420 || receipt.CPU.EngineCPUSystemMs != 310 {
		t.Fatalf("cpu engine facts = %+v", receipt.CPU)
	}
	if receipt.Scheduling.VoluntaryContextSwitches != 841 ||
		receipt.Scheduling.InvoluntaryContextSwitches != 23 ||
		receipt.Scheduling.MinorPageFaults != 4021 ||
		receipt.Scheduling.MajorPageFaults != 0 {
		t.Fatalf("scheduling section = %+v", receipt.Scheduling)
	}
}
