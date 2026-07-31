package native

import (
	"os"
	"path/filepath"
	"testing"

	"velox-worker-agent/pkg/video/pipeline"
)

func writeSidecarFixture(t *testing.T, payload string) string {
	t.Helper()
	dir := t.TempDir()
	output := filepath.Join(dir, "render.mp4")
	if err := os.WriteFile(output+".progress.json", []byte(payload), 0o600); err != nil {
		t.Fatalf("write sidecar fixture: %v", err)
	}
	return output
}

func TestReadEngineSidecar_LegacyPreservesExistingTelemetry(t *testing.T) {
	output := writeSidecarFixture(t, `{
		"frames": 120,
		"phase_ms": {"concat_ms": 12.5},
		"segments": [{
			"index": 2,
			"worker_index": 1,
			"source_type": "video",
			"total_ms": 44.25,
			"started_offset_ms": 10.5,
			"finished_offset_ms": 54.75,
			"worker_slot": 3,
			"cpu_threads": 8,
			"parallel_group": "scene-batch-1"
		}]
	}`)

	sc, err := readEngineSidecar(output)
	if err != nil {
		t.Fatalf("read legacy sidecar: %v", err)
	}
	if got := sc.PhaseMS["concat_ms"]; got != 12.5 {
		t.Fatalf("phase_ms concat_ms = %v, want 12.5", got)
	}
	if len(sc.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(sc.Segments))
	}
	seg := sc.Segments[0]
	if seg.StartedOffsetMs != 10.5 || seg.FinishedOffsetMs != 54.75 {
		t.Fatalf("parallel offsets = (%v,%v), want (10.5,54.75)", seg.StartedOffsetMs, seg.FinishedOffsetMs)
	}
	if seg.WorkerSlot != 3 || seg.CpuThreads != 8 || seg.ParallelGroup != "scene-batch-1" {
		t.Fatalf("parallel fields lost: %+v", seg)
	}
	if sc.Phases != nil {
		t.Fatalf("legacy phases = %#v, want nil", sc.Phases)
	}
}

func TestReadEngineSidecar_FullSchemaMapsAllTelemetry(t *testing.T) {
	output := writeSidecarFixture(t, `{
		"frames": 300,
		"phase_ms": {"decode": 14.25, "encode": 38.5},
		"segments": [{
			"index": 0,
			"worker_index": 0,
			"source_type": "image",
			"total_ms": 52.75,
			"started_offset_ms": 0.25,
			"finished_offset_ms": 53.0,
			"worker_slot": 0,
			"cpu_threads": 4,
			"parallel_group": "scene-0"
		}],
		"phases": [{
			"origin": "engine",
			"scope": "segment",
			"component": "engine.encode",
			"action": "frame_submit",
			"phase": "encode",
			"event_type": "completed",
			"event_name": "segment-0",
			"event_index": 7,
			"started_at": "2026-07-31T12:00:00.000Z",
			"completed_at": "2026-07-31T12:00:00.038Z",
			"duration_ms": 38,
			"status": "ok",
			"bytes_in": 1000,
			"bytes_out": 2000,
			"frames": 30,
			"metadata": {"codec":"h264"},
			"segment_index": 0,
			"started_offset_ms": 0.25,
			"finished_offset_ms": 38.25,
			"cpu_ms": 42.5,
			"queue_wait_ms": 1.75,
			"frames_in": 31,
			"frames_out": 30
		}]
	}`)

	sc, err := readEngineSidecar(output)
	if err != nil {
		t.Fatalf("read full sidecar: %v", err)
	}
	if len(sc.PhaseMS) != 2 || sc.PhaseMS["decode"] != 14.25 || sc.PhaseMS["encode"] != 38.5 {
		t.Fatalf("phase_ms not preserved: %#v", sc.PhaseMS)
	}
	if len(sc.Segments) != 1 || sc.Segments[0].FinishedOffsetMs != 53.0 {
		t.Fatalf("segment timing/parallelism not preserved: %#v", sc.Segments)
	}
	if len(sc.Phases) != 1 {
		t.Fatalf("phases = %d, want 1", len(sc.Phases))
	}
	phase := sc.Phases[0]
	if phase.Origin != "engine" || phase.Scope != "segment" || phase.Component != "engine.encode" || phase.Action != "frame_submit" {
		t.Fatalf("phase identity = %+v", phase)
	}
	if phase.EventIndex != 7 || phase.DurationMS != 38 || phase.BytesIn != 1000 || phase.BytesOut != 2000 || phase.Frames != 30 {
		t.Fatalf("phase counters = %+v", phase)
	}
	if phase.MetadataJSON != "" || string(phase.Metadata) != `{"codec":"h264"}` {
		t.Fatalf("phase metadata = metadata_json:%q metadata:%s", phase.MetadataJSON, phase.Metadata)
	}
	if phase.CPUMS != 42.5 || phase.QueueWaitMS != 1.75 || phase.FramesIn != 31 || phase.FramesOut != 30 {
		t.Fatalf("phase resource/parallel fields = %+v", phase)
	}
}

func TestMapEngineSidecar_PreservesSegmentsAndMapsPhases(t *testing.T) {
	sc := engineSidecar{
		PhaseMS:  map[string]float64{"encode": 5},
		Segments: []segmentTiming{{Index: 1, StartedOffsetMs: 2.5, FinishedOffsetMs: 8.5, WorkerSlot: 2, CpuThreads: 6, ParallelGroup: "g"}},
		Phases:   []detailedPhaseTiming{{Origin: "engine", Scope: "segment", Component: "engine.video", Action: "decode", EventIndex: 3, DurationMS: 6, SegmentIndex: 1, StartedOffsetMS: 2.5, FinishedOffsetMS: 8.5}},
	}
	mapped := pipeline.RenderMetrics{}
	mapEngineSidecar(&sc, &mapped)
	if mapped.PhaseMS["encode"] != 5 {
		t.Fatalf("mapped phase_ms = %#v", mapped.PhaseMS)
	}
	if len(mapped.Segments) != 1 || mapped.Segments[0].StartedOffsetMS != 2.5 || mapped.Segments[0].WorkerSlot != 2 || mapped.Segments[0].ParallelGroup != "g" {
		t.Fatalf("mapped segment parallelism = %#v", mapped.Segments)
	}
	if len(mapped.DetailedPhases) != 1 || mapped.DetailedPhases[0].EventIndex != 3 || mapped.DetailedPhases[0].Component != "engine.video" {
		t.Fatalf("mapped detailed phases = %#v", mapped.DetailedPhases)
	}
}
