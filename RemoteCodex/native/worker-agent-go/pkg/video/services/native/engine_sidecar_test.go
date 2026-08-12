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
		"observability": {
			"audio": {"events": 2, "wall_ms": 12.5, "bytes_in": 100},
			"subtitle": {"events": 1, "wall_ms": 3.5},
			"io": {"events": 4, "bytes_in": 4096, "bytes_out": 2048},
			"quality": {"events": 3, "wall_ms": 7},
			"retry": {"count": 2},
			"waste": {"wasted_cpu_ms": 88, "wasted_download_bytes": 512, "completed_segments": 2, "error_component": "engine", "error_phase": "encode"}
		},
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
		"io_counters": {
			"file_copy_count": 3,
			"file_copy_bytes": 4194304,
			"asset_bytes_copied": 4194304,
			"input_open_count": 5,
			"input_reopen_count": 2
		},
		"process_counters": {
			"external_spawn_count": 2,
			"ffmpeg_spawn_count": 1,
			"ffprobe_spawn_count": 1,
			"shell_spawn_count": 0,
			"curl_spawn_count": 0,
			"cpu_user_ms": 1420,
			"cpu_system_ms": 310,
			"voluntary_context_switches": 841,
			"involuntary_context_switches": 23,
			"minor_page_faults": 4021,
			"major_page_faults": 0
		},
		"frame_pipeline": {
			"producer_busy_ms": 2500,
			"producer_wait_ms": 700,
			"consumer_busy_ms": 2800,
			"consumer_wait_ms": 400,
			"queue_depth_avg": 3,
			"queue_depth_max": 7,
			"queue_empty_ms": 400,
			"queue_full_ms": 150,
			"producer_stall_ratio": 0.22,
			"encoder_starvation_ratio": 0.125,
			"backpressure_ratio": 0.05
		},
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
	if sc.Observability["audio"].(map[string]interface{})["events"] != float64(2) {
		t.Fatalf("audio observability not parsed: %#v", sc.Observability)
	}
	waste := sc.Observability["waste"].(map[string]interface{})
	if waste["wasted_cpu_ms"] != float64(88) || waste["completed_segments"] != float64(2) {
		t.Fatalf("waste observability not parsed: %#v", waste)
	}
	if len(sc.Segments) != 1 || sc.Segments[0].FinishedOffsetMs != 53.0 {
		t.Fatalf("segment timing/parallelism not preserved: %#v", sc.Segments)
	}
	if sc.IOCounters == nil {
		t.Fatalf("io_counters not parsed")
	}
	if sc.IOCounters.FileCopyCount != 3 || sc.IOCounters.FileCopyBytes != 4194304 ||
		sc.IOCounters.AssetBytesCopied != 4194304 || sc.IOCounters.InputOpenCount != 5 ||
		sc.IOCounters.InputReopenCount != 2 {
		t.Fatalf("io_counters = %+v", sc.IOCounters)
	}
	if sc.FramePipeline == nil {
		t.Fatalf("frame_pipeline not parsed")
	}
	fp := sc.FramePipeline
	if fp.ProducerBusyMS != 2500 || fp.ProducerWaitMS != 700 ||
		fp.ConsumerBusyMS != 2800 || fp.ConsumerWaitMS != 400 {
		t.Fatalf("frame_pipeline busy/wait = %+v", fp)
	}
	if fp.QueueDepthAvg != 3 || fp.QueueDepthMax != 7 ||
		fp.QueueEmptyMS != 400 || fp.QueueFullMS != 150 {
		t.Fatalf("frame_pipeline queue = %+v", fp)
	}
	if fp.ProducerStallRatio != 0.22 || fp.EncoderStarvationRatio != 0.125 || fp.BackpressureRatio != 0.05 {
		t.Fatalf("frame_pipeline ratios = %+v", fp)
	}
	if sc.ProcessCounters == nil {
		t.Fatalf("process_counters not parsed")
	}
	pc := sc.ProcessCounters
	if pc.ExternalSpawnCount != 2 || pc.FfmpegSpawnCount != 1 || pc.FfprobeSpawnCount != 1 ||
		pc.ShellSpawnCount != 0 || pc.CurlSpawnCount != 0 {
		t.Fatalf("process_counters spawn ledger = %+v", pc)
	}
	if pc.CPUUserMs != 1420 || pc.CPUSystemMs != 310 ||
		pc.VoluntaryContextSwitches != 841 || pc.InvoluntaryContextSwitches != 23 ||
		pc.MinorPageFaults != 4021 || pc.MajorPageFaults != 0 {
		t.Fatalf("process_counters usage = %+v", pc)
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

func TestMapEngineSidecar_MapsProcessCounters(t *testing.T) {
	sc := engineSidecar{
		ProcessCounters: &engineProcessCounters{
			ExternalSpawnCount:         2,
			FfmpegSpawnCount:           1,
			FfprobeSpawnCount:          1,
			CPUUserMs:                  1420,
			CPUSystemMs:                310,
			VoluntaryContextSwitches:   841,
			InvoluntaryContextSwitches: 23,
			MinorPageFaults:            4021,
		},
	}
	mapped := pipeline.RenderMetrics{}
	mapEngineSidecar(&sc, &mapped)
	if mapped.EngineExternalSpawnCount != 2 || mapped.EngineFfmpegSpawnCount != 1 || mapped.EngineFfprobeSpawnCount != 1 {
		t.Fatalf("mapped engine spawn ledger = %+v", mapped)
	}
	if mapped.EngineCPUUserMs != 1420 || mapped.EngineCPUSystemMs != 310 ||
		mapped.EngineVoluntaryContextSwitches != 841 || mapped.EngineInvoluntaryContextSwitches != 23 ||
		mapped.EngineMinorPageFaults != 4021 || mapped.EngineMajorPageFaults != 0 {
		t.Fatalf("mapped engine usage = %+v", mapped)
	}
}

func TestMapEngineSidecar_PreservesSegmentsAndMapsPhases(t *testing.T) {
	sc := engineSidecar{
		PhaseMS: map[string]float64{"encode": 5},
		Observability: map[string]interface{}{
			"audio":   map[string]interface{}{"events": float64(2)},
			"quality": map[string]interface{}{"events": float64(3)},
			"waste":   map[string]interface{}{"wasted_cpu_ms": float64(88), "wasted_download_bytes": float64(512), "completed_segments": float64(2), "error_component": "engine", "error_phase": "encode"},
		},
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
	if mapped.Observability["quality"].(map[string]interface{})["events"] != float64(3) {
		t.Fatalf("mapped category observability = %#v", mapped.Observability)
	}
}

func TestMapEngineSidecar_MapsFramePipeline(t *testing.T) {
	sc := engineSidecar{
		FramePipeline: &engineFramePipeline{
			ProducerBusyMS:         2500,
			ProducerWaitMS:         700,
			ConsumerBusyMS:         2800,
			ConsumerWaitMS:         400,
			QueueDepthAvg:          3,
			QueueDepthMax:          7,
			QueueEmptyMS:           400,
			QueueFullMS:            150,
			ProducerStallRatio:     0.22,
			EncoderStarvationRatio: 0.125,
			BackpressureRatio:      0.05,
		},
	}
	mapped := pipeline.RenderMetrics{}
	mapEngineSidecar(&sc, &mapped)
	if mapped.FramePipeline.ProducerBusyMS != 2500 || mapped.FramePipeline.ProducerWaitMS != 700 ||
		mapped.FramePipeline.ConsumerBusyMS != 2800 || mapped.FramePipeline.ConsumerWaitMS != 400 {
		t.Fatalf("mapped frame pipeline busy/wait = %+v", mapped.FramePipeline)
	}
	if mapped.FramePipeline.QueueDepthAvg != 3 || mapped.FramePipeline.QueueDepthMax != 7 ||
		mapped.FramePipeline.QueueEmptyMS != 400 || mapped.FramePipeline.QueueFullMS != 150 {
		t.Fatalf("mapped frame pipeline queue = %+v", mapped.FramePipeline)
	}
	if mapped.FramePipeline.ProducerStallRatio != 0.22 ||
		mapped.FramePipeline.EncoderStarvationRatio != 0.125 ||
		mapped.FramePipeline.BackpressureRatio != 0.05 {
		t.Fatalf("mapped frame pipeline ratios = %+v", mapped.FramePipeline)
	}

	// Nil block must leave the metrics zero (legacy engines / non-encode
	// paths).
	legacy := pipeline.RenderMetrics{}
	mapEngineSidecar(&engineSidecar{}, &legacy)
	if legacy.FramePipeline.ProducerBusyMS != 0 || legacy.FramePipeline.QueueDepthMax != 0 {
		t.Fatalf("legacy sidecar must not populate frame pipeline metrics: %+v", legacy.FramePipeline)
	}
}

func TestMapEngineSidecar_MapsIOCounters(t *testing.T) {
	sc := engineSidecar{
		IOCounters: &engineIOCounters{
			FileCopyCount:    2,
			FileCopyBytes:    1024,
			AssetBytesCopied: 512,
			InputOpenCount:   6,
			InputReopenCount: 1,
		},
	}
	mapped := pipeline.RenderMetrics{}
	mapEngineSidecar(&sc, &mapped)
	if mapped.FileCopyCount != 2 || mapped.FileCopyBytes != 1024 || mapped.AssetBytesCopied != 512 ||
		mapped.InputOpenCount != 6 || mapped.InputReopenCount != 1 {
		t.Fatalf("mapped io counters = %+v", mapped)
	}

	// Nil block must leave the metrics fields zero (legacy engines).
	legacy := pipeline.RenderMetrics{}
	mapEngineSidecar(&engineSidecar{}, &legacy)
	if legacy.FileCopyCount != 0 || legacy.InputOpenCount != 0 {
		t.Fatalf("legacy sidecar must not populate io counters: %+v", legacy)
	}
}
