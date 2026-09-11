package worker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

func TestAttachWorkerIdentityAndTimingsSplitsAudioWithoutFabrication(t *testing.T) {
	start := time.Unix(100, 0)
	report := &taskrunner.TaskExecutionReport{
		StartedAt:   start,
		CompletedAt: start.Add(2 * time.Second),
		RawMetrics: &telemetry.RawExecutionMetrics{
			AudioPrepareMs: 180, AudioTimelineBuildMs: 300,
			Sha256Ms: 800, OutputFinalizeMs: 120,
		},
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{Component: "engine.audio", Action: "download", Phase: "audio_download", DurationMS: 120},
			{Component: "engine.audio", Action: "mix", Phase: "audio", DurationMS: 1500},
			{Component: "engine.audio", Action: "encode", Phase: "audio_encode", DurationMS: 900},
			{Component: "engine.mux", Action: "audio", Phase: "encode", DurationMS: 300},
		},
	}
	attachWorkerIdentityAndTimings("worker-01", report)
	if len(report.PhaseMarkers) != 0 {
		t.Fatal("test report unexpectedly contains phase markers")
	}
	// The summary is deliberately kept only in the existing phase-note
	// channel; no report metrics map is required.
	report.PhaseMarkers = []taskrunner.PhaseMarker{{Name: taskrunner.PhaseReport}}
	attachWorkerIdentityAndTimings("worker-01", report)
	const prefix = "worker_observability="
	note := report.PhaseMarkers[0].Notes
	if !strings.HasPrefix(note, prefix) {
		t.Fatalf("phase note = %q, want worker observability prefix", note)
	}
	var envelope struct {
		Timings map[string]float64 `json:"timings_ms"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(note, prefix)), &envelope); err != nil {
		t.Fatalf("decode timing note: %v", err)
	}
	timings := envelope.Timings
	if timings["audio_total_ms"] != 1500 {
		t.Fatalf("audio_total_ms = %v, want 1500", timings["audio_total_ms"])
	}
	if timings["audio_mix_encode_ms"] != 900 {
		t.Fatalf("audio_mix_encode_ms = %v, want 900", timings["audio_mix_encode_ms"])
	}
	if timings["audio_mux_ms"] != 300 || timings["final_mux_ms"] != 300 {
		t.Fatalf("mux timings = %v/%v, want 300/300", timings["audio_mux_ms"], timings["final_mux_ms"])
	}
	if timings["audio_download_ms"] != 120 {
		t.Fatalf("audio_download_ms = %v, want 120", timings["audio_download_ms"])
	}
	if timings["audio_prepare_ms"] != 180 {
		t.Fatalf("audio_prepare_ms = %v, want 180", timings["audio_prepare_ms"])
	}
	if timings["audio_encode_ms"] != 0 {
		t.Fatalf("audio_encode_ms = %v, want zero because mix+AAC is one measured command", timings["audio_encode_ms"])
	}
	if timings["compile_plan_ms"] != 0 || timings["audio_timeline_compile_ms"] != 300 {
		t.Fatalf("render plan timings = %v/%v, want 0/300", timings["compile_plan_ms"], timings["audio_timeline_compile_ms"])
	}
	if timings["artifact_sha_ms"] != 800 || timings["artifact_finalize_ms"] != 120 {
		t.Fatalf("artifact timings = %v/%v, want 800/120", timings["artifact_sha_ms"], timings["artifact_finalize_ms"])
	}
}
