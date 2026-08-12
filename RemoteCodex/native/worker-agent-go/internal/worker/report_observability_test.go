package worker

import (
	"testing"
	"time"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/config"
)

func TestAttachWorkerIdentityAndTimingsSplitsAudioWithoutFabrication(t *testing.T) {
	start := time.Unix(100, 0)
	report := &taskrunner.TaskExecutionReport{
		StartedAt:   start,
		CompletedAt: start.Add(2 * time.Second),
		Metrics: map[string]interface{}{
			"engine.audio_download_ms":                 float64(120),
			"engine.audio_prepare_ms":                  float64(180),
			"render_profile.compile_plan_ms":           int64(2400),
			"render_profile.audio_timeline_compile_ms": int64(300),
			"render_profile.artifact_sha_ms":           int64(800),
			"render_profile.artifact_finalize_ms":      int64(120),
			"render_profile.artifact_total_ms":         int64(920),
		},
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{Component: "engine.audio", Action: "mix", Phase: "audio", DurationMS: 1500},
			{Component: "engine.audio", Action: "encode", Phase: "audio_encode", DurationMS: 900},
			{Component: "engine.mux", Action: "audio", Phase: "encode", DurationMS: 300},
		},
	}
	w := &Worker{config: &config.WorkerConfig{WorkerID: "worker-01"}}

	attachWorkerIdentityAndTimings(w, report)
	timings, ok := report.Metrics["timings_ms"].(map[string]float64)
	if !ok {
		t.Fatalf("timings_ms = %#v, want canonical timing map", report.Metrics["timings_ms"])
	}
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
	if timings["compile_plan_ms"] != 2400 || timings["audio_timeline_compile_ms"] != 300 {
		t.Fatalf("render plan timings = %v/%v, want 2400/300", timings["compile_plan_ms"], timings["audio_timeline_compile_ms"])
	}
	if timings["artifact_sha_ms"] != 800 || timings["artifact_finalize_ms"] != 120 || timings["artifact_total_ms"] != 920 {
		t.Fatalf("artifact timings = %v/%v/%v, want 800/120/920", timings["artifact_sha_ms"], timings["artifact_finalize_ms"], timings["artifact_total_ms"])
	}
}
