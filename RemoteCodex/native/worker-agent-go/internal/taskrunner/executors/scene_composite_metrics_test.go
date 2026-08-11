package executors

import (
	"testing"
	"time"

	"velox-worker-agent/pkg/video/pipeline"
)

func TestProjectRenderProfileUsesCanonicalExistingTimers(t *testing.T) {
	metrics := make(map[string]interface{})
	started := time.Now().Add(-10 * time.Millisecond)
	projectRenderProfile(metrics, pipeline.RunMetrics{
		CompileMs: 2400,
		RenderMs:  8400,
		RenderMetrics: pipeline.RenderMetrics{
			TotalMs: 7900,
			PhaseMS: map[string]float64{
				"asset_download_ms": 120,
				"audio_download_ms": 300,
				"audio_prepare_ms":  180,
				"mix_audio_ms":      5500,
				"mux_audio_ms":      700,
			},
		},
	}, started, 800)

	want := map[string]interface{}{
		"render_profile.compile_plan_ms":     int64(2400),
		"render_profile.render_ms":           int64(8400),
		"render_profile.native_total_ms":     int64(7900),
		"render_profile.artifact_sha_ms":     int64(800),
		"render_profile.asset_download_ms":   float64(120),
		"render_profile.audio_download_ms":   float64(300),
		"render_profile.audio_prepare_ms":    float64(180),
		"render_profile.audio_mix_encode_ms": float64(5500),
		"render_profile.mux_ms":              float64(700),
	}
	for key, expected := range want {
		if got := metrics[key]; got != expected {
			t.Errorf("%s = %v (%T), want %v (%T)", key, got, got, expected, expected)
		}
	}
	if total, ok := metrics["render_profile.artifact_total_ms"].(int64); !ok || total < 0 {
		t.Fatalf("artifact total = %#v, want non-negative int64", metrics["render_profile.artifact_total_ms"])
	}
}
