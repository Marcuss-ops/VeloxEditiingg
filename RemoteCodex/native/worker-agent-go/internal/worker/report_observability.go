package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"velox-worker-agent/internal/taskrunner"
)

// attachWorkerIdentityAndTimings adds the operator-facing identity and a
// complete timing ledger to the report metrics. PhaseMarkers and native
// DetailedPhases remain the authoritative event stream; this summary makes
// the common dashboard queries cheap and keeps every operation visible.
func attachWorkerIdentityAndTimings(w *Worker, report *taskrunner.TaskExecutionReport) {
	if w == nil || report == nil {
		return
	}
	// Worker identity and the compact timing ledger are still emitted for
	// compatibility with existing dashboards. Keep this entire surface at
	// the explicit legacy projection boundary; RawMetrics remains canonical
	// for raw facts.
	legacy := report.LegacyMetrics()
	hostname, _ := os.Hostname()
	display := workerDisplayName(hostname)
	workerIP := "unknown-ip"
	if parts := strings.Split(display, "_"); len(parts) >= 3 {
		workerIP = strings.Join(parts[2:], "_")
	}
	legacy["worker_identity"] = map[string]interface{}{
		"worker_id": w.config.WorkerID, "worker_name": display,
		"hostname": hostname, "ip": workerIP,
	}

	timings := map[string]float64{
		"queue_wait_ms": 0, "claim_ms": 0, "worker_start_delay_ms": 0,
		"asset_resolution_ms": 0, "cache_lookup_ms": 0, "asset_download_ms": 0,
		"probe_ms": 0, "compile_ms": 0, "render_ms": 0, "segment_encode_ms": 0,
		"concat_ms": 0, "audio_download_ms": 0, "audio_mix_ms": 0,
		"compile_plan_ms": 0, "audio_timeline_compile_ms": 0,
		"audio_prepare_ms":    0,
		"audio_mix_encode_ms": 0, "audio_mux_ms": 0, "final_mux_ms": 0,
		"aac_encode_ms": 0, "artifact_finalize_ms": 0, "artifact_sha_ms": 0,
		"total_artifact_ms": 0,
		"final_copy_ms":     0, "audio_total_ms": 0,
		"verification_ms": 0, "artifact_upload_ms": 0,
		"report_ms": 0, "total_ms": float64(report.CompletedAt.Sub(report.StartedAt).Milliseconds()),
	}
	for _, marker := range report.PhaseMarkers {
		if marker.Status == "deferred" {
			continue
		}
		ms := float64(marker.CompletedAt.Sub(marker.StartedAt).Milliseconds())
		switch marker.Name {
		case taskrunner.PhaseCacheLookup:
			timings["cache_lookup_ms"] += ms
		case taskrunner.PhasePrefetch:
			timings["asset_resolution_ms"] += ms
		case taskrunner.PhaseExecute:
			timings["render_ms"] += ms
		case taskrunner.PhaseUpload:
			timings["artifact_upload_ms"] += ms
		case taskrunner.PhaseReport:
			timings["report_ms"] += ms
		}
	}
	for _, phase := range report.DetailedPhases {
		ms := float64(phase.DurationMS)
		key := strings.ToLower(phase.Component + "." + phase.Action)
		phaseName := strings.ToLower(phase.Phase)
		switch {
		case phaseName == "audio":
			timings["audio_total_ms"] += ms
		case strings.Contains(key, "asset") && strings.Contains(key, "download"):
			timings["asset_download_ms"] += ms
		case strings.Contains(key, "audio") && strings.Contains(key, "download"):
			timings["audio_download_ms"] += ms
		case strings.Contains(key, "audio") && strings.Contains(key, "encode"):
			// The multi-track ffmpeg command performs filtering and AAC
			// encoding together; this is deliberately one combined bucket.
			timings["audio_mix_encode_ms"] += ms
		case strings.Contains(key, "mux"):
			timings["audio_mux_ms"] += ms
			timings["final_mux_ms"] += ms
		case strings.Contains(key, "audio") && strings.Contains(key, "mix"):
			timings["audio_mix_ms"] += ms
		case strings.Contains(key, "concat"):
			timings["concat_ms"] += ms
		case strings.Contains(key, "encode"):
			timings["segment_encode_ms"] += ms
		case strings.Contains(key, "probe"):
			timings["probe_ms"] += ms
		case strings.Contains(key, "compile"):
			timings["compile_ms"] += ms
		}
	}
	// Native phase_ms counters are the authoritative engine timers for the
	// actual ffmpeg/file operations. The detailed phase stream carries the
	// same boundaries to the Master; these values make the worker report's
	// compact timing ledger useful to local diagnostics as well.
	if value, ok := legacy["engine.audio_download_ms"]; ok {
		timings["audio_download_ms"] = metricFloat(value)
	}
	if value, ok := legacy["engine.audio_prepare_ms"]; ok {
		timings["audio_prepare_ms"] = metricFloat(value)
	}
	if value, ok := legacy["engine.mix_audio_ms"]; ok {
		timings["audio_mix_ms"] = metricFloat(value)
		// The multi-track command performs filtering and AAC encoding in one
		// ffmpeg invocation, so expose the combined cost honestly instead of
		// fabricating an independent encode duration.
		timings["audio_mix_encode_ms"] = metricFloat(value)
	}
	if value, ok := legacy["engine.mux_audio_ms"]; ok {
		timings["audio_mux_ms"] = metricFloat(value)
		timings["final_mux_ms"] = metricFloat(value)
	}
	if value, ok := legacy["engine.copy_final_ms"]; ok {
		timings["final_copy_ms"] = metricFloat(value)
	}
	// render_profile is the executor's canonical low-cardinality timing
	// surface. Copy only fields that are actually present: zero remains a
	// truthful "not measured", not a fabricated phase duration.
	for _, name := range []string{
		"compile_plan_ms", "asset_resolution_ms", "audio_timeline_compile_ms",
		"audio_prepare_ms", "audio_mix_ms", "aac_encode_ms", "mux_ms",
		"artifact_finalize_ms", "artifact_sha_ms", "artifact_total_ms",
	} {
		if value, ok := legacy["render_profile."+name]; ok {
			timings[name] = metricFloat(value)
		}
	}
	if records, ok := legacy["asset_operations"].([]AssetOperationRecord); ok {
		for _, record := range records {
			timings["asset_download_ms"] += float64(record.DownloadMS)
		}
	}
	legacy["timings_ms"] = timings
	if encoded, err := json.Marshal(map[string]interface{}{"worker": display, "timings_ms": timings}); err == nil {
		for i := range report.PhaseMarkers {
			if report.PhaseMarkers[i].Name == taskrunner.PhaseReport {
				report.PhaseMarkers[i].Notes = fmt.Sprintf("worker_observability=%s", encoded)
			}
		}
	}
}

func metricFloat(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	default:
		return 0
	}
}
