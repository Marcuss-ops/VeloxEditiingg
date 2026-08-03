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
	if report.Metrics == nil {
		report.Metrics = make(map[string]interface{})
	}
	hostname, _ := os.Hostname()
	display := workerDisplayName(hostname)
	workerIP := "unknown-ip"
	if parts := strings.Split(display, "_"); len(parts) >= 3 {
		workerIP = strings.Join(parts[2:], "_")
	}
	report.Metrics["worker_identity"] = map[string]interface{}{
		"worker_id": w.config.WorkerID, "worker_name": display,
		"hostname": hostname, "ip": workerIP,
	}

	timings := map[string]float64{
		"queue_wait_ms": 0, "claim_ms": 0, "worker_start_delay_ms": 0,
		"asset_resolution_ms": 0, "cache_lookup_ms": 0, "asset_download_ms": 0,
		"probe_ms": 0, "compile_ms": 0, "render_ms": 0, "segment_encode_ms": 0,
		"concat_ms": 0, "audio_download_ms": 0, "audio_mix_ms": 0,
		"audio_mux_ms": 0, "verification_ms": 0, "artifact_upload_ms": 0,
		"report_ms": 0, "total_ms": float64(report.CompletedAt.Sub(report.StartedAt).Milliseconds()),
	}
	for _, marker := range report.PhaseMarkers {
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
		switch {
		case strings.Contains(key, "asset") && strings.Contains(key, "download"):
			timings["asset_download_ms"] += ms
		case strings.Contains(key, "audio") && strings.Contains(key, "download"):
			timings["audio_download_ms"] += ms
		case strings.Contains(key, "audio") && strings.Contains(key, "mix"):
			timings["audio_mix_ms"] += ms
		case strings.Contains(key, "mux") || strings.Contains(key, "audio") && strings.Contains(key, "encode"):
			timings["audio_mux_ms"] += ms
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
	if records, ok := report.Metrics["asset_operations"].([]AssetOperationRecord); ok {
		for _, record := range records {
			timings["asset_download_ms"] += float64(record.DownloadMS)
		}
	}
	report.Metrics["timings_ms"] = timings
	if encoded, err := json.Marshal(map[string]interface{}{"worker": display, "timings_ms": timings}); err == nil {
		for i := range report.PhaseMarkers {
			if report.PhaseMarkers[i].Name == taskrunner.PhaseReport {
				report.PhaseMarkers[i].Notes = fmt.Sprintf("worker_observability=%s", encoded)
			}
		}
	}
}
