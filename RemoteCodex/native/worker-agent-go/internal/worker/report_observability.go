package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"velox-worker-agent/internal/taskrunner"
)

// attachWorkerIdentityAndTimings adds the operator-facing identity and a
// complete timing ledger to the report phase note. PhaseMarkers,
// DetailedPhases, and RawMetrics remain the authoritative typed sources; no
// legacy metrics projection is built.
func attachWorkerIdentityAndTimings(workerID string, report *taskrunner.TaskExecutionReport) {
	if report == nil {
		return
	}
	hostname, _ := os.Hostname()
	display := workerDisplayName(hostname)
	workerIP := "unknown-ip"
	if parts := strings.Split(display, "_"); len(parts) >= 3 {
		workerIP = strings.Join(parts[2:], "_")
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
	// RawMetrics is the authoritative engine/output timing source. The
	// detailed phase stream above remains the source for phase-specific
	// attribution; these typed fields fill the compact ledger where the
	// engine reports a direct aggregate.
	if raw := report.RawMetrics; raw != nil {
		if raw.AudioPrepareMs > 0 {
			timings["audio_prepare_ms"] = float64(raw.AudioPrepareMs)
		}
		if raw.AudioTimelineBuildMs > 0 {
			timings["audio_timeline_compile_ms"] = float64(raw.AudioTimelineBuildMs)
		}
		if raw.AudioEncodeMs > 0 {
			timings["audio_mix_encode_ms"] = float64(raw.AudioEncodeMs)
		}
		if raw.AudioMuxMs > 0 {
			timings["audio_mux_ms"] = float64(raw.AudioMuxMs)
			timings["final_mux_ms"] = float64(raw.AudioMuxMs)
		}
		if raw.AudioCopyMs > 0 {
			timings["final_copy_ms"] = float64(raw.AudioCopyMs)
		}
		if raw.Sha256Ms > 0 {
			timings["artifact_sha_ms"] = float64(raw.Sha256Ms)
		}
		if raw.FfprobeMs > 0 {
			timings["probe_ms"] = float64(raw.FfprobeMs)
		}
		if raw.OutputFinalizeMs > 0 {
			timings["artifact_finalize_ms"] = float64(raw.OutputFinalizeMs)
		}
	}
	for _, record := range report.AssetOperations {
		timings["asset_download_ms"] += float64(record.DownloadMS)
	}
	if encoded, err := json.Marshal(struct {
		WorkerID   string             `json:"worker_id"`
		WorkerName string             `json:"worker_name"`
		Hostname   string             `json:"hostname"`
		IP         string             `json:"ip"`
		Timings    map[string]float64 `json:"timings_ms"`
	}{workerID, display, hostname, workerIP, timings}); err == nil {
		for i := range report.PhaseMarkers {
			if report.PhaseMarkers[i].Name == taskrunner.PhaseReport {
				report.PhaseMarkers[i].Notes = fmt.Sprintf("worker_observability=%s", encoded)
			}
		}
	}
}
