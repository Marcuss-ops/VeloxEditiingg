package telemetry

import (
	"encoding/json"
	"sort"
	"strings"
)
// ComputeDerivedMetrics fills all derived ratio fields from the raw facts.
// Must be called AFTER MediaDurationSeconds and WallClockSeconds are set.
// Safe to call multiple times; always recomputes from current values.
func (t *RawExecutionMetrics) ComputeDerivedMetrics() {
	if t == nil {
		return
	}
	if t.MediaDurationSeconds > 0 && t.WallClockSeconds > 0 {
		t.RealTimeFactor = t.WallClockSeconds / t.MediaDurationSeconds
		t.ThroughputX = t.MediaDurationSeconds / t.WallClockSeconds
	}
}

// ComputeCriticalPath scans all fine-grained phase durations and identifies
// the single phase that dominates the job wall time — the critical path.
// Must be called AFTER PopulateFromJobPhaseTimer so all phase timings are
// populated. The result is written into CriticalPathComponent,
// CriticalPathMs, and CriticalPathPercent.
//
// Critical path semantics:
//   - In a sequential pipeline, the critical path is the single phase with
//     the highest duration.
//   - In a parallel pipeline, the critical path may span multiple phases
//     that execute on the critical chain. This method sums the durations
//     of the top-N phases until the total exceeds 50% of job wall time;
//     the "critical path component" becomes a concatenation of those
//     phases (e.g. "video.encode + video.composite").
//   - The percentage is computed against the job wall clock, not the sum
//     of all phase durations (which may exceed wall clock due to parallelism).
func (t *RawExecutionMetrics) ComputeCriticalPath() {
	if t == nil {
		return
	}
	// Collect all fine-grained phase durations into a sortable list.
	type namedDuration struct {
		name string
		ms   int64
	}
	phases := []namedDuration{
		{"queue_wait", t.QueueWaitMs},
		{"job_setup", t.JobSetupMs},
		{"asset.resolve", t.AssetResolveMs},
		{"asset.download", t.AssetDownloadMs},
		{"asset.verify", t.AssetVerifyMs},
		{"asset.materialize", t.AssetMaterializeMs},
		{"audio.prepare", t.AudioPrepareMs},
		{"audio.timeline_build", t.AudioTimelineBuildMs},
		{"render_plan_build", t.RenderPlanBuildMs},
		{"video.decode", t.VideoDecodeMs},
		{"video.subtitle", t.VideoSubtitleMs},
		{"video.subtitle_raster", t.VideoSubtitleRasterMs},
		{"video.subtitle_composite", t.VideoSubtitleCompositeMs},
		{"video.watermark", t.VideoWatermarkMs},
		{"video.watermark_upload", t.VideoWatermarkUploadMs},
		{"video.watermark_composite", t.VideoWatermarkCompositeMs},
		{"video.blur", t.VideoBlurMs},
		{"video.filter", t.VideoFilterMs},
		{"video.composite", t.VideoCompositeMs},
		{"video.encode", t.VideoEncodeMs},
		{"video.concat", t.VideoConcatMs},
		{"audio.mux", t.AudioMuxMs},
		{"output_finalize", t.OutputFinalizeMs},
		{"artifact.hash", t.Sha256Ms},
		{"artifact.probe", t.FfprobeMs},
		{"artifact.verify", t.ArtifactVerifyMs},
		{"drive.upload", t.DriveUploadMs},
		{"drive.verify", t.DriveVerifyMs},
		{"asset.download_drive", t.DriveDownloadMs},
		{"asset.download_blobstore", t.BlobstoreDownloadMs},
		{"asset.cache_read", t.LocalCacheReadMs},
		{"cleanup", t.CleanupMs},
	}

	// Sort descending by duration.
	sort.Slice(phases, func(i, j int) bool { return phases[i].ms > phases[j].ms })

	// Single-dominant-phase mode: the top phase IS the critical path.
	// We also accumulate top-N in case no single phase dominates.
	wallMs := int64(t.WallClockSeconds * 1000)
	if wallMs <= 0 {
		return
	}

	top := phases[0]
	if top.ms == 0 {
		return
	}

	// If the top phase alone is >20% of wall time, it's the single
	// dominant bottleneck.
	if float64(top.ms)/float64(wallMs) >= 0.20 {
		t.CriticalPathComponent = top.name
		t.CriticalPathMs = top.ms
		t.CriticalPathPercent = float64(top.ms) / float64(wallMs) * 100
		return
	}

	// Multi-phase critical path: accumulate top phases until >50% of wall.
	var cumulative int64
	var names []string
	for _, p := range phases {
		if p.ms == 0 {
			break
		}
		cumulative += p.ms
		names = append(names, p.name)
		if float64(cumulative)/float64(wallMs) > 0.50 || len(names) >= 3 {
			break
		}
	}
	t.CriticalPathComponent = strings.Join(names, " + ")
	t.CriticalPathMs = cumulative
	if wallMs > 0 {
		t.CriticalPathPercent = float64(cumulative) / float64(wallMs) * 100
	}
}

// ComputeDerivedBandwidth fills bandwidth metrics from byte counts and
// phase durations. Must be called AFTER PopulateFromJobPhaseTimer so
// DriveUploadMs and the disk timings are already populated.
func (t *RawExecutionMetrics) ComputeDerivedBandwidth() {
	if t == nil {
		return
	}
	// download_mbps_avg: total input bytes ÷ total download time.
	dlMs := t.AssetDownloadMs
	if dlMs == 0 {
		dlMs = t.DriveDownloadMs + t.BlobstoreDownloadMs
	}
	if dlMs > 0 && t.InputBytes > 0 {
		t.DownloadMbpsAvg = (float64(t.InputBytes) * 8 / 1_000_000) / (float64(dlMs) / 1000)
	}
	// upload_mbps_avg: output bytes ÷ drive upload time.
	if t.DriveUploadMs > 0 && t.OutputBytes > 0 {
		t.UploadMbpsAvg = (float64(t.OutputBytes) * 8 / 1_000_000) / (float64(t.DriveUploadMs) / 1000)
	}
	// drive_upload_mbps: same as upload but using DriveUpload bytes.
	if t.DriveUploadMs > 0 && t.OutputBytes > 0 {
		t.DriveUploadMbps = (float64(t.OutputBytes) * 8 / 1_000_000) / (float64(t.DriveUploadMs) / 1000)
	}
	// artifact_download_mbps: input bytes on drive download.
	if t.DriveDownloadMs > 0 && t.BytesFromDrive > 0 {
		t.ArtifactDownloadMbps = (float64(t.BytesFromDrive) * 8 / 1_000_000) / (float64(t.DriveDownloadMs) / 1000)
	}
}
func (t RawExecutionMetrics) CoverageMap() map[string]bool {
	if t.TelemetryCoverageJSON == "" {
		return nil
	}
	var coverage map[string]bool
	if err := json.Unmarshal([]byte(t.TelemetryCoverageJSON), &coverage); err != nil {
		return nil
	}
	return coverage
}
