// Package jobperf owns the JOB-LEVEL performance timeline: the
// chronological decomposition of one render task into canonical phases
// (asset download, decode, subtitle/blur/watermark/composite, encode,
// concat, mux, artifact verification, drive upload …), per-scene
// breakdowns, GPU↔CPU transfer counters, and resource sampling.
//
// This package is deliberately SEPARATE from internal/telemetry: the
// attempt event journal (EventRecorder + shared catalog) records
// receipt-level facts on the wire, while jobperf aggregates wall-clock
// phase durations into the operator-facing PerformanceReport. It does
// not introduce a second attempt-event registry — the timeline keys
// here are reporting labels, never emitted as catalog events.
package jobperf

import "sort"

// Canonical job-timeline phase keys. snake_case, stable ordering via
// CanonicalPhaseOrder. Producers must use these constants; unknown keys
// passed to Tracker.Begin/End are recorded anyway (forward-compatible)
// but excluded from the canonical report ordering.
const (
	PhaseQueueWait        = "queue_wait"
	PhaseJobSetup         = "job_setup"
	PhaseAssetResolve     = "asset_resolve"
	PhaseAssetDownload    = "asset_download"
	PhaseAssetVerify      = "asset_verify"
	PhaseAssetMaterialize = "asset_materialize"
	PhaseVoiceoverPrepare = "voiceover_prepare"
	PhaseAudioTimeline    = "audio_timeline_build"
	PhaseRenderPlanBuild  = "render_plan_build"
	PhaseVideoRender      = "video_render"
	PhaseVideoDecode      = "video_decode"
	PhaseVideoFilter      = "video_filter"
	PhaseVideoSubtitle    = "video_subtitle"
	PhaseVideoWatermark   = "video_watermark"
	PhaseVideoBlur        = "video_blur"
	PhaseVideoComposite   = "video_composite"
	PhaseVideoEncode      = "video_encode"
	PhaseConcat           = "concat"
	PhaseAudioMux         = "audio_mux"
	PhaseOutputFinalize   = "output_finalize"
	PhaseArtifactSHA256   = "artifact_sha256"
	PhaseArtifactProbe    = "artifact_ffprobe"
	PhaseArtifactVerify   = "artifact_verify"
	PhaseDriveUpload      = "drive_upload"
	PhaseDriveVerify      = "drive_verify"
	PhaseJobTotal         = "job_total"
)

// CanonicalPhaseOrder is the stable chronological ordering used by the
// final report. Phases outside this list are appended (sorted) after.
var CanonicalPhaseOrder = []string{
	PhaseQueueWait,
	PhaseJobSetup,
	PhaseAssetResolve,
	PhaseAssetDownload,
	PhaseAssetVerify,
	PhaseAssetMaterialize,
	PhaseVoiceoverPrepare,
	PhaseAudioTimeline,
	PhaseRenderPlanBuild,
	PhaseVideoRender,
	PhaseVideoDecode,
	PhaseVideoFilter,
	PhaseVideoSubtitle,
	PhaseVideoWatermark,
	PhaseVideoBlur,
	PhaseVideoComposite,
	PhaseVideoEncode,
	PhaseConcat,
	PhaseAudioMux,
	PhaseOutputFinalize,
	PhaseArtifactSHA256,
	PhaseArtifactProbe,
	PhaseArtifactVerify,
	PhaseDriveUpload,
	PhaseDriveVerify,
}

// canonicalRank maps each canonical key to its position for O(1) sort.
var canonicalRank = func() map[string]int {
	m := make(map[string]int, len(CanonicalPhaseOrder))
	for i, k := range CanonicalPhaseOrder {
		m[k] = i
	}
	return m
}()

// PhaseLabels are the human-readable headers rendered in the text
// report, keyed by canonical phase key.
var PhaseLabels = map[string]string{
	PhaseQueueWait:        "Queue wait",
	PhaseJobSetup:         "Job setup",
	PhaseAssetResolve:     "Asset resolve",
	PhaseAssetDownload:    "Asset download",
	PhaseAssetVerify:      "Asset verify",
	PhaseAssetMaterialize: "Asset materialize",
	PhaseVoiceoverPrepare: "Voiceover prepare",
	PhaseAudioTimeline:    "Audio timeline build",
	PhaseRenderPlanBuild:  "Plan build",
	PhaseVideoRender:      "Video render",
	PhaseVideoDecode:      "Video decode",
	PhaseVideoFilter:      "Filters/compositing",
	PhaseVideoSubtitle:    "Subtitles",
	PhaseVideoWatermark:   "Watermark",
	PhaseVideoBlur:        "Blur",
	PhaseVideoComposite:   "Composite",
	PhaseVideoEncode:      "Video encode",
	PhaseConcat:           "Concat",
	PhaseAudioMux:         "Audio mux",
	PhaseOutputFinalize:   "Output finalize",
	PhaseArtifactSHA256:   "SHA256",
	PhaseArtifactProbe:    "ffprobe",
	PhaseArtifactVerify:   "Artifact verify",
	PhaseDriveUpload:      "Drive upload",
	PhaseDriveVerify:      "Drive verify",
	PhaseJobTotal:         "JOB TOTAL",
}

func phaseLabel(key string) string {
	if l, ok := PhaseLabels[key]; ok {
		return l
	}
	return key
}

// sortPhases orders phase keys canonically; non-canonical keys follow,
// alphabetically, so report output is deterministic.
func sortPhases(keys []string) {
	sort.Slice(keys, func(i, j int) bool {
		ri, iok := canonicalRank[keys[i]]
		rj, jok := canonicalRank[keys[j]]
		switch {
		case iok && jok:
			return ri < rj
		case iok:
			return true
		case jok:
			return false
		default:
			return keys[i] < keys[j]
		}
	})
}
