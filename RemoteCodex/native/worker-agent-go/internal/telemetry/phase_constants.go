package telemetry

const (
	// ── Pre-render phases ─────────────────────────────────────────────
	PhaseQueueWait        = "queue_wait"
	PhaseJobSetup         = "job_setup"
	PhaseAssetResolve     = "asset.resolve"
	PhaseAssetDownload    = "asset.download"
	PhaseAssetVerify      = "asset.verify"
	PhaseAssetMaterialize = "asset.materialize"

	// ── Audio phases ──────────────────────────────────────────────────
	PhaseAudioPrepare       = "audio.prepare"
	PhaseAudioTimelineBuild = "audio.timeline_build"

	// ── Render planning ───────────────────────────────────────────────
	PhaseRenderPlanBuild = "render_plan_build"

	// ── Video pipeline: decode ────────────────────────────────────────
	PhaseVideoDecode = "video.decode"

	// ── Video pipeline: subtitle (libass) ─────────────────────────────
	PhaseVideoSubtitle          = "video.subtitle"
	PhaseVideoSubtitleRaster    = "video.subtitle_raster"
	PhaseVideoSubtitleComposite = "video.subtitle_composite"

	// ── Video pipeline: watermark ─────────────────────────────────────
	PhaseVideoWatermark          = "video.watermark"
	PhaseVideoWatermarkUpload    = "video.watermark_upload"
	PhaseVideoWatermarkComposite = "video.watermark_composite"

	// ── Video pipeline: effects ───────────────────────────────────────
	PhaseVideoBlur   = "video.blur"
	PhaseVideoFilter = "video.filter"

	// ── Video pipeline: composite ─────────────────────────────────────
	PhaseVideoComposite = "video.composite"

	// ── Video pipeline: encode ────────────────────────────────────────
	PhaseVideoEncode = "video.encode"

	// ── Video pipeline: concat ────────────────────────────────────────
	PhaseVideoConcat = "video.concat"

	// ── Audio mux ─────────────────────────────────────────────────────
	PhaseAudioMux = "audio.mux"

	// ── Output & verification ─────────────────────────────────────────
	PhaseOutputFinalize = "output_finalize"
	PhaseArtifactHash   = "artifact.hash"
	PhaseArtifactProbe  = "artifact.probe"
	PhaseArtifactVerify = "artifact.verify"
	PhaseArtifactWrite  = "artifact.write"

	// ── Drive / upload ────────────────────────────────────────────────
	PhaseDriveUpload = "drive.upload"
	PhaseDriveVerify = "drive.verify"

	// ── Download / cache timing sub-phases ────────────────────────────
	PhaseDriveDownload     = "asset.download_drive"
	PhaseBlobstoreDownload = "asset.download_blobstore"
	PhaseLocalCacheRead    = "asset.cache_read"
	PhaseAssetDownloadWait = "asset.download_wait"

	// ── Disk I/O sub-phases ───────────────────────────────────────────
	PhaseOutputWrite = "output.write"
	PhaseTempWrite   = "temp.write"
	PhaseFinalRead   = "final.read"

	// ── Cleanup ─────────────────────────────────────────────────────────
	PhaseCleanup = "cleanup"
)

// FineGrainedPhaseOrder defines the stable, deterministic iteration order for
// all fine-grained phases. Callers rely on this for consistent report output.
var FineGrainedPhaseOrder = []string{
	PhaseQueueWait,
	PhaseJobSetup,
	PhaseAssetResolve,
	PhaseAssetDownload,
	PhaseAssetVerify,
	PhaseAssetMaterialize,
	PhaseAudioPrepare,
	PhaseAudioTimelineBuild,
	PhaseRenderPlanBuild,
	PhaseVideoDecode,
	PhaseVideoSubtitle,
	PhaseVideoSubtitleRaster,
	PhaseVideoSubtitleComposite,
	PhaseVideoWatermark,
	PhaseVideoWatermarkUpload,
	PhaseVideoWatermarkComposite,
	PhaseVideoBlur,
	PhaseVideoFilter,
	PhaseVideoComposite,
	PhaseVideoEncode,
	PhaseVideoConcat,
	PhaseAudioMux,
	PhaseOutputFinalize,
	PhaseArtifactHash,
	PhaseArtifactProbe,
	PhaseArtifactVerify,
	PhaseArtifactWrite,
	PhaseDriveUpload,
	PhaseDriveVerify,
	PhaseDriveDownload,
	PhaseBlobstoreDownload,
	PhaseLocalCacheRead,
	PhaseAssetDownloadWait,
	PhaseOutputWrite,
	PhaseTempWrite,
	PhaseFinalRead,
	PhaseCleanup,
}

// PhaseDisplayNames maps phase constants to human-readable labels for reports.
var PhaseDisplayNames = map[string]string{
	PhaseQueueWait:               "Queue wait",
	PhaseJobSetup:                "Job setup",
	PhaseAssetResolve:            "Asset resolve",
	PhaseAssetDownload:           "Asset download",
	PhaseAssetVerify:             "Asset verify",
	PhaseAssetMaterialize:        "Asset materialize",
	PhaseAudioPrepare:            "Audio prepare",
	PhaseAudioTimelineBuild:      "Audio timeline build",
	PhaseRenderPlanBuild:         "Plan build",
	PhaseVideoDecode:             "Video decode",
	PhaseVideoSubtitle:           "Subtitle (libass)",
	PhaseVideoSubtitleRaster:     "  Subtitle raster",
	PhaseVideoSubtitleComposite:  "  Subtitle composite",
	PhaseVideoWatermark:          "Watermark",
	PhaseVideoWatermarkUpload:    "  Watermark upload",
	PhaseVideoWatermarkComposite: "  Watermark composite",
	PhaseVideoBlur:               "Blur",
	PhaseVideoFilter:             "Filter",
	PhaseVideoComposite:          "Composite",
	PhaseVideoEncode:             "Video encode",
	PhaseVideoConcat:             "Concat",
	PhaseAudioMux:                "Audio mux",
	PhaseOutputFinalize:          "Output finalize",
	PhaseArtifactHash:            "SHA256",
	PhaseArtifactProbe:           "ffprobe",
	PhaseArtifactVerify:          "Artifact verify",
	PhaseArtifactWrite:           "Artifact write",
	PhaseDriveUpload:             "Drive upload",
	PhaseDriveVerify:             "Drive verify",
	PhaseDriveDownload:           "  Drive download",
	PhaseBlobstoreDownload:       "  Blobstore download",
	PhaseLocalCacheRead:          "  Local cache read",
	PhaseAssetDownloadWait:       "  Download wait",
	PhaseOutputWrite:             "Output write",
	PhaseTempWrite:               "Temp write",
	PhaseFinalRead:               "Final read",
	PhaseCleanup:                 "Cleanup",
}

// fineGrainedSet enables O(1) phase name validation.
var fineGrainedSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(FineGrainedPhaseOrder))
	for _, p := range FineGrainedPhaseOrder {
		m[p] = struct{}{}
	}
	return m
}()

// IsFineGrainedPhase reports whether name is a registered fine-grained phase.
func IsFineGrainedPhase(name string) bool {
	_, ok := fineGrainedSet[name]
	return ok
}
