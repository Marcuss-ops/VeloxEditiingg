package native

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"velox-worker-agent/pkg/binaryresolver"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

// binary_resolver.go groups the four filesystem / "before-and-after
// the subprocess lifecycle" concerns: locating the engine binary,
// preparing the on-disk plan tempdir, mapping the parsed sidecar
// back into pipeline.RenderMetrics, and verifying the engine actually
// wrote its declared output. None of these touch the subprocess
// lifecycle itself; engine_process.go owns that.

// resolveBinary locates the velox_video_engine binary by checking the
// VELOX_VIDEO_ENGINE_CPP_BIN env var, /usr/local/bin, and a couple of
// relative paths into the sibling video-engine-cpp build tree.
func resolveBinary() (string, error) {
	r := binaryresolver.Resolver{
		Name:   "velox_video_engine",
		EnvVar: "VELOX_VIDEO_ENGINE_CPP_BIN",
		AbsCandidates: []string{
			"/usr/local/bin/velox_video_engine",
		},
		RelOffsets: []string{
			filepath.Join("..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "video-engine-cpp", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "velox_video_engine"),
		},
	}
	return r.Resolve(0)
}

// preparePlanTemp creates a fresh temp directory and writes the JSON
// marshalled RenderPlan to render_plan.json inside it. Returns
// (tempDir, planPath, planMarshalMs, planWriteMs). On partial failure
// (MarshalIndent or WriteFile error after MkdirTemp succeeded) the
// tempDir is cleaned up before returning, so the caller can rely on
// either a fully-prepared (tempDir, planPath) pair, OR a fresh empty
// string with err non-nil — never an orphaned directory.
func preparePlanTemp(p *plan.RenderPlan) (string, string, int64, int64, error) {
	tempDir, err := os.MkdirTemp("", "velox_render_*")
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("create temp dir: %w", err)
	}

	planPath := filepath.Join(tempDir, "render_plan.json")
	marshalStart := time.Now()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		os.RemoveAll(tempDir)
		return "", "", 0, 0, fmt.Errorf("marshal plan: %w", err)
	}
	planMarshalMs := time.Since(marshalStart).Milliseconds()

	writeStart := time.Now()
	if err := os.WriteFile(planPath, data, 0o644); err != nil {
		os.RemoveAll(tempDir)
		return "", "", 0, 0, fmt.Errorf("write plan: %w", err)
	}
	planWriteMs := time.Since(writeStart).Milliseconds()

	return tempDir, planPath, planMarshalMs, planWriteMs, nil
}

// mapEngineSidecar copies the sidecar-derived fields from a parsed
// engineSidecar into the supplied pipeline.RenderMetrics. The fields
// it writes are the same set the original inlined code copy-pasted
// at the end of RenderWithMetrics — Frames, Fps, SpeedX, EncodePasses,
// TempBytes, DurationSec, ConcatMode, TotalSize, OutTimeMs, Bitrate,
// DupFrames, DropFrames, PhaseMS, and Segments (with the segment
// mapping below). Pre-existing fields (PlanMarshalMs, PlanWriteMs,
// ProcessStartMs, ProcessWaitMs, TotalMs) are untouched.
func mapEngineSidecar(sc *engineSidecar, m *pipeline.RenderMetrics) {
	m.Frames = sc.Frames
	m.Fps = sc.Fps
	m.SpeedX = sc.SpeedX
	m.EncodePasses = sc.EncodePasses
	m.TempBytes = sc.TempBytes
	m.DurationSec = sc.DurationSec
	m.ConcatMode = sc.ConcatMode
	m.TotalSize = sc.TotalSize
	m.OutTimeMs = sc.OutTimeMs
	m.Bitrate = sc.Bitrate
	m.DupFrames = sc.DupFrames
	m.DropFrames = sc.DropFrames
	m.PhaseMS = sc.PhaseMS
	m.Segments = make([]pipeline.SegmentTiming, 0, len(sc.Segments))
	for _, seg := range sc.Segments {
		m.Segments = append(m.Segments, pipeline.SegmentTiming{
			SegmentIndex:     int(seg.Index),
			SceneWorkerIndex: int(seg.WorkerIndex),
			SourceType:       seg.SourceType,
			DurationMS:       seg.TotalMs,
			AssetDownloadMS:  seg.AssetDownloadMs,
			FfmpegEncodeMS:   seg.FfmpegEncodeMs,
			SourceBytes:      seg.SourceBytes,
			OutputBytes:      seg.OutputBytes,
			FramesEncoded:    seg.FramesEncoded,
			Codec:            seg.Codec,
			Preset:           seg.Preset,
			FfmpegThreads:    int(seg.FfmpegThreads),
			Status:           seg.Status,
			ErrorCode:        seg.ErrorCode,
			ErrorMessage:     seg.ErrorMessage,
			SourceURLHash:    seg.SourceURLHash,
			CacheKey:         seg.CacheKey,
			InputDurationMS:  seg.InputDurationMs,
			OutputDurationMS: seg.OutputDurationMs,
			MetadataJSON:     seg.MetadataJSON,
		})
	}
}

// verifyOutputExists confirms the engine actually wrote its declared
// outputPath before the orchestrator returns success.
func verifyOutputExists(outputPath string) error {
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("output file not created %s: %w", outputPath, err)
	}
	return nil
}
