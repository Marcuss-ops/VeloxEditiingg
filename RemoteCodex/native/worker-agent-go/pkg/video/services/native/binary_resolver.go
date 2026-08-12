package native

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if chrononBackendEnabled() {
		if path := strings.TrimSpace(os.Getenv("CHRONON3D_CLI")); path != "" {
			return path, nil
		}
	}
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
	m.FramesDecoded = sc.FramesDecoded
	m.FramesComposited = sc.FramesComposited
	m.Fps = sc.Fps
	m.SpeedX = sc.SpeedX
	m.EncodePasses = sc.EncodePasses
	m.TempBytes = sc.TempBytes
	m.OutputDurable = sc.OutputDurable
	m.DurationSec = sc.DurationSec
	m.ConcatMode = sc.ConcatMode
	m.TotalSize = sc.TotalSize
	m.OutTimeMs = sc.OutTimeMs
	m.Bitrate = sc.Bitrate
	m.DupFrames = sc.DupFrames
	m.DropFrames = sc.DropFrames
	m.PhaseMS = sc.PhaseMS
	if sc.IOCounters != nil {
		m.FileCopyCount = sc.IOCounters.FileCopyCount
		m.FileCopyBytes = sc.IOCounters.FileCopyBytes
		m.AssetBytesCopied = sc.IOCounters.AssetBytesCopied
		m.InputOpenCount = sc.IOCounters.InputOpenCount
		m.InputReopenCount = sc.IOCounters.InputReopenCount
	}
	if sc.ProcessCounters != nil {
		m.EngineExternalSpawnCount = sc.ProcessCounters.ExternalSpawnCount
		m.EngineFfmpegSpawnCount = sc.ProcessCounters.FfmpegSpawnCount
		m.EngineFfprobeSpawnCount = sc.ProcessCounters.FfprobeSpawnCount
		m.EngineShellSpawnCount = sc.ProcessCounters.ShellSpawnCount
		m.EngineCurlSpawnCount = sc.ProcessCounters.CurlSpawnCount
		m.EngineCPUUserMs = sc.ProcessCounters.CPUUserMs
		m.EngineCPUSystemMs = sc.ProcessCounters.CPUSystemMs
		m.EngineVoluntaryContextSwitches = sc.ProcessCounters.VoluntaryContextSwitches
		m.EngineInvoluntaryContextSwitches = sc.ProcessCounters.InvoluntaryContextSwitches
		m.EngineMinorPageFaults = sc.ProcessCounters.MinorPageFaults
		m.EngineMajorPageFaults = sc.ProcessCounters.MajorPageFaults
	}
	if sc.Observability != nil {
		m.Observability = make(map[string]interface{}, len(sc.Observability))
		for key, value := range sc.Observability {
			m.Observability[key] = value
		}
	}
	if sc.FramePipeline != nil {
		m.FramePipeline = pipeline.FramePipelineMetrics{
			ProducerBusyMS:         sc.FramePipeline.ProducerBusyMS,
			ProducerWaitMS:         sc.FramePipeline.ProducerWaitMS,
			ConsumerBusyMS:         sc.FramePipeline.ConsumerBusyMS,
			ConsumerWaitMS:         sc.FramePipeline.ConsumerWaitMS,
			QueueDepthAvg:          sc.FramePipeline.QueueDepthAvg,
			QueueDepthMax:          sc.FramePipeline.QueueDepthMax,
			QueueEmptyMS:           sc.FramePipeline.QueueEmptyMS,
			QueueFullMS:            sc.FramePipeline.QueueFullMS,
			ProducerStallRatio:     sc.FramePipeline.ProducerStallRatio,
			EncoderStarvationRatio: sc.FramePipeline.EncoderStarvationRatio,
			BackpressureRatio:      sc.FramePipeline.BackpressureRatio,
		}
	}
	m.Segments = make([]pipeline.SegmentTiming, 0, len(sc.Segments))
	for _, seg := range sc.Segments {
		m.Segments = append(m.Segments, pipeline.SegmentTiming{
			SegmentIndex:     int(seg.Index),
			SceneWorkerIndex: int(seg.WorkerIndex),
			SceneID:          seg.SceneID,
			SourceType:       seg.SourceType,
			DurationMS:       seg.TotalMs,
			AssetDownloadMS:  seg.AssetDownloadMs,
			FfmpegEncodeMS:   seg.FfmpegEncodeMs,
			SourceBytes:      seg.SourceBytes,
			OutputBytes:      seg.OutputBytes,
			FramesEncoded:    seg.FramesEncoded,
			FramesDecoded:    seg.FramesDecoded,
			FramesComposited: seg.FramesComposited,
			FfmpegSpeedX:     seg.FfmpegSpeedX,
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
			StartedOffsetMS:  seg.StartedOffsetMs,
			FinishedOffsetMS: seg.FinishedOffsetMs,
			WorkerSlot:       int(seg.WorkerSlot),
			CPUThreads:       int(seg.CpuThreads),
			ParallelGroup:    seg.ParallelGroup,
		})
	}
	m.DetailedPhases = make([]pipeline.DetailedPhaseTiming, 0, len(sc.Phases))
	for _, phase := range sc.Phases {
		metadata := phase.MetadataJSON
		if metadata == "" && len(phase.Metadata) > 0 {
			metadata = string(phase.Metadata)
		}
		m.DetailedPhases = append(m.DetailedPhases, pipeline.DetailedPhaseTiming{
			Origin:           phase.Origin,
			Scope:            phase.Scope,
			Component:        phase.Component,
			Action:           phase.Action,
			Phase:            phase.Phase,
			EventType:        phase.EventType,
			EventName:        phase.EventName,
			EventIndex:       phase.EventIndex,
			StartedAt:        phase.StartedAt,
			CompletedAt:      phase.CompletedAt,
			DurationMS:       phase.DurationMS,
			Status:           phase.Status,
			ErrorCode:        phase.ErrorCode,
			ErrorMessage:     phase.ErrorMessage,
			BytesIn:          phase.BytesIn,
			BytesOut:         phase.BytesOut,
			Frames:           phase.Frames,
			MetadataJSON:     metadata,
			SegmentIndex:     phase.SegmentIndex,
			TrackKind:        phase.TrackKind,
			TrackIndex:       phase.TrackIndex,
			StartedOffsetMS:  phase.StartedOffsetMS,
			FinishedOffsetMS: phase.FinishedOffsetMS,
			CPUMS:            phase.CPUMS,
			QueueWaitMS:      phase.QueueWaitMS,
			FramesIn:         phase.FramesIn,
			FramesOut:        phase.FramesOut,
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
