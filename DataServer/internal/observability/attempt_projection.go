package observability

import (
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// attemptOverlayDecision is the result of the one reconciliation authority
// used by every observability projection. A nil durable pointer means the
// live row is a temporary claim/accept overlay only; it never becomes durable
// history and never overrides a terminal task.
type attemptOverlayDecision struct {
	durable  *taskattempts.TaskAttempt
	eligible bool
}

// reconcileLiveAttempt is the single authority for live/durable precedence.
// Durable terminal state always wins over volatile worker_task_runtime state.
// It also owns task/attempt identity, retry ordering, and worker liveness
// checks so callers cannot implement subtly different reconciliation rules.
func reconcileLiveAttempt(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) attemptOverlayDecision {
	decision := attemptOverlayDecision{}
	if live == nil || task == nil || task.Status.IsTerminal() || live.AttemptID == "" || live.AttemptNumber <= 0 {
		return decision
	}
	if (live.TaskID != "" && live.TaskID != task.ID) || (live.JobID != "" && live.JobID != task.JobID) {
		return decision
	}

	switch live.RuntimeStatus {
	case "ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING":
	default:
		return decision
	}
	switch live.WorkerConnectionState {
	case "", "CONNECTED":
	default:
		return decision
	}

	latestAttemptNumber := 0
	for i := range attempts {
		if attempts[i].AttemptNumber > latestAttemptNumber {
			latestAttemptNumber = attempts[i].AttemptNumber
		}
	}
	if live.AttemptNumber < latestAttemptNumber || live.AttemptNumber < task.AttemptCount {
		return decision
	}
	for i := range attempts {
		if attempts[i].ID != live.AttemptID {
			continue
		}
		if attempts[i].TaskID != "" && attempts[i].TaskID != task.ID {
			return decision
		}
		if attempts[i].JobID != "" && attempts[i].JobID != task.JobID {
			return decision
		}
		if attempts[i].AttemptNumber > 0 && attempts[i].AttemptNumber != live.AttemptNumber {
			return decision
		}
		decision.durable = &attempts[i]
		decision.eligible = !attempts[i].Status.IsTerminal()
		return decision
	}

	// Claim/accept can publish the volatile identity just before the durable
	// row is visible. Permit this temporary overlay, but never promote it to
	// durable state or use it to override a terminal task.
	decision.eligible = true
	return decision
}

func (d attemptOverlayDecision) overlaysAttempt(attemptID string) bool {
	return d.eligible && d.durable != nil && d.durable.ID == attemptID
}

func (d attemptOverlayDecision) hasTemporaryOverlay() bool {
	return d.eligible && d.durable == nil
}

// liveAttemptIsEligible remains a small compatibility wrapper for focused
// callers and tests. All reconciliation decisions flow through the helper
// above; there is no second precedence implementation.
func liveAttemptIsEligible(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) bool {
	return reconcileLiveAttempt(live, task, attempts).eligible
}

// applyLiveAttemptOverlay copies only volatile execution fields onto a
// durable/non-terminal summary. Identity, status, errors, timestamps, and
// final metrics remain owned by the durable attempt.
func applyLiveAttemptOverlay(target *AttemptSummary, live *LiveAttempt) {
	if target == nil || live == nil {
		return
	}
	target.Live = true
	target.Phase = live.ProgressPhase
	target.ProgressPercent = live.ProgressPercent
	target.CurrentScene = live.CurrentScene
	target.TotalScenes = live.TotalScenes
	target.CurrentSegment = live.CurrentSegment
	target.TotalSegments = live.TotalSegments
	target.FramesEncoded = live.FramesEncoded
	target.FramesDecoded = live.FramesDecoded
	target.FramesComposited = live.FramesComposited
	target.FFmpegSpeedX = live.FFmpegSpeedX
	target.ElapsedMS = live.ElapsedMS
	if target.StartedAt == "" {
		target.StartedAt = live.StartedAt
	}
	target.LastProgressAt = live.LastProgressAt
	target.CumulativeMetrics = live.CumulativeMetrics
	target.CanonicalAttemptEvents = live.CanonicalAttemptEvents
}

func liveAttemptStatus(live *LiveAttempt) taskattempts.AttemptStatus {
	if live == nil {
		return taskattempts.AttemptStatusRunning
	}
	return taskattempts.AttemptStatusRunning
}

func applyExecutionLiveOverlay(summary *ExecutionSummary, attempt *AttemptSummary) {
	if summary == nil || attempt == nil || !attempt.Live {
		return
	}
	summary.AttemptID = attempt.AttemptID
	summary.WorkerID = attempt.WorkerID
	summary.Phase = attempt.Phase
	summary.Progress = &ExecutionProgress{
		Percent: attempt.ProgressPercent, Scene: attempt.CurrentScene, ScenesTotal: attempt.TotalScenes,
		Segment: attempt.CurrentSegment, SegmentsTotal: attempt.TotalSegments,
	}
	summary.LiveMetrics = &ExecutionLiveMetrics{
		ElapsedMS: attempt.ElapsedMS, FramesEncoded: attempt.FramesEncoded,
		FramesDecoded: attempt.FramesDecoded, FramesComposited: attempt.FramesComposited,
		FFmpegSpeedX: attempt.FFmpegSpeedX, CumulativeMetrics: attempt.CumulativeMetrics,
	}
	summary.LastProgressAt = attempt.LastProgressAt
}

func liveAttemptMetrics(live *LiveAttempt) *taskattempts.AttemptMetrics {
	if live == nil {
		return nil
	}
	metrics := &taskattempts.AttemptMetrics{
		AttemptID:         live.AttemptID,
		FramesEncoded:     live.FramesEncoded,
		FramesDecoded:     live.FramesDecoded,
		FramesComposited:  live.FramesComposited,
		FFmpegSpeedRatio:  live.FFmpegSpeedX,
		SceneCount:        live.TotalScenes,
		SegmentCount:      live.TotalSegments,
		CompletedSegments: live.CurrentSegment,
		WallClockSeconds:  float64(live.ElapsedMS) / 1000,
	}
	for key, value := range live.CumulativeMetrics {
		switch key {
		case "input_bytes":
			metrics.InputBytes = int64Value(value)
		case "output_bytes":
			metrics.OutputBytes = int64Value(value)
		case "bytes_from_drive":
			metrics.BytesFromDrive = int64Value(value)
		case "bytes_from_blobstore":
			metrics.BytesFromBlobstore = int64Value(value)
		case "bytes_from_local_cache":
			metrics.BytesFromLocalCache = int64Value(value)
		case "cpu_time_ms":
			metrics.CPUTimeMS = int64Value(value)
		case "gpu_time_ms":
			metrics.GPUTimeMS = int64Value(value)
		case "peak_rss_bytes":
			metrics.PeakRSSBytes = int64Value(value)
		case "peak_vram_bytes":
			metrics.PeakVRAMBytes = int64Value(value)
		case "frames_encoded":
			metrics.FramesEncoded = int64Value(value)
		case "frames_decoded":
			metrics.FramesDecoded = int64Value(value)
		case "frames_composited":
			metrics.FramesComposited = int64Value(value)
		case "ffmpeg_speed_ratio", "ffmpeg_speed_x":
			metrics.FFmpegSpeedRatio = float64Value(value)
		case "pipeline_render_ms":
			metrics.PipelineRenderMs = int64Value(value)
		case "pipeline_total_ms":
			metrics.PipelineTotalMs = int64Value(value)
		}
	}
	return metrics
}

func int64Value(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	case float32:
		return int64(number)
	default:
		return 0
	}
}

func float64Value(value any) float64 {
	switch number := value.(type) {
	case int:
		return float64(number)
	case int64:
		return float64(number)
	case float64:
		return number
	case float32:
		return float64(number)
	default:
		return 0
	}
}
