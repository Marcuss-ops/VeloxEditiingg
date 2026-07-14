// Package metrics / collector_engine.go
//
// Engine-side phase + segment timing histogram recorders sliced out
// of collector.go so the Collector struct definition stays focused
// on registration.
//
// Scorecard v2 / Step 7: two histogram families capture per-phase
// and per-segment durations from the C++ engine sidecar (and the Go
// pipeline phases).
//
//   - velox_engine_phase_duration_seconds — labels
//     executor_id, worker_id, phase, status
//   - velox_engine_segment_duration_seconds — labels
//     executor_id, worker_id, source_type, status
//
// Cardinality discipline: NO job_id / task_id / attempt_id labels.
// Granular sub-second buckets because engine phases are fast (asset
// download, ffmpeg encode, concat, etc.).
package metrics

import (
	"velox-server/internal/taskattempts"
)

// RecordEngineAggregate ingests the engine-aggregate phase columns from
// an AttemptMetrics row into the engine phase histogram (dotted phase
// names like "pipeline.resolve", "engine.asset_download"). Called from
// ScanAttemptWithLabels after the existing RecordAttempt path, so the
// per-phase histogram captures the same attempt-phase-duration mapping
// that operators query in SQL.
func (c *Collector) RecordEngineAggregate(am *taskattempts.AttemptMetrics, execID, workerID string) {
	if am == nil {
		return
	}
	// Pipeline phases.
	if am.PipelineResolveMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "pipeline.resolve", "ok"}, float64(am.PipelineResolveMs)/1000)
	}
	if am.PipelineValidateMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "pipeline.validate", "ok"}, float64(am.PipelineValidateMs)/1000)
	}
	if am.PipelineCompileMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "pipeline.compile", "ok"}, float64(am.PipelineCompileMs)/1000)
	}
	if am.PipelineRenderMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "pipeline.render", "ok"}, float64(am.PipelineRenderMs)/1000)
	}
	if am.PipelineTotalMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "pipeline.total", "ok"}, float64(am.PipelineTotalMs)/1000)
	}
	// Native process.
	if am.NativeTotalMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "native.total", "ok"}, float64(am.NativeTotalMs)/1000)
	}
	if am.NativeProcessWaitMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "native.process_wait", "ok"}, float64(am.NativeProcessWaitMs)/1000)
	}
	// Engine phases.
	if am.EngineAssetDownloadMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.asset_download", "ok"}, float64(am.EngineAssetDownloadMs)/1000)
	}
	if am.EngineSegmentBuildMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.segment_build", "ok"}, float64(am.EngineSegmentBuildMs)/1000)
	}
	if am.EngineConcatMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.concat", "ok"}, float64(am.EngineConcatMs)/1000)
	}
	if am.EngineAudioDownloadMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.audio_download", "ok"}, float64(am.EngineAudioDownloadMs)/1000)
	}
	if am.EngineMuxAudioMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.mux_audio", "ok"}, float64(am.EngineMuxAudioMs)/1000)
	}
	if am.EngineCopyFinalMs > 0 {
		c.enginePhaseDurations.Observe([]string{execID, workerID, "engine.copy_final", "ok"}, float64(am.EngineCopyFinalMs)/1000)
	}
}

// RecordEnginePhase ingests a single detailed phase timing row into the
// engine phase histogram. Called from the supervisor tick for rows
// returned by GetPhaseTimingsDetailed. The phase label is the dotted
// component.action name (mirrors the DB insertion convention).
func (c *Collector) RecordEnginePhase(pt taskattempts.PhaseTimingDetailed, execID, workerID string) {
	if pt.DurationMS <= 0 {
		return
	}
	phase := pt.Component + "." + pt.Action
	if phase == "." {
		return
	}
	status := pt.Status
	if status == "" {
		status = "ok"
	}
	c.enginePhaseDurations.Observe([]string{execID, workerID, phase, status}, float64(pt.DurationMS)/1000)
}

// RecordEngineSegment ingests a single segment timing row into the
// engine segment histogram. Called from the supervisor tick for rows
// returned by GetSegmentTimings. source_type is the segment type
// (clip, color, image, audio, etc.).
func (c *Collector) RecordEngineSegment(seg taskattempts.SegmentTiming, execID, workerID string) {
	if seg.DurationMS <= 0 {
		return
	}
	sourceType := seg.SourceType
	if sourceType == "" {
		sourceType = "unknown"
	}
	status := seg.Status
	if status == "" {
		status = "ok"
	}
	c.engineSegmentDurations.Observe([]string{execID, workerID, sourceType, status}, seg.DurationMS/1000)
}
