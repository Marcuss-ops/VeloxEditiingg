package store

// sqlite_task_atomic_persistence_parallelism.go: segment-timings +
// parallelism attempt write helpers used by IngestTaskResultAtomic.
// Split out of sqlite_task_atomic_persistence_attempt.go.

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// persistSegmentTimings replaces per-segment sidecar timings for the attempt.
// persistSegmentTimings delegates to the shared insertSegmentTimingsAndParallelism.
// Kept as a named wrapper for call-site clarity in IngestTaskResultAtomic.
func persistSegmentTimings(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	return insertSegmentTimingsAndParallelism(ctx, tx, cmd.AttemptID, cmd.SegmentTimings, cmd.Metrics)
}

// persistParallelism is now a no-op — parallelism is computed and persisted
// inside insertSegmentTimingsAndParallelism, which is called by persistSegmentTimings.
// Retained to avoid a diff in IngestTaskResultAtomic's call sequence.
func persistParallelism(_ context.Context, _ *sql.Tx, _ taskgraph.IngestResultCommand, _ string) error {
	return nil
}

// computeParallelism derives parallelism aggregates from segment timings
// and attempt metrics. Pure function — no DB access.
func computeParallelism(segments []taskattempts.SegmentTiming, metrics taskattempts.AttemptMetrics) taskattempts.AttemptParallelism {
	p := taskattempts.AttemptParallelism{
		CalculatedAt: nowRFC3339(),
	}

	if len(segments) == 0 {
		return p
	}

	valid := collectValidSegments(segments)
	if len(valid) == 0 {
		return p
	}

	// serial_work_ms = sum of all segment durations.
	var serial float64
	for _, s := range valid {
		serial += s.durMS
	}
	p.SerialWorkMS = serial

	// render_window_ms = last finished - first started.
	firstStart := valid[0].startMS
	lastEnd := valid[0].endMS
	for _, s := range valid {
		if s.endMS > lastEnd {
			lastEnd = s.endMS
		}
	}
	p.RenderWindowMS = lastEnd - firstStart
	if p.RenderWindowMS <= 0 {
		p.RenderWindowMS = serial
	}

	// union_busy_ms / overlap_ms / peak_concurrency via sweep line.
	p.UnionBusyMS, p.OverlapMS, p.PeakConcurrency = sweepSegments(valid)

	// idle_gap_ms = render_window - union_busy (time within the window
	// where no segment was active — gaps between segments).
	idle := p.RenderWindowMS - p.UnionBusyMS
	if idle < 0 {
		idle = 0
	}
	p.IdleGapMS = idle

	// average_concurrency = serial_work / union_busy.
	if p.UnionBusyMS > 0 {
		p.AverageConcurrency = serial / p.UnionBusyMS
	} else {
		p.AverageConcurrency = 1
	}

	// speedup_vs_serial = serial_work / render_window.
	if p.RenderWindowMS > 0 {
		p.SpeedupVsSerial = serial / p.RenderWindowMS
	} else {
		p.SpeedupVsSerial = 1
	}

	// parallel_efficiency_ratio = average_concurrency / peak_concurrency.
	if p.PeakConcurrency > 0 {
		p.ParallelEfficiency = p.AverageConcurrency / float64(p.PeakConcurrency)
	} else {
		p.ParallelEfficiency = 1
	}

	// CPU oversubscription — derive from segment data.
	p.FFmpegThreadsPerSegment = segments[0].FfmpegThreads
	// Count unique non-zero worker slots.
	slotsUsed := make(map[int]bool)
	for _, s := range valid {
		if s.slot > 0 {
			slotsUsed[s.slot] = true
		}
	}
	p.ConfiguredSegmentWorkers = len(slotsUsed)
	if p.ConfiguredSegmentWorkers == 0 {
		p.ConfiguredSegmentWorkers = 1
	}
	// Use the per-attempt CPU capacity telemetry sent by the worker.
	// Fall back to the pre-099 approximation only when the worker did
	// not yet emit the new fields (older workers).
	p.LogicalCPUCount = metrics.LogicalCPUCount
	if p.LogicalCPUCount <= 0 {
		p.LogicalCPUCount = int(metrics.ActiveWorkersAtStart)
	}
	p.CPUBudget = metrics.EffectiveCPUCount
	if p.CPUBudget <= 0 {
		p.CPUBudget = p.LogicalCPUCount
	}
	if p.CPUBudget <= 0 {
		p.CPUBudget = 1
	}
	if p.CPUBudget > 0 && p.FFmpegThreadsPerSegment > 0 {
		totalThreads := p.ConfiguredSegmentWorkers * p.FFmpegThreadsPerSegment
		p.CPUOversubscription = float64(totalThreads) / float64(p.CPUBudget)
	}

	// Determine bottleneck phase from segment durations.
	maxDur := valid[0].durMS
	for _, s := range valid[1:] {
		if s.durMS > maxDur {
			maxDur = s.durMS
		}
	}
	p.BottleneckPhase = fmt.Sprintf("longest_segment_%.0fms", maxDur)

	// Determine parallel strategy from overlap.
	if p.OverlapMS > 0 {
		p.ParallelStrategy = "concurrent_segments"
	} else {
		p.ParallelStrategy = "serial_segments"
	}

	// Clamp efficiency to [0, 1].
	if p.ParallelEfficiency > 1.0 {
		p.ParallelEfficiency = 1.0
	}
	if p.ParallelEfficiency < 0 {
		p.ParallelEfficiency = 0
	}

	// Clamp oversubscription — avoid division by zero.
	if math.IsNaN(p.CPUOversubscription) || math.IsInf(p.CPUOversubscription, 0) {
		p.CPUOversubscription = 0
	}

	return p
}

// segmentSpan is a normalized, start-sorted view of one valid segment
// timing row used by computeParallelism.
type segmentSpan struct {
	startMS float64
	endMS   float64
	durMS   float64
	slot    int
}

// collectValidSegments filters and normalizes segment timings into a
// start-sorted slice. Segments without a positive duration are dropped;
// zero offsets fall back to serial placement (end = accumulated serial work).
func collectValidSegments(segments []taskattempts.SegmentTiming) []segmentSpan {
	var valid []segmentSpan
	for _, s := range segments {
		if s.Status != "ok" && s.Status != "" {
			continue
		}
		dur := s.DurationMS
		start := s.StartedOffsetMS
		end := s.FinishedOffsetMS
		if dur <= 0 {
			continue
		}
		if start == 0 && end == 0 {
			end = dur
		}
		if end <= start {
			end = start + dur
		}
		valid = append(valid, segmentSpan{startMS: start, endMS: end, durMS: dur, slot: s.WorkerSlot})
	}
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].startMS < valid[j].startMS
	})
	return valid
}

// sweepSegments computes union busy time, overlap time and peak concurrency
// over a start-sorted segment list using a sweep line over start/end events.
func sweepSegments(valid []segmentSpan) (unionBusyMS, overlapMS float64, peakConcurrency int) {
	type event struct {
		time float64
		down bool // true = segment ending, false = segment starting
	}
	events := make([]event, 0, len(valid)*2)
	for _, s := range valid {
		events = append(events, event{time: s.startMS, down: false})
		events = append(events, event{time: s.endMS, down: true})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].time == events[j].time {
			return events[i].down && !events[j].down // end before start at same time
		}
		return events[i].time < events[j].time
	})
	active := 0
	var lastTime float64
	for _, e := range events {
		if active > 0 && e.time > lastTime {
			unionBusyMS += e.time - lastTime
			if active > 1 {
				overlapMS += e.time - lastTime
			}
		}
		if e.down {
			active--
		} else {
			active++
		}
		if active > peakConcurrency {
			peakConcurrency = active
		}
		lastTime = e.time
	}
	return unionBusyMS, overlapMS, peakConcurrency
}

// insertSegmentTimingsAndParallelism is the shared implementation for both
// the atomic IngestTaskResultAtomic path and the standalone PersistSegmentTimings
// path. It inserts segment timing rows and computes + persists the parallelism
// aggregate — ensuring both code paths produce identical behavior.
func insertSegmentTimingsAndParallelism(ctx context.Context, tx *sql.Tx, attemptID string, segments []taskattempts.SegmentTiming, metrics taskattempts.AttemptMetrics) error {
	if len(segments) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_attempt_segment_timings WHERE attempt_id = ?`, attemptID); err != nil {
		return fmt.Errorf("shared segment timings delete: %w", err)
	}
	nowSeg := nowRFC3339()
	for _, seg := range segments {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO task_attempt_segment_timings (
				attempt_id, job_id, task_id, worker_id,
				segment_index, scene_worker_index, source_type,
				scene_id,
				duration_ms, asset_download_ms, ffmpeg_encode_ms,
				source_bytes, output_bytes, frames_encoded,
				frames_decoded, frames_composited, ffmpeg_speed_x,
				codec, preset, ffmpeg_threads,
				status, error_code, error_message,
				source_url_hash, cache_key,
				input_duration_ms, output_duration_ms,
				metadata_json, created_at,
				started_offset_ms, finished_offset_ms,
				worker_slot, cpu_threads, parallel_group
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?
			)`,
			attemptID, seg.JobID, seg.TaskID, seg.WorkerID,
			seg.SegmentIndex, seg.SceneWorkerIndex, seg.SourceType, seg.SceneID,
			seg.DurationMS, seg.AssetDownloadMS, seg.FfmpegEncodeMS,
			seg.SourceBytes, seg.OutputBytes, seg.FramesEncoded,
			seg.FramesDecoded, seg.FramesComposited, seg.FfmpegSpeedX,
			seg.Codec, seg.Preset, seg.FfmpegThreads,
			seg.Status, seg.ErrorCode, seg.ErrorMessage,
			seg.SourceURLHash, seg.CacheKey,
			seg.InputDurationMS, seg.OutputDurationMS,
			seg.MetadataJSON, nowSeg,
			seg.StartedOffsetMS, seg.FinishedOffsetMS,
			seg.WorkerSlot, seg.CPUThreads, seg.ParallelGroup,
		)
		if err != nil {
			return fmt.Errorf("shared segment timing insert %d: %w", seg.SegmentIndex, err)
		}
	}

	p := computeParallelism(segments, metrics)
	p.AttemptID = attemptID

	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_parallelism (
			attempt_id,
			configured_segment_workers, ffmpeg_threads_per_segment,
			logical_cpu_count, cpu_budget,
			serial_work_ms, render_window_ms, union_busy_ms,
			overlap_ms, idle_gap_ms,
			peak_concurrency, average_concurrency,
			speedup_vs_serial, parallel_efficiency_ratio,
			cpu_oversubscription_ratio,
			bottleneck_phase, parallel_strategy, calculated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.AttemptID,
		p.ConfiguredSegmentWorkers, p.FFmpegThreadsPerSegment,
		p.LogicalCPUCount, p.CPUBudget,
		p.SerialWorkMS, p.RenderWindowMS, p.UnionBusyMS,
		p.OverlapMS, p.IdleGapMS,
		p.PeakConcurrency, p.AverageConcurrency,
		p.SpeedupVsSerial, p.ParallelEfficiency,
		p.CPUOversubscription,
		p.BottleneckPhase, p.ParallelStrategy, p.CalculatedAt,
	)
	if err != nil {
		return fmt.Errorf("shared parallelism persist: %w", err)
	}
	return nil
}
