package observability

import (
	"context"
	"fmt"
	"time"
)

// SummarizeTask returns the aggregated execution diagnostics for a task.
func (s *Service) SummarizeTask(ctx context.Context, taskID string) (*ExecutionSummary, error) {
	task, err := s.tasks.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize: %w", err)
	}
	if task == nil {
		return nil, fmt.Errorf("observability summarize: task %s not found", taskID)
	}

	attempts, err := s.attempts.ListByTaskID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize attempts: %w", err)
	}

	summary := &ExecutionSummary{
		TaskID:       task.ID,
		JobID:        task.JobID,
		TaskStatus:   task.Status,
		AttemptCount: task.AttemptCount,
		PhaseTotals:  make(map[string]int64),
	}

	// Durable metrics are final-report data. Overlay the same canonical
	// Attempt with the live worker_task_runtime row while it is RUNNING.
	// Keep the live row aside until durable attempts are loaded so a matching
	// attempt is enriched in place rather than appended twice.
	var live *LiveAttempt
	if s.liveAttempts != nil {
		var candidate *LiveAttempt
		var liveErr error
		if taskReader, ok := s.liveAttempts.(LiveAttemptTaskReader); ok {
			candidate, liveErr = taskReader.GetWorkerTaskRuntimeByTask(ctx, task.ID, task.JobID)
		} else {
			candidate, liveErr = s.liveAttempts.GetWorkerTaskRuntimeByJob(ctx, task.JobID)
		}
		if liveErr == nil && candidate != nil && candidate.TaskID == task.ID && candidate.AttemptID != "" {
			live = candidate
			if live.AttemptNumber > summary.AttemptCount {
				summary.AttemptCount = live.AttemptNumber
			}
		}
	}
	liveDecision := reconcileLiveAttempt(live, task, attempts)

	var firstStart *time.Time
	var lastEnd *time.Time

	for _, a := range attempts {
		as := AttemptSummary{
			AttemptID:      a.ID,
			AttemptNumber:  a.AttemptNumber,
			Status:         a.Status,
			WorkerID:       a.WorkerID,
			ErrorCode:      a.ErrorCode,
			ErrorMessage:   a.ErrorMessage,
			PhaseBreakdown: make(map[string]int64),
		}
		as.WorkerName = s.workerDisplayName(as.WorkerID)
		// Reconciliation is decided once before this loop. Only volatile
		// progress fields are overlaid; durable identity, status, errors,
		// timestamps, and final metrics remain authoritative.
		if live != nil && liveDecision.overlaysAttempt(a.ID) {
			applyLiveAttemptOverlay(&as, live)
		}
		if a.StartedAt != nil {
			as.StartedAt = a.StartedAt.UTC().Format(time.RFC3339Nano)
		}
		if a.CompletedAt != nil {
			as.CompletedAt = a.CompletedAt.UTC().Format(time.RFC3339Nano)
		}

		// Phase timings
		timings, err := s.attempts.GetPhaseTimings(ctx, a.ID)
		if err == nil {
			var totalDur int64
			var attemptFirstStart *time.Time
			var attemptLastEnd *time.Time
			for _, pt := range timings {
				as.PhaseBreakdown[pt.Phase] += pt.DurationMS
				summary.PhaseTotals[pt.Phase] += pt.DurationMS
				summary.PhaseTimings = append(summary.PhaseTimings, PhaseSnapshot{
					AttemptID: pt.AttemptID, Phase: pt.Phase, DurationMS: pt.DurationMS,
					WallStart: pt.WallStart, WallEnd: pt.WallEnd,
				})
				totalDur += pt.DurationMS
				if !pt.WallStart.IsZero() && (attemptFirstStart == nil || pt.WallStart.Before(*attemptFirstStart)) {
					start := pt.WallStart
					attemptFirstStart = &start
				}
				if !pt.WallEnd.IsZero() && (attemptLastEnd == nil || pt.WallEnd.After(*attemptLastEnd)) {
					end := pt.WallEnd
					attemptLastEnd = &end
				}
			}
			// Phase timings can overlap (for example render contains
			// compile/encode/audio). Report wall duration for the attempt;
			// retain the sum only for legacy rows without wall bounds.
			if attemptFirstStart != nil && attemptLastEnd != nil {
				as.DurationMS = attemptLastEnd.Sub(*attemptFirstStart).Milliseconds()
			} else {
				as.DurationMS = totalDur
			}

			for _, timing := range timings {
				// Summary rows (for example quality/ffprobe) may carry no
				// wall-clock bounds. Never let a zero timestamp replace a
				// valid execution bound: time.Time.Sub would otherwise
				// saturate and expose MaxInt64-like wall times.
				if !timing.WallStart.IsZero() && (firstStart == nil || timing.WallStart.Before(*firstStart)) {
					start := timing.WallStart
					firstStart = &start
				}
				if !timing.WallEnd.IsZero() && (lastEnd == nil || timing.WallEnd.After(*lastEnd)) {
					end := timing.WallEnd
					lastEnd = &end
				}
			}
		}

		// Metrics
		metrics, err := s.attempts.GetMetrics(ctx, a.ID)
		if err == nil && metrics != nil {
			as.Metrics = metrics
			summary.TotalInputBytes += metrics.InputBytes
			summary.TotalOutputBytes += metrics.OutputBytes
			summary.BytesFromDrive += metrics.BytesFromDrive
			summary.BytesFromBlobstore += metrics.BytesFromBlobstore
			summary.BytesFromLocalCache += metrics.BytesFromLocalCache
			summary.CPUTimeMS += metrics.CPUTimeMS
			summary.GPUTimeMS += metrics.GPUTimeMS
			if metrics.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = metrics.PeakRSSBytes
			}
			if metrics.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = metrics.PeakVRAMBytes
			}
		} else if live != nil && liveDecision.overlaysAttempt(a.ID) {
			// Before final TaskResult ingest, expose the same typed metric
			// shape that the final report will persist. This is a projection
			// of worker_task_runtime, not a second telemetry store; once the
			// durable row exists it remains authoritative above.
			as.Metrics = liveAttemptMetrics(live)
			summary.TotalInputBytes += as.Metrics.InputBytes
			summary.TotalOutputBytes += as.Metrics.OutputBytes
			summary.BytesFromDrive += as.Metrics.BytesFromDrive
			summary.BytesFromBlobstore += as.Metrics.BytesFromBlobstore
			summary.BytesFromLocalCache += as.Metrics.BytesFromLocalCache
			summary.CPUTimeMS += as.Metrics.CPUTimeMS
			summary.GPUTimeMS += as.Metrics.GPUTimeMS
			if as.Metrics.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = as.Metrics.PeakRSSBytes
			}
			if as.Metrics.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = as.Metrics.PeakVRAMBytes
			}
		}

		// Cache counters are a separate typed row because they are not
		// part of the legacy execution-metrics envelope.
		cacheStats, cacheErr := s.attempts.GetCacheStats(ctx, a.ID)
		if cacheErr == nil && cacheStats != nil {
			as.CacheStats = cacheStats
			summary.Cache.Hits += cacheStats.CacheHits
			summary.Cache.Misses += cacheStats.CacheMisses
			summary.Cache.Evictions += cacheStats.CacheEvictions
			summary.Cache.Corruptions += cacheStats.CacheCorruptions
			summary.Cache.BytesUsed += cacheStats.CacheBytesUsed
			summary.Cache.Entries += int64(cacheStats.CacheEntries)
			summary.Cache.Lookups += cacheStats.CacheLookups
			summary.Cache.UniqueAssetsRequested += cacheStats.UniqueAssetsRequested
			summary.Cache.DownloadCount += cacheStats.CacheDownloadCount
			summary.Cache.DownloadBytes += cacheStats.CacheDownloadBytes
		}

		if segmentReader, ok := s.attempts.(SegmentReader); ok {
			if segments, segmentErr := segmentReader.ListSegmentTimings(ctx, a.ID); segmentErr == nil {
				for _, segment := range segments {
					summary.Segments = append(summary.Segments, SegmentSnapshot{
						AttemptID: segment.AttemptID, SegmentIndex: segment.SegmentIndex,
						SceneID: segment.SceneID, SourceType: segment.SourceType,
						AssetKey: segment.CacheKey, SourceURLHash: segment.SourceURLHash,
						Codec: segment.Codec, Preset: segment.Preset,
						DurationMS:      segment.DurationMS,
						AssetDownloadMS: segment.AssetDownloadMS, FFmpegEncodeMS: segment.FfmpegEncodeMS,
						SourceBytes: segment.SourceBytes, OutputBytes: segment.OutputBytes,
						InputDurationMS: segment.InputDurationMS, OutputDurationMS: segment.OutputDurationMS,
						FramesEncoded: segment.FramesEncoded, FramesDecoded: segment.FramesDecoded,
						FramesComposited: segment.FramesComposited, FFmpegSpeedX: segment.FfmpegSpeedX,
						Status: segment.Status,
					})
				}
			}
		}

		summary.Attempts = append(summary.Attempts, as)
	}
	if live != nil && liveDecision.hasTemporaryOverlay() {
		// Claim/accept visibility window: expose the volatile row as a
		// temporary overlay until the durable attempt becomes readable.
		pending := AttemptSummary{
			AttemptID: live.AttemptID, AttemptNumber: live.AttemptNumber,
			Status: liveAttemptStatus(live), WorkerID: live.WorkerID,
			WorkerName: s.workerDisplayName(live.WorkerID),
			Metrics:    liveAttemptMetrics(live),
		}
		applyLiveAttemptOverlay(&pending, live)
		summary.Attempts = append(summary.Attempts, pending)
	}

	// Derive the top-level live projection from the reconciled attempt row,
	// never directly from the volatile reader. This keeps the endpoint's
	// compact fields and Attempts slice on one authority.
	for i := range summary.Attempts {
		if summary.Attempts[i].Live {
			applyExecutionLiveOverlay(summary, &summary.Attempts[i])
			break
		}
	}
	if firstStart != nil && lastEnd != nil {
		summary.TotalWallTimeMS = lastEnd.Sub(*firstStart).Milliseconds()
	}
	if task.AttemptCount > 1 {
		summary.Retries = task.AttemptCount - 1
	}
	cacheTotal := summary.Cache.Hits + summary.Cache.Misses
	if cacheTotal > 0 {
		summary.Cache.HitRatio = float64(summary.Cache.Hits) / float64(cacheTotal)
	}

	return summary, nil
}

// SummarizeJob returns the aggregated diagnostics for the task owning a job.
func (s *Service) SummarizeJob(ctx context.Context, jobID string) (*ExecutionSummary, error) {
	task, err := s.tasks.GetByJobID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("observability: no task for job %s", jobID)
	}
	return s.SummarizeTask(ctx, task.ID)
}
