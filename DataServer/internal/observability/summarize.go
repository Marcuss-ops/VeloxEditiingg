package observability

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
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
		if liveErr != nil {
			return nil, fmt.Errorf("observability summarize live runtime: %w", liveErr)
		}
		if candidate != nil && candidate.TaskID == task.ID && candidate.AttemptID != "" {
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
		lifecycleDuration, hasLifecycleDuration := attemptLifecycleDurationMS(a)
		if err == nil {
			phaseDuration := rollupPhaseTimings(timings, &as, summary)
			if !hasLifecycleDuration {
				as.DurationMS = phaseDuration
				firstStart, lastEnd = mergeWallBounds(timings, firstStart, lastEnd)
			}
		}
		if hasLifecycleDuration {
			// Attempt lifecycle timestamps are the authoritative wall-clock
			// boundary. Phase rows are overlapping detail and may contain a
			// late/malformed finalization timestamp; they must never extend the
			// reported attempt duration or task wall time.
			as.DurationMS = lifecycleDuration
			firstStart = earlierTime(firstStart, *a.StartedAt)
			lastEnd = laterTime(lastEnd, *a.CompletedAt)
		}
		if reports, ok := s.attempts.(AttemptReportReader); ok {
			raw, reportErr := reports.GetRawReportJSON(ctx, a.ID)
			if reportErr != nil {
				return nil, fmt.Errorf("observability summarize waterfall for attempt %s: %w", a.ID, reportErr)
			}
			as.Waterfall, as.WaterfallValid = decodeWaterfall(raw, a.StartedAt, a.CompletedAt)
			if hasLifecycleDuration {
				as.AttemptWaterfall = decodeAttemptWaterfall(raw, a.ID, lifecycleDuration)
			}
			// Master-local report timestamps for the result_ingest diagnostic.
			// Both come from the Master clock (received_at / persisted_at), so
			// receive→commit lag is computed locally; the worker clock is never
			// subtracted from these. Zero time (no report row) leaves them empty.
			received, receivedErr := reports.GetReportReceivedAt(ctx, a.ID)
			if receivedErr != nil {
				return nil, fmt.Errorf("observability summarize report received_at for attempt %s: %w", a.ID, receivedErr)
			}
			committed, committedErr := reports.GetReportCommittedAt(ctx, a.ID)
			if committedErr != nil {
				return nil, fmt.Errorf("observability summarize report committed_at for attempt %s: %w", a.ID, committedErr)
			}
			if !received.IsZero() {
				as.MasterReceivedAt = received.UTC().Format(time.RFC3339Nano)
			}
			if !committed.IsZero() {
				as.MasterCommittedAt = committed.UTC().Format(time.RFC3339Nano)
			}
		}

		// Metrics
		metrics, err := s.attempts.GetMetrics(ctx, a.ID)
		if err == nil && metrics != nil {
			// The worker metric is retained for resource accounting, but the
			// canonical wall-clock value exposed by this read model comes from
			// the durable attempt lifecycle. This prevents a malformed phase
			// timestamp from presenting a five-minute render as eight minutes.
			metricsCopy := *metrics
			if lifecycleDuration, ok := attemptLifecycleDurationMS(a); ok {
				metricsCopy.WallClockSeconds = float64(lifecycleDuration) / 1000
			}
			as.Metrics = &metricsCopy
			summary.TotalInputBytes += metricsCopy.InputBytes
			summary.TotalOutputBytes += metricsCopy.OutputBytes
			summary.BytesFromDrive += metricsCopy.BytesFromDrive
			summary.BytesFromBlobstore += metricsCopy.BytesFromBlobstore
			summary.BytesFromLocalCache += metricsCopy.BytesFromLocalCache
			summary.CPUTimeMS += metricsCopy.CPUTimeMS
			summary.GPUTimeMS += metricsCopy.GPUTimeMS
			if metricsCopy.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = metricsCopy.PeakRSSBytes
			}
			if metricsCopy.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = metricsCopy.PeakVRAMBytes
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
		if cacheErr != nil {
			return nil, fmt.Errorf("observability summarize cache stats for attempt %s: %w", a.ID, cacheErr)
		}
		if cacheStats != nil {
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
			segments, segmentErr := segmentReader.ListSegmentTimings(ctx, a.ID)
			if segmentErr != nil {
				return nil, fmt.Errorf("observability summarize segment timings for attempt %s: %w", a.ID, segmentErr)
			}
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
	// Execution-level waterfall projection (§20): surface the most recent
	// attempt's durable milestone timeline at the top of the summary so
	// `fleetctl job inspect` reads wall_ms / accounted_ms / coverage_pct /
	// buckets without digging into attempts[]. The reversed scan keeps this
	// honest: the newest execution truth wins, an earlier attempt with a
	// durable report is the fallback, and jobs predating milestone support
	// keep the legacy shape (omitempty → no waterfall field).
	for i := len(summary.Attempts) - 1; i >= 0; i-- {
		if summary.Attempts[i].AttemptWaterfall != nil {
			summary.Waterfall = summary.Attempts[i].AttemptWaterfall
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

// attemptLifecycleDurationMS returns the authoritative elapsed wall time for
// a terminal attempt. Phase timings are intentionally excluded: they overlap
// and are diagnostic detail, not the attempt clock.
func attemptLifecycleDurationMS(a taskattempts.TaskAttempt) (int64, bool) {
	if a.StartedAt == nil || a.CompletedAt == nil {
		return 0, false
	}
	duration := a.CompletedAt.Sub(*a.StartedAt).Milliseconds()
	if duration < 0 {
		return 0, false
	}
	return duration, true
}

// rollupPhaseTimings folds one attempt's phase timings into its phase
// breakdown and the shared ExecutionSummary (phase totals and the ordered
// PhaseTimings list), and returns the attempt wall duration: the span from
// the earliest WallStart to the latest WallEnd, or the summed durations when
// the rows carry no wall bounds (legacy rows / summary-only phases such as
// quality/ffprobe).
func rollupPhaseTimings(timings []taskattempts.PhaseTiming, as *AttemptSummary, summary *ExecutionSummary) int64 {
	var totalDur int64
	var firstStart, lastEnd *time.Time
	for _, pt := range timings {
		as.PhaseBreakdown[pt.Phase] += pt.DurationMS
		summary.PhaseTotals[pt.Phase] += pt.DurationMS
		summary.PhaseTimings = append(summary.PhaseTimings, PhaseSnapshot{
			AttemptID: pt.AttemptID, Phase: pt.Phase, DurationMS: pt.DurationMS,
			WallStart: pt.WallStart, WallEnd: pt.WallEnd,
		})
		totalDur += pt.DurationMS
		firstStart = earlierTime(firstStart, pt.WallStart)
		lastEnd = laterTime(lastEnd, pt.WallEnd)
	}
	if firstStart != nil && lastEnd != nil {
		return lastEnd.Sub(*firstStart).Milliseconds()
	}
	return totalDur
}

// mergeWallBounds extends the task-level wall-clock bounds with this attempt's
// phase timing rows. A zero timestamp never replaces a valid bound:
// time.Time.Sub on a zero value would otherwise saturate and expose
// MaxInt64-like wall times.
func mergeWallBounds(timings []taskattempts.PhaseTiming, firstStart, lastEnd *time.Time) (*time.Time, *time.Time) {
	for _, timing := range timings {
		firstStart = earlierTime(firstStart, timing.WallStart)
		lastEnd = laterTime(lastEnd, timing.WallEnd)
	}
	return firstStart, lastEnd
}

func earlierTime(cur *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() {
		return cur
	}
	if cur == nil || candidate.Before(*cur) {
		return &candidate
	}
	return cur
}

func laterTime(cur *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() {
		return cur
	}
	if cur == nil || candidate.After(*cur) {
		return &candidate
	}
	return cur
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
