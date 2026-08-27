package worker

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func classifyTaskResultOutcome(report *taskrunner.TaskExecutionReport, execErr error) (string, string, string) {
	status := "succeeded"
	var code, detail string
	if report != nil && report.Status == "failed" {
		status = "failed"
		code = report.ErrorCode
		detail = report.ErrorDetail
	}
	if execErr != nil {
		status = "failed"
		if errors.Is(execErr, context.Canceled) {
			status = "cancelled"
		}
		detail = execErr.Error()
		if report != nil && report.ErrorCode != "" {
			code = report.ErrorCode
		}
	}
	return status, code, detail
}

func buildTaskResult(r *taskResultReporter, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, status, errorCode, errorDetail string) *pb.TaskResult {
	result := &pb.TaskResult{
		TaskId: taskID, JobId: pte.JobID, AttemptId: attemptID,
		Status: status, ErrorCode: errorCode, ErrorDetail: errorDetail,
		ExecutorId: pte.ExecutorID, LeaseId: pte.LeaseID,
		AttemptNumber: int32(pte.AttemptNumber), Revision: int32(pte.Revision),
		ReportSchemaVersion: 1, ReportVersion: 1,
		TelemetrySchemaVersion: telemetry.SchemaVersion,
	}
	if report == nil {
		return result
	}
	attachWorkerIdentityAndTimings(r.workerID, report)
	result.ExecutorKey = report.ExecutorKey
	metrics := report.RawMetrics
	if metrics == nil {
		metrics = report.TypedMetrics
	}
	if metrics == nil && len(report.Metrics) > 0 {
		metrics = taskrunner.TypedMetricsFromMap(report.Metrics)
	}
	if metrics != nil {
		copyMetrics := *metrics
		if copyMetrics.OutputSha256 == "" && len(report.Outputs) > 0 {
			copyMetrics.OutputSha256 = report.Outputs[0].Hash
		}
		result.ExecutionMetrics = copyMetrics.ToProto()
	}
	for _, marker := range report.PhaseMarkers {
		result.PhaseMarkers = append(result.PhaseMarkers, &pb.PhaseMarker{
			Name: marker.Name, StartedAt: timestamppb.New(marker.StartedAt),
			CompletedAt: timestamppb.New(marker.CompletedAt), Status: marker.Status,
			Notes: marker.Notes,
		})
	}
	result.PhaseTimings = appendDetailedPhaseTimings(result.PhaseTimings, report.DetailedPhases, pte.LeaseID, pte.ExecutorID, int32(pte.ExecutorVersion))
	for _, stage := range report.Waterfall {
		result.Waterfall = append(result.Waterfall, &pb.AttemptWaterfallStage{
			Name: stage.Name, StartedAt: timestamppb.New(stage.StartedAt),
			CompletedAt: timestamppb.New(stage.CompletedAt), DurationMs: stage.DurationMS,
			Status: stage.Status,
		})
	}
	for _, ms := range report.Milestones {
		result.Milestones = append(result.Milestones, &pb.AttemptMilestone{
			Name: string(ms.Name), Sequence: ms.Sequence, ElapsedMs: ms.ElapsedMS, OccurredAt: ms.OccurredAt,
		})
	}
	if report.AssetPreparation != nil {
		p := report.AssetPreparation
		result.AssetPreparation = &pb.AssetPreparationBreakdown{
			AssetsRequired:          p.AssetsRequired,
			AssetsUnique:            p.AssetsUnique,
			CacheHits:               p.CacheHits,
			CacheMisses:             p.CacheMisses,
			ReadyBeforeAttempt:      p.ReadyBeforeAttempt,
			DownloadedDuringAttempt: p.DownloadedDuringAttempt,
			CacheLookupMs:           p.CacheLookupMS,
			RemoteWaitMs:            p.RemoteWaitMS,
			RemoteWaitCount:         p.RemoteWaitCount,
			DownloadWallMs:          p.DownloadWallMS,
			DownloadWorkMs:          p.DownloadWorkMS,
			HashVerifyMs:            p.HashVerifyMS,
			MetadataProbeMs:         p.MetadataProbeMS,
			MaterializeLocalMs:      p.MaterializeLocalMS,
		}
	}
	for _, segment := range report.Segments {
		result.SegmentTimings = append(result.SegmentTimings, &pb.SegmentTiming{
			SegmentIndex: int32(segment.SegmentIndex), SceneWorkerIndex: int32(segment.SceneWorkerIndex),
			SceneId: segment.SceneID, SourceType: segment.SourceType, DurationMs: segment.DurationMS,
			AssetDownloadMs: segment.AssetDownloadMS, FfmpegEncodeMs: segment.FfmpegEncodeMS,
			SourceBytes: segment.SourceBytes, OutputBytes: segment.OutputBytes,
			FramesEncoded: segment.FramesEncoded, FramesDecoded: segment.FramesDecoded,
			FramesComposited: segment.FramesComposited, FfmpegSpeedX: segment.FfmpegSpeedX,
			Codec: segment.Codec, Preset: segment.Preset, FfmpegThreads: int32(segment.FfmpegThreads),
			Status: segment.Status, ErrorCode: segment.ErrorCode, ErrorMessage: segment.ErrorMessage,
			SourceUrlHash: segment.SourceURLHash, CacheKey: segment.CacheKey,
			InputDurationMs: segment.InputDurationMS, OutputDurationMs: segment.OutputDurationMS,
			MetadataJson: segment.MetadataJSON, StartedOffsetMs: segment.StartedOffsetMS,
			FinishedOffsetMs: segment.FinishedOffsetMS, WorkerSlot: int32(segment.WorkerSlot),
			CpuThreads: int32(segment.CPUThreads), ParallelGroup: segment.ParallelGroup,
		})
	}
	for _, ref := range report.Outputs {
		artifactID := ref.ArtifactID
		if artifactID == "" {
			artifactID = ref.Hash
		}
		if value, err := structpb.NewStruct(map[string]interface{}{
			"artifact_id": artifactID, "artifact_type": ref.Type,
			"artifact_path": ref.URI, "size_bytes": ref.SizeBytes, "sha256": ref.Hash,
		}); err == nil {
			result.OutputArtifacts = append(result.OutputArtifacts, value)
		}
	}
	return result
}

func appendDetailedPhaseTimings(dst []*pb.PhaseTimingDetailed, phases []taskrunner.DetailedPhaseTiming, leaseID, executorID string, executorVersion int32) []*pb.PhaseTimingDetailed {
	for _, phase := range phases {
		value := phase.ToProto()
		if value.ExecutorId == "" {
			value.ExecutorId = executorID
		}
		if value.ExecutorVersion == 0 {
			value.ExecutorVersion = executorVersion
		}
		if value.LeaseId == "" {
			value.LeaseId = leaseID
		}
		dst = append(dst, value)
	}
	return dst
}

// publishTaskResult builds the wire envelope, sends it, and returns the
// WorkerToMasterEnvelope.sent_at timestamp. The caller uses this to stamp
// result.sent with the exact transport boundary rather than the wall clock
// after Submit() returns.
func (r *taskResultReporter) publishTaskResult(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, result *pb.TaskResult, status string, startedAt time.Time) time.Time {
	sentAt := time.Now().UTC()
	if r.spool == nil {
		transport := r.transport()
		if transport == nil {
			r.logger.Error("[TASK] TaskResult transport unavailable for %s", taskID)
			return sentAt
		}
		msg := controltransport.NewTypedMessage(controltransport.MsgTaskResult, r.workerID, r.protocol, result)
		if err := transport.Send(ctx, msg); err != nil {
			r.logger.Error("[TASK] Failed to submit TaskResult for %s: %v", taskID, err)
			return sentAt
		}
		// The message's SentAt is the true wire timestamp — it was set when
		// NewTypedMessage created the envelope, before Send() serialized it.
		sentAt = msg.SentAt
		r.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d, sent_at: %s)", taskID, status, artifactReportOutputCount(report), sentAt.Format(time.RFC3339Nano))
		if r.logArtifact != nil {
			r.logArtifact("TASK_RESULT_SENT", pte, startedAt, "", "", "", map[string]interface{}{"status": status, "report_hash": result.GetReportHash(), "artifact_count": artifactReportOutputCount(report), "sent_at": sentAt.Format(time.RFC3339Nano)})
		}
		return sentAt
	}
	if err := r.persistTaskResult(ctx, result); err != nil {
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to persist TaskResult for %s: %v", taskID, err)
		return sentAt
	}
	payload, err := proto.Marshal(result)
	if err != nil {
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to marshal TaskResult for %s: %v", taskID, err)
		return sentAt
	}
	entry := spool.TaskResultOutboxEntry{TaskID: taskID, AttemptID: attemptID, ReportHash: result.GetReportHash(), Payload: payload}
	ackCh := r.registerTaskResultAck(pte.JobID, taskID, attemptID)
	defer r.unregisterTaskResultAck(pte.JobID, taskID, attemptID)
	if err := r.sendTaskResultAttempt(ctx, entry); err != nil {
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to submit TaskResult for %s: %v", taskID, err)
		return sentAt
	}
	wait := taskResultAckWait
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait {
		wait = time.Until(deadline)
	}
	if wait <= 0 {
		return sentAt
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ackCh:
	case <-timer.C:
		r.logger.Warn("[TASK_RESULT_OUTBOX] TaskResultAck not received before wait window task=%s attempt=%s", taskID, attemptID)
	case <-ctx.Done():
	}
	r.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d, sent_at: %s)", taskID, status, artifactReportOutputCount(report), sentAt.Format(time.RFC3339Nano))
	return sentAt
}

func (r *taskResultReporter) persistTaskResult(ctx context.Context, result *pb.TaskResult) error {
	if r.spool == nil {
		return errors.New("task result outbox is not configured")
	}
	payload, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	return r.spool.UpsertTaskResult(ctx, result.GetTaskId(), result.GetAttemptId(), result.GetReportHash(), payload)
}

func (r *taskResultReporter) sendTaskResultAttempt(ctx context.Context, entry spool.TaskResultOutboxEntry) error {
	var result pb.TaskResult
	if err := proto.Unmarshal(entry.Payload, &result); err != nil {
		return err
	}
	transport := r.transport()
	if transport == nil {
		return errors.New("task result transport is unavailable")
	}
	claimed, err := r.spool.ClaimTaskResultAttempt(ctx, entry.TaskID, entry.AttemptID, entry.ReportHash, entry.AttemptCount, time.Now(), time.Now().Add(taskResultRetryDelay(entry.AttemptCount+1)))
	if err != nil {
		return err
	}
	if !claimed {
		return errors.New("task result outbox entry was claimed by another sender")
	}
	return transport.Send(ctx, controltransport.NewTypedMessage(controltransport.MsgTaskResult, r.workerID, r.protocol, &result))
}

func taskResultRetryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return taskResultRetryInitial
	}
	delay := float64(taskResultRetryInitial) * math.Pow(2, float64(attempt-1))
	if delay > float64(taskResultRetryMax) {
		return taskResultRetryMax
	}
	return time.Duration(delay)
}

type taskResultAckCacheEntry struct {
	ack        *pb.TaskResultAck
	receivedAt time.Time
}

func taskResultAckKey(jobID, taskID, attemptID string) string {
	return jobID + "\x00" + taskID + "\x00" + attemptID
}

func (r *taskResultReporter) registerTaskResultAck(jobID, taskID, attemptID string) chan *pb.TaskResultAck {
	r.acksMu.Lock()
	defer r.acksMu.Unlock()
	if r.acks == nil {
		r.acks = make(map[string]chan *pb.TaskResultAck)
	}
	if r.ackCache == nil {
		r.ackCache = make(map[string]taskResultAckCacheEntry)
	}
	key := taskResultAckKey(jobID, taskID, attemptID)
	ch := make(chan *pb.TaskResultAck, 1)
	if cached := r.ackCache[key]; cached.ack != nil {
		delete(r.ackCache, key)
		ch <- cached.ack
		return ch
	}
	r.acks[key] = ch
	return ch
}

func (r *taskResultReporter) unregisterTaskResultAck(jobID, taskID, attemptID string) {
	r.acksMu.Lock()
	defer r.acksMu.Unlock()
	delete(r.acks, taskResultAckKey(jobID, taskID, attemptID))
}

func taskResultAckIsTerminal(ack *pb.TaskResultAck) bool {
	return ack != nil && (ack.GetError() == "" || ack.GetError() == "report_conflict")
}

func (r *taskResultReporter) validateTaskResultAck(ctx context.Context, ack *pb.TaskResultAck) (string, bool) {
	if ack == nil || ack.GetTaskId() == "" || ack.GetJobId() == "" || ack.GetAttemptId() == "" || r.spool == nil {
		return "", false
	}
	entries, err := r.spool.ListTaskResultsForAttempt(ctx, ack.GetTaskId(), ack.GetAttemptId())
	if err != nil {
		return "", false
	}
	for _, entry := range entries {
		var result pb.TaskResult
		if proto.Unmarshal(entry.Payload, &result) != nil {
			continue
		}
		if result.GetTaskId() == ack.GetTaskId() && result.GetJobId() == ack.GetJobId() && result.GetAttemptId() == ack.GetAttemptId() && result.GetReportHash() == entry.ReportHash {
			return entry.ReportHash, true
		}
	}
	return "", false
}

func (r *taskResultReporter) HandleAck(ack *pb.TaskResultAck) {
	if ack == nil || ack.GetTaskId() == "" || ack.GetJobId() == "" || ack.GetAttemptId() == "" {
		return
	}
	if _, ok := r.validateTaskResultAck(context.Background(), ack); !ok {
		return
	}
	key := taskResultAckKey(ack.GetJobId(), ack.GetTaskId(), ack.GetAttemptId())
	r.acksMu.Lock()
	ch := r.acks[key]
	if ch == nil {
		if r.ackCache == nil {
			r.ackCache = make(map[string]taskResultAckCacheEntry)
		}
		r.ackCache[key] = taskResultAckCacheEntry{ack: ack, receivedAt: time.Now()}
		r.expireTaskResultAckCacheLocked(time.Now())
	}
	if ch != nil {
		select {
		case ch <- ack:
		default:
		}
	}
	r.acksMu.Unlock()
	if !taskResultAckIsTerminal(ack) {
		return
	}
	if r.spool != nil {
		_, _ = r.spool.DeleteTaskResultsForAttempt(context.Background(), ack.GetTaskId(), ack.GetAttemptId())
		r.cleanupCommittedAttemptOutputs(ack.GetTaskId(), ack.GetAttemptId())
	}
	if r.onTerminal != nil {
		r.onTerminal()
	}
}

func (r *taskResultReporter) cleanupCommittedAttemptOutputs(taskID, attemptID string) {
	if r.spool == nil {
		return
	}
	entries, err := r.spool.ListByAttempt(context.Background(), taskID, attemptID)
	if err != nil {
		return
	}
	roots := []string{r.outputDir}
	if r.storageResolver != nil {
		cfg := r.storageResolver.Config()
		roots = append(roots, cfg.ArtifactDir, cfg.ArtifactStaging.Dir)
	}
	for _, entry := range entries {
		if entry.Status != spool.StatusCommitted && entry.Status != spool.StatusRejected {
			continue
		}
		if entry.LocalPath != "" {
			path, absErr := filepath.Abs(entry.LocalPath)
			if absErr != nil || !pathWithinAnyRoot(roots, path) {
				continue
			}
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				continue
			}
			if entry.StorageTier == spool.StorageTierTmpfsVolatile && r.storageResolver != nil {
				r.storageResolver.ReleaseStagingPath(path)
			}
		}
		_ = r.spool.MarkCleaned(context.Background(), entry.SpoolID)
	}
}

func (r *taskResultReporter) expireTaskResultAckCacheLocked(now time.Time) {
	for key, cached := range r.ackCache {
		if now.Sub(cached.receivedAt) >= taskResultAckCacheTTL {
			delete(r.ackCache, key)
		}
	}
}

func (r *taskResultReporter) replayDueTaskResults(ctx context.Context) {
	if r.spool == nil || r.transport() == nil {
		return
	}
	entries, err := r.spool.ListDueTaskResults(ctx, time.Now(), taskResultReplayBatch)
	if err != nil {
		return
	}
	for _, entry := range entries {
		_ = r.sendTaskResultAttempt(ctx, entry)
	}
}

func (r *taskResultReporter) StartReplayLoop(ctx context.Context) {
	if r.wg == nil {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(taskResultRetryInitial)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopChan:
				return
			case <-ticker.C:
				r.acksMu.Lock()
				r.expireTaskResultAckCacheLocked(time.Now())
				r.acksMu.Unlock()
				r.replayDueTaskResults(ctx)
			}
		}
	}()
}
