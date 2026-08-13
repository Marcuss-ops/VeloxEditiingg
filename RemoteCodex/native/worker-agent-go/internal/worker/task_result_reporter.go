// Package worker — durable TaskResult reporting subsystem.
//
// task_result_reporter.go owns the reporting slice: it turns a
// (taskrunner.TaskExecutionReport, execErr) pair into the canonical
// pb.TaskResult wire message, persists it through the durable outbox,
// waits for the master's terminal TaskResultAck, replays pending rows on
// a bounded loop, and performs the committed-output cleanup that follows
// a terminal ACK. The Worker composes this subsystem behind the small
// TaskResultReporter interface and does not own the outbox state or the
// ACK waiter registry directly.
package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/logger"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	taskResultRetryInitial = 2 * time.Second
	taskResultRetryMax     = 2 * time.Minute
	taskResultReplayBatch  = 32
	taskResultAckWait      = 30 * time.Second
	taskResultAckCacheTTL  = 2 * time.Minute
)

// TaskResultReporter is the reporting subsystem seam. The Worker holds only
// this interface; the composition root (New) builds the concrete
// implementation and wires its dependencies. The implementation owns the
// durable TaskResult outbox, the ACK waiter registry, the replay loop, and
// the terminal-output cleanup that the Worker previously scattered across
// its own methods.
type TaskResultReporter interface {
	// Submit builds the canonical pb.TaskResult from the report + execErr,
	// durably persists it (when a spool is wired), and sends it through the
	// transport. Terminal cleanup is NOT performed here; it is gated on the
	// master's TaskResultAck (HandleAck).
	Submit(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error)
	// HandleAck dispatches a master TaskResultAck: it unblocks the registered
	// waiter, performs terminal cleanup (outbox delete + committed-output
	// removal), and signals the terminal observer.
	HandleAck(ack *pb.TaskResultAck)
	// StartReplayLoop launches the bounded durable-outbox replay ticker under
	// the supplied context. It is a session-scoped goroutine tracked by the
	// worker's WaitGroup and stopped via the reporter's stop channel.
	StartReplayLoop(ctx context.Context)
}

// artifactProtocolLogger is the callback seam for the structured
// artifact-publication protocol log. The Worker owns the concrete logging
// method; the reporter invokes it without depending on *Worker.
type artifactProtocolLogger func(event string, pte *PendingTaskExecution, startedAt time.Time, commitID, artifactID, uploadID string, fields map[string]interface{})

// taskResultReporter implements TaskResultReporter. All dependencies are
// explicit constructor inputs (never *Worker), so the reporting subsystem
// stays decoupled from the Worker god-struct.
type taskResultReporter struct {
	spool     *spool.Store
	transport func() controltransport.ControlTransport
	workerID  string
	protocol  string
	outputDir string
	logger    *logger.Logger

	// onTerminal is the observer invoked after a terminal TaskResultAck
	// (the Worker wires it to signalTaskTerminal / jobDone).
	onTerminal func()

	logArtifact artifactProtocolLogger

	acksMu   sync.RWMutex
	acks     map[string]chan *pb.TaskResultAck
	ackCache map[string]taskResultAckCacheEntry

	wg       *sync.WaitGroup
	stopChan <-chan struct{}
}

func newTaskResultReporter(cfg taskResultReporterConfig) *taskResultReporter {
	return &taskResultReporter{
		spool:       cfg.spool,
		transport:   cfg.transport,
		workerID:    cfg.workerID,
		protocol:    cfg.protocol,
		outputDir:   cfg.outputDir,
		logger:      cfg.logger,
		onTerminal:  cfg.onTerminal,
		logArtifact: cfg.logArtifact,
		acks:        make(map[string]chan *pb.TaskResultAck),
		ackCache:    make(map[string]taskResultAckCacheEntry),
		wg:          cfg.wg,
		stopChan:    cfg.stopChan,
	}
}

type taskResultReporterConfig struct {
	spool     *spool.Store
	transport func() controltransport.ControlTransport
	workerID  string
	protocol  string
	outputDir string
	logger    *logger.Logger
	onTerminal func()
	logArtifact artifactProtocolLogger
	wg          *sync.WaitGroup
	stopChan    <-chan struct{}
}

// Submit durably sends a typed pb.TaskResult. Terminal cleanup is
// signaled by HandleAck after the master ACK deletes the durable row.
func (r *taskResultReporter) Submit(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) {
	resultStartedAt := time.Now()
	status := "succeeded"
	var errorCode, errorDetail string
	if report != nil && report.Status == "failed" {
		// Preserve a failed report even when the execution wrapper has no
		// separate error. This is important for partial renders whose
		// terminal failure was already classified by TaskRunner.
		status = "failed"
		errorCode = report.ErrorCode
		errorDetail = report.ErrorDetail
	}
	if execErr != nil {
		status = "failed"
		if errors.Is(execErr, context.Canceled) {
			status = "cancelled"
		}
		errorDetail = execErr.Error()
		if report != nil && report.ErrorCode != "" {
			errorCode = report.ErrorCode
		}
	}

	tr := &pb.TaskResult{
		TaskId:        taskID,
		JobId:         pte.JobID,
		AttemptId:     attemptID,
		Status:        status,
		ErrorCode:     errorCode,
		ErrorDetail:   errorDetail,
		ExecutorId:    pte.ExecutorID,
		LeaseId:       pte.LeaseID,
		AttemptNumber: int32(pte.AttemptNumber),
		Revision:      int32(pte.Revision),
	}

	// Stamp PerformanceReport metadata. The worker emits exactly one report
	// per attempt; report_version tracks re-emissions (always 1 on first
	// send) and report_schema_version tracks the report shape.
	tr.ReportSchemaVersion = 1
	tr.ReportVersion = 1
	tr.TelemetrySchemaVersion = sharedtelemetry.SchemaVersion

	if report != nil {
		attachWorkerIdentityAndTimings(r.workerID, report)
		tr.ExecutorKey = report.ExecutorKey

		// Build typed execution_metrics from the canonical raw envelope.
		// Legacy reports are adapted only as a compatibility fallback at
		// this transport boundary.
		if report.RawMetrics == nil && report.TypedMetrics == nil && report.HasLegacyMetrics() {
			report.RawMetrics = (taskrunner.LegacyMetricsAdapter{}).FromMap(report.LegacyMetrics())
			report.TypedMetrics = report.RawMetrics
		}
		metrics := report.RawMetrics
		if metrics == nil {
			metrics = report.TypedMetrics
		}
		if metrics != nil {
			m := *metrics
			// Fall back to the first output artifact's hash when the
			// executor didn't explicitly stamp output_sha256.
			if m.OutputSha256 == "" && len(report.Outputs) > 0 {
				m.OutputSha256 = report.Outputs[0].Hash
			}
			tr.ExecutionMetrics = m.ToProto()
		}

		// Build typed phase_markers.
		for _, pm := range report.PhaseMarkers {
			tr.PhaseMarkers = append(tr.PhaseMarkers, &pb.PhaseMarker{
				Name:        pm.Name,
				StartedAt:   timestamppb.New(pm.StartedAt),
				CompletedAt: timestamppb.New(pm.CompletedAt),
				Status:      pm.Status,
				Notes:       pm.Notes,
			})
		}

		// Build the full detailed phase stream (proto field 20). This is
		// the block-1 replacement for the legacy partial_phase_metrics
		// (field 19); legacy masters ignore it, block-1 masters ingest it
		// into task_execution_events. Lease identity is stamped here (the
		// runner does not know it); the master overrides all identity
		// fields at ingest. Keep this conversion independent of report
		// status: failed attempts carry the same complete prefix/event
		// stream as successful attempts.
		tr.PhaseTimings = appendDetailedPhaseTimings(
			tr.PhaseTimings,
			report.DetailedPhases,
			pte.LeaseID,
			pte.ExecutorID,
			int32(pte.ExecutorVersion),
		)

		// Build per-segment C++ sidecar timings.
		for _, seg := range report.Segments {
			tr.SegmentTimings = append(tr.SegmentTimings, &pb.SegmentTiming{
				SegmentIndex:     int32(seg.SegmentIndex),
				SceneWorkerIndex: int32(seg.SceneWorkerIndex),
				SceneId:          seg.SceneID,
				SourceType:       seg.SourceType,
				DurationMs:       seg.DurationMS,
				AssetDownloadMs:  seg.AssetDownloadMS,
				FfmpegEncodeMs:   seg.FfmpegEncodeMS,
				SourceBytes:      seg.SourceBytes,
				OutputBytes:      seg.OutputBytes,
				FramesEncoded:    seg.FramesEncoded,
				FramesDecoded:    seg.FramesDecoded,
				FramesComposited: seg.FramesComposited,
				FfmpegSpeedX:     seg.FfmpegSpeedX,
				Codec:            seg.Codec,
				Preset:           seg.Preset,
				FfmpegThreads:    int32(seg.FfmpegThreads),
				Status:           seg.Status,
				ErrorCode:        seg.ErrorCode,
				ErrorMessage:     seg.ErrorMessage,
				SourceUrlHash:    seg.SourceURLHash,
				CacheKey:         seg.CacheKey,
				InputDurationMs:  seg.InputDurationMS,
				OutputDurationMs: seg.OutputDurationMS,
				MetadataJson:     seg.MetadataJSON,
				StartedOffsetMs:  seg.StartedOffsetMS,
				FinishedOffsetMs: seg.FinishedOffsetMS,
				WorkerSlot:       int32(seg.WorkerSlot),
				CpuThreads:       int32(seg.CPUThreads),
				ParallelGroup:    seg.ParallelGroup,
			})
		}

		// Build output_artifacts as repeated structpb.Struct.
		// artifact_id is now separate from sha256; SizeBytes carries real byte count.
		for _, ref := range report.Outputs {
			artifactID := ref.ArtifactID
			if artifactID == "" {
				// Backward-compat fallback: use Hash when ArtifactID is not set.
				artifactID = ref.Hash
			}
			if s, err := structpb.NewStruct(map[string]interface{}{
				"artifact_id":   artifactID,
				"artifact_type": ref.Type,
				"artifact_path": ref.URI,
				"size_bytes":    ref.SizeBytes,
				"sha256":        ref.Hash,
			}); err == nil {
				tr.OutputArtifacts = append(tr.OutputArtifacts, s)
			}
		}
	}

	// Compute the report hash over the canonical protojson serialization of
	// the final TaskResult. The hash field itself is empty during hashing,
	// then stamped onto the wire message so the master can use it for
	// idempotency and conflict detection.
	tr.ReportHash = ""
	reportJSON, err := protojson.Marshal(tr)
	if err != nil {
		r.logger.Error("[TASK] Failed to marshal TaskResult to protojson for %s: %v", taskID, err)
	} else {
		tr.ReportHash = fmt.Sprintf("%x", sha256.Sum256(reportJSON))
	}

	if r.spool == nil {
		// Hand-built legacy test workers do not open the production spool.
		// Keep those fixtures usable while New() remains fail-closed with a
		// durable outbox in every real worker.
		submitStartedAt := time.Now()
		if sendErr := r.transport().Send(ctx, controltransport.NewTypedMessage(controltransport.MsgTaskResult, r.workerID, r.protocol, tr)); sendErr != nil {
			telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
			r.logger.Error("[TASK] Failed to submit TaskResult for %s: %v", taskID, sendErr)
			return
		}
		telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
		artifactCount := artifactReportOutputCount(report)
		r.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d)", taskID, status, artifactCount)
		r.logArtifact("TASK_RESULT_SENT", pte, resultStartedAt, "", "", "", map[string]interface{}{
			"status": status, "report_hash": tr.GetReportHash(), "artifact_count": artifactCount,
		})
		// A direct send is retained for legacy/headless fixtures, but it is
		// not an authoritative terminal confirmation because no TaskResultAck
		// exists on this path.
		return
	}
	submitStartedAt := time.Now()
	if err := r.persistTaskResult(ctx, tr); err != nil {
		telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to persist TaskResult for %s: %v", taskID, err)
		r.logArtifact("TASK_RESULT_OUTBOX_PERSIST_FAILED", pte, resultStartedAt, "", "", "", map[string]interface{}{
			"status": status, "report_hash": tr.GetReportHash(), "error": err.Error(),
		})
		return
	}
	payload, marshalErr := proto.Marshal(tr)
	if marshalErr != nil {
		telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to marshal TaskResult for %s: %v", taskID, marshalErr)
		return
	}
	entry := spool.TaskResultOutboxEntry{
		TaskID: taskID, AttemptID: attemptID, ReportHash: tr.GetReportHash(),
		Payload: payload,
	}
	ackCh := r.registerTaskResultAck(pte.JobID, taskID, attemptID)
	defer r.unregisterTaskResultAck(pte.JobID, taskID, attemptID)
	if sendErr := r.sendTaskResultAttempt(ctx, entry); sendErr != nil {
		telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
		r.logger.Error("[TASK_RESULT_OUTBOX] Failed to submit TaskResult for %s: %v", taskID, sendErr)
		r.logArtifact("TASK_RESULT_SEND_FAILED", pte, resultStartedAt, "", "", "", map[string]interface{}{
			"status": status, "report_hash": tr.GetReportHash(), "error": sendErr.Error(),
		})
		return
	}
	ackWait := taskResultAckWait
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < ackWait {
			ackWait = remaining
		}
	}
	telemetry.GetPrometheusMetrics().RecordTaskResultSubmit(time.Since(submitStartedAt))
	ackWaitStartedAt := time.Now()
	ackReceived := false
	if ackWait > 0 {
		timer := time.NewTimer(ackWait)
		select {
		case ack := <-ackCh:
			timer.Stop()
			ackReceived = taskResultAckIsTerminal(ack)
			if !taskResultAckIsTerminal(ack) {
				r.logger.Warn("[TASK_RESULT_OUTBOX] TaskResultAck was non-terminal task=%s attempt=%s error=%q; replay remains durable", taskID, attemptID, ack.GetError())
			}
		case <-timer.C:
			r.logger.Warn("[TASK_RESULT_OUTBOX] TaskResultAck not received before wait window task=%s attempt=%s; replay remains durable", taskID, attemptID)
		case <-ctx.Done():
			timer.Stop()
		}
	}
	if ackReceived {
		telemetry.GetPrometheusMetrics().RecordTaskResultAck(time.Since(ackWaitStartedAt))
	}
	artifactCount := artifactReportOutputCount(report)
	r.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d)", taskID, status, artifactCount)
	r.logArtifact("TASK_RESULT_SENT", pte, resultStartedAt, "", "", "", map[string]interface{}{
		"status": status, "report_hash": tr.GetReportHash(), "artifact_count": artifactCount,
	})
}

// appendDetailedPhaseTimings converts every worker-reported detailed phase
// without filtering or coalescing. Repeated events such as ten distinct
// engine.encode segment operations must remain ten distinct wire entries.
// The helper is deliberately status-agnostic so successful and failed
// TaskResults use exactly the same cardinality and ordering contract.
func appendDetailedPhaseTimings(
	dst []*pb.PhaseTimingDetailed,
	phases []taskrunner.DetailedPhaseTiming,
	leaseID string,
	executorID string,
	executorVersion int32,
) []*pb.PhaseTimingDetailed {
	for _, phase := range phases {
		p := phase.ToProto()
		// Native sidecar events do not know the task offer identity. Stamp
		// the worker's canonical execution tuple here when the event did not
		// already carry it. The master still overwrites all identity fields
		// from task_attempts at ingest; this makes the wire report complete
		// without allowing worker echoes to become authoritative.
		if p.ExecutorId == "" && executorID != "" {
			p.ExecutorId = executorID
		}
		if p.ExecutorVersion == 0 && executorVersion > 0 {
			p.ExecutorVersion = executorVersion
		}
		if p.LeaseId == "" && leaseID != "" {
			p.LeaseId = leaseID
		}
		dst = append(dst, p)
	}
	return dst
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
	return transport.Send(ctx, controltransport.NewTypedMessage(
		controltransport.MsgTaskResult,
		r.workerID,
		r.protocol,
		&result,
	))
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

// validateTaskResultAck checks the ACK against the durable payload before
// touching the spool. TaskResultAck has no report_hash, so the stored
// TaskResult protobuf is the authoritative local identity tuple.
func (r *taskResultReporter) validateTaskResultAck(ctx context.Context, ack *pb.TaskResultAck) (string, bool) {
	if ack == nil || ack.GetTaskId() == "" || ack.GetJobId() == "" || ack.GetAttemptId() == "" || r.spool == nil {
		return "", false
	}
	entries, err := r.spool.ListTaskResultsForAttempt(ctx, ack.GetTaskId(), ack.GetAttemptId())
	if err != nil {
		r.logger.Warn("[TASK_RESULT_OUTBOX] ACK validation lookup failed task=%s attempt=%s: %v", ack.GetTaskId(), ack.GetAttemptId(), err)
		return "", false
	}
	for _, entry := range entries {
		var result pb.TaskResult
		if err := proto.Unmarshal(entry.Payload, &result); err != nil {
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
		r.logger.Warn("[TASK_RESULT_OUTBOX] ignoring TaskResultAck identity mismatch task=%s job=%s attempt=%s", ack.GetTaskId(), ack.GetJobId(), ack.GetAttemptId())
		return
	}

	key := taskResultAckKey(ack.GetJobId(), ack.GetTaskId(), ack.GetAttemptId())
	r.acksMu.Lock()
	ch := r.acks[key]
	if ch == nil {
		// The replay loop can receive an ACK before Submit has
		// registered its waiter. Cache it under the full identity tuple;
		// registerTaskResultAck consumes this entry atomically.
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
		r.logger.Warn("[TASK_RESULT_OUTBOX] non-terminal TaskResultAck retained for retry task=%s attempt=%s error=%q", ack.GetTaskId(), ack.GetAttemptId(), ack.GetError())
		return
	}
	r.cleanupCommittedAttemptOutputs(ack.GetTaskId(), ack.GetAttemptId())
	deleted, err := r.spool.DeleteTaskResultsForAttempt(context.Background(), ack.GetTaskId(), ack.GetAttemptId())
	if err != nil {
		r.logger.Warn("[TASK_RESULT_OUTBOX] ACK cleanup failed task=%s attempt=%s: %v", ack.GetTaskId(), ack.GetAttemptId(), err)
		return
	}
	if !deleted {
		return
	}
	telemetry.GetPrometheusMetrics().RecordTaskResultAckReceived()
	// This is the authoritative terminal boundary, including ACKs that
	// arrive after Submit's synchronous wait or after a reconnect.
	if r.onTerminal != nil {
		r.onTerminal()
	}
	r.logger.Info("[TASK_RESULT_OUTBOX] TaskResultAck received task=%s attempt=%s error=%q", ack.GetTaskId(), ack.GetAttemptId(), ack.GetError())
}

// cleanupCommittedAttemptOutputs releases large local render artifacts only
// after the master has acknowledged the terminal TaskResult. The output spool
// remains the durability fence until that point; deleting earlier would make
// a reconnect/replay unable to re-upload a committed artifact. Paths are
// constrained to the configured render output directory so an unexpected
// spool row can never turn an ACK into an arbitrary filesystem delete.
func (r *taskResultReporter) cleanupCommittedAttemptOutputs(taskID, attemptID string) {
	if r.spool == nil {
		return
	}
	entries, err := r.spool.ListByAttempt(context.Background(), taskID, attemptID)
	if err != nil {
		r.logger.Warn("[TASK_RESULT_OUTBOX] output cleanup list failed task=%s attempt=%s: %v", taskID, attemptID, err)
		return
	}
	root := "/tmp/velox/scene-composite"
	if r.outputDir != "" {
		root = r.outputDir
	}
	root, err = filepath.Abs(root)
	if err != nil {
		r.logger.Warn("[TASK_RESULT_OUTBOX] output cleanup root invalid task=%s attempt=%s: %v", taskID, attemptID, err)
		return
	}
	for _, entry := range entries {
		if entry.Status != spool.StatusCommitted && entry.Status != spool.StatusRejected {
			continue
		}
		if entry.LocalPath != "" {
			path, absErr := filepath.Abs(entry.LocalPath)
			rel, relErr := filepath.Rel(root, path)
			outside := relErr != nil || absErr != nil || rel == "." || rel == ".." || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)
			if outside {
				r.logger.Warn("[TASK_RESULT_OUTBOX] refusing output cleanup outside output dir task=%s attempt=%s path=%q root=%q", taskID, attemptID, entry.LocalPath, root)
				continue
			}
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				r.logger.Warn("[TASK_RESULT_OUTBOX] output file cleanup failed task=%s attempt=%s path=%q: %v", taskID, attemptID, path, removeErr)
				continue
			}
		}
		if cleanErr := r.spool.MarkCleaned(context.Background(), entry.SpoolID); cleanErr != nil {
			r.logger.Warn("[TASK_RESULT_OUTBOX] output spool cleanup failed task=%s attempt=%s spool=%s: %v", taskID, attemptID, entry.SpoolID, cleanErr)
		}
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
		r.logger.Warn("[TASK_RESULT_OUTBOX] list due results failed: %v", err)
		return
	}
	for _, entry := range entries {
		if err := r.sendTaskResultAttempt(ctx, entry); err != nil {
			r.logger.Warn("[TASK_RESULT_OUTBOX] replay failed task=%s attempt=%s: %v", entry.TaskID, entry.AttemptID, err)
		}
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
