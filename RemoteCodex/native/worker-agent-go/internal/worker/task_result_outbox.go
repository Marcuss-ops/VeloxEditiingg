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
	"velox-worker-agent/internal/telemetry"

	"google.golang.org/protobuf/proto"
)

const (
	taskResultRetryInitial = 2 * time.Second
	taskResultRetryMax     = 2 * time.Minute
	taskResultReplayBatch  = 32
	taskResultAckWait      = 30 * time.Second
	taskResultAckCacheTTL  = 2 * time.Minute
)

func (w *Worker) persistTaskResult(ctx context.Context, result *pb.TaskResult) error {
	if w.outputSpool == nil {
		return errors.New("task result outbox is not configured")
	}
	payload, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	return w.outputSpool.UpsertTaskResult(ctx, result.GetTaskId(), result.GetAttemptId(), result.GetReportHash(), payload)
}

func (w *Worker) sendTaskResultAttempt(ctx context.Context, entry spool.TaskResultOutboxEntry) error {
	var result pb.TaskResult
	if err := proto.Unmarshal(entry.Payload, &result); err != nil {
		return err
	}
	if w.transport == nil {
		return errors.New("task result transport is unavailable")
	}
	claimed, err := w.outputSpool.ClaimTaskResultAttempt(ctx, entry.TaskID, entry.AttemptID, entry.ReportHash, entry.AttemptCount, time.Now(), time.Now().Add(taskResultRetryDelay(entry.AttemptCount+1)))
	if err != nil {
		return err
	}
	if !claimed {
		return errors.New("task result outbox entry was claimed by another sender")
	}
	return w.transport.Send(ctx, controltransport.NewTypedMessage(
		controltransport.MsgTaskResult,
		w.config.WorkerID,
		w.config.ProtocolVersion,
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

func (w *Worker) registerTaskResultAck(jobID, taskID, attemptID string) chan *pb.TaskResultAck {
	w.pendingTaskResultAcksMu.Lock()
	defer w.pendingTaskResultAcksMu.Unlock()
	if w.pendingTaskResultAcks == nil {
		w.pendingTaskResultAcks = make(map[string]chan *pb.TaskResultAck)
	}
	if w.pendingTaskResultAckCache == nil {
		w.pendingTaskResultAckCache = make(map[string]taskResultAckCacheEntry)
	}
	key := taskResultAckKey(jobID, taskID, attemptID)
	ch := make(chan *pb.TaskResultAck, 1)
	if cached := w.pendingTaskResultAckCache[key]; cached.ack != nil {
		delete(w.pendingTaskResultAckCache, key)
		ch <- cached.ack
		return ch
	}
	w.pendingTaskResultAcks[key] = ch
	return ch
}

func (w *Worker) unregisterTaskResultAck(jobID, taskID, attemptID string) {
	w.pendingTaskResultAcksMu.Lock()
	defer w.pendingTaskResultAcksMu.Unlock()
	delete(w.pendingTaskResultAcks, taskResultAckKey(jobID, taskID, attemptID))
}

func taskResultAckIsTerminal(ack *pb.TaskResultAck) bool {
	return ack != nil && (ack.GetError() == "" || ack.GetError() == "report_conflict")
}

// validateTaskResultAck checks the ACK against the durable payload before
// touching the spool. TaskResultAck has no report_hash, so the stored
// TaskResult protobuf is the authoritative local identity tuple.
func (w *Worker) validateTaskResultAck(ctx context.Context, ack *pb.TaskResultAck) (string, bool) {
	if ack == nil || ack.GetTaskId() == "" || ack.GetJobId() == "" || ack.GetAttemptId() == "" || w.outputSpool == nil {
		return "", false
	}
	entries, err := w.outputSpool.ListTaskResultsForAttempt(ctx, ack.GetTaskId(), ack.GetAttemptId())
	if err != nil {
		w.logger.Warn("[TASK_RESULT_OUTBOX] ACK validation lookup failed task=%s attempt=%s: %v", ack.GetTaskId(), ack.GetAttemptId(), err)
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

func (w *Worker) handleTaskResultAck(ack *pb.TaskResultAck) {
	if ack == nil || ack.GetTaskId() == "" || ack.GetJobId() == "" || ack.GetAttemptId() == "" {
		return
	}
	if _, ok := w.validateTaskResultAck(context.Background(), ack); !ok {
		w.logger.Warn("[TASK_RESULT_OUTBOX] ignoring TaskResultAck identity mismatch task=%s job=%s attempt=%s", ack.GetTaskId(), ack.GetJobId(), ack.GetAttemptId())
		return
	}

	key := taskResultAckKey(ack.GetJobId(), ack.GetTaskId(), ack.GetAttemptId())
	w.pendingTaskResultAcksMu.Lock()
	ch := w.pendingTaskResultAcks[key]
	if ch == nil {
		// The replay loop can receive an ACK before submitTaskResult has
		// registered its waiter. Cache it under the full identity tuple;
		// registerTaskResultAck consumes this entry atomically.
		if w.pendingTaskResultAckCache == nil {
			w.pendingTaskResultAckCache = make(map[string]taskResultAckCacheEntry)
		}
		w.pendingTaskResultAckCache[key] = taskResultAckCacheEntry{ack: ack, receivedAt: time.Now()}
		w.expireTaskResultAckCacheLocked(time.Now())
	}
	if ch != nil {
		select {
		case ch <- ack:
		default:
		}
	}
	w.pendingTaskResultAcksMu.Unlock()

	if !taskResultAckIsTerminal(ack) {
		w.logger.Warn("[TASK_RESULT_OUTBOX] non-terminal TaskResultAck retained for retry task=%s attempt=%s error=%q", ack.GetTaskId(), ack.GetAttemptId(), ack.GetError())
		return
	}
	w.cleanupCommittedAttemptOutputs(ack.GetTaskId(), ack.GetAttemptId())
	deleted, err := w.outputSpool.DeleteTaskResultsForAttempt(context.Background(), ack.GetTaskId(), ack.GetAttemptId())
	if err != nil {
		w.logger.Warn("[TASK_RESULT_OUTBOX] ACK cleanup failed task=%s attempt=%s: %v", ack.GetTaskId(), ack.GetAttemptId(), err)
		return
	}
	if !deleted {
		return
	}
	telemetry.GetPrometheusMetrics().RecordTaskResultAckReceived()
	// This is the authoritative terminal boundary, including ACKs that
	// arrive after executeTask's synchronous wait or after a reconnect.
	w.signalTaskTerminal()
	w.logger.Info("[TASK_RESULT_OUTBOX] TaskResultAck received task=%s attempt=%s error=%q", ack.GetTaskId(), ack.GetAttemptId(), ack.GetError())
}

// cleanupCommittedAttemptOutputs releases large local render artifacts only
// after the master has acknowledged the terminal TaskResult. The output spool
// remains the durability fence until that point; deleting earlier would make
// a reconnect/replay unable to re-upload a committed artifact. Paths are
// constrained to the configured render output directory so an unexpected
// spool row can never turn an ACK into an arbitrary filesystem delete.
func (w *Worker) cleanupCommittedAttemptOutputs(taskID, attemptID string) {
	if w == nil || w.outputSpool == nil {
		return
	}
	entries, err := w.outputSpool.ListByAttempt(context.Background(), taskID, attemptID)
	if err != nil {
		w.logger.Warn("[TASK_RESULT_OUTBOX] output cleanup list failed task=%s attempt=%s: %v", taskID, attemptID, err)
		return
	}
	root := "/tmp/velox/scene-composite"
	if w.config != nil && w.config.OutputDir != "" {
		root = w.config.OutputDir
	}
	root, err = filepath.Abs(root)
	if err != nil {
		w.logger.Warn("[TASK_RESULT_OUTBOX] output cleanup root invalid task=%s attempt=%s: %v", taskID, attemptID, err)
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
				w.logger.Warn("[TASK_RESULT_OUTBOX] refusing output cleanup outside output dir task=%s attempt=%s path=%q root=%q", taskID, attemptID, entry.LocalPath, root)
				continue
			}
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				w.logger.Warn("[TASK_RESULT_OUTBOX] output file cleanup failed task=%s attempt=%s path=%q: %v", taskID, attemptID, path, removeErr)
				continue
			}
		}
		if cleanErr := w.outputSpool.MarkCleaned(context.Background(), entry.SpoolID); cleanErr != nil {
			w.logger.Warn("[TASK_RESULT_OUTBOX] output spool cleanup failed task=%s attempt=%s spool=%s: %v", taskID, attemptID, entry.SpoolID, cleanErr)
		}
	}
}

func (w *Worker) expireTaskResultAckCacheLocked(now time.Time) {
	for key, cached := range w.pendingTaskResultAckCache {
		if now.Sub(cached.receivedAt) >= taskResultAckCacheTTL {
			delete(w.pendingTaskResultAckCache, key)
		}
	}
}

func (w *Worker) replayDueTaskResults(ctx context.Context) {
	if w.outputSpool == nil || w.transport == nil {
		return
	}
	entries, err := w.outputSpool.ListDueTaskResults(ctx, time.Now(), taskResultReplayBatch)
	if err != nil {
		w.logger.Warn("[TASK_RESULT_OUTBOX] list due results failed: %v", err)
		return
	}
	for _, entry := range entries {
		if err := w.sendTaskResultAttempt(ctx, entry); err != nil {
			w.logger.Warn("[TASK_RESULT_OUTBOX] replay failed task=%s attempt=%s: %v", entry.TaskID, entry.AttemptID, err)
		}
	}
}

func (w *Worker) startTaskResultReplayLoop(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(taskResultRetryInitial)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			case <-ticker.C:
				w.pendingTaskResultAcksMu.Lock()
				w.expireTaskResultAckCacheLocked(time.Now())
				w.pendingTaskResultAcksMu.Unlock()
				w.replayDueTaskResults(ctx)
			}
		}
	}()
}
