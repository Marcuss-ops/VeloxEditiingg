package worker

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"

	"google.golang.org/protobuf/proto"
)

func TestHandleTaskResultAck_DeletesOutboxAndUnblocksWaiter(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-outbox-test", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
	}
	rep := wireTestReporter(w, store)
	ackJobID := "job-ack"
	resultPayload, err := proto.Marshal(&pb.TaskResult{TaskId: "task-ack", JobId: ackJobID, AttemptId: "attempt-ack", ReportHash: "hash-ack"})
	if err != nil {
		t.Fatalf("marshal ACK payload: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), "task-ack", "attempt-ack", "hash-ack", resultPayload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	ch := rep.registerTaskResultAck(ackJobID, "task-ack", "attempt-ack")
	rep.HandleAck(&pb.TaskResultAck{TaskId: "task-ack", JobId: ackJobID, AttemptId: "attempt-ack"})
	select {
	case ack := <-ch:
		if ack.GetTaskId() != "task-ack" {
			t.Fatalf("ack task = %q", ack.GetTaskId())
		}
	case <-time.After(time.Second):
		t.Fatal("waiter was not notified")
	}
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("outbox count = %d, err=%v; want 0", count, err)
	}
}

func TestSubmitTaskResult_PersistsBeforeSendAndLeavesRowWithoutAck(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	transport := &recordingTransport{}
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-submit-outbox", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
		transport:   transport,
	}
	pte := &PendingTaskExecution{TaskID: "task-submit", JobID: "job-submit", AttemptID: "attempt-submit", LeaseID: "lease-submit"}
	submitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	rep := wireTestReporter(w, store)
	rep.Submit(submitCtx, pte, pte.TaskID, pte.AttemptID, nil, nil)
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("outbox count = %d, err=%v; want 1 without ACK", count, err)
	}
	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v; want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result.GetTaskId() != pte.TaskID || result.GetReportHash() == "" {
		t.Fatalf("sent result = %#v", message.TypedPayload)
	}
}

func TestHandleTaskResultAck_IgnoresIdentityMismatchAndNonTerminalError(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-ack-validation", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
	}
	rep := wireTestReporter(w, store)
	payload, err := proto.Marshal(&pb.TaskResult{TaskId: "task-validation", JobId: "job-validation", AttemptId: "attempt-validation", ReportHash: "hash-validation"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), "task-validation", "attempt-validation", "hash-validation", payload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rep.HandleAck(&pb.TaskResultAck{TaskId: "task-validation", JobId: "wrong-job", AttemptId: "attempt-validation"})
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("count after mismatched ACK = %d, err=%v; want 1", count, err)
	}
	rep.HandleAck(&pb.TaskResultAck{TaskId: "task-validation", JobId: "job-validation", AttemptId: "attempt-validation", Error: "internal_error"})
	count, err = store.PendingTaskResultCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("count after non-terminal ACK = %d, err=%v; want 1", count, err)
	}
}

func TestHandleTaskResultAck_CachesAckBeforeWaiterRegistration(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-early-ack", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
	}
	rep := wireTestReporter(w, store)
	payload, err := proto.Marshal(&pb.TaskResult{TaskId: "task-early", JobId: "job-early", AttemptId: "attempt-early", ReportHash: "hash-early"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), "task-early", "attempt-early", "hash-early", payload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rep.HandleAck(&pb.TaskResultAck{TaskId: "task-early", JobId: "job-early", AttemptId: "attempt-early"})
	ch := rep.registerTaskResultAck("job-early", "task-early", "attempt-early")
	select {
	case ack := <-ch:
		if ack.GetAttemptId() != "attempt-early" {
			t.Fatalf("cached ACK attempt = %q", ack.GetAttemptId())
		}
	case <-time.After(time.Second):
		t.Fatal("cached ACK was not delivered to late waiter")
	}
}

func TestTaskResultOutbox_RetryFailureThenAckDeletes(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	transport := &retryRecordingTransport{failSends: 1}
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-retry-lifecycle", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
		transport:   transport,
	}
	result := &pb.TaskResult{TaskId: "task-retry", AttemptId: "attempt-retry", ReportHash: "hash-retry", JobId: "job-retry", Status: "failed"}
	payload, err := proto.Marshal(result)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), result.TaskId, result.AttemptId, result.ReportHash, payload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rep := wireTestReporter(w, store)

	// First replay claims the row, then the transport fails. The row must
	// remain durable and be scheduled for a future retry.
	rep.replayDueTaskResults(context.Background())
	entries, err := store.ListTaskResultsForAttempt(context.Background(), result.TaskId, result.AttemptId)
	if err != nil || len(entries) != 1 || entries[0].AttemptCount != 1 {
		t.Fatalf("after failed send entries=%#v err=%v; want one attempt", entries, err)
	}

	// Make the claimed row due again in the test database, then let the
	// real replay path perform the second atomic claim.
	if _, err := store.DB().ExecContext(context.Background(), `UPDATE task_result_outbox SET next_attempt_at = ? WHERE task_id = ? AND attempt_id = ? AND report_hash = ?`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), result.TaskId, result.AttemptId, result.ReportHash); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	rep.replayDueTaskResults(context.Background())
	if got := transport.sendCount(); got != 2 {
		t.Fatalf("send count = %d, want failed send plus retry", got)
	}

	rep.HandleAck(&pb.TaskResultAck{TaskId: result.TaskId, JobId: result.JobId, AttemptId: result.AttemptId})
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("after retry ACK count=%d err=%v; want 0", count, err)
	}
}

func TestReplayDueTaskResults_ResendsPersistedPayload(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	transport := &recordingTransport{}
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-replay-test", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:      logger.New(logger.InfoLevel, io.Discard),
		outputSpool: store,
		transport:   transport,
	}
	result := &pb.TaskResult{TaskId: "task-replay", AttemptId: "attempt-replay", ReportHash: "hash-replay", JobId: "job-replay", Status: "succeeded"}
	payload, err := proto.Marshal(result)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), result.TaskId, result.AttemptId, result.ReportHash, payload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rep := wireTestReporter(w, store)
	rep.replayDueTaskResults(context.Background())
	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v; want TaskResult", message)
	}
	got, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || got.GetTaskId() != result.GetTaskId() || got.GetReportHash() != result.GetReportHash() {
		t.Fatalf("replayed payload = %#v", message.TypedPayload)
	}
}

// wireTestReporter attaches a reporting subsystem to w using w's current
// transport/config/logger and the supplied spool, mirroring New()'s wiring.
// It returns the concrete reporter so tests can drive the outbox/ACK
// internals directly. Logging is delegated to w.logArtifactProtocol and the
// terminal observer to w.signalTaskTerminal, preserving the production seams.
func wireTestReporter(w *Worker, store *spool.Store) *taskResultReporter {
	if w.logger == nil {
		w.logger = logger.New(logger.InfoLevel, io.Discard)
	}
	if w.stopChan == nil {
		w.stopChan = make(chan struct{})
	}
	workerID, protocol, outputDir := "", "", ""
	if w.config != nil {
		workerID = w.config.WorkerID
		protocol = w.config.ProtocolVersion
		outputDir = w.config.OutputDir
	}
	rep := newTaskResultReporter(taskResultReporterConfig{
		spool: store,
		transport: func() controltransport.ControlTransport {
			w.transportMu.RLock()
			defer w.transportMu.RUnlock()
			return w.transport
		},
		workerID:        workerID,
		protocol:        protocol,
		outputDir:       outputDir,
		storageResolver: w.storageResolver,
		logger:          w.logger,
		onTerminal:      w.signalTaskTerminal,
		logArtifact: func(event string, pte *PendingTaskExecution, startedAt time.Time, commitID, artifactID, uploadID string, fields map[string]interface{}) {
			w.logArtifactProtocol(event, pte, startedAt, commitID, artifactID, uploadID, fields)
		},
		wg:       &w.wg,
		stopChan: w.stopChan,
	})
	w.reporter = rep
	return rep
}

type retryRecordingTransport struct {
	mu        sync.Mutex
	failSends int
	sends     int
}

func (t *retryRecordingTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (t *retryRecordingTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (t *retryRecordingTransport) Send(context.Context, controltransport.ControlMessage) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sends++
	if t.failSends > 0 {
		t.failSends--
		return context.DeadlineExceeded
	}
	return nil
}
func (t *retryRecordingTransport) Close() error { return nil }
func (t *retryRecordingTransport) sendCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sends
}
