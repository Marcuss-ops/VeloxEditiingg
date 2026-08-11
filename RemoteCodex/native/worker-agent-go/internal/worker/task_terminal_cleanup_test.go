package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"

	"google.golang.org/protobuf/proto"
)

func TestDispatchTaskRunner_DoesNotSignalCleanupAfterRender(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	w.jobDone = make(chan struct{}, 1)
	pte := &PendingTaskExecution{
		TaskID:     "task-render-only",
		JobID:      "job-render-only",
		AttemptID:  "attempt-render-only",
		ExecutorID: "scene.composite.v1",
		Spec:       mapTaskSpec("job-render-only", "scene.composite.v1"),
	}

	if _, err := w.dispatchTaskRunner(context.Background(), pte); err != nil {
		t.Fatalf("dispatchTaskRunner: %v", err)
	}
	select {
	case <-w.JobDone():
		t.Fatal("cleanup signaled after render dispatch, before artifact/result terminal confirmation")
	default:
	}
}

func TestExecuteTask_SignalsCleanupOnlyAfterTaskResultAck(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer store.Close()

	w, _ := newDispatchTestWorker(t)
	w.outputSpool = store
	w.pendingTaskResultAcks = make(map[string]chan *pb.TaskResultAck)
	w.pendingTaskResultAckCache = make(map[string]taskResultAckCacheEntry)
	ackTransport := &terminalAckTransport{}
	ackTransport.worker = w
	w.transport = ackTransport
	w.jobDone = make(chan struct{}, 1)

	pte := &PendingTaskExecution{
		TaskID:          "task-terminal-ack",
		JobID:           "job-terminal-ack",
		AttemptID:       "attempt-terminal-ack",
		AttemptNumber:   1,
		LeaseID:         "lease-terminal-ack",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		Spec:            mapTaskSpec("job-terminal-ack", "scene.composite.v1"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.executeTask(ctx, pte, pte.TaskID, pte.AttemptID)

	select {
	case <-w.JobDone():
	case <-time.After(time.Second):
		t.Fatal("cleanup was not signaled after terminal TaskResultAck")
	}
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil {
		t.Fatalf("PendingTaskResultCount: %v", err)
	}
	if count != 0 {
		t.Fatalf("pending outbox rows=%d after terminal ACK; want 0", count)
	}
}

func TestSubmitTaskResult_WithoutSpoolDoesNotSignalCleanup(t *testing.T) {
	w, transport := newDispatchTestWorker(t)
	w.jobDone = make(chan struct{}, 1)
	pte := &PendingTaskExecution{
		TaskID:    "task-legacy-send",
		JobID:     "job-legacy-send",
		AttemptID: "attempt-legacy-send",
		LeaseID:   "lease-legacy-send",
	}
	w.submitTaskResult(context.Background(), pte, pte.TaskID, pte.AttemptID, nil, nil)
	if _, ok := transport.last(); !ok {
		t.Fatal("legacy direct TaskResult was not sent")
	}
	select {
	case <-w.JobDone():
		t.Fatal("cleanup signaled without authoritative TaskResultAck")
	default:
	}
}

func TestHandleTaskResultAck_LateAckSignalsCleanup(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer store.Close()
	w := &Worker{
		logger:                    logger.New(logger.InfoLevel, io.Discard),
		outputSpool:               store,
		jobDone:                   make(chan struct{}, 1),
		pendingTaskResultAcks:     make(map[string]chan *pb.TaskResultAck),
		pendingTaskResultAckCache: make(map[string]taskResultAckCacheEntry),
	}
	payload, err := proto.Marshal(&pb.TaskResult{TaskId: "task-late", JobId: "job-late", AttemptId: "attempt-late", ReportHash: "hash-late"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), "task-late", "attempt-late", "hash-late", payload); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	w.handleTaskResultAck(&pb.TaskResultAck{TaskId: "task-late", JobId: "job-late", AttemptId: "attempt-late"})
	select {
	case <-w.JobDone():
	case <-time.After(time.Second):
		t.Fatal("late terminal ACK did not signal cleanup")
	}
	count, err := store.PendingTaskResultCount(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("pending rows=%d err=%v; want 0", count, err)
	}
}

func TestHandleTaskResultAck_CleansCommittedRenderOutputAfterTerminalAck(t *testing.T) {
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer store.Close()

	outputDir := t.TempDir()
	outputPath := filepath.Join(outputDir, "rendered.mp4")
	if err := os.WriteFile(outputPath, []byte("rendered-output"), 0o640); err != nil {
		t.Fatalf("write output: %v", err)
	}
	entry, err := store.Insert(context.Background(), spool.SpoolEntry{
		TaskID: "task-output-cleanup", AttemptID: "attempt-output-cleanup",
		WorkerSpoolKey: "task-output-cleanup:output:0", LocalPath: outputPath,
		Status: spool.StatusRendering,
	})
	if err != nil {
		t.Fatalf("insert spool entry: %v", err)
	}
	for _, step := range []func() error{
		func() error {
			return store.MarkReady(context.Background(), entry.SpoolID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 14)
		},
		func() error {
			return store.MarkUploadPending(context.Background(), entry.SpoolID, "upload-output-cleanup")
		},
		func() error { return store.MarkUploading(context.Background(), entry.SpoolID, 0) },
		func() error { return store.MarkUploaded(context.Background(), entry.SpoolID) },
		func() error { return store.MarkCommitted(context.Background(), entry.SpoolID) },
	} {
		if err := step(); err != nil {
			t.Fatalf("commit spool lifecycle: %v", err)
		}
	}

	payload, err := proto.Marshal(&pb.TaskResult{
		TaskId: "task-output-cleanup", JobId: "job-output-cleanup",
		AttemptId: "attempt-output-cleanup", ReportHash: "hash-output-cleanup",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), "task-output-cleanup", "attempt-output-cleanup", "hash-output-cleanup", payload); err != nil {
		t.Fatalf("upsert task result: %v", err)
	}
	w := &Worker{
		config: &config.WorkerConfig{WorkerID: "worker-output-cleanup", OutputDir: outputDir},
		logger: logger.New(logger.InfoLevel, io.Discard), outputSpool: store,
		pendingTaskResultAcks:     make(map[string]chan *pb.TaskResultAck),
		pendingTaskResultAckCache: make(map[string]taskResultAckCacheEntry),
	}
	w.handleTaskResultAck(&pb.TaskResultAck{
		TaskId: "task-output-cleanup", JobId: "job-output-cleanup",
		AttemptId: "attempt-output-cleanup",
	})
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("committed output stat error=%v; output must be removed after terminal ACK", err)
	}
	cleaned, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("get cleaned spool entry: %v", err)
	}
	if cleaned.Status != spool.StatusCleaned || cleaned.LocalPath != "" {
		t.Fatalf("spool after terminal ACK = status=%q path=%q; want CLEANED/empty", cleaned.Status, cleaned.LocalPath)
	}
}

func mapTaskSpec(jobID, executorID string) executor.TaskSpec {
	return executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: executorID,
		Payload:    map[string]interface{}{"scenes": []interface{}{"one"}},
	}
}

type terminalAckTransport struct {
	worker *Worker
}

func (t *terminalAckTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (t *terminalAckTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (t *terminalAckTransport) Send(_ context.Context, msg controltransport.ControlMessage) error {
	result, ok := msg.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		return nil
	}
	t.worker.handleTaskResultAck(&pb.TaskResultAck{
		TaskId:    result.GetTaskId(),
		JobId:     result.GetJobId(),
		AttemptId: result.GetAttemptId(),
	})
	return nil
}
func (*terminalAckTransport) Close() error { return nil }
