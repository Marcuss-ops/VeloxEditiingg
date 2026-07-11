package worker

import (
	"context"
	"io"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestReceiveLoop_TaskOfferRejectsUnsupportedExecutorBeforeAccept(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	reg := executor.NewRegistry()
	rt := &recordingTransport{}

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "test-worker-unsupported-exec",
			WorkerName:      "test-worker-unsupported-exec",
			LogLevel:        "info",
			MaxActiveJobs:   1,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
		},
		logger:             log,
		transport:          rt,
		status:             StatusIdle,
		stopChan:           make(chan struct{}),
		heartbeatBackoff:   &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:       make(map[string]time.Time),
		recentLogs:         newRecentLogBuffer(50),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		pendingTasks:       make(map[string]*PendingTaskExecution),
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		executorRegistry:   reg,
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recvCh := make(chan controltransport.ControlMessage, 1)
	w.wg.Add(1)
	go w.receiveLoop(ctx, recvCh)

	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgTaskOffer,
		"master",
		controltransport.ProtocolVersionCurrent,
		&pb.TaskOffer{
			TaskId:          "task-unsupported-001",
			JobId:           "job-unsupported-001",
			AttemptId:       "attempt-unsupported-001",
			LeaseId:         "lease-unsupported-001",
			AttemptNumber:   1,
			Revision:        7,
			ExecutorId:      "scene.composite.v1",
			ExecutorVersion: 1,
		},
	)

	var got controltransport.ControlMessage
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if msg, ok := rt.last(); ok {
			got = msg
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got.Type == "" {
		t.Fatal("worker did not send any reply to unsupported executor offer")
	}
	if got.Type != controltransport.MsgTaskRejected {
		t.Fatalf("worker reply type = %q, want %q", got.Type, controltransport.MsgTaskRejected)
	}
	reject, ok := got.TypedPayload.(*pb.TaskRejected)
	if !ok || reject == nil {
		t.Fatalf("reply payload = %T, want *pb.TaskRejected", got.TypedPayload)
	}
	if reject.GetReason() != "unsupported_executor" {
		t.Fatalf("reject reason = %q, want %q", reject.GetReason(), "unsupported_executor")
	}

	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if len(w.pendingTasks) != 0 {
		t.Fatalf("pendingTasks must stay empty on unsupported executor reject, got %d entries", len(w.pendingTasks))
	}

	cancel()
	close(recvCh)
	w.wg.Wait()
}
