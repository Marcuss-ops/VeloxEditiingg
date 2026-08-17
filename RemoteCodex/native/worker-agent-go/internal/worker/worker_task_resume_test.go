package worker

import (
	"context"
	"io"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// blockUntilCanceledExecutor blocks on its context so a test can observe
// whether the task survived a session teardown (it should) or was canceled
// by worker shutdown (it should).
type blockUntilCanceledExecutor struct {
	started  chan struct{}
	canceled chan struct{}
}

func (blockUntilCanceledExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalGlobal,
		Deterministic: true,
		Cacheable:     true,
		OutputTypes:   []string{"video/mp4"},
	}
}

func (blockUntilCanceledExecutor) Validate(_ executor.TaskSpec) error { return nil }

func (e blockUntilCanceledExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	close(e.started)
	<-ctx.Done()
	close(e.canceled)
	return executor.ExecutionResult{Status: "failed", ErrorCode: "cancelled"}, ctx.Err()
}

// TestTaskSurvivesSessionTeardownButNotWorkerStop pins the PR-master-restart
// resume contract: a task dispatched through the receive loop runs under the
// worker-lifetime task context, so a transient session teardown (master
// restart) does NOT cancel the in-flight render; only worker shutdown (Stop)
// cancels it. Regression for the observed "master restart during RUNNING →
// job CANCELLED (context canceled)" gap.
func TestTaskSurvivesSessionTeardownButNotWorkerStop(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	reg := executor.NewRegistry()
	blocking := blockUntilCanceledExecutor{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	if err := reg.Register(blocking); err != nil {
		t.Fatalf("register blocking executor: %v", err)
	}
	rt := &recordingTransport{}

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "test-worker-resume",
			WorkerName:      "test-worker-resume",
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
		taskRunner:         taskrunner.NewTaskRunner(reg, log),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}
	wireTestReporter(w, nil)

	// Simulate Start(): derive the worker-lifetime task context.
	taskBaseCtx, taskBaseCancel := context.WithCancel(context.Background())
	defer taskBaseCancel()
	w.taskBaseCtx = taskBaseCtx
	w.taskBaseCancel = taskBaseCancel

	// Session context: canceled on a transient session teardown (master
	// restart), exactly like runSession cancels sessionCtx.
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	defer sessionCancel()

	recvCh := make(chan controltransport.ControlMessage, 2)
	w.wg.Add(1)
	go w.receiveLoop(sessionCtx, recvCh)

	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgTaskOffer,
		"master",
		controltransport.ProtocolVersionCurrent,
		&pb.TaskOffer{
			TaskId:          "task-resume-001",
			JobId:           "job-resume-001",
			AttemptId:       "attempt-resume-001",
			LeaseId:         "lease-resume-001",
			AttemptNumber:   1,
			Revision:        1,
			ExecutorId:      "scene.composite.v1",
			ExecutorVersion: 1,
			TaskSpec: mustStruct(t, map[string]interface{}{
				"payload_contract_version": 1,
				"job_id":                   "job-resume-001",
				"job_type":                 "scene.composite.v1",
				"created_at":               "2026-01-01T00:00:00Z",
			}),
		},
	)
	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgTaskLeaseGranted,
		"master",
		controltransport.ProtocolVersionCurrent,
		&pb.TaskLeaseGranted{
			TaskId:        "task-resume-001",
			JobId:         "job-resume-001",
			AttemptId:     "attempt-resume-001",
			LeaseId:       "lease-resume-001",
			AttemptNumber: 1,
			Revision:      1,
		},
	)

	// Wait for the executor to start (render in flight).
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("executor never started")
	}

	// Simulate a master restart: the session is torn down.
	sessionCancel()

	// The in-flight task must NOT be canceled by the session teardown.
	select {
	case <-blocking.canceled:
		t.Fatal("task was canceled by session teardown (master restart) — it must survive for the reconnect to resume/report it")
	case <-time.After(300 * time.Millisecond):
		// OK: task survived the session teardown.
	}

	// Worker shutdown (Stop) must cancel the in-flight task.
	taskBaseCancel()
	select {
	case <-blocking.canceled:
		// OK: worker shutdown canceled the task.
	case <-time.After(5 * time.Second):
		t.Fatal("task was not canceled by worker shutdown")
	}

	close(recvCh)
	w.wg.Wait()
}

// TestTaskContextFallsBackToSessionCtx pins the legacy-fixture fallback:
// hand-built workers that never went through Start() keep the historical
// session-scoped behavior instead of nil-panicking.
func TestTaskContextFallsBackToSessionCtx(t *testing.T) {
	w := &Worker{}
	sessionCtx := context.Background()
	if got := w.taskContext(sessionCtx); got != sessionCtx {
		t.Fatalf("taskContext(nil taskBaseCtx) = %v, want session ctx", got)
	}
}
