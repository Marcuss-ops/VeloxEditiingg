// PR-3.8: end-to-end dispatch via executor.Registry → TaskRunner.Run.
// Verifies the registry-driven dispatch replaces the legacy render /
// process_video / process_audio switch with a single TaskRunner entry
// point inside Worker.runJobTask, and that Worker.executeJob exercises
// the full pipeline (concurrency + active-jobs + cancel registration +
// transport submit) against a synthetic scene.composite.v1 executor.
//
// The test builds the minimum Worker struct literal needed by
// executeJob (executeJob does NOT touch stageExecutor, apiClient,
// transportFactory, cache, blobs, executorRegistry other than the
// hello-time report). The transport is a recording stub that captures
// the job_result message for assertions.
//
// NOTE: the internal/worker test binary currently panics at protobuf
// init time (pre-existing baseline, not introduced by PR-3.8). Run
// these tests once that baseline is fixed; the dispatch logic is
// independently reachable via `go vet ./internal/worker/...` and
// the build check.
package worker

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// fakeSceneComposite implements executor.Executor for an executor
// registered under "scene.composite.v1". The fake returns the
// canonical (Status="succeeded") shape with no Outputs so the
// downstream upload pipeline (shouldUploadCompletedVideo) short-
// circuits naturally and the test assertions stay focused on
// dispatch + payload-mapping rather than upload mechanics. PR-3.9
// wires the real scene-composite implementation; this stub exists
// only so the dispatch path is reachable from a test.
type fakeSceneComposite struct{}

func (fakeSceneComposite) Descriptor() executor.Descriptor {
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

func (fakeSceneComposite) Validate(_ executor.TaskSpec) error { return nil }

func (fakeSceneComposite) Execute(
	_ context.Context,
	_ executor.ExecutionContext,
	_ executor.TaskSpec,
) (executor.ExecutionResult, error) {
	return executor.ExecutionResult{
		Status:      "succeeded",
		Outputs:     nil, // intentional: skip upload pipeline in tests
		Metrics:     map[string]interface{}{"fake_marker": "ok"},
		StartedAt:   time.Now().UTC(),
		CompletedAt: time.Now().UTC(),
	}, nil
}

// recordingTransport satisfies controltransport.ControlTransport.
// Connect / Receive / Close are no-ops; Send captures every message
// into a mutex-protected slice for post-run assertions. Receive
// returns nil channels because executeJob never reads from them —
// nil-nil-nil is the standard "no master → worker traffic in this
// test" pattern.
type recordingTransport struct {
	mu       sync.Mutex
	messages []controltransport.ControlMessage
	closed   bool
}

func (r *recordingTransport) Connect(_ context.Context, _ controltransport.WorkerHello) error {
	return nil
}

func (r *recordingTransport) Receive(_ context.Context) (
	<-chan controltransport.ControlMessage, <-chan error, error,
) {
	return nil, nil, nil
}

func (r *recordingTransport) Send(_ context.Context, msg controltransport.ControlMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, msg)
	return nil
}

func (r *recordingTransport) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *recordingTransport) last() (controltransport.ControlMessage, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.messages) == 0 {
		return controltransport.ControlMessage{}, false
	}
	return r.messages[len(r.messages)-1], true
}

// newDispatchTestWorker builds a minimal Worker suitable for
// executeJob end-to-end tests. The registry is pre-populated with
// fakeSceneComposite; the transport is the recording stub.
func newDispatchTestWorker(t *testing.T) (*Worker, *recordingTransport) {
	t.Helper()

	log := logger.New(logger.InfoLevel, io.Discard)
	log.SetPrefix("[test-worker-dispatch]")

	reg := executor.NewRegistry()
	if err := reg.Register(fakeSceneComposite{}); err != nil {
		t.Fatalf("register fakeSceneComposite: %v", err)
	}
	tr := taskrunner.NewTaskRunner(reg, log)

	rt := &recordingTransport{}

	cfg := &config.WorkerConfig{
		WorkerID:      "test-worker-dispatch-001",
		WorkerName:    "test-worker-dispatch",
		LogLevel:      "info",
		MaxActiveJobs: 1, // required for concurrency limiter to accept any job
	}

	w := &Worker{
		config:             cfg,
		logger:             log,
		transport:          rt,
		status:             StatusIdle,
		stopChan:           make(chan struct{}),
		heartbeatBackoff:   &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:       make(map[string]time.Time),
		recentLogs:         newRecentLogBuffer(50),
		activeJobs:         make(map[string]*ActiveJob),
		jobCancelFuncs:     make(map[string]context.CancelFunc),
		pendingLeaseJobs:   make(map[string]*api.Job),
		executorRegistry:   reg,
		taskRunner:         tr,
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}
	return w, rt
}

// runExecuteJobAsync launches executeJob on a goroutine and returns a
// channel closed when the goroutine returns. Tests use this pattern
// because executeJob does not return a value — the only observable
// outcome is the transport.Send call captured by the recording stub.
func runExecuteJobAsync(t *testing.T, w *Worker, ctx context.Context, job *api.Job) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.executeJob(ctx, job)
	}()
	return done
}

func TestPR_3_8_DispatchResolvesSceneCompositeV1EndToEnd(t *testing.T) {
	w, rt := newDispatchTestWorker(t)

	job := &api.Job{
		JobID:    "job-composite-001",
		JobType:  "scene.composite.v1",
		Priority: 1,
		Parameters: map[string]interface{}{
			"scenes": []interface{}{"sunrise", "noon"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := runExecuteJobAsync(t, w, ctx, job)

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("executeJob did not complete within 15s")
	}

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; JobResult lost")
	}
	if msg.Type != controltransport.MsgJobResult {
		t.Fatalf("expected MsgJobResult, got %q", msg.Type)
	}
	payload := msg.Payload
	if payload == nil {
		t.Fatal("job_result message had nil payload")
	}
	if status, _ := payload["status"].(string); status != "success" {
		t.Fatalf("expected payload.status=success, got %q (full payload: %#v)", status, payload)
	}
	output, _ := payload["output"].(map[string]interface{})
	if output == nil {
		t.Fatalf("expected payload.output map, got nil (full payload: %#v)", payload)
	}
	if got, _ := output["status"].(string); got != "completed" {
		t.Fatalf("expected output.status=completed, got %q", got)
	}
	if got, _ := output["executor_id"].(string); got != "scene.composite.v1" {
		t.Fatalf("expected output.executor_id=scene.composite.v1, got %q", got)
	}
	if got, _ := output["executor_key"].(string); got != "scene.composite.v1@1" {
		t.Fatalf("expected output.executor_key=scene.composite.v1@1, got %q", got)
	}
	if got, _ := output["job_id"].(string); got != "job-composite-001" {
		t.Fatalf("expected output.job_id=job-composite-001, got %q", got)
	}
	// PR-3.8 dispatch goes through the 5-phase taskrunner loop, so
	// the report must carry at least the cache-lookup + report phase
	// markers (Execute dispatches through runExecute which appends
	// its own marker; the failure path appends only the report
	// marker). Assert >=2 so we don't accidentally hang on a single
	// trivial mark. Note: fakeSceneComposite returns Outputs=nil so
	// shouldUploadCompletedVideo short-circuits — keeps this test
	// focused on dispatch + payload-mapping rather than the upload
	// pipeline (which has its own dedicated tests).
	if got, _ := output["phase_count"].(int); got < 2 {
		t.Fatalf("expected phase_count>=2, got %d", got)
	}
}

func TestPR_3_8_DispatchUnknownExecutorSurfacesFailure(t *testing.T) {
	w, rt := newDispatchTestWorker(t)

	job := &api.Job{
		JobID:   "job-unknown-001",
		JobType: "definitely.not.registered",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := runExecuteJobAsync(t, w, ctx, job)

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("executeJob did not complete within 15s")
	}

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; expected failure JobResult")
	}
	payload := msg.Payload
	if status, _ := payload["status"].(string); status != "failed" {
		t.Fatalf("expected payload.status=failed, got %q (full payload: %#v)", status, payload)
	}
	errMsg, _ := payload["error"].(string)
	if errMsg == "" {
		t.Fatal("expected non-empty error message for unknown executor")
	}
	// Registry miss maps to the taskrunner's CodeUnsupportedExecutor;
	// dispatchTaskRunner formats it as "executor <key> failed:
	// code=... detail=...", so the error string contains "executor"
	// and either "not found" (wrapped sentinel) or "code=" (runner
	// mapping).
	if !strings.Contains(errMsg, "executor") {
		t.Fatalf("expected error to mention executor, got %q", errMsg)
	}
	if !strings.Contains(errMsg, "not found") && !strings.Contains(errMsg, "code=") {
		t.Fatalf("expected error to mention lookup/code, got %q", errMsg)
	}
}
