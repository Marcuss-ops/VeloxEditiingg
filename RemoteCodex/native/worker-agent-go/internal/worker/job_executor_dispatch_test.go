// PR-3.8/3.9: end-to-end dispatch via executor.Registry → TaskRunner.Run.
//
// NOTE: the internal/worker test binary currently panics at protobuf
// init time (pre-existing baseline). Run these tests once that is fixed.
package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// fakeSceneComposite implements executor.Executor for "scene.composite.v1".
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
		Outputs:     nil,
		Metrics:     map[string]interface{}{"fake_marker": "ok"},
		StartedAt:   time.Now().UTC(),
		CompletedAt: time.Now().UTC(),
	}, nil
}

type recordingSceneComposite struct {
	mu       sync.Mutex
	lastSpec executor.TaskSpec
	gotSpec  bool
}

func (r *recordingSceneComposite) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalGlobal,
	}
}

func (r *recordingSceneComposite) Validate(spec executor.TaskSpec) error {
	r.record(spec)
	return nil
}

func (r *recordingSceneComposite) record(spec executor.TaskSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSpec = spec
	r.gotSpec = true
}

func (r *recordingSceneComposite) Execute(
	_ context.Context,
	_ executor.ExecutionContext,
	spec executor.TaskSpec,
) (executor.ExecutionResult, error) {
	r.record(spec)
	return executor.ExecutionResult{
		Status:      "succeeded",
		Outputs:     nil,
		StartedAt:   time.Now().UTC(),
		CompletedAt: time.Now().UTC(),
	}, nil
}

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
		MaxActiveJobs: 1,
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
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		executorRegistry:   reg,
		taskRunner:         tr,
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}
	return w, rt
}

type uploadRecordingExecutor struct {
	outputPath string
}

func (e uploadRecordingExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalGlobal,
	}
}

func (e uploadRecordingExecutor) Validate(_ executor.TaskSpec) error { return nil }

func (e uploadRecordingExecutor) Execute(
	_ context.Context,
	_ executor.ExecutionContext,
	_ executor.TaskSpec,
) (executor.ExecutionResult, error) {
	if err := os.WriteFile(e.outputPath, []byte("fake-mp4-bytes"), 0o644); err != nil {
		return executor.ExecutionResult{}, err
	}
	return executor.ExecutionResult{
		Status: "succeeded",
		Outputs: []executor.ArtifactRef{{
			Type:      "render.output",
			Hash:      "deadbeefcafebabe",
			URI:       e.outputPath,
			SizeBytes: int64(len("fake-mp4-bytes")),
		}},
		StartedAt:   time.Now().UTC(),
		CompletedAt: time.Now().UTC(),
	}, nil
}

func TestPR_3_9_DispatchResolvesVoiceoverAssetBeforeExecutor(t *testing.T) {
	wantAudioBytes := []byte("ID3recorded-audio-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(wantAudioBytes)
	}))
	defer srv.Close()

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:      "test-worker-voiceover-resolve",
			WorkerName:    "test-worker-voiceover-resolve",
			LogLevel:      "info",
			MaxActiveJobs: 1,
			MasterURL:     srv.URL,
			WorkDir:       t.TempDir(),
		},
		apiClient:          api.NewClient(srv.URL),
		logger:             logger.New(logger.InfoLevel, io.Discard),
		status:             StatusIdle,
		stopChan:           make(chan struct{}),
		heartbeatBackoff:   &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:       make(map[string]time.Time),
		recentLogs:         newRecentLogBuffer(50),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}
	w.apiClient.SetAuthToken("worker-token-voiceover")

	rec := &recordingSceneComposite{}
	registry := executor.NewRegistry()
	if err := registry.Register(rec); err != nil {
		t.Fatalf("register recordingSceneComposite: %v", err)
	}
	tr := taskrunner.NewTaskRunner(registry, w.logger)

	jobCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Build a PendingTaskExecution for the dispatch test.
	pte := &PendingTaskExecution{
		JobID:      "job-voiceover-resolve",
		ExecutorID: "scene.composite.v1",
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-voiceover-resolve",
			ExecutorID: "scene.composite.v1",
			Payload: map[string]interface{}{
				"audio_path":  "velox-asset://asset-recording-001",
				"script_text": "voiceover asset resolve test",
				"output_path": "/tmp/voiceover-resolve.mp4",
			},
		},
	}

	resolvedLocal, err := w.resolveVoiceoverAudioPath(jobCtx, "velox-asset://asset-recording-001", pte.Spec.Payload)
	if err != nil {
		t.Fatalf("resolveVoiceoverAudioPath: %v", err)
	}
	if resolvedLocal == "velox-asset://asset-recording-001" {
		t.Fatalf("resolveVoiceoverAudioPath must rewrite velox-asset:// URI; got same value %q", resolvedLocal)
	}

	// Build a fresh spec with resolved path and feed through TaskRunner.
	resolvedSpec := executor.TaskSpec{
		Version:    1,
		JobID:      "job-voiceover-resolve",
		ExecutorID: "scene.composite.v1",
		Payload: map[string]interface{}{
			"audio_path":  resolvedLocal,
			"script_text": "voiceover asset resolve test",
			"output_path": "/tmp/voiceover-resolve.mp4",
		},
	}

	report, runErr := tr.Run(jobCtx, resolvedSpec)
	if runErr != nil {
		t.Fatalf("taskrunner.Run: %v", runErr)
	}
	if report.Status != "succeeded" {
		t.Fatalf("expected status=succeeded, got %q (code=%q detail=%q)",
			report.Status, report.ErrorCode, report.ErrorDetail)
	}
	if !rec.gotSpec {
		t.Fatal("recordingSceneComposite was never invoked")
	}
	gotAudio, ok := rec.lastSpec.Payload["audio_path"].(string)
	if !ok {
		t.Fatalf("recordingSceneComposite Payload[audio_path] = %T, want string", rec.lastSpec.Payload["audio_path"])
	}
	if gotAudio != resolvedLocal {
		t.Fatalf("recordingSceneComposite Payload[audio_path] = %q, want resolved local %q", gotAudio, resolvedLocal)
	}
	if !strings.HasPrefix(gotAudio, w.config.WorkDir) {
		t.Fatalf("resolved audio path %q must live under worker.WorkDir %q", gotAudio, w.config.WorkDir)
	}
}

func runExecuteTaskAsync(t *testing.T, w *Worker, ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.executeTask(ctx, pte, taskID, attemptID)
	}()
	return done
}

func TestPR_3_8_DispatchResolvesSceneCompositeV1EndToEnd(t *testing.T) {
	w, rt := newDispatchTestWorker(t)

	pte := &PendingTaskExecution{
		TaskID:     "task-composite-001",
		JobID:      "job-composite-001",
		AttemptID:  "attempt-composite-001",
		LeaseID:    "lease-composite-001",
		ExecutorID: "scene.composite.v1",
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-composite-001",
			ExecutorID: "scene.composite.v1",
			Payload: map[string]interface{}{
				"scenes": []interface{}{"sunrise", "noon"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := runExecuteTaskAsync(t, w, ctx, pte, "task-composite-001", "attempt-composite-001")

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("executeTask did not complete within 15s")
	}

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; TaskResult lost")
	}
	if msg.Type != controltransport.MsgTaskResult {
		t.Fatalf("expected MsgTaskResult, got %q", msg.Type)
	}
	tr, ok := msg.TypedPayload.(*pb.TaskResult)
	if !ok || tr == nil {
		t.Fatal("task_result TypedPayload is not *pb.TaskResult")
	}
	if tr.GetStatus() != "succeeded" {
		t.Fatalf("expected status=succeeded, got %q", tr.GetStatus())
	}
	if tr.GetTaskId() != "task-composite-001" {
		t.Fatalf("expected task_id=task-composite-001, got %q", tr.GetTaskId())
	}
	if tr.GetJobId() != "job-composite-001" {
		t.Fatalf("expected job_id=job-composite-001, got %q", tr.GetJobId())
	}
	if tr.GetExecutorId() != "scene.composite.v1" {
		t.Fatalf("expected executor_id=scene.composite.v1, got %q", tr.GetExecutorId())
	}
}

func TestPR_3_8_DispatchUnknownExecutorSurfacesFailure(t *testing.T) {
	w, rt := newDispatchTestWorker(t)

	pte := &PendingTaskExecution{
		TaskID:     "task-unknown-001",
		JobID:      "job-unknown-001",
		AttemptID:  "attempt-unknown-001",
		LeaseID:    "lease-unknown-001",
		ExecutorID: "definitely.not.registered",
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-unknown-001",
			ExecutorID: "definitely.not.registered",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := runExecuteTaskAsync(t, w, ctx, pte, "task-unknown-001", "attempt-unknown-001")

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("executeTask did not complete within 15s")
	}

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; expected failure TaskResult")
	}
	tr, ok := msg.TypedPayload.(*pb.TaskResult)
	if !ok || tr == nil {
		t.Fatalf("task_result TypedPayload is not *pb.TaskResult, got %T", msg.TypedPayload)
	}
	if tr.GetStatus() != "failed" {
		t.Fatalf("expected status=failed, got %q", tr.GetStatus())
	}
	errDetail := tr.GetErrorDetail()
	if errDetail == "" {
		t.Fatal("expected non-empty error_detail for unknown executor")
	}
	if !strings.Contains(errDetail, "executor") {
		t.Fatalf("expected error to mention executor, got %q", errDetail)
	}
	if !strings.Contains(errDetail, "not found") && !strings.Contains(errDetail, "code=") {
		t.Fatalf("expected error to mention lookup/code, got %q", errDetail)
	}
}

func TestExecuteTask_UploadsOutputBeforeSubmittingTaskResult(t *testing.T) {
	var mu sync.Mutex
	uploadCalls := 0
	var uploadedJobID, uploadedLeaseID string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/video/upload-completed" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if err := r.ParseMultipartForm(16 << 20); err != nil {
			t.Fatalf("ParseMultipartForm: %v", err)
		}
		uploadedJobID = r.FormValue("job_id")
		uploadedLeaseID = r.FormValue("lease_id")
		file, _, err := r.FormFile("video")
		if err != nil {
			t.Fatalf("FormFile(video): %v", err)
		}
		defer file.Close()
		payload, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("ReadAll(video): %v", err)
		}
		if string(payload) != "fake-mp4-bytes" {
			t.Fatalf("uploaded payload = %q, want fake-mp4-bytes", string(payload))
		}
		mu.Lock()
		uploadCalls++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":          true,
			"job_id":      r.FormValue("job_id"),
			"artifact_id": "artifact-upload-001",
			"upload_id":   "upload-001",
			"status":      "SUCCEEDED",
			"size":        len(payload),
			"sha256":      "deadbeefcafebabe",
		})
	}))
	defer srv.Close()

	log := logger.New(logger.InfoLevel, io.Discard)
	reg := executor.NewRegistry()
	outputPath := filepath.Join(t.TempDir(), "rendered.mp4")
	if err := reg.Register(uploadRecordingExecutor{outputPath: outputPath}); err != nil {
		t.Fatalf("register uploadRecordingExecutor: %v", err)
	}

	rt := &recordingTransport{}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:      "test-worker-upload-001",
			WorkerName:    "test-worker-upload",
			LogLevel:      "info",
			MaxActiveJobs: 1,
			MasterURL:     srv.URL,
		},
		apiClient:          api.NewClient(srv.URL, api.WithWorkerID("test-worker-upload-001")),
		logger:             log,
		transport:          rt,
		status:             StatusIdle,
		stopChan:           make(chan struct{}),
		heartbeatBackoff:   &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:       make(map[string]time.Time),
		recentLogs:         newRecentLogBuffer(50),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		executorRegistry:   reg,
		taskRunner:         taskrunner.NewTaskRunner(reg, log),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}

	pte := &PendingTaskExecution{
		TaskID:          "task-upload-001",
		JobID:           "job-upload-001",
		AttemptID:       "attempt-upload-001",
		AttemptNumber:   3,
		LeaseID:         "lease-upload-001",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		Revision:        17,
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-upload-001",
			ExecutorID: "scene.composite.v1",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	<-runExecuteTaskAsync(t, w, ctx, pte, pte.TaskID, pte.AttemptID)

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; TaskResult lost")
	}
	tr, ok := msg.TypedPayload.(*pb.TaskResult)
	if !ok || tr == nil {
		t.Fatalf("task_result TypedPayload is not *pb.TaskResult, got %T", msg.TypedPayload)
	}
	if tr.GetStatus() != "succeeded" {
		t.Fatalf("expected status=succeeded, got %q", tr.GetStatus())
	}
	if uploadedJobID != pte.JobID || uploadedLeaseID != pte.LeaseID {
		t.Fatalf("upload fields mismatch: job=%q lease=%q", uploadedJobID, uploadedLeaseID)
	}
	mu.Lock()
	defer mu.Unlock()
	if uploadCalls != 1 {
		t.Fatalf("uploadCalls = %d, want 1", uploadCalls)
	}
}

func TestExecuteTask_FailsTaskWhenUploadFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"ok":false,"error":"begin upload rejected"}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	log := logger.New(logger.InfoLevel, io.Discard)
	reg := executor.NewRegistry()
	outputPath := filepath.Join(t.TempDir(), "rendered.mp4")
	if err := reg.Register(uploadRecordingExecutor{outputPath: outputPath}); err != nil {
		t.Fatalf("register uploadRecordingExecutor: %v", err)
	}

	rt := &recordingTransport{}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:      "test-worker-upload-fail-001",
			WorkerName:    "test-worker-upload-fail",
			LogLevel:      "info",
			MaxActiveJobs: 1,
			MasterURL:     srv.URL,
		},
		apiClient:          api.NewClient(srv.URL, api.WithWorkerID("test-worker-upload-fail-001")),
		logger:             log,
		transport:          rt,
		status:             StatusIdle,
		stopChan:           make(chan struct{}),
		heartbeatBackoff:   &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:       make(map[string]time.Time),
		recentLogs:         newRecentLogBuffer(50),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		executorRegistry:   reg,
		taskRunner:         taskrunner.NewTaskRunner(reg, log),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		version:            "test",
	}

	pte := &PendingTaskExecution{
		TaskID:          "task-upload-fail-001",
		JobID:           "job-upload-fail-001",
		AttemptID:       "attempt-upload-fail-001",
		AttemptNumber:   2,
		LeaseID:         "lease-upload-fail-001",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		Revision:        11,
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-upload-fail-001",
			ExecutorID: "scene.composite.v1",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	<-runExecuteTaskAsync(t, w, ctx, pte, pte.TaskID, pte.AttemptID)

	msg, ok := rt.last()
	if !ok {
		t.Fatal("transport.Send was never called; expected failure TaskResult")
	}
	tr, ok := msg.TypedPayload.(*pb.TaskResult)
	if !ok || tr == nil {
		t.Fatalf("task_result TypedPayload is not *pb.TaskResult, got %T", msg.TypedPayload)
	}
	if tr.GetStatus() != "failed" {
		t.Fatalf("expected status=failed, got %q", tr.GetStatus())
	}
	if !strings.Contains(tr.GetErrorDetail(), "upload task outputs") {
		t.Fatalf("expected upload failure in error_detail, got %q", tr.GetErrorDetail())
	}
}

func TestNormalizeOfferedExecutorID_StripsVersionedKey(t *testing.T) {
	if got := normalizeOfferedExecutorID("scene.composite.v1@1"); got != "scene.composite.v1" {
		t.Fatalf("normalizeOfferedExecutorID(versioned) = %q, want %q", got, "scene.composite.v1")
	}
	if got := normalizeOfferedExecutorID("scene.composite.v1"); got != "scene.composite.v1" {
		t.Fatalf("normalizeOfferedExecutorID(unversioned) = %q, want unchanged", got)
	}
}
