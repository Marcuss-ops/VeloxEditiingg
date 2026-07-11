package taskrunner

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-worker-agent/internal/executor"
)

// recordingLogger captures Info/Warn/Error invocations so we can assert
// the executor-context fields surface in emitted logs.
type recordingLogger struct {
	infos []string
	warns []string
	errs  []string
}

func (r *recordingLogger) Info(msg string, _ map[string]interface{}) { r.infos = append(r.infos, msg) }
func (r *recordingLogger) Warn(msg string, _ map[string]interface{}) { r.warns = append(r.warns, msg) }
func (r *recordingLogger) Error(msg string, err error, _ map[string]interface{}) {
	if err != nil {
		r.errs = append(r.errs, msg+" "+err.Error())
	} else {
		r.errs = append(r.errs, msg)
	}
}

func newSpec(jobID, execID string) executor.TaskSpec {
	return executor.TaskSpec{Version: 1, JobID: jobID, ExecutorID: execID}
}

func TestNewRunnerContext_RequiresLogger(t *testing.T) {
	_, err := newRunnerContext(ContextOptions{
		Spec:      newSpec("j", "e"),
		ParentCtx: context.Background(),
	})
	if err == nil {
		t.Fatal("expected error when Logger is nil, got nil")
	}
	if !errors.Is(err, ErrInternalRunnerFault) {
		t.Errorf("err = %v, want ErrInternalRunnerFault", err)
	}
}

func TestNewRunnerContext_DefaultsApplied(t *testing.T) {
	logger := &recordingLogger{}
	rc, err := newRunnerContext(ContextOptions{
		Spec:      newSpec("j-1", "exec.v1"),
		ParentCtx: context.Background(),
		Logger:    logger,
		// all other deps nil → defaults
	})
	if err != nil {
		t.Fatalf("newRunnerContext: %v", err)
	}
	if rc == nil {
		t.Fatal("nil RunnerContext")
	}
	if got := rc.Artifacts(); got == nil {
		t.Errorf("default Artifacts should be non-nil")
	}
	if got := rc.LocalCache(); got == nil {
		t.Errorf("default LocalCache should be non-nil")
	}
	if got := rc.Telemetry(); got == nil {
		t.Errorf("default Telemetry should be non-nil")
	}
	if got := rc.Resources(); got == nil {
		t.Errorf("default Resources should be non-nil")
	}
	if got := rc.Clock(); got == nil {
		t.Errorf("default Clock should be non-nil")
	}
	if got := rc.Spec(); got.JobID != "j-1" || got.ExecutorID != "exec.v1" {
		t.Errorf("Spec wrong: %+v", got)
	}
}

func TestNewRunnerContext_NilParent_DefaultsToBackground(t *testing.T) {
	rc, err := newRunnerContext(ContextOptions{
		Spec:   newSpec("j", "e"),
		Logger: &recordingLogger{},
		// ParentCtx nil
	})
	if err != nil {
		t.Fatalf("newRunnerContext: %v", err)
	}
	if err := rc.Err(); err != nil {
		t.Errorf("Err() should be nil with default background ctx, got %v", err)
	}
}

func TestRunnerContext_DonePropagatesFromParent(t *testing.T) {
	rc, _ := newRunnerContext(ContextOptions{
		Spec:      newSpec("j", "e"),
		ParentCtx: context.Background(),
		Logger:    &recordingLogger{},
	})
	parent, cancel := context.WithCancel(context.Background())
	_ = rc // constructed above; replacement with parent-bound
	rc, _ = newRunnerContext(ContextOptions{
		Spec:      newSpec("j", "e"),
		ParentCtx: parent,
		Logger:    &recordingLogger{},
	})

	// Done must NOT be closed yet.
	select {
	case <-rc.Done():
		t.Fatalf("Done() closed before parent canceled")
	default:
	}

	cancel()

	// Now Done must close and Err() must report cancelation.
	select {
	case <-rc.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Done() did not close within 100ms after parent canceled")
	}
	if err := rc.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Err() = %v, want context.Canceled", err)
	}
}

func TestRunnerContext_ExplicitCancel(t *testing.T) {
	rc, _ := newRunnerContext(ContextOptions{
		Spec:      newSpec("j", "e"),
		ParentCtx: context.Background(),
		Logger:    &recordingLogger{},
	})
	rc.Cancel()
	select {
	case <-rc.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Done() did not close after Cancel()")
	}
	if err := rc.Err(); err == nil {
		t.Errorf("Err() should be non-nil after Cancel()")
	}
}

func TestLoggerFormat_IncludesFields(t *testing.T) {
	wl := &workerExecLogger{
		fields: map[string]interface{}{"executor_id": "x.v1", "job_id": "j-1"},
	}
	wl.Info("hello", map[string]interface{}{"k": 1})
	wl.Warn("oops", nil)
	wl.Error("oh no", errors.New("boom"), nil)
}

func TestFormatFields_NoFields(t *testing.T) {
	if got := formatFields(nil); got != "" {
		t.Errorf("formatFields(nil) = %q, want empty", got)
	}
}

func TestDefaultResources_NonZeroCPU(t *testing.T) {
	r := DefaultResources()
	if r == nil {
		t.Fatal("DefaultResources returned nil")
	}
	if cpu := r.CPU(); cpu <= 0 {
		t.Errorf("CPU = %d, want > 0", cpu)
	}
	if mc := r.MaxConcurrent(); mc <= 0 {
		t.Errorf("MaxConcurrent = %d, want > 0", mc)
	}
}

func TestNoopArtifacts_RespectsPolicy(t *testing.T) {
	a := noopArtifacts{}
	if _, err := a.Get(context.Background(), "h"); err == nil {
		t.Errorf("noopArtifacts.Get should return error (explicit-fail policy)")
	}
	if err := a.Put(context.Background(), "h", nil); err == nil {
		t.Errorf("noopArtifacts.Put should return error (explicit-fail policy)")
	}
}

func TestNoopCache_ReturnsNotFound(t *testing.T) {
	c := noopCache{}
	data, found, err := c.Get(context.Background(), "h")
	if err != nil {
		t.Errorf("noopCache.Get err = %v, want nil", err)
	}
	if found {
		t.Errorf("noopCache.Get found = true, want false")
	}
	if data != nil {
		t.Errorf("noopCache.Get data = %v, want nil", data)
	}
	if err := c.Put(context.Background(), "h", nil); err != nil {
		t.Errorf("noopCache.Put err = %v, want nil", err)
	}
}
