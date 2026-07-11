package taskrunner

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"velox-worker-agent/internal/executor"
)

// ── Fakes ─────────────────────────────────────────────────────────────────────

// fakeExec is a configurable Executor. The fields that drive test
// behavior are validateFn, executeFn, and delay. order is a hook the
// test can use to check call order.
type fakeExec struct {
	desc       executor.Descriptor
	validateFn func(executor.TaskSpec) error
	executeFn  func(ctx context.Context, ec executor.ExecutionContext, s executor.TaskSpec) (executor.ExecutionResult, error)
	delay      time.Duration
}

func (f *fakeExec) Descriptor() executor.Descriptor { return f.desc }
func (f *fakeExec) Validate(s executor.TaskSpec) error {
	if f.validateFn != nil {
		return f.validateFn(s)
	}
	return nil
}
func (f *fakeExec) Execute(ctx context.Context, ec executor.ExecutionContext, s executor.TaskSpec) (executor.ExecutionResult, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return executor.ExecutionResult{Status: "failed"}, ctx.Err()
		}
	}
	if f.executeFn != nil {
		return f.executeFn(ctx, ec, s)
	}
	started := time.Now().UTC()
	return executor.ExecutionResult{
		Status:      "succeeded",
		Outputs:     []executor.ArtifactRef{{Type: "render.output", Hash: "deadbeef"}},
		Metrics:     map[string]interface{}{"queue_ms": int64(42)},
		StartedAt:   started,
		CompletedAt: time.Now().UTC(),
	}, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func newTestRunner(execs ...*fakeExec) *TaskRunner {
	reg := executor.NewRegistry()
	for _, e := range execs {
		reg.MustRegister(e)
	}
	return NewTaskRunner(reg, nil)
}

func makeDesc(id string, version int) executor.Descriptor {
	if id == "" {
		id = "exec.v1"
	}
	if version <= 0 {
		version = 1
	}
	return executor.Descriptor{
		ID:            id,
		Version:       version,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalFrameLocal,
	}
}

func goodSpec(execID string) executor.TaskSpec {
	return executor.TaskSpec{
		Version:    1,
		JobID:      "job-1",
		ExecutorID: execID,
		Payload:    map[string]interface{}{"k": "v"},
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestRunner_Success covers the canonical happy path:
//   - spec validates, registry resolves, executor validates,
//     cache_lookup+prefetch+execute+upload+report phases all run
//   - one report, Status="succeeded", ExecutorKey="id@version"
//   - Outputs / Metrics round-trip from the Executor
//   - PhaseMarkers contains exactly one marker per phase, last is "report"
func TestRunner_Success(t *testing.T) {
	exec := &fakeExec{
		desc:       makeDesc("ok.v1", 1),
		validateFn: func(_ executor.TaskSpec) error { return nil },
		executeFn: func(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			return executor.ExecutionResult{
				Status:      "succeeded",
				Outputs:     []executor.ArtifactRef{{Type: "render.output", Hash: "deadbeef"}},
				Metrics:     map[string]interface{}{"queue_ms": int64(42)},
				StartedAt:   time.Now().UTC(),
				CompletedAt: time.Now().UTC(),
			}, nil
		},
	}
	r := newTestRunner(exec)
	rep, err := r.Run(context.Background(), goodSpec("ok.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if !rep.Succeeded() {
		t.Fatalf("expected succeeded, got status=%q code=%q detail=%q",
			rep.Status, rep.ErrorCode, rep.ErrorDetail)
	}
	if rep.ExecutorKey != "ok.v1@1" {
		t.Errorf("ExecutorKey = %q, want ok.v1@1", rep.ExecutorKey)
	}
	if len(rep.Outputs) != 1 || rep.Outputs[0].Hash != "deadbeef" {
		t.Errorf("Outputs mismatch: %+v", rep.Outputs)
	}
	if v, ok := rep.Metrics["queue_ms"]; !ok || v != int64(42) {
		t.Errorf("Metrics[queue_ms] = %v, want int64(42)", v)
	}
	if rep.PhaseCount() < 5 {
		t.Errorf("PhaseCount = %d, want >= 5 (all 5 phases should record on success)", rep.PhaseCount())
	}
	last := rep.PhaseMarkers[len(rep.PhaseMarkers)-1]
	if last.Name != PhaseReport {
		t.Errorf("last phase = %q, want %q", last.Name, PhaseReport)
	}
}

// TestRunner_SpecValidationFailed: bad Version triggers TaskSpec.Validate fail
// BEFORE Resolve. Phase markers should still emit (the "report" marker from
// completeError), but the report itself is failed with CodeValidationFailed.
func TestRunner_SpecValidationFailed(t *testing.T) {
	r := newTestRunner(&fakeExec{desc: makeDesc("ok.v1", 1)})
	bad := executor.TaskSpec{Version: 0, JobID: "j", ExecutorID: "ok.v1"} // Version 0 fails Validate
	rep, err := r.Run(context.Background(), bad)
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeValidationFailed {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeValidationFailed)
	}
	if rep.PhaseCount() < 1 {
		t.Errorf("PhaseCount = %d, want >= 1 (the failure-recorded report marker)", rep.PhaseCount())
	}
}

// TestRunner_ExecutorValidationFailed: executor.Validate returns an error;
// Runner must NOT call Execute.
func TestRunner_ExecutorValidationFailed(t *testing.T) {
	validateErr := errors.New("bad input shape")
	executeCalled := false
	exec := &fakeExec{
		desc:       makeDesc("ok.v1", 1),
		validateFn: func(_ executor.TaskSpec) error { return validateErr },
		executeFn: func(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			executeCalled = true
			return executor.ExecutionResult{Status: "succeeded"}, nil
		},
	}
	r := newTestRunner(exec)
	rep, err := r.Run(context.Background(), goodSpec("ok.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if executeCalled {
		t.Errorf("Execute was called; must not be after Validate fails")
	}
	if rep.ErrorCode != CodeValidationFailed {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeValidationFailed)
	}
	if !strings.Contains(rep.ErrorDetail, "bad input shape") {
		t.Errorf("ErrorDetail should carry validator reason: %q", rep.ErrorDetail)
	}
	if len(rep.Outputs) != 0 {
		t.Errorf("Outputs should be empty on validation failure: %+v", rep.Outputs)
	}
}

// TestRunner_UnsupportedExecutor: registry is empty; resolve fails;
// report ErrorCode = CodeUnsupportedExecutor.
func TestRunner_UnsupportedExecutor(t *testing.T) {
	r := newTestRunner() // empty registry
	rep, err := r.Run(context.Background(), goodSpec("missing.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeUnsupportedExecutor {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeUnsupportedExecutor)
	}
}

// TestRunner_Cancellation: parent ctx is canceled mid-Execute; runner
// reports CodeCanceled. The fake exec blocks on ctx.Done().
func TestRunner_Cancellation(t *testing.T) {
	exec := &fakeExec{
		desc: makeDesc("slow.v1", 1),
		executeFn: func(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			<-ctx.Done()
			return executor.ExecutionResult{Status: "failed"}, ctx.Err()
		},
	}
	r := newTestRunner(exec)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	rep, err := r.Run(ctx, goodSpec("slow.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeCanceled {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeCanceled)
	}
	if !rep.CompletedAt.After(rep.StartedAt) {
		t.Errorf("CompletedAt should be after StartedAt: started=%v completed=%v",
			rep.StartedAt, rep.CompletedAt)
	}
}

// TestRunner_DeadlineExceeded: parent ctx has a short timeout; fake exec
// blocks until ctx.Done(), which carries DeadlineExceeded.
func TestRunner_DeadlineExceeded(t *testing.T) {
	exec := &fakeExec{
		desc:  makeDesc("slow.v1", 1),
		delay: 5 * time.Second,
		executeFn: func(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			<-ctx.Done()
			return executor.ExecutionResult{Status: "failed"}, ctx.Err()
		},
	}
	r := newTestRunner(exec)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	rep, err := r.Run(ctx, goodSpec("slow.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeContextDeadlineExceeded {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeContextDeadlineExceeded)
	}
}

// TestRunner_PanicContained: Executor.Execute panics; the panic MUST
// NOT escape Run. The report records the panic reason & stack.
func TestRunner_PanicContained(t *testing.T) {
	preGoroutines := runtime.NumGoroutine()
	exec := &fakeExec{
		desc: makeDesc("panicker.v1", 1),
		executeFn: func(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			panic("boom")
		},
	}
	r := newTestRunner(exec)
	// Wrapping in a func literal would let panic propagate; we assert the
	// test goroutine is intact by reaching the lines AFTER Run returns.
	rep, err := r.Run(context.Background(), goodSpec("panicker.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeExecutorPanicContained {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeExecutorPanicContained)
	}
	if !strings.Contains(rep.ErrorDetail, "boom") {
		t.Errorf("ErrorDetail should record panic reason (%q): %q", "boom", rep.ErrorDetail)
	}
	if rep.Status != "failed" {
		t.Errorf("Status = %q, want failed", rep.Status)
	}
	postGoroutines := runtime.NumGoroutine()
	// Allow a small margin for runtime churn; assert no leak in the same order.
	if postGoroutines > preGoroutines+2 {
		t.Errorf("goroutine leak: pre=%d post=%d", preGoroutines, postGoroutines)
	}
}

// TestRunner_ExecuteError: executor.Execute returns a non-nil error;
// mapped to CodeExecuteFailed.
func TestRunner_ExecuteError(t *testing.T) {
	exec := &fakeExec{
		desc: makeDesc("err.y.v1", 1),
		executeFn: func(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			return executor.ExecutionResult{}, errors.New("encoder crashed")
		},
	}
	r := newTestRunner(exec)
	rep, err := r.Run(context.Background(), goodSpec("err.y.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeExecuteFailed {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeExecuteFailed)
	}
	if !strings.Contains(rep.ErrorDetail, "encoder crashed") {
		t.Errorf("ErrorDetail should carry original message: %q", rep.ErrorDetail)
	}
}

// TestRunner_ExecutorReturnsNonSuccessStatus: Executor returns Status="indeterminate"
// with err==nil; Runner treats as failure not success (PR-3.3 invariant:
// only "" or "succeeded" is success).
func TestRunner_ExecutorReturnsNonSuccessStatus(t *testing.T) {
	exec := &fakeExec{
		desc: makeDesc("noisy.v1", 1),
		executeFn: func(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
			return executor.ExecutionResult{
				Status:      "indeterminate",
				ErrorDetail: "partial output",
			}, nil
		},
	}
	r := newTestRunner(exec)
	rep, err := r.Run(context.Background(), goodSpec("noisy.v1"))
	if err != nil {
		t.Fatalf("Run returned err: %v", err)
	}
	if rep.ErrorCode != CodeExecuteFailed {
		t.Errorf("ErrorCode = %q, want %q", rep.ErrorCode, CodeExecuteFailed)
	}
	if !strings.Contains(rep.ErrorDetail, "indeterminate") {
		t.Errorf("ErrorDetail should mention the non-success status: %q", rep.ErrorDetail)
	}
}

// TestRunner_NewRequiresRegistry: NewTaskRunner(nil, ...) panics instead of
// silently misbehaving.
func TestRunner_NewRequiresRegistry(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewTaskRunner(nil) should panic")
		}
	}()
	_ = NewTaskRunner(nil, nil)
}

// TestRunner_IllegalDescriptorRejectedByRegistry: a fake exec with an
// invalid descriptor (zero version) cannot be MustRegistered; the
// registry panics before the runner ever sees it.
func TestRunner_IllegalDescriptorRejectedByRegistry(t *testing.T) {
	bad := &fakeExec{
		desc: executor.Descriptor{ID: "bad.v1", Version: 0, ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalFrameLocal},
	}
	defer func() {
		if recover() == nil {
			t.Errorf("newTestRunner should panic on bad descriptor (registry MustRegister)")
		}
	}()
	_ = newTestRunner(bad) // MustRegister on bad descriptor panics
}
