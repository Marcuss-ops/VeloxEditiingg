package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/plan"
)

// fakeRenderClient is a stub RenderClient used by runner_test.go to
// verify that RenderClient() exposes the same instance the constructor
// was given.
type fakeRenderClient struct {
	called  bool
	payload []byte
}

type hookTestCompiler struct{}

func (hookTestCompiler) ID() string { return "hook-test.v1" }

func (hookTestCompiler) Validate(map[string]interface{}) error { return nil }

func (hookTestCompiler) Compile(_ context.Context, jobID string, _ map[string]interface{}, outputPath string) (*plan.RenderPlan, error) {
	return &plan.RenderPlan{
		Version: 1,
		JobID:   jobID,
		Canvas:  plan.CanvasSpec{Width: 1, Height: 1, Fps: 1},
		Timeline: []plan.TimelineItem{{
			Source:          plan.MediaSource{Type: "color", ColorHex: "#000000"},
			DurationSeconds: 0.1,
		}},
		OutputPath: outputPath,
	}, nil
}

func (f *fakeRenderClient) Render(_ context.Context, p *plan.RenderPlan) error {
	_, err := f.RenderWithMetrics(context.Background(), p)
	return err
}

func (f *fakeRenderClient) RenderWithMetrics(_ context.Context, p *plan.RenderPlan) (RenderMetrics, error) {
	f.called = true
	if p == nil || p.OutputPath == "" {
		return RenderMetrics{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(p.OutputPath), 0o755); err != nil {
		return RenderMetrics{}, err
	}
	return RenderMetrics{}, os.WriteFile(p.OutputPath, f.payload, 0o644)
}

// TestRunner_RenderClientAccessor: the accessor's contract is "return
// the SAME pointer the constructor was given". A test that synthesises
// a Runner, then calls its Render accessor, verifies the resulting
// interface still drives the underlying fake.
func TestRunner_RenderClientAccessor(t *testing.T) {
	reg := NewRegistry()
	rc := &fakeRenderClient{payload: []byte("accessor-test-bytes")}
	log := logger.New(logger.InfoLevel, os.Stderr)
	runner := NewRunner(reg, rc, log)
	if runner == nil {
		t.Fatalf("NewRunner returned nil")
	}
	rcViaAccessor := runner.RenderClient()
	if rcViaAccessor == nil {
		t.Fatalf("RenderClient() returned nil on non-nil Runner")
	}
	// The returned interface is structurally identical to bootstrap.RenderClientIface,
	// so pkg/bootstrap.Run’s engine step can drive it through this accessor without
	// any adapter struct.
	_ = rcViaAccessor
	// Drive the render via the accessor path. If the accessor routes
	// the SAME pointer, the embedded fake's `called` flag flips.
	p := &plan.RenderPlan{
		Version: 1,
		JobID:   "test.accessor",
		Canvas:  plan.CanvasSpec{Width: 1, Height: 1, Fps: 1},
		Timeline: []plan.TimelineItem{
			{Source: plan.MediaSource{Type: "color", ColorHex: "#000000"}, DurationSeconds: 0.1},
		},
		OutputPath: filepath.Join(t.TempDir(), "out.mp4"),
	}
	if err := rcViaAccessor.Render(context.Background(), p); err != nil {
		t.Fatalf("Render via accessor failed: %v", err)
	}
	if !rc.called {
		t.Fatalf("fakeRenderClient.Render not invoked \u2014 accessor returned a different instance")
	}
	if _, err := os.Stat(p.OutputPath); err != nil {
		t.Fatalf("output not written via accessor: %v", err)
	}
}

// TestRunner_RenderClient_NilRunner: nil-receiver safety. Ensures
// pkg/bootstrap can defensively read accessors without panicking on a
// partially-initialised Runner.
func TestRunner_RenderClient_NilRunner(t *testing.T) {
	var r *Runner
	if got := r.RenderClient(); got != nil {
		t.Fatalf("RenderClient on nil Runner should return nil; got %v", got)
	}
}

func TestRunnerCompileCompletedHookRunsBeforeRender(t *testing.T) {
	reg := NewRegistry()
	reg.Register(hookTestCompiler{})
	rc := &fakeRenderClient{payload: []byte("hook-test-bytes")}
	runner := NewRunner(reg, rc, logger.New(logger.InfoLevel, os.Stderr))
	hookCalled := false
	hookSawRender := false
	ctx := WithCompileCompletedHook(context.Background(), func() {
		hookCalled = true
		hookSawRender = rc.called
	})

	_, err := runner.RunWithMetrics(ctx, "hook-test.v1", "hook-test", nil, filepath.Join(t.TempDir(), "out.mp4"))
	if err != nil {
		t.Fatalf("RunWithMetrics failed: %v", err)
	}
	if !hookCalled {
		t.Fatal("compile-completed hook was not called")
	}
	if hookSawRender {
		t.Fatal("compile-completed hook ran after render started")
	}
}
