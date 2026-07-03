package executors

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

// ── Fakes ──────────────────────────────────────────────────────────────────────

// fakeCompiler implements pipeline.Compiler with hard-coded behavior.
type fakeCompiler struct {
	id          string
	validate    bool
	plan        *plan.RenderPlan
	validateErr error
	compileErr  error
}

func (f *fakeCompiler) ID() string { return f.id }
func (f *fakeCompiler) Validate(_ map[string]interface{}) error {
	if !f.validate {
		return errors.New("fakecompiler: validation disabled")
	}
	return f.validateErr
}
func (f *fakeCompiler) Compile(_ context.Context, jobID string, input map[string]interface{}, outputPath string) (*plan.RenderPlan, error) {
	if f.compileErr != nil {
		return nil, f.compileErr
	}
	if f.plan == nil {
		f.plan = &plan.RenderPlan{
			Version: 1,
			JobID:   jobID,
			Canvas:  plan.DefaultCanvas(),
			Timeline: []plan.TimelineItem{{
				Source:          plan.MediaSource{Type: "image", URL: "img1.png"},
				DurationSeconds: 5.0,
			}},
			OutputPath: outputPath,
		}
	}
	return f.plan, nil
}

// fakeRenderClient implements pipeline.RenderClient with hard-coded behavior.
type fakeRenderClient struct {
	renderErr error
	called    bool
	lastPlan  *plan.RenderPlan
}

func (f *fakeRenderClient) Render(_ context.Context, p *plan.RenderPlan) error {
	f.called = true
	f.lastPlan = p
	return f.renderErr
}

// newTestSceneComposite builds minimal pipeline + executor wiring.
func newTestSceneComposite(renderErr error) (*SceneComposite, *fakeRenderClient) {
	pipeRegistry := pipeline.NewRegistry()
	pipeRegistry.Register(&fakeCompiler{id: "hybrid.v1", validate: true})
	pipeRegistry.Register(&fakeCompiler{id: "clips.v1", validate: true})

	rclient := &fakeRenderClient{renderErr: renderErr}
	runner := pipeline.NewRunner(pipeRegistry, rclient, logger.New(logger.InfoLevel, &strings.Builder{}))
	return NewSceneComposite(runner, "/tmp/velox/scene-composite-test"), rclient
}

func goodPayload(jobID string) map[string]interface{} {
	return map[string]interface{}{
		"job_id":      jobID,
		"images":      []interface{}{"a.png", "b.png"},
		"clips":       []interface{}{"c.mp4"},
		"script_text": "hello world",
		"output_path": "/tmp/out.mp4",
	}
}

// ── Tests ──────────────────────────────────────────────────────────────────────

func TestSceneComposite_Descriptor(t *testing.T) {
	exec, _ := newTestSceneComposite(nil)
	d := exec.Descriptor()
	if d.ID != SceneCompositeID {
		t.Errorf("ID = %q, want %q", d.ID, SceneCompositeID)
	}
	if d.Version != SceneCompositeVersion {
		t.Errorf("Version = %d, want %d", d.Version, SceneCompositeVersion)
	}
	if !d.ResourceClass.Valid() || d.ResourceClass != executor.ResourceCPU {
		t.Errorf("ResourceClass = %q, want cpu", d.ResourceClass)
	}
	if !d.TemporalMode.Valid() || d.TemporalMode != executor.TemporalGlobal {
		t.Errorf("TemporalMode = %q, want global", d.TemporalMode)
	}
	if !d.Deterministic {
		t.Errorf("Deterministic = false, want true")
	}
	if !d.Cacheable {
		t.Errorf("Cacheable = false, want true")
	}
	if strings.Contains(d.ID, "@") {
		t.Errorf("ID must not contain '@' (registry key format): %q", d.ID)
	}
}

func TestSceneComposite_Validate_NilPayload(t *testing.T) {
	exec, _ := newTestSceneComposite(nil)
	spec := executor.TaskSpec{Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID, Payload: nil}
	// Payload nil: Validate requires at least one media-source slice.
	if err := exec.Validate(spec); err == nil {
		t.Errorf("Validate with nil payload should reject")
	}
}

func TestSceneComposite_Validate_NoMediaSources(t *testing.T) {
	exec, _ := newTestSceneComposite(nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID,
		Payload: map[string]interface{}{"script_text": "no media"},
	}
	if err := exec.Validate(spec); err == nil {
		t.Errorf("Validate with no media sources should reject")
	}
}

func TestSceneComposite_Validate_OK(t *testing.T) {
	exec, _ := newTestSceneComposite(nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-1"),
	}
	if err := exec.Validate(spec); err != nil {
		t.Errorf("Validate with full payload: err = %v, want nil", err)
	}
}

func TestSceneComposite_Execute_Success(t *testing.T) {
	exec, rclient := newTestSceneComposite(nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-42", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-42"),
	}
	res, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute err = %v, want nil", err)
	}
	if res.Status != "succeeded" {
		t.Errorf("res.Status = %q, want succeeded (code=%q detail=%q)",
			res.Status, res.ErrorCode, res.ErrorDetail)
	}
	if !rclient.called {
		t.Errorf("RenderClient.Render was not invoked")
	}
	if len(res.Outputs) != 1 {
		t.Fatalf("len(Outputs) = %d, want 1", len(res.Outputs))
	}
	wantURI := filepath.Join("/tmp/velox/scene-composite-test", "j-42.mp4")
	if got, want := res.Outputs[0].URI, wantURI; got != want {
		t.Errorf("Output URI = %q, want %q (local path, not payload output_path)", got, want)
	}
	if res.Outputs[0].Type != "render.output" {
		t.Errorf("Output Type = %q, want render.output", res.Outputs[0].Type)
	}
}

func TestSceneComposite_Execute_RenderErrorMapsToFailure(t *testing.T) {
	exec, rclient := newTestSceneComposite(errors.New("ffmpeg crashed"))
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-err", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-err"),
	}
	res, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute returns error as second value: err = %v, want nil (failure should be in res)", err)
	}
	if res.Status != "failed" {
		t.Errorf("res.Status = %q, want failed", res.Status)
	}
	if res.ErrorCode == "" {
		t.Errorf("res.ErrorCode should be set on failure (adapter maps render error to execute_failed)")
	}
	if !strings.Contains(res.ErrorDetail, "ffmpeg crashed") {
		t.Errorf("res.ErrorDetail should carry ffmpeg error, got %q", res.ErrorDetail)
	}
	// Adapter should not swallow caller error: ensure the render was attempted.
	if !rclient.called {
		t.Errorf("RenderClient.Render should still have been invoked")
	}
}

func TestSceneComposite_Execute_SynthesizesOutputPath(t *testing.T) {
	exec, _ := newTestSceneComposite(nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-no-path", ExecutorID: SceneCompositeID,
		Payload: map[string]interface{}{
			"images": []interface{}{"a.png"},
		},
	}
	res, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute err = %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("res.Status = %q, want succeeded (code=%q detail=%q)",
			res.Status, res.ErrorCode, res.ErrorDetail)
	}
	wantPath := filepath.Join("/tmp/velox/scene-composite-test", "j-no-path.mp4")
	if got := res.Outputs[0].URI; got != wantPath {
		t.Errorf("synthesized path = %q, want %q", got, wantPath)
	}
}

func TestSceneComposite_Execute_UsesExplicitPipelineID(t *testing.T) {
	exec, rclient := newTestSceneComposite(nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-clips", ExecutorID: SceneCompositeID,
		Payload: map[string]interface{}{
			"pipeline_id": "clips.v1",
			"items": []interface{}{
				map[string]interface{}{
					"type":     "video",
					"url":      "https://example.com/clip.mp4",
					"duration": 4.0,
				},
			},
			"output_path": "/tmp/clips.mp4",
		},
	}

	res, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute err = %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("res.Status = %q, want succeeded (code=%q detail=%q)", res.Status, res.ErrorCode, res.ErrorDetail)
	}
	if !rclient.called || rclient.lastPlan == nil {
		t.Fatalf("RenderClient.Render was not invoked")
	}
	if got := resolvePipelineID(spec.Payload); got != "clips.v1" {
		t.Fatalf("resolvePipelineID = %q, want clips.v1", got)
	}
}

func TestNewSceneComposite_NilRunnerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewSceneComposite(nil) should panic")
		}
	}()
	_ = NewSceneComposite(nil, "")
}
