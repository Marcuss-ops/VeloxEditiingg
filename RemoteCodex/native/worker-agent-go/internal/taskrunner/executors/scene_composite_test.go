package executors

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
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
	renderErr        error
	partialMetrics   pipeline.RenderMetrics
	engineSpawnCount int64
	called           bool
	lastPlan         *plan.RenderPlan
}

func (f *fakeRenderClient) Render(_ context.Context, p *plan.RenderPlan) error {
	_, err := f.RenderWithMetrics(context.Background(), p)
	return err
}

func (f *fakeRenderClient) RenderWithMetrics(_ context.Context, p *plan.RenderPlan) (pipeline.RenderMetrics, error) {
	f.called = true
	f.lastPlan = p
	if f.renderErr != nil {
		return f.partialMetrics, f.renderErr
	}
	if err := os.MkdirAll(filepath.Dir(p.OutputPath), 0o750); err != nil {
		return pipeline.RenderMetrics{}, err
	}
	if err := os.WriteFile(p.OutputPath, []byte("fake-mp4-bytes"), 0o640); err != nil {
		return pipeline.RenderMetrics{}, err
	}
	if err := os.WriteFile(p.OutputPath+".progress.json", []byte(`{"phase_ms":{"render":1},"segments":[{"index":0,"started_offset_ms":1.25,"finished_offset_ms":4.5,"worker_slot":2,"cpu_threads":4,"parallel_group":"scene-0"}],"phases":[{"origin":"engine","scope":"segment","component":"engine.video","action":"decode","event_index":4,"duration_ms":3,"segment_index":0,"started_offset_ms":1.25,"finished_offset_ms":4.25}],"frames":1}`), 0o640); err != nil {
		return pipeline.RenderMetrics{}, err
	}
	return pipeline.RenderMetrics{
		EngineSpawnCount: f.engineSpawnCount,
		CPUUserMs:        1500,
		CPUSystemMs:      300,
		PeakRSSBytes:     320_000_000,
		CurrentRSSBytes:  300_000_000,
		PhaseMS:          map[string]float64{"render": 1},
		Observability: map[string]interface{}{
			"audio":    map[string]interface{}{"events": float64(2), "wall_ms": float64(12.5)},
			"subtitle": map[string]interface{}{"events": float64(1)},
			"io":       map[string]interface{}{"bytes_in": float64(4096)},
			"quality":  map[string]interface{}{"events": float64(3)},
			"retry":    map[string]interface{}{"count": float64(2)},
			"waste":    map[string]interface{}{"wasted_cpu_ms": float64(88), "wasted_download_bytes": float64(512), "completed_segments": float64(2), "error_component": "engine", "error_phase": "encode"},
		},
		Segments: []pipeline.SegmentTiming{{
			SegmentIndex: 0, StartedOffsetMS: 1.25, FinishedOffsetMS: 4.5,
			WorkerSlot: 2, CPUThreads: 4, ParallelGroup: "scene-0",
		}},
		DetailedPhases: []pipeline.DetailedPhaseTiming{{
			Origin: "engine", Scope: "segment", Component: "engine.video",
			Action: "decode", EventIndex: 4, DurationMS: 3, SegmentIndex: 0,
			StartedOffsetMS: 1.25, FinishedOffsetMS: 4.25,
		}},
	}, nil
}

// newTestSceneComposite builds minimal pipeline + executor wiring.
func newTestSceneComposite(t *testing.T, renderErr error) (*SceneComposite, *fakeRenderClient) {
	t.Helper()
	outputBase := t.TempDir()
	pipeRegistry := pipeline.NewRegistry()
	pipeRegistry.Register(&fakeCompiler{id: "hybrid.v1", validate: true})
	pipeRegistry.Register(&fakeCompiler{id: "clips.v1", validate: true})

	rclient := &fakeRenderClient{renderErr: renderErr}
	runner := pipeline.NewRunner(pipeRegistry, rclient, logger.New(logger.InfoLevel, &strings.Builder{}))
	return NewSceneComposite(runner, outputBase), rclient
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
	exec, _ := newTestSceneComposite(t, nil)
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
	exec, _ := newTestSceneComposite(t, nil)
	spec := executor.TaskSpec{Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID, Payload: nil}
	// Payload nil: Validate requires at least one media-source slice.
	if err := exec.Validate(spec); err == nil {
		t.Errorf("Validate with nil payload should reject")
	}
}

func TestSceneComposite_Validate_NoMediaSources(t *testing.T) {
	exec, _ := newTestSceneComposite(t, nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID,
		Payload: map[string]interface{}{"script_text": "no media"},
	}
	if err := exec.Validate(spec); err == nil {
		t.Errorf("Validate with no media sources should reject")
	}
}

func TestSceneComposite_Validate_OK(t *testing.T) {
	exec, _ := newTestSceneComposite(t, nil)
	spec := executor.TaskSpec{
		Version: 1, JobID: "j-1", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-1"),
	}
	if err := exec.Validate(spec); err != nil {
		t.Errorf("Validate with full payload: err = %v, want nil", err)
	}
}

func TestSceneComposite_Execute_Success(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, nil)
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
	if len(res.Outputs) != 2 {
		t.Fatalf("len(Outputs) = %d, want 2 (video + progress sidecar)", len(res.Outputs))
	}
	wantURI := filepath.Join(exec.outputBase, "j-42.mp4")
	if got, want := res.Outputs[0].URI, wantURI; got != want {
		t.Errorf("Output URI = %q, want %q (local path, not payload output_path)", got, want)
	}
	if res.Outputs[0].Type != "render.output" || res.Outputs[0].Hash == "" || res.Outputs[0].SizeBytes <= 0 {
		t.Errorf("primary output = %#v, want render.output with real hash and size", res.Outputs[0])
	}
	if got, want := res.Outputs[1].URI, wantURI+".progress.json"; got != want {
		t.Errorf("sidecar URI = %q, want %q", got, want)
	}
	if res.Outputs[1].Type != "engine.progress.sidecar" || res.Outputs[1].Hash == "" || res.Outputs[1].SizeBytes <= 0 {
		t.Errorf("sidecar output = %#v, want engine.progress.sidecar with real hash and size", res.Outputs[1])
	}
	if len(res.Segments) != 1 || res.Segments[0].FinishedOffsetMS != 4.5 || res.Segments[0].WorkerSlot != 2 {
		t.Fatalf("segment timing/parallelism not propagated: %#v", res.Segments)
	}
	if len(res.DetailedPhases) < 7 || res.DetailedPhases[0].Component != "engine.video" || res.DetailedPhases[0].EventIndex != 4 {
		t.Fatalf("detailed phases not propagated: %#v", res.DetailedPhases)
	}
	seenSummary := map[string]bool{}
	for _, phase := range res.DetailedPhases[1:] {
		if phase.EventType == "summary" {
			seenSummary[phase.Component] = true
		}
	}
	for _, category := range []string{"audio", "subtitle", "io", "quality", "retry", "waste"} {
		if !seenSummary[category] {
			t.Errorf("missing %s summary phase: %#v", category, res.DetailedPhases)
		}
	}
	if res.Metrics["audio.events"] != float64(2) || res.Metrics["subtitle.events"] != float64(1) || res.Metrics["io.bytes_in"] != float64(4096) || res.Metrics["quality.events"] != float64(3) || res.Metrics["retry.count"] != float64(2) || res.Metrics["waste.wasted_cpu_ms"] != float64(88) {
		t.Fatalf("category observability not propagated: %#v", res.Metrics)
	}
	// CPU/RSS counters share the receipt's derivation: cpu.total_ms is
	// user + system, the memory keys carry the sampler values verbatim.
	if res.Metrics["cpu.user_ms"] != int64(1500) ||
		res.Metrics["cpu.system_ms"] != int64(300) ||
		res.Metrics["cpu.total_ms"] != int64(1800) ||
		res.Metrics["memory.peak_rss_bytes"] != int64(320_000_000) ||
		res.Metrics["memory.current_rss_bytes"] != int64(300_000_000) {
		t.Fatalf("cpu/memory telemetry not propagated: %#v", res.Metrics)
	}
	wallMs, hasWall := res.Metrics["cpu.wall_ms"].(int64)
	if !hasWall {
		t.Fatalf("cpu.wall_ms must be an int64, got %#v", res.Metrics["cpu.wall_ms"])
	}
	ratio, ok := res.Metrics["cpu.wall_ratio"].(float64)
	if !ok || ratio < 0 {
		t.Fatalf("cpu.wall_ratio must be a non-negative float, got %#v", res.Metrics["cpu.wall_ratio"])
	}
	// The fake render is instantaneous, so the wall clock can round to
	// zero and the ratio legitimately stays zero; whenever wall_ms > 0
	// the ratio must be positive.
	if wallMs > 0 && ratio <= 0 {
		t.Fatalf("cpu.wall_ratio must be positive when wall_ms > 0, got %v (wall=%d)", ratio, wallMs)
	}
	// Derived KPIs share the receipt's single Deriver. With the fake's
	// only detailed phase being a span_child (engine.video.decode) and
	// no process/IO byte counts, accounted, amplification,
	// processes_per_clip and useful_work_ratio are all zero while the
	// whole wall clock is unaccounted; cpu_wall_ratio mirrors the
	// cpu.* projection exactly (same Deriver, same wall clock).
	if res.Metrics["derived.unaccounted_ms"] != wallMs {
		t.Fatalf("derived.unaccounted_ms = %v, want cpu.wall_ms (%d): with no exclusive phases the whole wall clock is unaccounted", res.Metrics["derived.unaccounted_ms"], wallMs)
	}
	if res.Metrics["derived.accounted_ratio"] != float64(0) ||
		res.Metrics["derived.read_amplification"] != float64(0) ||
		res.Metrics["derived.write_amplification"] != float64(0) ||
		res.Metrics["derived.processes_per_clip"] != float64(0) ||
		res.Metrics["derived.useful_work_ratio"] != float64(0) {
		t.Fatalf("derived KPIs must be zero for a span-child-only fake: %#v", res.Metrics)
	}
	if res.Metrics["derived.cpu_wall_ratio"] != res.Metrics["cpu.wall_ratio"] {
		t.Fatalf("derived.cpu_wall_ratio = %v, cpu.wall_ratio = %v; the two projections must agree", res.Metrics["derived.cpu_wall_ratio"], res.Metrics["cpu.wall_ratio"])
	}
	// The accounted_ratio budget flag: "not measured" (ratio 0) is never
	// a violation, so the flag must be true for the span-child-only fake.
	if ok, isBool := res.Metrics["derived.accounted_ratio_budget_ok"].(bool); !isBool || !ok {
		t.Fatalf("derived.accounted_ratio_budget_ok = %#v, want true (not-measured is not a violation)", res.Metrics["derived.accounted_ratio_budget_ok"])
	}
}

func TestSceneComposite_Execute_RenderErrorMapsToFailure(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, errors.New("ffmpeg crashed"))
	rclient.partialMetrics = pipeline.RenderMetrics{
		PhaseMS: map[string]float64{"decode": 12},
		Segments: []pipeline.SegmentTiming{{
			SegmentIndex: 2, StartedOffsetMS: 3.5, FinishedOffsetMS: 8.25,
			WorkerSlot: 1, CPUThreads: 4, ParallelGroup: "g1",
		}},
		DetailedPhases: []pipeline.DetailedPhaseTiming{{
			Origin: "engine", Scope: "segment", Component: "engine.encode",
			Action: "frame_submit", EventIndex: 9, Status: "failed",
			ErrorCode: "encoder_crashed", SegmentIndex: 2,
		}},
	}
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
	if res.Metrics["engine.decode"] != float64(12) {
		t.Errorf("partial phase metrics = %#v, want engine.decode=12", res.Metrics)
	}
	if len(res.Segments) != 1 || res.Segments[0].SegmentIndex != 2 || res.Segments[0].FinishedOffsetMS != 8.25 {
		t.Fatalf("partial segment timings = %#v", res.Segments)
	}
	if len(res.DetailedPhases) != 1 || res.DetailedPhases[0].EventIndex != 9 || res.DetailedPhases[0].ErrorCode != "encoder_crashed" {
		t.Fatalf("partial detailed phases = %#v", res.DetailedPhases)
	}
	// Adapter should not swallow caller error: ensure the render was attempted.
	if !rclient.called {
		t.Errorf("RenderClient.Render should still have been invoked")
	}
}

func TestRenderErrorCodeClassifiesCopyOnlyContractFailure(t *testing.T) {
	if got := renderErrorCode(errors.New("engine failed: copy_only media contract rejected video segment 0")); got != "COPY_ONLY_MEDIA_INCOMPATIBLE" {
		t.Fatalf("renderErrorCode(copy-only) = %q, want COPY_ONLY_MEDIA_INCOMPATIBLE", got)
	}
	if got := renderErrorCode(errors.New("ffmpeg crashed")); got != "execute_failed" {
		t.Fatalf("renderErrorCode(generic) = %q, want execute_failed", got)
	}
}

func TestSceneComposite_Execute_CancellationPreservesPartialTelemetry(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, context.Canceled)
	rclient.partialMetrics = pipeline.RenderMetrics{
		PhaseMS:  map[string]float64{"decode": 7},
		Segments: []pipeline.SegmentTiming{{SegmentIndex: 4, StartedOffsetMS: 2, FinishedOffsetMS: 5}},
		DetailedPhases: []pipeline.DetailedPhaseTiming{{
			Origin: "engine", Scope: "segment", Component: "engine.decode",
			Action: "frame_reorder", EventIndex: 12, Status: "ok", SegmentIndex: 4,
		}},
	}
	res, err := exec.Execute(context.Background(), nil, executor.TaskSpec{
		Version: 1, JobID: "j-cancel", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-cancel"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if res.ErrorCode != "cancelled" {
		t.Fatalf("error code = %q, want cancelled", res.ErrorCode)
	}
	if res.Metrics["engine.decode"] != float64(7) || len(res.Segments) != 1 || len(res.DetailedPhases) != 1 {
		t.Fatalf("partial cancellation telemetry = metrics:%#v segments:%#v phases:%#v", res.Metrics, res.Segments, res.DetailedPhases)
	}
	if !rclient.called {
		t.Fatal("render client was not invoked")
	}
}

func TestSceneComposite_Execute_SynthesizesOutputPath(t *testing.T) {
	exec, _ := newTestSceneComposite(t, nil)
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
	wantPath := filepath.Join(exec.outputBase, "j-no-path.mp4")
	if got := res.Outputs[0].URI; got != wantPath {
		t.Errorf("synthesized path = %q, want %q", got, wantPath)
	}
}

func TestSceneComposite_Execute_UsesExplicitPipelineID(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, nil)
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

// ── PROCESS_STARTED (worker.engine.spawn) emission ───────────────────────────

// recorderExecutionContext exposes the attempt recorder to the executor, the
// same way the real runner does through executor.ExecutionContext. The
// remaining interface methods are promoted from the package's existing
// sinkExecutionContext stub.
type recorderExecutionContext struct {
	*sinkExecutionContext
	rec *telemetry.EventRecorder
}

func (c *recorderExecutionContext) Recorder() *telemetry.EventRecorder { return c.rec }

func countSpawnEvents(t *testing.T, rec *telemetry.EventRecorder) int {
	t.Helper()
	count := 0
	for _, event := range rec.Flush() {
		if event.Component == "worker.engine" && event.Action == "spawn" {
			count++
		}
	}
	return count
}

// TestSceneComposite_EmitsProcessStartedEventOnSpawn pins the canonical
// PROCESS_STARTED fact: when the native client reports the explicit spawn
// fact (EngineSpawnCount=1), the ProcessRunner boundary emits exactly one
// worker.engine.spawn event — the event is the authoritative spawn counter,
// never a ProcessStartMs-derived inference.
func TestSceneComposite_EmitsProcessStartedEventOnSpawn(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, nil)
	rclient.engineSpawnCount = 1
	rec := telemetry.NewEventRecorder()

	res, err := exec.Execute(context.Background(), &recorderExecutionContext{rec: rec}, executor.TaskSpec{
		Version: 1, JobID: "j-spawn", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-spawn"),
	})
	if err != nil {
		t.Fatalf("Execute err = %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("res.Status = %q, want succeeded", res.Status)
	}
	if got := countSpawnEvents(t, rec); got != 1 {
		t.Fatalf("worker.engine.spawn events = %d, want exactly 1", got)
	}
}

// TestSceneComposite_NoSpawnEventWhenEngineNeverStarted pins the inverse: a
// run where the engine never started must NOT fabricate a PROCESS_STARTED
// event.
func TestSceneComposite_NoSpawnEventWhenEngineNeverStarted(t *testing.T) {
	exec, _ := newTestSceneComposite(t, nil) // engineSpawnCount stays 0
	rec := telemetry.NewEventRecorder()

	res, err := exec.Execute(context.Background(), &recorderExecutionContext{rec: rec}, executor.TaskSpec{
		Version: 1, JobID: "j-nospawn", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-nospawn"),
	})
	if err != nil {
		t.Fatalf("Execute err = %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("res.Status = %q, want succeeded", res.Status)
	}
	if got := countSpawnEvents(t, rec); got != 0 {
		t.Fatalf("worker.engine.spawn events = %d, want 0 (no spawn happened)", got)
	}
}

// TestSceneComposite_SpawnEventOnFailedRender pins that a failed render still
// records the spawn: the spawn cost is part of the attempt even when the
// engine exits non-zero.
func TestSceneComposite_SpawnEventOnFailedRender(t *testing.T) {
	exec, rclient := newTestSceneComposite(t, errors.New("engine crashed"))
	rclient.partialMetrics = pipeline.RenderMetrics{EngineSpawnCount: 1}
	rec := telemetry.NewEventRecorder()

	res, err := exec.Execute(context.Background(), &recorderExecutionContext{rec: rec}, executor.TaskSpec{
		Version: 1, JobID: "j-spawnfail", ExecutorID: SceneCompositeID,
		Payload: goodPayload("j-spawnfail"),
	})
	if err != nil {
		t.Fatalf("Execute err = %v", err)
	}
	if res.Status != "failed" {
		t.Fatalf("res.Status = %q, want failed", res.Status)
	}
	if got := countSpawnEvents(t, rec); got != 1 {
		t.Fatalf("worker.engine.spawn events on failed render = %d, want exactly 1", got)
	}
}
