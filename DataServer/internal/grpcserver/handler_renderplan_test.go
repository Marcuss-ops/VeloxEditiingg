package grpcserver

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/renderplan"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

var errFakeCompile = errors.New("fake compile failure")

// fakePlanCompiler implements the renderPlanCompiler seam for handler tests.
type fakePlanCompiler struct {
	compileErr    error
	calls         int
	lastAttemptID string
}

func (f *fakePlanCompiler) Compile(_ context.Context, payload map[string]interface{}, attemptID string) (*renderplan.CompiledRenderPlan, error) {
	f.calls++
	f.lastAttemptID = attemptID
	if f.compileErr != nil {
		return nil, f.compileErr
	}
	return &renderplan.CompiledRenderPlan{
		PlanVersion:   renderplan.PlanVersion,
		JobID:         "job-plan",
		AttemptID:     attemptID,
		DurationMS:    1000,
		MediaContract: renderplan.MediaContract{VideoCodec: "h264", Width: 1920, Height: 1080, FpsNum: 30, FpsDen: 1},
		Segments:      []renderplan.Segment{{SegmentID: "seg_000", AssetID: "asset-a", TimelineStartMS: 0}},
	}, nil
}

func TestStampAttemptRenderPlan_PersistsPlanIdentity(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	compiler := &fakePlanCompiler{}
	handler.SetRenderPlanCompiler(compiler)
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts

	tws := &taskgraph.TaskWithSpec{
		Task:        taskgraph.Task{ID: "task-plan", JobID: "job-plan"},
		SpecPayload: map[string]interface{}{"job_id": "job-plan", "video_name": "plan test"},
	}
	attempt := &taskattempts.TaskAttempt{ID: "attempt-plan", TaskID: "task-plan", JobID: "job-plan", AttemptNumber: 1}

	planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(), tws, attempt)

	if attempts.upsertPlanCalls != 1 {
		t.Fatalf("upsert calls = %d, want 1", attempts.upsertPlanCalls)
	}
	if attempts.lastPlanVersion != renderplan.PlanVersion {
		t.Fatalf("plan version = %d, want %d", attempts.lastPlanVersion, renderplan.PlanVersion)
	}
	if attempts.lastPlanSHA256 == "" {
		t.Fatal("plan_sha256 must be non-empty")
	}
	if attempts.lastPlanJSON == "" {
		t.Fatal("render_plan_json must be non-empty")
	}
	if compiler.calls != 1 || compiler.lastAttemptID != "attempt-plan" {
		t.Fatalf("compiler calls = %d (attempt %q), want 1 for attempt-plan", compiler.calls, compiler.lastAttemptID)
	}
	// The returned document must be the persisted canonical JSON and the
	// returned hash must match it — the same pair delivered in the offer.
	if planJSON == "" || planJSON != attempts.lastPlanJSON {
		t.Fatalf("returned planJSON = %q; want persisted canonical %q", planJSON, attempts.lastPlanJSON)
	}
	if planSHA == "" || planSHA != attempts.lastPlanSHA256 {
		t.Fatalf("returned planSHA = %q; want persisted %q", planSHA, attempts.lastPlanSHA256)
	}
	if planSHA != renderplan.HashCanonical([]byte(planJSON)) {
		t.Fatal("returned planSHA must be SHA256 of the returned canonical JSON")
	}
}

func TestStampAttemptRenderPlan_NilCompilerSkips(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	if planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(),
		&taskgraph.TaskWithSpec{SpecPayload: map[string]interface{}{"job_id": "job-x"}},
		&taskattempts.TaskAttempt{ID: "attempt-x"}); planJSON != "" || planSHA != "" {
		t.Fatalf("nil compiler returned %q/%q; want empty/empty", planJSON, planSHA)
	}
	if attempts.upsertPlanCalls != 0 {
		t.Fatalf("upsert calls = %d, want 0 (nil compiler)", attempts.upsertPlanCalls)
	}
}

func TestStampAttemptRenderPlan_CompileErrorSkips(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	handler.SetRenderPlanCompiler(&fakePlanCompiler{compileErr: errFakeCompile})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	if planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(),
		&taskgraph.TaskWithSpec{SpecPayload: map[string]interface{}{"job_id": "job-x"}},
		&taskattempts.TaskAttempt{ID: "attempt-x"}); planJSON != "" || planSHA != "" {
		t.Fatalf("compile error returned %q/%q; want empty/empty", planJSON, planSHA)
	}
	if attempts.upsertPlanCalls != 0 {
		t.Fatalf("upsert calls = %d, want 0 (compile error is best-effort)", attempts.upsertPlanCalls)
	}
}
