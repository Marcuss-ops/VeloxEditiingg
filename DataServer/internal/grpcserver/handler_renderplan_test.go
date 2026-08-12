package grpcserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"velox-server/internal/renderplan"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

var errFakeCompile = errors.New("fake compile failure")

// fakePlanCompiler implements the renderPlanCompiler seam for handler tests.
type fakePlanCompiler struct {
	compileErr    error
	returnNilPlan bool
	wrongIdentity bool
	calls         int
	lastAttemptID string
}

func (f *fakePlanCompiler) Compile(_ context.Context, payload map[string]interface{}, attemptID string) (*renderplan.CompiledRenderPlan, error) {
	f.calls++
	f.lastAttemptID = attemptID
	if f.compileErr != nil {
		return nil, f.compileErr
	}
	if f.returnNilPlan {
		return nil, nil
	}
	jobID := "job-plan"
	if f.wrongIdentity {
		jobID = "different-job"
	}
	return &renderplan.CompiledRenderPlan{
		PlanVersion:   renderplan.PlanVersion,
		JobID:         jobID,
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

func TestStampAttemptRenderPlan_PersistsStrictV2Payload(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	const timelineSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const videoSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const audioSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 1, TimelineSHA256: timelineSHA, DurationUS: 1_000_000,
		Output:      contract.OutputContractV2{Container: "mp4", VideoCodec: "h264", Width: 640, Height: 360, FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p"},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA, SizeBytes: 10, Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 1, TimelineSHA256: timelineSHA},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "seg", AssetID: "video", SHA256: videoSHA, TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets:      []contract.AssetRefV2{{AssetID: "audio", SHA256: audioSHA, SizeBytes: 10, Kind: "final_audio", DurationUS: 1_000_000}, {AssetID: "video", SHA256: videoSHA, SizeBytes: 10, Kind: "video", DurationUS: 1_000_000}},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical V2 plan: %v", err)
	}
	digest := sha256.Sum256(canonical)
	rawSHA := hex.EncodeToString(digest[:])
	tws := &taskgraph.TaskWithSpec{Task: taskgraph.Task{ID: "task-v2", JobID: "job-v2"}, SpecPayload: map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical), contract.PayloadKeyCompiledRenderPlanSHA: rawSHA,
	}}
	attempt := &taskattempts.TaskAttempt{ID: "attempt-v2", TaskID: "task-v2", JobID: "job-v2", AttemptNumber: 1}

	gotJSON, gotSHA := handler.compileAndStampAttemptRenderPlan(context.Background(), tws, attempt)
	if gotJSON != string(canonical) || gotSHA != rawSHA {
		t.Fatalf("stamped V2 pair = %q/%q, want canonical pair", gotJSON, gotSHA)
	}
	if attempts.lastPlanVersion != contract.CompiledPlanVersionV2 || attempts.lastPlanJSON != string(canonical) || attempts.lastPlanSHA256 != rawSHA {
		t.Fatalf("persisted V2 plan = version:%d sha:%q json:%q", attempts.lastPlanVersion, attempts.lastPlanSHA256, attempts.lastPlanJSON)
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

func TestStampAttemptRenderPlan_NilPlanSkips(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	handler.SetRenderPlanCompiler(&fakePlanCompiler{returnNilPlan: true})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	if planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(),
		&taskgraph.TaskWithSpec{Task: taskgraph.Task{ID: "task-plan", JobID: "job-plan"}},
		&taskattempts.TaskAttempt{ID: "attempt-plan"}); planJSON != "" || planSHA != "" {
		t.Fatalf("nil plan returned %q/%q; want empty/empty", planJSON, planSHA)
	}
	if attempts.upsertPlanCalls != 0 {
		t.Fatalf("upsert calls = %d, want 0 (nil plan)", attempts.upsertPlanCalls)
	}
}

func TestStampAttemptRenderPlan_WrongIdentitySkips(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	handler.SetRenderPlanCompiler(&fakePlanCompiler{wrongIdentity: true})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	if planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(),
		&taskgraph.TaskWithSpec{Task: taskgraph.Task{ID: "task-plan", JobID: "job-plan"}},
		&taskattempts.TaskAttempt{ID: "attempt-plan"}); planJSON != "" || planSHA != "" {
		t.Fatalf("wrong identity returned %q/%q; want empty/empty", planJSON, planSHA)
	}
	if attempts.upsertPlanCalls != 0 {
		t.Fatalf("upsert calls = %d, want 0 (wrong identity)", attempts.upsertPlanCalls)
	}
}
