package grpcserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

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

// TestStampAttemptRenderPlan_NoCompiledV2Skips pins the retirement of the
// legacy V1 RenderPlanCompiler: a task without a pre-compiled V2 envelope is
// never re-compiled at claim time, so no plan identity is stamped and the
// worker offer is not blocked.
func TestStampAttemptRenderPlan_NoCompiledV2Skips(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	attempts := &spoofStubAttemptRepo{}
	handler.taskAttemptRepo = attempts
	if planJSON, planSHA := handler.compileAndStampAttemptRenderPlan(context.Background(),
		&taskgraph.TaskWithSpec{Task: taskgraph.Task{ID: "task-x", JobID: "job-x"}, SpecPayload: map[string]interface{}{"job_id": "job-x"}},
		&taskattempts.TaskAttempt{ID: "attempt-x"}); planJSON != "" || planSHA != "" {
		t.Fatalf("no-V2 payload returned %q/%q; want empty/empty", planJSON, planSHA)
	}
	if attempts.upsertPlanCalls != 0 {
		t.Fatalf("upsert calls = %d, want 0 (legacy V1 compile retired)", attempts.upsertPlanCalls)
	}
}
