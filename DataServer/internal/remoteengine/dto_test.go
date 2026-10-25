package remoteengine

import (
	"errors"
	"strings"
	"testing"

	"velox-shared/contract"
)

// ── ValidateInitialResponse ──────────────────────────────────────────────────

func TestValidateInitialResponse_Valid(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
		"status": "queued",
		"ok":     true,
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "job_123" {
		t.Fatalf("JobID: got %q, want job_123", resp.JobID)
	}
	if resp.Status != "queued" {
		t.Fatalf("Status: got %q, want queued", resp.Status)
	}
	if resp.RawResult == nil {
		t.Fatal("RawResult should not be nil")
	}
}

func TestValidateInitialResponse_TraceIDFallback(t *testing.T) {
	raw := map[string]interface{}{
		"trace_id": "trace_456",
		"status":   "running",
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "trace_456" {
		t.Fatalf("JobID: got %q, want trace_456", resp.JobID)
	}
}

func TestValidateInitialResponse_IDFallback(t *testing.T) {
	raw := map[string]interface{}{
		"id":     "id_789",
		"status": "completed",
	}
	resp, err := ValidateInitialResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "id_789" {
		t.Fatalf("JobID: got %q, want id_789", resp.JobID)
	}
}

func TestValidateInitialResponse_MissingJobID(t *testing.T) {
	raw := map[string]interface{}{
		"status": "queued",
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on missing job_id")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "CONTRACT_MISSING_JOB_ID" {
		t.Fatalf("code: got %s, want CONTRACT_MISSING_JOB_ID", re.Code)
	}
}

func TestValidateInitialResponse_MissingStatus(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on missing status")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
}

func TestValidateInitialResponse_UnknownStatus(t *testing.T) {
	raw := map[string]interface{}{
		"job_id": "job_123",
		"status": "pending", // not in knownRemoteStatuses
	}
	_, err := ValidateInitialResponse(raw)
	if err == nil {
		t.Fatal("should error on unknown status")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "CONTRACT_UNKNOWN_STATUS" {
		t.Fatalf("code: got %s, want CONTRACT_UNKNOWN_STATUS", re.Code)
	}
}

func TestValidateInitialResponse_NilMap(t *testing.T) {
	_, err := ValidateInitialResponse(nil)
	if err == nil {
		t.Fatal("should error on nil map")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorMalformed {
		t.Fatalf("class: got %s, want MALFORMED_RESPONSE", re.Class)
	}
}

func TestValidateInitialResponse_AllKnownStatuses(t *testing.T) {
	for _, status := range []string{"queued", "running", "completed", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			raw := map[string]interface{}{
				"job_id": "job_1",
				"status": status,
			}
			resp, err := ValidateInitialResponse(raw)
			if err != nil {
				t.Fatalf("status %q should be valid: %v", status, err)
			}
			if resp.Status != status {
				t.Fatalf("Status: got %q, want %q", resp.Status, status)
			}
		})
	}
}

// ── Producer-owned CompiledRenderPlanV2 boundary ─────────────────────────────

func remoteEngineV2Plan(t *testing.T) (string, string) {
	t.Helper()
	const durationUS int64 = 1_000_000
	timelineSHA := strings.Repeat("c", 64)
	videoSHA := strings.Repeat("b", 64)
	audioSHA := strings.Repeat("a", 64)
	profile := contract.CanonicalVideoProfileFMP4StreamV1Default
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: contract.CompiledPlanVersionV2, TimelineRevision: 1,
		TimelineSHA256: timelineSHA, DurationUS: durationUS,
		Output: contract.OutputContractV2{
			Container: profile.Container, VideoCodec: profile.Codec, Width: profile.Width, Height: profile.Height,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, PixelFormat: profile.PixelFormat,
			ProfileID: profile.ProfileID, CodecProfile: profile.CodecProfile, CodecLevel: profile.CodecLevel,
			GOPSize: profile.GOPSize, BFrames: profile.BFrames, ClosedGOP: profile.ClosedGOP,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-final", SHA256: audioSHA, SizeBytes: 200,
			Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: durationUS,
			TimelineRevision: 1, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "segment-0", AssetID: "video-source", SHA256: videoSHA,
			TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 0, SourceDurationUS: durationUS,
		}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "audio-final", SHA256: audioSHA, SizeBytes: 200, Kind: "final_audio", MIME: "audio/mp4", DurationUS: durationUS},
			{AssetID: "video-source", SHA256: videoSHA, SizeBytes: 100, Kind: "video", DurationUS: durationUS, Width: profile.Width, Height: profile.Height},
		},
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatal(err)
	}
	data, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	sha, err := plan.PlanSHA256()
	if err != nil {
		t.Fatal(err)
	}
	return string(data), sha
}

func TestParseRemotePipelineResultRequiresProducerCompiledV2(t *testing.T) {
	_, err := ParseRemotePipelineResult(map[string]interface{}{
		"job_id": "legacy", "status": "completed", "scenes": []interface{}{}, "voiceover_path": "/tmp/voice.mp3",
	})
	if err == nil || !strings.Contains(err.Error(), "CompiledRenderPlanV2 is required") {
		t.Fatalf("legacy scene payload error = %v, want producer V2 plan required", err)
	}
}

func TestParseRemotePipelineResultPreservesCanonicalProducerPlan(t *testing.T) {
	planJSON, planSHA := remoteEngineV2Plan(t)
	raw := map[string]interface{}{
		"job_id": "job_v2", "status": "completed", "result": map[string]interface{}{
			"video_name": "V2 video", "script_text": "Producer-owned timeline",
			contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
			contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
		},
	}
	dto, err := ParseRemotePipelineResult(raw)
	if err != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", err)
	}
	if dto.RemoteJobID != "job_v2" {
		t.Fatalf("RemoteJobID = %q", dto.RemoteJobID)
	}
	payload, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
	if payload[contract.PayloadKeyCompiledRenderPlanJSON] != planJSON || payload[contract.PayloadKeyCompiledRenderPlanSHA] != planSHA {
		t.Fatal("worker projection changed producer plan or hash")
	}
}

func TestParseRemotePipelineResultRejectsPlanHashDrift(t *testing.T) {
	planJSON, _ := remoteEngineV2Plan(t)
	_, err := ParseRemotePipelineResult(map[string]interface{}{
		"job_id": "job_bad", "status": "completed",
		contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
		contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("f", 64),
	})
	if err == nil {
		t.Fatal("expected producer plan hash mismatch rejection")
	}
}

func TestToWorkerPayloadCheckedRejectsEmptyDTO(t *testing.T) {
	if _, err := (&RemotePipelineResult{}).ToWorkerPayloadChecked(); err == nil {
		t.Fatal("empty DTO must not produce a renderable worker payload")
	}
	var dto *RemotePipelineResult
	if _, err := dto.ToWorkerPayloadChecked(); err == nil {
		t.Fatal("nil DTO must not produce a renderable worker payload")
	}
}
