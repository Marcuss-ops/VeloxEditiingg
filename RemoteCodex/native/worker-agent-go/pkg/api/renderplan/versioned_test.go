package renderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"velox-shared/contract"
)

func validV2Payload() map[string]interface{} {
	return map[string]interface{}{
		"render_plan_version": "v2",
		"executor_id":         "scene.composite.v1",
		"executor_version":    2,
		"assets": []interface{}{
			map[string]interface{}{"id": "clip-1", "uri": "velox-asset://clip-1"},
		},
		"timeline": []interface{}{
			map[string]interface{}{"asset_id": "clip-1", "start_ms": 0, "duration_ms": 1000},
		},
		"output_contract": map[string]interface{}{
			"container":   "mp4",
			"video_codec": "h264",
			"audio_codec": "aac",
		},
	}
}

func TestValidateTaskPayload_V2RequiresCompiledShape(t *testing.T) {
	if err := ValidateTaskPayload(validV2Payload()); err != nil {
		t.Fatalf("valid v2 payload rejected: %v", err)
	}

	for _, field := range []string{"executor_id", "executor_version", "assets", "timeline", "output_contract"} {
		t.Run("missing_"+field, func(t *testing.T) {
			payload := validV2Payload()
			delete(payload, field)
			if err := ValidateTaskPayload(payload); err == nil {
				t.Fatalf("payload missing %q was accepted", field)
			}
		})
	}
}

func TestValidateTaskPayload_RejectsUnknownAndUnversionedPayloads(t *testing.T) {
	cases := []map[string]interface{}{
		{"version": "v3"},
		{"executor_id": "scene.composite.v1", "assets": []interface{}{}},
	}
	for i, payload := range cases {
		if err := ValidateTaskPayload(payload); err == nil {
			t.Fatalf("case %d was accepted without a supported version", i)
		}
	}

	err := ValidateTaskPayload(map[string]interface{}{"version": "v3"})
	if !strings.Contains(err.Error(), string(ERR_PLAN_UNSUPPORTED_VERSION)) {
		t.Fatalf("unknown payload version error = %v, want unsupported version error", err)
	}
}

func TestValidateTaskPayload_AllowsOnlyExplicitLegacyAdapter(t *testing.T) {
	legacy := map[string]interface{}{
		"version":    "v1",
		"job_id":     "job-legacy",
		"job_type":   "render",
		"created_at": "2026-07-31T00:00:00Z",
		"parameters": map[string]interface{}{
			"start_clip_paths": []interface{}{"clip.mp4"},
			"voiceover_paths":  []interface{}{"voice.mp3"},
		},
	}
	if err := ValidateTaskPayload(legacy); err != nil {
		t.Fatalf("explicit v1 legacy payload rejected: %v", err)
	}

	currentMasterPayload := map[string]interface{}{
		"payload_contract_version": 2,
		"job_id":                   "job-current",
		"job_type":                 "process_video",
		"created_at":               "2026-07-31T00:00:00Z",
	}
	if err := ValidateTaskPayload(currentMasterPayload); err != nil {
		t.Fatalf("explicit payload_contract_version adapter rejected: %v", err)
	}

	delete(currentMasterPayload, "payload_contract_version")
	if err := ValidateTaskPayload(currentMasterPayload); err == nil {
		t.Fatal("unversioned current payload was accepted")
	}
}

func nativePacketCopyPayload(t *testing.T) map[string]interface{} {
	t.Helper()
	const timelineSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const videoSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const audioSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   timelineSHA,
		DurationUS:       1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p",
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA,
			SizeBytes: 1, Codec: "aac", SampleRateHz: 48_000, Channels: 2,
			DurationUS: 1_000_000, TimelineRevision: 1, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "segment-0", AssetID: "video", SHA256: videoSHA,
			TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000,
		}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "audio", SHA256: audioSHA, SizeBytes: 1, Kind: "final_audio", DurationUS: 1_000_000},
			{AssetID: "video", SHA256: videoSHA, SizeBytes: 1, Kind: "video", DurationUS: 1_000_000},
		},
	}
	data, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical native plan: %v", err)
	}
	digest := sha256.Sum256(data)
	return map[string]interface{}{
		"executor_id":                             "video.assemble.copy.v1",
		"executor_version":                        1,
		"payload_contract_version":                2,
		contract.PayloadKeyCompiledRenderPlanJSON: string(data),
		contract.PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(digest[:]),
	}
}

func TestValidateTaskPayload_NativePacketCopyUsesStrictV2Contract(t *testing.T) {
	if err := ValidateTaskPayload(nativePacketCopyPayload(t)); err != nil {
		t.Fatalf("native packet-copy V2 payload rejected: %v", err)
	}

	legacy := nativePacketCopyPayload(t)
	legacy["render_plan_version"] = "v2"
	if err := ValidateTaskPayload(legacy); err == nil {
		t.Fatal("native packet-copy payload with a parallel render_plan envelope was accepted")
	}

	missing := nativePacketCopyPayload(t)
	delete(missing, contract.PayloadKeyCompiledRenderPlanJSON)
	if err := ValidateTaskPayload(missing); err == nil {
		t.Fatal("native packet-copy payload without CompiledRenderPlanV2 was accepted")
	}
}
