package grpcserver

import (
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestProjectPayloadForWorkerPreservesCompiledRenderPlanV2(t *testing.T) {
	const videoSHA = "1111111111111111111111111111111111111111111111111111111111111111"
	const audioSHA = "2222222222222222222222222222222222222222222222222222222222222222"
	const timelineSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	plan := &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   timelineSHA,
		DurationUS:       1_000_000,
		Output: contract.OutputContractV2{
			Container:  "mp4",
			VideoCodec: "h264",
			Width:      640,
			Height:     360,
			FPSNum:     30,
			FPSDen:     1,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode:             contract.AudioModeFinalAudioCopy,
			AssetID:          "final-audio",
			SHA256:           audioSHA,
			SizeBytes:        200,
			Codec:            "aac",
			SampleRateHz:     48_000,
			Channels:         2,
			DurationUS:       1_000_000,
			TimelineRevision: 1,
			TimelineSHA256:   timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main",
			Segments: []contract.VideoSegmentV2{{
				SegmentID:          "main-0",
				AssetID:            "video",
				SHA256:             videoSHA,
				TimelineStartFrame: 0,
				FrameCount:         30,
				SourceInUS:         0,
				SourceDurationUS:   1_000_000,
			}},
		}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video", SHA256: videoSHA, SizeBytes: 100, Kind: "video", DurationUS: 1_000_000},
			{AssetID: "final-audio", SHA256: audioSHA, SizeBytes: 200, Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
		},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("plan validation: %v", err)
	}
	planSHA := contract.HashCompiledPlanV2(canonical)
	payload := map[string]interface{}{
		"video_name": "V2 worker offer",
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
		contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
	}

	projected, err := projectPayloadForWorker(payload, 1)
	if err != nil {
		t.Fatalf("projectPayloadForWorker: %v", err)
	}
	if got := projected[contract.PayloadKeyCompiledRenderPlanJSON]; got != string(canonical) {
		t.Fatalf("worker plan bytes changed: got %v, want exact canonical bytes", got)
	}
	if got := projected[contract.PayloadKeyCompiledRenderPlanSHA]; got != planSHA {
		t.Fatalf("worker plan SHA changed: got %v, want %s", got, planSHA)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(projected); err != nil {
		t.Fatalf("projected V2 envelope rejected: %v", err)
	}
	if strings.Contains(projected[contract.PayloadKeyCompiledRenderPlanJSON].(string), "local_path") {
		t.Fatal("worker V2 plan unexpectedly contains a local path")
	}
}
