package assets

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestPlanHasDriveStockRecognizesHTTPSOverlaySources(t *testing.T) {
	sha := strings.Repeat("a", 64)
	profile := contract.CanonicalVideoProfileV1Default
	plan := contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64), DurationUS: 3_125_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p", ProfileID: profile.ProfileID,
			CodecProfile: profile.CodecProfile, CodecLevel: profile.CodecLevel, GOPSize: profile.GOPSize,
			BFrames: 0, ClosedGOP: true, TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "overlay-window", AssetID: "overlay-source", SHA256: sha,
			TimelineStartFrame: 0, FrameCount: 75, SourceInUS: 1_250_000, SourceDurationUS: 3_125_000,
		}}}},
		Assets: []contract.AssetRefV2{{AssetID: "overlay-source", SHA256: sha, SizeBytes: 2048,
			Kind: "video", MIME: "video/mp4", DurationUS: 5_000_000, FrameCount: 120}},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	bindings := []map[string]interface{}{{
		"asset_id": "overlay-source", "role": "overlay", "source_uri": "https://objects.example/overlay.mp4",
		"sha256": sha, "size_bytes": int64(2048),
	}}
	if !PlanHasDriveStock(string(planJSON), bindings) {
		t.Fatal("worker preparation classifier missed an HTTPS overlay source")
	}
}
