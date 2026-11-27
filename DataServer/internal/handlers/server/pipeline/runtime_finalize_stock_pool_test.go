package pipeline

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestStockPoolSnapshotPreservesSourceSHA(t *testing.T) {
	sha := strings.Repeat("b", 64)
	plan := &contract.CompiledRenderPlanV2{TimelineSHA256: strings.Repeat("a", 64), StockPool: &contract.StockPoolV2{DriveFolderID: "folder", Selection: "shuffle"}, Output: contract.OutputContractV2{FPSNum: 24, FPSDen: 1}, VideoTracks: []contract.VideoTrackV2{{Segments: []contract.VideoSegmentV2{{StockPoolSlot: true, FrameCount: 24}}}}}
	bindings := []map[string]interface{}{{"asset_id": "clip", "drive_file_id": "clip", "size_bytes": int64(100), "duration_ms": int64(1000), "stock_pool_member": true, "source_sha256": sha}}
	_, snapshot, err := snapshotRuntimeStockPool(plan, "job", bindings)
	if err != nil {
		t.Fatal(err)
	}
	members := snapshot["members"].([]map[string]interface{})
	if len(members) != 1 || members[0]["source_sha256"] != sha {
		t.Fatalf("snapshot lost source digest: %+v", snapshot)
	}
}

func TestStockPoolSlotsExpandOnlyAfterSourceCertificates(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	sha := strings.Repeat("a", 64)
	timelineSHA := strings.Repeat("b", 64)
	audioSHA := strings.Repeat("c", 64)
	plan := contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 1, TimelineSHA256: timelineSHA, DurationUS: 3_000_000,
		StockPool: &contract.StockPoolV2{SourceKind: "drive", DriveFolderID: "folder", Selection: "shuffle"},
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: profile.Width, Height: profile.Height,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, PixelFormat: profile.PixelFormat,
			ProfileID: profile.ProfileID, CodecProfile: profile.CodecProfile, CodecLevel: profile.CodecLevel,
			GOPSize: profile.GOPSize, BFrames: profile.BFrames, ClosedGOP: profile.ClosedGOP,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA,
			SizeBytes: 100, Codec: "aac", SampleRateHz: 48_000, Channels: 2,
			DurationUS: 3_000_000, TimelineRevision: 1, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{
			{SegmentID: "slot-a", StockPoolSlot: true, TimelineStartFrame: 0, FrameCount: 48},
			{SegmentID: "slot-b", StockPoolSlot: true, TimelineStartFrame: 48, FrameCount: 24},
		}}},
		Assets: []contract.AssetRefV2{{AssetID: "audio", Kind: "final_audio", SHA256: audioSHA, SizeBytes: 100, DurationUS: 3_000_000}},
	}
	input, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	bindings := []map[string]interface{}{{
		"asset_id": "source", "drive_file_id": "drive-1", "size_bytes": int64(1000),
		"duration_ms": int64(4000), "source_sha256": sha, "stock_pool_member": true,
	}}
	prepared, selectedBindings, snapshot, err := prepareRuntimeStockPoolPlan(string(input), "job-1", bindings)
	if err != nil {
		t.Fatalf("prepare stock pool plan: %v", err)
	}
	var awaiting contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(prepared), &awaiting); err != nil {
		t.Fatal(err)
	}
	if awaiting.StockPool == nil || len(awaiting.VideoTracks[0].Segments) != 2 || !awaiting.VideoTracks[0].Segments[0].StockPoolSlot {
		t.Fatalf("stock slots were expanded before source certification: %+v", awaiting.VideoTracks)
	}

	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	level, err := strconv.Atoi(strings.ReplaceAll(profile.CodecLevel, ".", ""))
	if err != nil {
		t.Fatal(err)
	}
	certificate := &contract.VideoSourceCertificateV1{
		Version: 1, SHA256: sha, SizeBytes: 1000, ProfileID: profile.ProfileID,
		PreparationRevision: revision, DurationUS: 4_000_000, KeyframesUS: []int64{0, 3_000_000},
		Evidence: contract.VideoWindowOutputEvidenceV1{
			Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: level,
			Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: 1, TimeBaseDen: 90_000,
			FrameCount: 96, FirstFrameKeyframe: true, ExtradataSHA256: contract.CanonicalVideoExtradataSHA256,
		},
	}
	if err := certificate.Validate(); err != nil {
		t.Fatalf("test certificate: %v", err)
	}
	for i := range awaiting.Assets {
		if awaiting.Assets[i].AssetID != "stockpool:drive-1" {
			continue
		}
		awaiting.Assets[i].ProfileID = profile.StreamProfile()
		awaiting.Assets[i].Width, awaiting.Assets[i].Height = profile.Width, profile.Height
		awaiting.Assets[i].FrameCount = certificate.Evidence.FrameCount
		awaiting.Assets[i].DurationUS = certificate.DurationUS
		awaiting.Assets[i].FirstFrameKeyframe = true
		awaiting.Assets[i].ClosedGOP = profile.ClosedGOP
		awaiting.Assets[i].ExtradataSHA256 = certificate.Evidence.ExtradataSHA256
		awaiting.Assets[i].SourceCertificate = certificate
	}
	certified, err := json.Marshal(awaiting)
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := expandRuntimeStockPoolPlanFromSnapshot(string(certified), "job-1", snapshot, selectedBindings)
	if err != nil {
		t.Fatalf("expand certified stock pool plan: %v", err)
	}
	var final contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(expanded), &final); err != nil {
		t.Fatal(err)
	}
	segments := final.VideoTracks[0].Segments
	if final.StockPool != nil || len(segments) != 2 {
		t.Fatalf("certified stock pool did not expand: %+v", final)
	}
	if segments[1].SourceInUS != 3_000_000 {
		t.Fatalf("second source window starts at %d us, want next certified keyframe at 3000000 us", segments[1].SourceInUS)
	}
	asset := contract.AssetRefV2{}
	for _, candidate := range final.Assets {
		if candidate.AssetID == segments[1].AssetID {
			asset = candidate
			break
		}
	}
	if !contract.PacketCopySourceWindowCertified(&final, segments[1], asset) {
		t.Fatalf("expanded segment is not covered by its source certificate: segment=%+v asset=%+v", segments[1], asset)
	}
}
