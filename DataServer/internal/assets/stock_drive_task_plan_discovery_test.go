package assets

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestPlanDriveStockWindowTasksPlansDiscoveryWithoutDownload(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	plan := contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 4, TimelineSHA256: strings.Repeat("d", 64), DurationUS: 6_250_000,
		Output: contract.OutputContractV2{Container: "mp4", VideoCodec: "h264", Width: profile.Width,
			Height: profile.Height, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
			PixelFormat: profile.PixelFormat, ProfileID: profile.ProfileID, CodecProfile: profile.CodecProfile,
			CodecLevel: profile.CodecLevel, GOPSize: profile.GOPSize, BFrames: profile.BFrames,
			ClosedGOP: profile.ClosedGOP, TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{
			{SegmentID: "first-use", AssetID: "drive-source", TimelineStartFrame: 0, FrameCount: 75, SourceInUS: 1_250_000, SourceDurationUS: 3_125_000},
			{SegmentID: "second-use", AssetID: "drive-source", TimelineStartFrame: 75, FrameCount: 75, SourceInUS: 1_250_000, SourceDurationUS: 3_125_000},
		}}},
		Assets: []contract.AssetRefV2{{AssetID: "drive-source", SourceKind: "drive", DriveFileID: "drive-file-1",
			SizeBytes: 1_000_000, Kind: "video", MIME: "video/mp4"}},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	service := &AssetService{videoTrimmer: NewVideoTrimmer()}
	tasks, err := service.PlanDriveStockWindowTasks(string(planJSON), []map[string]interface{}{{
		"asset_id": "drive-source", "drive_file_id": "drive-file-1",
		"source_uri": "https://objects.example/drive-file-1.mp4", "size_bytes": int64(1_000_000),
	}})
	if err != nil {
		t.Fatalf("PlanDriveStockWindowTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("planned preparation tasks = %d, want one deduplicated discovery task", len(tasks))
	}
	request := tasks[0].Request
	if !request.SourceIdentityDiscovery || request.Source.SHA256 != "" {
		t.Fatalf("request = %+v; want source identity discovery without a pre-declared SHA", request)
	}
	if request.Source.SizeBytes != 1_000_000 || request.Source.DriveFileID != "drive-file-1" {
		t.Fatalf("discovery request lost the source identity hints: %+v", request.Source)
	}
	if _, err := contract.VideoPrepareWindowCacheIdentity(request); err == nil {
		t.Fatal("discovery request must not be accepted as a content cache identity")
	}
	identity, err := contract.VideoPrepareWindowDiscoveryIdentity(request)
	if err != nil || tasks[0].Identity != identity {
		t.Fatalf("task identity = %q err=%v; want discovery identity %q", tasks[0].Identity, err, identity)
	}
	if len(tasks[0].SegmentIDs) != 2 {
		t.Fatalf("segment bindings = %v; want both placements", tasks[0].SegmentIDs)
	}
}
