package assets

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestPlanDriveStockWindowTasksDeduplicatesImmutableWindowsWithoutDownloading(t *testing.T) {
	sha := strings.Repeat("c", 64)
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
			SHA256: sha, SizeBytes: 1_000_000, Kind: "video", MIME: "video/mp4"}},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	service := &AssetService{videoWindowVerifier: newVideoWindowVerifier()}
	tasks, err := service.PlanDriveStockWindowTasks(string(planJSON), []map[string]interface{}{{
		"asset_id": "drive-source", "drive_file_id": "drive-file-1", "source_uri": "https://objects.example/drive-file-1.mp4",
		"sha256": sha, "size_bytes": int64(1_000_000),
	}})
	if err != nil {
		t.Fatalf("PlanDriveStockWindowTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("known immutable source scheduled %d preparation tasks; direct assembly should decide compatibility", len(tasks))
	}

	// If the immutable digest has not yet been copied into the plan itself, the
	// enrichment step must do that before deciding to bypass preparation. Until
	// then the planner still publishes a worker task with the known digest.
	plan.Assets[0].SHA256 = ""
	sourcePlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	known, err := service.PlanDriveStockWindowTasks(string(sourcePlan), []map[string]interface{}{{
		"asset_id": "drive-source", "source_sha256": sha, "size_bytes": int64(1_000_000),
	}})
	if err != nil || len(known) != 1 {
		t.Fatalf("known source planning: tasks=%v err=%v", known, err)
	}
	if known[0].Request.SourceIdentityDiscovery || known[0].Request.Source.SHA256 != sha {
		t.Fatalf("known SHA lost: %+v", known[0].Request)
	}
	plan.Assets[0].SHA256 = sha

	plan.Assets[0].DurationUS = 10_000_000
	plan.Assets[0].Width, plan.Assets[0].Height = profile.Width, profile.Height
	plan.Assets[0].ProfileID = profile.StreamProfile()
	plan.Assets[0].FrameCount = 240
	plan.Assets[0].FirstFrameKeyframe = true
	plan.Assets[0].ClosedGOP = true
	plan.Assets[0].ExtradataSHA256 = contract.CanonicalVideoExtradataSHA256
	for i := range plan.VideoTracks[0].Segments {
		plan.VideoTracks[0].Segments[i].SHA256 = sha
	}
	planJSON, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err = service.PlanDriveStockWindowTasks(string(planJSON), []map[string]interface{}{{
		"asset_id": "drive-source", "drive_file_id": "drive-file-1", "source_uri": "https://objects.example/drive-file-1.mp4",
		"sha256": sha, "size_bytes": int64(1_000_000),
	}})
	if err != nil {
		t.Fatalf("PlanDriveStockWindowTasks for certified source: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("certified source still scheduled %d intermediate preparation tasks", len(tasks))
	}
}
