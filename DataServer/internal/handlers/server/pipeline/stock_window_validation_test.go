package pipeline

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestCompiledPlanHasDriveStockForPartialWindow(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		Output: contract.OutputContractV2{FPSNum: 24, FPSDen: 1},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "r58-window", AssetID: "drive:drive-stock-1", TimelineStartFrame: 0, FrameCount: 75, SourceInUS: 0, SourceDurationUS: 3_125_000,
		}}}},
		Assets: []contract.AssetRefV2{{AssetID: "drive:drive-stock-1", DriveFileID: "drive-stock-1", SourceKind: "drive", Kind: "video"}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !compiledPlanHasDriveStock(string(encoded)) {
		t.Fatal("partial Drive stock plan was not routed through Master preparation")
	}
}

func TestCompiledPlanHasDriveStockWhenRuntimeBindingSuppliesDriveIdentity(t *testing.T) {
	sha := strings.Repeat("c", 64)
	plan := contract.CompiledRenderPlanV2{
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "r58-window", AssetID: sha, SHA256: sha, TimelineStartFrame: 0, FrameCount: 75, SourceDurationUS: 3_125_000}}}},
		Assets:      []contract.AssetRefV2{{AssetID: sha, SHA256: sha, Kind: "video", DurationUS: 5_000_000, FrameCount: 120}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	bindings := []map[string]interface{}{{"role": "stock", "asset_id": sha, "sha256": sha, "drive_file_id": "drive-file-1", "source_uri": "https://drive.google.com/uc?export=download&id=drive-file-1"}}
	if !compiledPlanHasDriveStock(string(encoded), bindings) {
		t.Fatal("runtime Drive stock binding did not map to the plan's content identity")
	}
}

func TestCompiledPlanHasDriveStockRequiresTimelineReference(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "local", AssetID: "local-asset", FrameCount: 24}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "local-asset", Kind: "video"},
			{AssetID: "unused-drive", Kind: "video", DriveFileID: "drive-unused"},
		},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if compiledPlanHasDriveStock(string(encoded)) {
		t.Fatal("unused Drive asset incorrectly triggered stock preparation")
	}
}
