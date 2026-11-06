package pipeline

import (
	"encoding/json"
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

func TestRemovePreparedStockBindingsKeepsNonStockAssets(t *testing.T) {
	assets := []map[string]interface{}{
		{"role": "stock", "drive_file_id": "stock-1"},
		{"role": "stock", "source_uri": "https://cdn.example/stock.mp4"},
		{"role": "overlay", "asset_id": "overlay-1"},
		{"role": "final_audio", "asset_id": "audio-1"},
	}
	got := removePreparedStockBindings(assets)
	if len(got) != 3 || got[0]["source_uri"] != "https://cdn.example/stock.mp4" || got[1]["asset_id"] != "overlay-1" || got[2]["asset_id"] != "audio-1" {
		t.Fatalf("remaining runtime assets = %#v", got)
	}
}
