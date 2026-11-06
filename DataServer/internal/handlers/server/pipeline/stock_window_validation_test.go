package pipeline

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestValidateStockPacketCopyWindows(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		Output:     contract.OutputContractV2{ProfileID: "profile-h264"},
		FinalAudio: contract.FinalAudioV2{AssetID: "audio-1"},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "segment-stock-1", AssetID: "stock-1", SourceInUS: 0, SourceDurationUS: 5_000_000, FrameCount: 120,
		}}}},
		Assets: []contract.AssetRefV2{{AssetID: "stock-1", Kind: "video", DurationUS: 5_000_000, ProfileID: "profile-h264", FrameCount: 120, FirstFrameKeyframe: true, ClosedGOP: true}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	assets := []map[string]interface{}{{"asset_id": "stock-1", "role": "stock", "duration_ms": 5000}}
	if err := validateStockPacketCopyWindows(string(encoded), assets, nil); err != nil {
		t.Fatalf("whole-file stock segment rejected: %v", err)
	}

	plan.VideoTracks[0].Segments[0].SourceInUS = 4_475_000
	plan.VideoTracks[0].Segments[0].SourceDurationUS = 525_000
	plan.VideoTracks[0].Segments[0].FrameCount = 13
	encoded, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	err = validateStockPacketCopyWindows(string(encoded), assets, nil)
	if err == nil || !strings.Contains(err.Error(), `segment "segment-stock-1"`) || !strings.Contains(err.Error(), "source_in_us=4475000") {
		t.Fatalf("partial stock window error = %v", err)
	}
}

func TestValidateStockPacketCopyWindowsRejectsDivergentCertification(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		Output: contract.OutputContractV2{ProfileID: "profile-h264"},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "segment-stock-1", AssetID: "stock-1", SourceInUS: 0, SourceDurationUS: 5_000_000, FrameCount: 120,
		}}}},
		Assets: []contract.AssetRefV2{{AssetID: "stock-1", Kind: "video", DurationUS: 5_000_000, ProfileID: "profile-other", FrameCount: 120, FirstFrameKeyframe: true, ClosedGOP: true}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	err = validateStockPacketCopyWindows(string(encoded), []map[string]interface{}{{"asset_id": "stock-1", "role": "stock", "duration_ms": 5000}}, nil)
	if err == nil || !strings.Contains(err.Error(), "divergent certified profile") {
		t.Fatalf("profile divergence error = %v", err)
	}
}

func TestValidateStockPacketCopyWindowsLeavesStockPoolSlotsToPoolContract(t *testing.T) {
	plan := contract.CompiledRenderPlanV2{
		StockPool: &contract.StockPoolV2{SourceKind: "drive", DriveFolderID: "folder-1", Selection: "shuffle"},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "slot-1", StockPoolSlot: true, SourceInUS: 4_475_000, SourceDurationUS: 525_000, FrameCount: 13,
		}}}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStockPacketCopyWindows(string(encoded), nil, nil); err != nil {
		t.Fatalf("stock_pool slot rejected by per-file window validator: %v", err)
	}
}
