package pipeline

import (
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
