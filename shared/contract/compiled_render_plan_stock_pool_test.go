package contract

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompiledStockPoolSelectionIsStableAndSnapshotWins(t *testing.T) {
	plan := &CompiledRenderPlanV2{
		TimelineSHA256: strings.Repeat("a", 64),
		StockPool:      &StockPoolV2{DriveFolderID: "folder-1", Selection: "shuffle"},
		Output:         OutputContractV2{FPSNum: 24, FPSDen: 1},
		VideoTracks: []VideoTrackV2{{Segments: []VideoSegmentV2{
			{StockPoolSlot: true, FrameCount: 240},
		}}},
	}
	candidates := []StockPoolCandidateV2{
		{AssetID: "asset-c", DriveFileID: "drive-c", SizeBytes: 100, DurationMS: 4_000, StockPoolMember: true, SourceSHA256: strings.Repeat("c", 64)},
		{AssetID: "asset-a", DriveFileID: "drive-a", SizeBytes: 100, DurationMS: 4_000, StockPoolMember: true, SourceSHA256: strings.Repeat("a", 64)},
		{AssetID: "asset-b", DriveFileID: "drive-b", SizeBytes: 100, DurationMS: 4_000, StockPoolMember: true, SourceSHA256: strings.Repeat("b", 64)},
	}
	first, err := SelectCompiledStockPoolMembers(plan, "job-1", candidates)
	if err != nil {
		t.Fatalf("first selection: %v", err)
	}
	reversed := []StockPoolCandidateV2{candidates[2], candidates[1], candidates[0]}
	second, err := SelectCompiledStockPoolMembers(plan, "job-1", reversed)
	if err != nil {
		t.Fatalf("selection with reordered candidates: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("selection changed with candidate order: first=%+v second=%+v", first, second)
	}

	snapshot := StockPoolSnapshotV1{
		Version: 1, JobID: "job-1", TimelineSHA256: plan.TimelineSHA256,
		DriveFolderID: plan.StockPool.DriveFolderID, Members: first,
	}
	// A retry must preserve the persisted choice even if another eligible file
	// appears in the Drive listing.
	reversed = append(reversed, StockPoolCandidateV2{
		AssetID: "asset-new", DriveFileID: "drive-new", SizeBytes: 100,
		DurationMS: 4_000, StockPoolMember: true, SourceSHA256: strings.Repeat("d", 64),
	})
	retried, err := ResolveCompiledStockPoolMembers(plan, "job-1", snapshot, reversed)
	if err != nil {
		t.Fatalf("resolve persisted snapshot: %v", err)
	}
	if !reflect.DeepEqual(first, retried) {
		t.Fatalf("retry changed persisted selection: first=%+v retry=%+v", first, retried)
	}
}

func TestResolveCompiledStockPoolRejectsChangedSnapshotIdentity(t *testing.T) {
	plan := &CompiledRenderPlanV2{
		TimelineSHA256: strings.Repeat("a", 64),
		StockPool:      &StockPoolV2{DriveFolderID: "folder-1", Selection: "shuffle"},
		Output:         OutputContractV2{FPSNum: 24, FPSDen: 1},
		VideoTracks: []VideoTrackV2{{Segments: []VideoSegmentV2{
			{StockPoolSlot: true, FrameCount: 24},
		}}},
	}
	member := StockPoolCandidateV2{
		AssetID: "asset-a", DriveFileID: "drive-a", SizeBytes: 100,
		DurationMS: 1_000, StockPoolMember: true, SourceSHA256: strings.Repeat("a", 64),
	}
	snapshot := StockPoolSnapshotV1{
		Version: 1, JobID: "job-1", TimelineSHA256: plan.TimelineSHA256,
		DriveFolderID: plan.StockPool.DriveFolderID, Members: []StockPoolCandidateV2{member},
	}
	changed := member
	changed.SourceSHA256 = strings.Repeat("b", 64)
	_, err := ResolveCompiledStockPoolMembers(plan, "job-1", snapshot, []StockPoolCandidateV2{changed})
	if err == nil {
		t.Fatal("changed stock-pool source identity accepted")
	}
}
