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

func TestExpandCompiledStockPoolSlotsCyclesAndDoesNotFabricateCertification(t *testing.T) {
	plan := &CompiledRenderPlanV2{
		TimelineSHA256: strings.Repeat("a", 64), StockPool: &StockPoolV2{DriveFolderID: "folder-1", Selection: "shuffle"},
		Output:      OutputContractV2{FPSNum: 24, FPSDen: 1},
		VideoTracks: []VideoTrackV2{{TrackID: "main", Segments: []VideoSegmentV2{{SegmentID: "slot", StockPoolSlot: true, TimelineStartFrame: 0, FrameCount: 50}}}},
	}
	members := []StockPoolCandidateV2{{AssetID: "drive-a", DriveFileID: "drive-a", SizeBytes: 100, DurationMS: 1_000, StockPoolMember: true, SourceSHA256: strings.Repeat("b", 64)}}
	if err := ExpandCompiledStockPoolSlots(plan, members); err != nil {
		t.Fatalf("expand: %v", err)
	}
	if plan.StockPool != nil {
		t.Fatal("expanded plan still has an unresolved stock pool")
	}
	segments := plan.VideoTracks[0].Segments
	if len(segments) != 3 || segments[0].FrameCount != 24 || segments[1].FrameCount != 24 || segments[2].FrameCount != 2 {
		t.Fatalf("cyclic segments = %+v", segments)
	}
	for i, segment := range segments {
		if segment.StockPoolSlot || segment.SourceInUS != 0 || segment.TimelineStartFrame != int64(i*24) {
			t.Fatalf("segment %d has invalid expanded placement: %+v", i, segment)
		}
	}
	if len(plan.Assets) != 1 || plan.Assets[0].FirstFrameKeyframe || plan.Assets[0].ClosedGOP {
		t.Fatalf("expansion fabricated source certification: %+v", plan.Assets)
	}
}

func TestExpandStockPoolContinuesSourceAcrossSlots(t *testing.T) {
	plan := &CompiledRenderPlanV2{StockPool: &StockPoolV2{DriveFolderID: "folder", Selection: "shuffle"}, Output: OutputContractV2{FPSNum: 24, FPSDen: 1}, VideoTracks: []VideoTrackV2{{TrackID: "main", Segments: []VideoSegmentV2{
		{SegmentID: "first", StockPoolSlot: true, FrameCount: 24},
		{SegmentID: "second", StockPoolSlot: true, TimelineStartFrame: 24, FrameCount: 24},
		{SegmentID: "cycle", StockPoolSlot: true, TimelineStartFrame: 48, FrameCount: 24},
	}}}}
	members := []StockPoolCandidateV2{{AssetID: "clip", DriveFileID: "clip", SizeBytes: 100, DurationMS: 2000, StockPoolMember: true}}
	if err := ExpandCompiledStockPoolSlots(plan, members); err != nil {
		t.Fatal(err)
	}
	segments := plan.VideoTracks[0].Segments
	for i, want := range []int64{0, 1000000, 0} {
		if segments[i].SourceInUS != want {
			t.Fatalf("slot %d offset=%d, want %d", i, segments[i].SourceInUS, want)
		}
	}
}

func TestStockPoolFramesUseCertifiedSourceIdentity(t *testing.T) {
	profile, err := KnownCanonicalVideoProfileV1("VELOX_ASSEMBLY_READY_V1")
	if err != nil {
		t.Fatal(err)
	}
	member := StockPoolCandidateV2{DriveFileID: "clip", SourceSHA256: strings.Repeat("b", 64), SizeBytes: 100, DurationMS: 1041}
	asset := AssetRefV2{AssetID: "stockpool:clip", Kind: "video", SHA256: member.SourceSHA256, SizeBytes: 100, DurationUS: 1041667, FrameCount: 25, Width: profile.Width, Height: profile.Height, ProfileID: profile.StreamProfile(), FirstFrameKeyframe: true, ClosedGOP: true, ExtradataSHA256: CanonicalVideoExtradataSHA256}
	plan := &CompiledRenderPlanV2{Output: OutputContractV2{ProfileID: profile.ProfileID, FPSNum: 24, FPSDen: 1}, Assets: []AssetRefV2{asset}}
	if frames, bad := stockPoolMemberFrames(plan, member); bad || frames != 25 {
		t.Fatalf("certified frames=%d invalid=%v", frames, bad)
	}
	plan.Assets[0].SHA256 = strings.Repeat("c", 64)
	if frames, bad := stockPoolMemberFrames(plan, member); bad || frames != 24 {
		t.Fatalf("changed content reused certificate: frames=%d invalid=%v", frames, bad)
	}
}
