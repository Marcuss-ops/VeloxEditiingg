package contract

import (
	"strconv"
	"strings"
	"testing"
)

func TestVideoSourceCertificateRejectsIncompleteAndIncompatibleEvidence(t *testing.T) {
	source := sourceCertificateFixture(t)
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
	if !source.HasKeyframe(1000000) || !source.HasKeyframe(1000001) || source.HasKeyframe(1000002) || source.HasKeyframe(500000) {
		t.Fatal("keyframe lookup does not use exact source coordinates")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*VideoSourceCertificateV1)
	}{
		{"wrong hash", func(c *VideoSourceCertificateV1) { c.SHA256 = "bad" }},
		{"wrong duration", func(c *VideoSourceCertificateV1) { c.DurationUS++ }},
		{"no frames", func(c *VideoSourceCertificateV1) { c.Evidence.FrameCount = 0 }},
		{"b frames", func(c *VideoSourceCertificateV1) { c.Evidence.BFrames = 2 }},
		{"wrong avcc", func(c *VideoSourceCertificateV1) { c.Evidence.ExtradataSHA256 = strings.Repeat("b", 64) }},
		{"old revision", func(c *VideoSourceCertificateV1) { c.PreparationRevision = "old" }},
		{"duplicate keyframe", func(c *VideoSourceCertificateV1) { c.KeyframesUS = []int64{0, 0} }},
		{"nonzero origin", func(c *VideoSourceCertificateV1) { c.KeyframesUS = []int64{1} }},
		{"out of bounds", func(c *VideoSourceCertificateV1) { c.KeyframesUS = []int64{0, 2000000} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := source
			tc.mutate(&copy)
			if copy.Validate() == nil {
				t.Fatal("invalid source evidence accepted")
			}
		})
	}
}

func sourceCertificateFixture(t *testing.T) VideoSourceCertificateV1 {
	t.Helper()
	p := CanonicalVideoProfileV1Default
	revision, err := VideoWindowPreparationRevision(p, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	level, _ := strconv.Atoi(strings.ReplaceAll(p.CodecLevel, ".", ""))
	source := VideoSourceCertificateV1{Version: 1, SHA256: strings.Repeat("a", 64), SizeBytes: 100, ProfileID: p.ProfileID, PreparationRevision: revision, DurationUS: 2000000, KeyframesUS: []int64{0, 1000000}, Evidence: VideoWindowOutputEvidenceV1{Codec: p.Codec, Profile: strings.ToLower(p.CodecProfile), Level: level, Width: p.Width, Height: p.Height, PixelFormat: p.PixelFormat, FPSNum: p.FPSNum, FPSDen: p.FPSDen, TimeBaseNum: 1, TimeBaseDen: 90000, FrameCount: 48, FirstFrameKeyframe: true, ExtradataSHA256: CanonicalVideoExtradataSHA256}}
	return source
}

func TestStockPoolChoosesCertifiedKeyframesAfterPartialConsumption(t *testing.T) {
	source := sourceCertificateFixture(t)
	p := CanonicalVideoProfileV1Default
	asset := AssetRefV2{AssetID: "stockpool:clip", Kind: "video", SHA256: source.SHA256, SizeBytes: source.SizeBytes, ProfileID: p.StreamProfile(), Width: p.Width, Height: p.Height, FrameCount: source.Evidence.FrameCount, DurationUS: source.DurationUS, FirstFrameKeyframe: true, ClosedGOP: true, ExtradataSHA256: CanonicalVideoExtradataSHA256, SourceCertificate: &source}
	plan := &CompiledRenderPlanV2{StockPool: &StockPoolV2{DriveFolderID: "folder", Selection: "shuffle"}, Output: OutputContractV2{ProfileID: p.ProfileID, FPSNum: 24, FPSDen: 1}, Assets: []AssetRefV2{asset}, VideoTracks: []VideoTrackV2{{Segments: []VideoSegmentV2{{SegmentID: "first", StockPoolSlot: true, FrameCount: 12}, {SegmentID: "second", StockPoolSlot: true, TimelineStartFrame: 12, FrameCount: 12}, {SegmentID: "third", StockPoolSlot: true, TimelineStartFrame: 24, FrameCount: 12}}}}}
	members := []StockPoolCandidateV2{{DriveFileID: "clip", StockPoolMember: true, SizeBytes: source.SizeBytes, SourceSHA256: source.SHA256, DurationMS: 1999}}
	unknown := StockPoolCandidateV2{DriveFileID: "unknown", StockPoolMember: true, SizeBytes: 100, SourceSHA256: strings.Repeat("b", 64), DurationMS: 10000}
	selected, err := SelectCompiledStockPoolMembers(plan, "job", append([]StockPoolCandidateV2{unknown}, members...))
	if err != nil || len(selected) != 1 || selected[0].DriveFileID != "clip" {
		t.Fatalf("selection ignored compatible certified source: members=%+v err=%v", selected, err)
	}
	// Extra candidates certified before selection must never become downloads.
	plan.Assets = append(plan.Assets, AssetRefV2{AssetID: "unused-candidate", Kind: "video"})
	if err := ExpandCompiledStockPoolSlots(plan, members); err != nil {
		t.Fatal(err)
	}
	if len(plan.Assets) != 1 {
		t.Fatalf("unreferenced candidates survived expansion: %+v", plan.Assets)
	}
	for i, want := range []int64{0, 1000000, 0} {
		segment := plan.VideoTracks[0].Segments[i]
		if segment.SourceInUS != want || !PacketCopySourceWindowCertified(plan, segment, plan.Assets[0]) {
			t.Fatalf("slot %d did not choose a certified cut: %+v", i, segment)
		}
	}
	bad := source
	bad.KeyframesUS = []int64{1000000}
	plan.StockPool = &StockPoolV2{DriveFolderID: "folder", Selection: "shuffle"}
	plan.VideoTracks[0].Segments = []VideoSegmentV2{{StockPoolSlot: true, FrameCount: 12}}
	plan.Assets[0].SourceCertificate = &bad
	if ExpandCompiledStockPoolSlots(plan, members) == nil {
		t.Fatal("invalid source certification was silently used")
	}
}

func TestAssetValidationRejectsCertificateForDifferentContent(t *testing.T) {
	source := sourceCertificateFixture(t)
	asset := AssetRefV2{AssetID: "source", Kind: "video", SHA256: strings.Repeat("b", 64), SizeBytes: source.SizeBytes, FrameCount: source.Evidence.FrameCount, DurationUS: source.DurationUS, Width: source.Evidence.Width, Height: source.Evidence.Height, ExtradataSHA256: source.Evidence.ExtradataSHA256, SourceCertificate: &source}
	var violations CompiledRenderPlanV2ValidationErrors
	validateAsset(&violations, "assets[0]", asset)
	found := false
	for _, violation := range violations {
		if violation.Issue == "source_evidence_mismatch" {
			found = true
		}
	}
	if !found {
		t.Fatal("asset certificate for different bytes was accepted")
	}
}
