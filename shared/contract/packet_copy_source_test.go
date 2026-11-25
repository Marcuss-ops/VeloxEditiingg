package contract

import "testing"

func TestPacketCopySourceWindowCertified(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	plan := &CompiledRenderPlanV2{Output: OutputContractV2{ProfileID: profile.ProfileID}}
	asset := AssetRefV2{
		AssetID: "source", SHA256: "source-sha", SizeBytes: 1000, Kind: "video",
		DurationUS: 10_000_000, Width: profile.Width, Height: profile.Height,
		ProfileID: profile.StreamProfile(), FrameCount: 240, FirstFrameKeyframe: true,
		ClosedGOP: true, ExtradataSHA256: CanonicalVideoExtradataSHA256,
	}
	segment := VideoSegmentV2{AssetID: "source", SHA256: asset.SHA256, SourceInUS: 2_000_000, SourceDurationUS: 3_000_000, FrameCount: 72}
	if !PacketCopySourceWindowCertified(plan, segment, asset) {
		t.Fatal("complete canonical source evidence should admit direct packet-copy assembly")
	}

	cases := []struct {
		name   string
		mutate func(*AssetRefV2, *VideoSegmentV2)
	}{
		{"bad extradata", func(a *AssetRefV2, _ *VideoSegmentV2) { a.ExtradataSHA256 = "other" }},
		{"missing closed GOP", func(a *AssetRefV2, _ *VideoSegmentV2) { a.ClosedGOP = false }},
		{"window past source end", func(a *AssetRefV2, s *VideoSegmentV2) { s.SourceInUS = 8_000_001 }},
		{"wrong asset identity", func(_ *AssetRefV2, s *VideoSegmentV2) { s.SHA256 = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidateAsset, candidateSegment := asset, segment
			tc.mutate(&candidateAsset, &candidateSegment)
			if PacketCopySourceWindowCertified(plan, candidateSegment, candidateAsset) {
				t.Fatal("incomplete or inconsistent source evidence was accepted")
			}
		})
	}
}
