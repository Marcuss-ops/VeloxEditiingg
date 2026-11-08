package contract

import (
	"strings"
	"testing"
)

func TestVideoPrepareWindowV1ValidatesCanonicalSourceAndWindow(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	revision, err := VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	request := VideoPrepareWindowV1{
		Version:          VideoPrepareWindowVersion,
		Source:           AssetRefV2{AssetID: "drive:source-1", AssetKey: "source-1", SourceKind: "drive", DriveFileID: "source-1", SHA256: strings.Repeat("a", 64), SizeBytes: 1024, Kind: "video"},
		SourceStartFrame: 24, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 7, TimelineSHA256: strings.Repeat("b", 64),
		TimelineStartFrame: 120, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	if _, err := request.CanonicalJSON(); err != nil {
		t.Fatalf("valid preparation request rejected: %v", err)
	}
	request.OutputProfileRevision = "video-window-preparer.v1:" + strings.Repeat("f", 64)
	if err := request.Validate(); err != nil {
		t.Fatalf("revision is an opaque identity at the wire-contract layer: %v", err)
	}
	request.SourceStartFrame, request.SourceFrameCount = 0, 0
	request.SourceFPSNum, request.SourceFPSDen = 0, 0
	request.SourceStartUS, request.SourceDurationUS = 1_250_000, 2_500_000
	if err := request.Validate(); err != nil {
		t.Fatalf("valid source timestamp interval rejected: %v", err)
	}
}

func TestVideoPrepareWindowV1RejectsMissingIdentityAndOverflow(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	revision, _ := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	base := VideoPrepareWindowV1{
		Version:          VideoPrepareWindowVersion,
		Source:           AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 1, Kind: "video"},
		SourceStartFrame: 1, SourceFrameCount: 2, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 2, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	cases := []struct {
		name string
		edit func(*VideoPrepareWindowV1)
	}{
		{"unknown source bytes", func(r *VideoPrepareWindowV1) { r.Source.SHA256 = "" }},
		{"invalid timeline identity", func(r *VideoPrepareWindowV1) { r.TimelineSHA256 = "bad" }},
		{"frame interval overflow", func(r *VideoPrepareWindowV1) { r.SourceStartFrame = int64(^uint64(0) >> 1); r.SourceFrameCount = 1 }},
		{"ambiguous frame and timestamp intervals", func(r *VideoPrepareWindowV1) { r.SourceStartUS = 1; r.SourceDurationUS = 10 }},
		{"timestamp interval overflow", func(r *VideoPrepareWindowV1) {
			r.SourceStartFrame = 0
			r.SourceFrameCount = 0
			r.SourceFPSNum = 0
			r.SourceFPSDen = 0
			r.SourceStartUS = int64(^uint64(0) >> 1)
			r.SourceDurationUS = 1
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := base
			tc.edit(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("invalid preparation request was accepted")
			}
		})
	}
}

func TestVideoPrepareWindowCacheIdentityIgnoresTimelinePlacement(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := VideoPrepareWindowV1{
		Version:       VideoPrepareWindowVersion,
		Source:        AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 1000, Kind: "video"},
		SourceStartUS: 500_000, SourceDurationUS: 2_000_000, OutputFrameCount: 48,
		TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64), TimelineStartFrame: 0,
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	first, err := VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	request.TimelineRevision++
	request.TimelineSHA256 = strings.Repeat("c", 64)
	request.TimelineStartFrame = 900
	second, err := VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("cache identity changed with timeline placement: %s != %s", first, second)
	}
	request.SourceStartUS++
	third, err := VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("cache identity did not change when source window changed")
	}
}
