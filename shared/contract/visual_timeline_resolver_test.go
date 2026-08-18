package contract

import (
	"strings"
	"testing"
)

func TestResolveVisualReplacements_NoReplacementsReturnsBase(t *testing.T) {
	base := []VideoSegmentV2{{SegmentID: "seg_000", AssetID: "base", FrameCount: 3600}}
	got, err := ResolveVisualReplacements(base, nil, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].AssetID != "base" {
		t.Fatalf("expected base unchanged, got %+v", got)
	}
}

func TestResolveVisualReplacements_SingleReplacementSplitsBase(t *testing.T) {
	got, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{{
		ReplacementID:   "vr_001",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("b", 64),
		TimelineStartUS: 60_000_000,
		TimelineEndUS:   65_000_000,
		ProfileID:       "velox-h264-1080p30-v1",
	}}, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 segments, got %d: %+v", len(got), got)
	}
	assertSegment(t, got[0], "base", 0, 1800, 0, 60_000_000)             // base 0→60
	assertSegment(t, got[1], "prepared", 1800, 150, 0, 5_000_000)        // prepared 60→65
	assertSegment(t, got[2], "base", 1950, 1650, 65_000_000, 55_000_000) // base 65→120
}

func TestResolveVisualReplacements_SplitsInsideClip(t *testing.T) {
	// clip C: timeline 42→70s, source window 0→28s.
	base := []VideoSegmentV2{{
		SegmentID:          "seg_000",
		AssetID:            "clip_c",
		SHA256:             strings.Repeat("c", 64),
		TimelineStartFrame: 42 * 30,
		FrameCount:         28 * 30,
		SourceInUS:         0,
		SourceDurationUS:   28_000_000,
	}}
	got, err := ResolveVisualReplacements(base, []VisualReplacement{{
		ReplacementID:   "vr",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("d", 64),
		TimelineStartUS: 60_000_000,
		TimelineEndUS:   65_000_000,
	}}, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 segments, got %d: %+v", len(got), got)
	}
	assertSegment(t, got[0], "clip_c", 1260, 540, 0, 18_000_000)         // 42→60
	assertSegment(t, got[1], "prepared", 1800, 150, 0, 5_000_000)        // 60→65
	assertSegment(t, got[2], "clip_c", 1950, 150, 23_000_000, 5_000_000) // 65→70
}

func TestResolveVisualReplacements_MultiClipFiveSegments(t *testing.T) {
	// Section 7: A 0→20, B 20→42, C 42→70, D 70→95, E 95→120, replacement 60→65
	// falls inside clip C. Expected: A, B, C-prefix, PREPARED, C-suffix, D, E.
	base := []VideoSegmentV2{
		{SegmentID: "seg_000", AssetID: "clip_A", SHA256: strings.Repeat("a", 64), TimelineStartFrame: 0, FrameCount: 600, SourceInUS: 0, SourceDurationUS: 20_000_000},
		{SegmentID: "seg_001", AssetID: "clip_B", SHA256: strings.Repeat("b", 64), TimelineStartFrame: 600, FrameCount: 660, SourceInUS: 0, SourceDurationUS: 22_000_000},
		{SegmentID: "seg_002", AssetID: "clip_C", SHA256: strings.Repeat("c", 64), TimelineStartFrame: 1260, FrameCount: 840, SourceInUS: 0, SourceDurationUS: 28_000_000},
		{SegmentID: "seg_003", AssetID: "clip_D", SHA256: strings.Repeat("d", 64), TimelineStartFrame: 2100, FrameCount: 750, SourceInUS: 0, SourceDurationUS: 25_000_000},
		{SegmentID: "seg_004", AssetID: "clip_E", SHA256: strings.Repeat("e", 64), TimelineStartFrame: 2850, FrameCount: 750, SourceInUS: 0, SourceDurationUS: 25_000_000},
	}
	got, err := ResolveVisualReplacements(base, []VisualReplacement{{
		ReplacementID:   "vr",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("z", 64),
		TimelineStartUS: 60_000_000,
		TimelineEndUS:   65_000_000,
	}}, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 7 {
		t.Fatalf("expected 7 segments, got %d: %+v", len(got), got)
	}
	assertSegment(t, got[0], "clip_A", 0, 600, 0, 20_000_000)            // A 0→20
	assertSegment(t, got[1], "clip_B", 600, 660, 0, 22_000_000)          // B 20→42
	assertSegment(t, got[2], "clip_C", 1260, 540, 0, 18_000_000)         // C 42→60
	assertSegment(t, got[3], "prepared", 1800, 150, 0, 5_000_000)        // PREPARED 60→65
	assertSegment(t, got[4], "clip_C", 1950, 150, 23_000_000, 5_000_000) // C 65→70 (source trimmed)
	assertSegment(t, got[5], "clip_D", 2100, 750, 0, 25_000_000)         // D 70→95
	assertSegment(t, got[6], "clip_E", 2850, 750, 0, 25_000_000)         // E 95→120
}

func TestResolveVisualReplacements_StraddlesSegments(t *testing.T) {
	base := []VideoSegmentV2{
		{SegmentID: "seg_000", AssetID: "clip_a", SHA256: strings.Repeat("a", 64), TimelineStartFrame: 0, FrameCount: 600, SourceInUS: 0, SourceDurationUS: 20_000_000},
		{SegmentID: "seg_001", AssetID: "clip_b", SHA256: strings.Repeat("b", 64), TimelineStartFrame: 600, FrameCount: 600, SourceInUS: 0, SourceDurationUS: 20_000_000},
	}
	got, err := ResolveVisualReplacements(base, []VisualReplacement{{
		ReplacementID:   "vr",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("c", 64),
		TimelineStartUS: 18_000_000,
		TimelineEndUS:   22_000_000,
	}}, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 segments, got %d: %+v", len(got), got)
	}
	assertSegment(t, got[0], "clip_a", 0, 540, 0, 18_000_000)           // clip_a 0→18
	assertSegment(t, got[1], "prepared", 540, 60, 0, 2_000_000)         // prepared 18→20
	assertSegment(t, got[2], "prepared", 600, 60, 0, 2_000_000)         // prepared 20→22
	assertSegment(t, got[3], "clip_b", 660, 540, 2_000_000, 18_000_000) // clip_b 22→40
}

func TestResolveVisualReplacements_InvalidRange(t *testing.T) {
	_, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{{
		ReplacementID: "r", AssetID: "p", TimelineStartUS: 65_000_000, TimelineEndUS: 60_000_000,
	}}, 30, 1)
	assertReplacementErr(t, err, VisualReplacementCodeInvalidRange, "r")
}

func TestResolveVisualReplacements_ZeroDurationRange(t *testing.T) {
	_, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{{
		ReplacementID: "r", AssetID: "p", TimelineStartUS: 60_000_000, TimelineEndUS: 60_000_000,
	}}, 30, 1)
	assertReplacementErr(t, err, VisualReplacementCodeInvalidRange, "r")
}

func TestResolveVisualReplacements_MultipleReplacements(t *testing.T) {
	// Section 10: replacements 60→65 and 90→94 over a 120s base must yield
	// five packet-copy segments: BASE/PREPARED/BASE/PREPARED/BASE.
	got, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{
		{
			ReplacementID:   "vr_a",
			AssetID:         "prepared_a",
			SHA256:          strings.Repeat("a", 64),
			TimelineStartUS: 90_000_000,
			TimelineEndUS:   94_000_000,
		},
		{
			ReplacementID:   "vr_b",
			AssetID:         "prepared_b",
			SHA256:          strings.Repeat("b", 64),
			TimelineStartUS: 60_000_000,
			TimelineEndUS:   65_000_000,
		},
	}, 30, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 segments, got %d: %+v", len(got), got)
	}
	// Input order is reversed; the resolver must sort by timeline position.
	assertSegment(t, got[0], "base", 0, 1800, 0, 60_000_000)            // BASE 0→60
	assertSegment(t, got[1], "prepared_b", 1800, 150, 0, 5_000_000)     // PREPARED 60→65
	assertSegment(t, got[2], "base", 1950, 750, 65_000_000, 25_000_000) // BASE 65→90
	assertSegment(t, got[3], "prepared_a", 2700, 120, 0, 4_000_000)     // PREPARED 90→94
	assertSegment(t, got[4], "base", 2820, 780, 94_000_000, 26_000_000) // BASE 94→120
}

func TestResolveVisualReplacements_Overlap(t *testing.T) {
	_, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{
		{ReplacementID: "r1", AssetID: "p", TimelineStartUS: 60_000_000, TimelineEndUS: 65_000_000},
		{ReplacementID: "r2", AssetID: "p", TimelineStartUS: 63_000_000, TimelineEndUS: 70_000_000},
	}, 30, 1)
	assertReplacementErr(t, err, VisualReplacementCodeOverlap, "r2")
}

func TestResolveVisualReplacements_OutOfBounds(t *testing.T) {
	_, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{{
		ReplacementID: "r", AssetID: "p", TimelineStartUS: 119_000_000, TimelineEndUS: 125_000_000,
	}}, 30, 1)
	assertReplacementErr(t, err, VisualReplacementCodeOutOfBounds, "r")
}

func TestResolveVisualReplacements_MissingAsset(t *testing.T) {
	_, err := ResolveVisualReplacements(singleBase(), []VisualReplacement{{
		ReplacementID: "r", TimelineStartUS: 60_000_000, TimelineEndUS: 65_000_000,
	}}, 30, 1)
	assertReplacementErr(t, err, VisualReplacementCodeAssetInvalid, "r")
}

func TestParseVisualReplacements(t *testing.T) {
	raw := []interface{}{
		map[string]interface{}{
			"replacement_id":    "r1",
			"asset_id":          "a",
			"sha256":            "abc",
			"timeline_start_us": int64(60_000_000),
			"timeline_end_us":   int64(65_000_000),
			"profile_id":        "p",
		},
	}
	got, err := ParseVisualReplacements(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].ReplacementID != "r1" || got[0].AssetID != "a" ||
		got[0].TimelineStartUS != 60_000_000 || got[0].TimelineEndUS != 65_000_000 || got[0].ProfileID != "p" {
		t.Fatalf("unexpected parse result: %+v", got)
	}
}

func singleBase() []VideoSegmentV2 {
	return []VideoSegmentV2{{
		SegmentID:          "seg_000",
		AssetID:            "base",
		SHA256:             strings.Repeat("a", 64),
		TimelineStartFrame: 0,
		FrameCount:         3600,
		SourceInUS:         0,
		SourceDurationUS:   120_000_000,
	}}
}

func assertSegment(t *testing.T, seg VideoSegmentV2, assetID string, startFrame, frameCount, sourceIn, sourceDur int64) {
	t.Helper()
	if seg.AssetID != assetID || seg.TimelineStartFrame != startFrame || seg.FrameCount != frameCount ||
		seg.SourceInUS != sourceIn || seg.SourceDurationUS != sourceDur {
		t.Fatalf("segment mismatch:\n got  %+v\n want asset=%s start=%d frames=%d sourceIn=%d sourceDur=%d",
			seg, assetID, startFrame, frameCount, sourceIn, sourceDur)
	}
}

func assertReplacementErr(t *testing.T, err error, code, replacementID string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	re, ok := err.(*VisualReplacementError)
	if !ok {
		t.Fatalf("expected *VisualReplacementError, got %T: %v", err, err)
	}
	if re.Code != code || re.ReplacementID != replacementID {
		t.Fatalf("expected code=%s id=%s, got code=%s id=%s", code, replacementID, re.Code, re.ReplacementID)
	}
}
