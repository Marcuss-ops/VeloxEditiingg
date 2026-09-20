package contract

import "testing"

func TestResolveOverlayTimeline_ReplaceUsesOneContiguousTrack(t *testing.T) {
	base := []VideoSegmentV2{{AssetID: "base", TimelineStartFrame: 0, FrameCount: 720}}
	overlays := []Overlay{
		{ID: "overlay_01", AssetID: "a", StartFrame: 120, FrameCount: 120, Mode: "replace", AudioMode: OverlayAudioPreserveFinal},
		{ID: "overlay_02", AssetID: "b", StartFrame: 240, FrameCount: 81, Mode: "replace", AudioMode: OverlayAudioPreserveFinal},
	}
	got, windows, err := ResolveOverlayTimeline(base, overlays, 24, 1)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(windows) != 0 {
		t.Fatalf("replace overlays produced composite windows: %+v", windows)
	}
	if len(got) != 4 {
		t.Fatalf("segments = %d, want 4: %+v", len(got), got)
	}
	want := []struct {
		asset        string
		start, count int64
	}{{"base", 0, 120}, {"a", 120, 120}, {"b", 240, 81}, {"base", 321, 399}}
	if len(got) != len(want) {
		t.Fatalf("segments = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].AssetID != w.asset || got[i].TimelineStartFrame != w.start || got[i].FrameCount != w.count {
			t.Fatalf("segment[%d] = %+v, want %s [%d,%d)", i, got[i], w.asset, w.start, w.start+w.count)
		}
	}
}

func TestResolveOverlayTimeline_EmitsExplicitExclusiveEndFrame(t *testing.T) {
	base := []VideoSegmentV2{{AssetID: "base", TimelineStartFrame: 0, FrameCount: 480}}
	got, windows, err := ResolveOverlayTimeline(base, []Overlay{{
		ID: "overlay-end", AssetID: "overlay-end", StartFrame: 120, EndFrame: 240,
		Mode: string(OverlayModeReplace), AudioMode: OverlayAudioPreserveFinal,
	}}, 24, 1)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(windows) != 0 || len(got) != 3 {
		t.Fatalf("resolved overlay = segments=%d windows=%d: %+v", len(got), len(windows), got)
	}
	if got[1].TimelineStartFrame != 120 || got[1].FrameCount != 120 {
		t.Fatalf("overlay segment = %+v, want [120,240)", got[1])
	}
}

func TestParseOverlaysUpgradesFrameCountToExplicitEndFrame(t *testing.T) {
	overlays, err := ParseOverlays([]interface{}{map[string]interface{}{
		"id": "legacy", "asset_id": "asset", "start_frame": float64(24), "frame_count": float64(120),
		"mode": "replace", "audio_mode": OverlayAudioPreserveFinal,
	}})
	if err != nil {
		t.Fatalf("ParseOverlays: %v", err)
	}
	if overlays[0].EndFrame != 144 || overlays[0].FrameCount != 120 {
		t.Fatalf("normalized overlay = %+v, want [24,144) count=120", overlays[0])
	}
}

func TestParseOverlaysRejectsInconsistentStartEndAndCount(t *testing.T) {
	_, err := ParseOverlays([]interface{}{map[string]interface{}{
		"id": "bad", "asset_id": "asset", "start_frame": float64(24), "end_frame": float64(200), "frame_count": float64(120),
		"mode": "replace", "audio_mode": OverlayAudioPreserveFinal,
	}})
	if err == nil {
		t.Fatal("expected inconsistent overlay window to fail")
	}
}

func TestResolveOverlayWindows_OverlapsBecomeDeterministicWindows(t *testing.T) {
	windows, err := ResolveOverlayWindows([]Overlay{
		{ID: "a", AssetID: "a", StartFrame: 240, FrameCount: 120, Mode: "composite", ZIndex: 10, AudioMode: OverlayAudioPreserveFinal},
		{ID: "b", AssetID: "b", StartFrame: 288, FrameCount: 48, Mode: "composite", ZIndex: 20, AudioMode: OverlayAudioPreserveFinal},
		{ID: "c", AssetID: "c", StartFrame: 312, FrameCount: 72, Mode: "composite", ZIndex: 20, AudioMode: OverlayAudioPreserveFinal},
	}, 480)
	if err != nil {
		t.Fatalf("resolve windows: %v", err)
	}
	if len(windows) != 5 {
		t.Fatalf("windows = %d, want 5: %+v", len(windows), windows)
	}
	for i, w := range windows {
		if w.WindowID == "" {
			t.Fatalf("window[%d] has no deterministic ID", i)
		}
		if i > 0 && windows[i-1].StartFrame+windows[i-1].FrameCount != w.StartFrame {
			t.Fatalf("windows are not contiguous: %+v", windows)
		}
	}
	if windows[0].StartFrame != 240 || windows[0].FrameCount != 48 || len(windows[0].Overlays) != 1 {
		t.Fatalf("first window = %+v", windows[0])
	}
	if windows[1].StartFrame != 288 || windows[1].FrameCount != 24 || len(windows[1].Overlays) != 2 {
		t.Fatalf("overlap window = %+v", windows[1])
	}
	if windows[1].Overlays[0].ID != "a" || windows[1].Overlays[1].ID != "b" {
		t.Fatalf("z ordering = %+v", windows[1].Overlays)
	}
	if windows[2].StartFrame != 312 || windows[2].FrameCount != 24 || len(windows[2].Overlays) != 3 {
		t.Fatalf("triple window = %+v", windows[2])
	}
}

func TestResolveOverlayTimeline_ReplaceOverlapFailsClosed(t *testing.T) {
	_, _, err := ResolveOverlayTimeline([]VideoSegmentV2{{AssetID: "base", FrameCount: 480}}, []Overlay{
		{ID: "a", AssetID: "a", StartFrame: 10, FrameCount: 20, Mode: "replace"},
		{ID: "b", AssetID: "b", StartFrame: 20, FrameCount: 20, Mode: "replace"},
	}, 24, 1)
	if err == nil {
		t.Fatal("expected overlapping replace windows to fail")
	}
}

func TestApplyPreparedOverlayFragments_ProducesSingleTrack(t *testing.T) {
	base := []VideoSegmentV2{{AssetID: "base", TimelineStartFrame: 0, FrameCount: 480}}
	windows, err := ResolveOverlayWindows([]Overlay{{ID: "a", AssetID: "raw-a", StartFrame: 120, FrameCount: 120, Mode: "composite", AudioMode: OverlayAudioPreserveFinal}}, 480)
	if err != nil {
		t.Fatalf("windows: %v", err)
	}
	got, err := ApplyPreparedOverlayFragments(base, windows, []PreparedOverlayFragment{{WindowID: windows[0].WindowID, AssetID: "chronon-a", ProfileID: CanonicalVideoProfileIDV1}}, 24, 1)
	if err != nil {
		t.Fatalf("fragments: %v", err)
	}
	if len(got) != 3 || got[1].AssetID != "chronon-a" || got[1].TimelineStartFrame != 120 || got[1].FrameCount != 120 {
		t.Fatalf("resolved fragments = %+v", got)
	}
}

func TestParseOverlaysCanonicalizesDriveAuthoringReference(t *testing.T) {
	overlays, err := ParseOverlays([]interface{}{map[string]interface{}{
		"id":          "overlay-01",
		"url":         "https://drive.google.com/file/d/drive-overlay-01/view?usp=drive_link",
		"start_frame": float64(120),
		"frame_count": float64(120),
		"mode":        "replace",
		"audio_mode":  OverlayAudioPreserveFinal,
	}})
	if err != nil {
		t.Fatalf("ParseOverlays: %v", err)
	}
	if len(overlays) != 1 {
		t.Fatalf("overlays = %#v", overlays)
	}
	if overlays[0].AssetID != "drive-overlay-01" || overlays[0].DriveFileID != "drive-overlay-01" || overlays[0].URL != "velox-drive://drive-overlay-01" {
		t.Fatalf("canonical overlay asset = %+v", overlays[0])
	}
}

func TestParseOverlaysPreservesAlreadyTypedValues(t *testing.T) {
	input := []Overlay{{
		ID: "typed-overlay", AssetID: "drive-typed", DriveFileID: "drive-typed",
		URL: "velox-drive://drive-typed", StartFrame: 120, FrameCount: 120,
		Mode: string(OverlayModeReplace), ZIndex: 10, AudioMode: OverlayAudioPreserveFinal,
	}}

	got, err := ParseOverlays(input)
	if err != nil {
		t.Fatalf("ParseOverlays: %v", err)
	}
	if len(got) != 1 || got[0].ID != input[0].ID || got[0].AssetID != input[0].AssetID || got[0].StartFrame != input[0].StartFrame || got[0].FrameCount != input[0].FrameCount || got[0].EndFrame != 240 {
		t.Fatalf("typed overlays = %#v, want normalized timing [120,240)", got)
	}
}
