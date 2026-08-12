package contract

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func testCompiledRenderPlanV2() *CompiledRenderPlanV2 {
	return &CompiledRenderPlanV2{
		PlanVersion:      CompiledPlanVersionV2,
		TimelineRevision: 7,
		TimelineSHA256:   "timeline-sha",
		DurationUS:       60_000_000,
		Output: OutputContractV2{
			Container:   "mp4",
			VideoCodec:  "h264",
			Width:       1920,
			Height:      1080,
			FPSNum:      30,
			FPSDen:      1,
			PixelFormat: "yuv420p",
		},
		FinalAudio: FinalAudioV2{
			Mode:             AudioModeFinalAudioCopy,
			AssetID:          "audio-master",
			SHA256:           strings.Repeat("a", 64),
			SizeBytes:        8_123_456,
			Codec:            "aac",
			SampleRateHz:     48_000,
			Channels:         2,
			DurationUS:       60_000_000,
			TimelineRevision: 7,
			TimelineSHA256:   "timeline-sha",
		},
		VideoTracks: []VideoTrackV2{{
			TrackID: "main",
			Segments: []VideoSegmentV2{{
				SegmentID:          "comedian_a",
				AssetID:            "video-a",
				SHA256:             strings.Repeat("b", 64),
				TimelineStartFrame: 372,
				FrameCount:         168,
				SourceInUS:         33_200_000,
				SourceDurationUS:   5_600_000,
			}},
		}},
		// Deliberately reverse the canonical order: assets are a set and must
		// be sorted by AssetID without changing the caller's slice.
		Assets: []AssetRefV2{
			{
				AssetID:    "video-a",
				SHA256:     strings.Repeat("b", 64),
				SizeBytes:  123,
				Kind:       "video",
				MIME:       "video/mp4",
				DurationUS: 5_600_000,
				Width:      1920,
				Height:     1080,
			},
			{
				AssetID:    "audio-master",
				SHA256:     strings.Repeat("a", 64),
				SizeBytes:  8_123_456,
				Kind:       "final_audio",
				MIME:       "audio/mp4",
				DurationUS: 60_000_000,
			},
		},
	}
}

func TestCompiledRenderPlanV2_CanonicalJSONIsExact(t *testing.T) {
	plan := testCompiledRenderPlanV2()
	got, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}

	want := fmt.Sprintf(
		`{"plan_version":2,"timeline_revision":7,"timeline_sha256":"timeline-sha","duration_us":60000000,"output":{"container":"mp4","video_codec":"h264","width":1920,"height":1080,"fps_num":30,"fps_den":1,"pixel_format":"yuv420p"},"final_audio":{"mode":"FINAL_AUDIO_COPY","asset_id":"audio-master","sha256":"%s","size_bytes":8123456,"codec":"aac","sample_rate_hz":48000,"channels":2,"duration_us":60000000,"timeline_revision":7,"timeline_sha256":"timeline-sha"},"video_tracks":[{"track_id":"main","segments":[{"segment_id":"comedian_a","asset_id":"video-a","sha256":"%s","timeline_start_frame":372,"frame_count":168,"source_in_us":33200000,"source_duration_us":5600000}]}],"assets":[{"asset_id":"audio-master","sha256":"%s","size_bytes":8123456,"kind":"final_audio","mime":"audio/mp4","duration_us":60000000},{"asset_id":"video-a","sha256":"%s","size_bytes":123,"kind":"video","mime":"video/mp4","duration_us":5600000,"width":1920,"height":1080}]}`,
		strings.Repeat("a", 64),
		strings.Repeat("b", 64),
		strings.Repeat("a", 64),
		strings.Repeat("b", 64),
	)
	if string(got) != want {
		t.Fatalf("canonical JSON mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestCompiledRenderPlanV2_CanonicalizationSortsAssetsWithoutMutation(t *testing.T) {
	plan := testCompiledRenderPlanV2()
	if plan.Assets[0].AssetID != "video-a" {
		t.Fatalf("test fixture must start in non-canonical order")
	}

	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if !strings.Contains(string(canonical), `"assets":[{"asset_id":"audio-master"`) {
		t.Fatalf("assets were not sorted by asset_id: %s", canonical)
	}
	if plan.Assets[0].AssetID != "video-a" {
		t.Fatalf("CanonicalJSON mutated caller assets: first asset = %q", plan.Assets[0].AssetID)
	}
}

func TestCompiledRenderPlanV2_PlanSHA256MatchesCanonicalBytes(t *testing.T) {
	plan := testCompiledRenderPlanV2()
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	got, err := plan.PlanSHA256()
	if err != nil {
		t.Fatalf("PlanSHA256: %v", err)
	}
	if want := HashCompiledPlanV2(canonical); got != want {
		t.Fatalf("PlanSHA256 = %q, HashCompiledPlanV2 = %q", got, want)
	}
	if len(got) != 64 {
		t.Fatalf("SHA256 length = %d, want 64", len(got))
	}
}

func TestCompiledRenderPlanV2_NormalizesNilSlicesToJSONArrays(t *testing.T) {
	plan := &CompiledRenderPlanV2{
		PlanVersion:      CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   "timeline-sha",
		Output:           OutputContractV2{},
		FinalAudio:       FinalAudioV2{},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	text := string(canonical)
	if strings.Contains(text, `"video_tracks":null`) || strings.Contains(text, `"assets":null`) {
		t.Fatalf("nil slices must be canonicalized as arrays: %s", text)
	}
	if !strings.Contains(text, `"video_tracks":[]`) || !strings.Contains(text, `"assets":[]`) {
		t.Fatalf("canonical arrays missing: %s", text)
	}
}

func TestCompiledRenderPlanV2_JSONRoundTrip(t *testing.T) {
	plan := testCompiledRenderPlanV2()
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}

	var decoded CompiledRenderPlanV2
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	decodedCanonical, err := decoded.CanonicalJSON()
	if err != nil {
		t.Fatalf("decoded CanonicalJSON: %v", err)
	}
	if string(decodedCanonical) != string(canonical) {
		t.Fatalf("round-trip changed canonical JSON:\n got: %s\nwant: %s", decodedCanonical, canonical)
	}
	if decoded.PlanVersion != CompiledPlanVersionV2 || decoded.FinalAudio.Mode != AudioModeFinalAudioCopy {
		t.Fatalf("decoded contract identity = %+v", decoded)
	}
}

func TestCompiledRenderPlanV2_DuplicateAssetIDsHaveDeterministicTieBreak(t *testing.T) {
	first := testCompiledRenderPlanV2()
	first.Assets = append(first.Assets, AssetRefV2{
		AssetID: "video-a", SHA256: "alternate", SizeBytes: 999, Kind: "video",
	})
	second := testCompiledRenderPlanV2()
	second.Assets = append([]AssetRefV2{{
		AssetID: "video-a", SHA256: "alternate", SizeBytes: 999, Kind: "video",
	}}, second.Assets...)

	firstJSON, err := first.CanonicalJSON()
	if err != nil {
		t.Fatalf("first CanonicalJSON: %v", err)
	}
	secondJSON, err := second.CanonicalJSON()
	if err != nil {
		t.Fatalf("second CanonicalJSON: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("duplicate asset ordering is not deterministic:\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
}

func TestCompiledRenderPlanV2_DoesNotCarryJobOrAttemptIdentity(t *testing.T) {
	canonical, err := testCompiledRenderPlanV2().CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(canonical, &document); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, key := range []string{"job_id", "attempt_id"} {
		if _, ok := document[key]; ok {
			t.Fatalf("V2 canonical document must not carry %s: %s", key, canonical)
		}
	}
}
