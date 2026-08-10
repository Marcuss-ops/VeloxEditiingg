package assetref

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestExtractAssetKeys_LegacyClipSlots exercises the canonical walker
// over the legacy scene slots (clip_link / clip_links / video_url /
// source_url) that used to be covered by the dedicated Drive extractor.
func TestExtractAssetKeys_LegacyClipSlots(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want []string // unsorted in source; we compare as SET
	}{
		{
			name: "empty payload returns empty set",
			in:   ``,
			want: nil,
		},
		{
			name: "json null returns empty set",
			in:   `null`,
			want: nil,
		},
		{
			name: "json without scenes key returns empty set",
			in:   `{"title":"foo","created_at":"2026-07-27T00:00:00Z"}`,
			want: nil,
		},
		{
			name: "single scene with clip_link",
			in:   `{"scenes":[{"clip_link":"https://drive.google.com/file/d/ABC123/view"}]}`,
			want: []string{"ABC123"},
		},
		{
			name: "single scene with clip_links slice",
			in: `{"scenes":[{"clip_links":[
				"https://drive.google.com/file/d/X1/view",
				"https://drive.google.com/file/d/X2/view"
			]}]}`,
			want: []string{"X1", "X2"},
		},
		{
			name: "single scene all four fields combined",
			in:   `{"scenes":[{"clip_link":"https://drive.google.com/uc?id=A1","clip_links":["https://drive.google.com/file/d/A2/view"],"video_url":"https://drive.google.com/file/d/A3/preview","source_url":"https://drive.google.com/open?id=A4"}]}`,
			want: []string{"A1", "A2", "A3", "A4"},
		},
		{
			name: "duplicate url across fields within one scene dedupes",
			in:   `{"scenes":[{"clip_link":"https://drive.google.com/file/d/SAME/view","clip_links":["https://drive.google.com/uc?id=SAME","https://drive.google.com/open?id=SAME"]}]}`,
			want: []string{"SAME"},
		},
		{
			name: "duplicate id across scenes dedupes",
			in: `{"scenes":[
				{"clip_link":"https://drive.google.com/file/d/SAME/view"},
				{"clip_links":["https://drive.google.com/uc?id=SAME"]}
			]}`,
			want: []string{"SAME"},
		},
		{
			name: "different URL forms for the same file dedupe to one id",
			in: `{"scenes":[{"clip_links":[
				"https://drive.google.com/file/d/NORM/view",
				"https://drive.google.com/file/d/NORM/view?usp=sharing",
				"https://drive.google.com/uc?id=NORM",
				"https://drive.google.com/u/0/uc?id=NORM&export=download"
			]}]}`,
			want: []string{"NORM"},
		},
		{
			name: "non-Drive URLs silently dropped",
			in:   `{"scenes":[{"clip_link":"https://example.com/foo.mp4","clip_links":["https://drive.google.com/file/d/VALID/view"]}]}`,
			want: []string{"VALID"},
		},
		{
			name: "non-Drive https url across multiple slots",
			in: `{"scenes":[
				{"clip_link":""},
				{"clip_links":["https://www.youtube.com/watch?v=xyz","http://internal/foo.mp4"]},
				{"video_url":"https://drive.google.com/file/d/MULTI1/view"},
				{"source_url":"https://drive.google.com/uc?id=MULTI2"}
			]}`,
			want: []string{"MULTI1", "MULTI2"},
		},
		{
			name: "folder URLs are rejected (Drive URL but not a file)",
			in:   `{"scenes":[{"clip_link":"https://drive.google.com/drive/folders/abc1234567","clip_links":["https://drive.google.com/file/d/KEEPID/view"]}]}`,
			want: []string{"KEEPID"},
		},
		{
			name: "realistic long id with usp query",
			in:   `{"scenes":[{"clip_link":"https://drive.google.com/file/d/19m3s1-_guIYqEZE2Ywy77s_mJZMR7686/view?usp=drive_link","source_url":"https://drive.google.com/file/d/1S6NiFUeLEAQwtGZISX96nRsv6sv_p7f_/view"}]}`,
			want: []string{"19m3s1-_guIYqEZE2Ywy77s_mJZMR7686", "1S6NiFUeLEAQwtGZISX96nRsv6sv_p7f_"},
		},
		{
			name: "invalid JSON returns empty set (no panic)",
			in:   `{"scenes":[ this is not valid json`,
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSet := ExtractAssetKeys(json.RawMessage(tc.in))

			// Build wantSet as a NON-NIL map so reflect.DeepEqual matches
			// the extractor's always-non-nil return contract.
			wantSet := make(map[string]struct{}, len(tc.want))
			for _, id := range tc.want {
				wantSet[id] = struct{}{}
			}
			if !reflect.DeepEqual(gotSet, wantSet) {
				t.Errorf("ExtractAssetKeys set mismatch:\n got  = %v\n want = %v", gotSet, wantSet)
			}
		})
	}
}

// TestExtractAssetKeys_NeverReturnsNil is a regression guard: the
// snapshot service and worker lease path range over the result without
// nil-checking. If ExtractAssetKeys ever returns nil on the empty path,
// those range loops panic.
func TestExtractAssetKeys_NeverReturnsNil(t *testing.T) {
	t.Parallel()
	if got := ExtractAssetKeys(nil); got == nil {
		t.Error("ExtractAssetKeys(nil) returned nil; must always return non-nil map")
	}
	if got := ExtractAssetKeys([]byte("")); got == nil {
		t.Error("ExtractAssetKeys(empty) returned nil; must always return non-nil map")
	}
	if got := ExtractAssetKeys([]byte(`{"scenes":[]}`)); got == nil {
		t.Error("ExtractAssetKeys(empty scenes) returned nil; must always return non-nil map")
	}
}

func TestExtractAssetKeys_CanonicalClipStockVoiceover(t *testing.T) {
	payload := []byte(`{"scenes":[{"clip":{"asset_id":"clip-01","url":"velox-asset://clip-01"},"stock":[{"asset_id":"stock-01","url":"velox-asset://stock-01"}],"voiceover":{"asset_id":"voice-01","url":"velox-asset://voice-01"}}]}`)
	got := ExtractAssetKeys(payload)
	for _, key := range []string{"clip-01", "stock-01", "voice-01"} {
		if _, ok := got[key]; !ok {
			t.Errorf("missing canonical asset key %q in %v", key, got)
		}
	}
}

func TestExtractAssetKeys_DeferredDriveWireScheme(t *testing.T) {
	payload := []byte(`{"scenes":[{"clip":{"asset_id":"drive-file-123456","url":"velox-drive://drive-file-123456"}}]}`)
	got := ExtractAssetKeys(payload)
	if _, ok := got["drive-file-123456"]; !ok {
		t.Errorf("missing deferred drive key %q in %v", "drive-file-123456", got)
	}
}
