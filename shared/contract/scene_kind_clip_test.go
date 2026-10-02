package contract

import (
	"testing"
)

// TestFindKindClipAnomalies_FlagsTestimonyShapedScenes locks the regression
// behind job_e2adca259c034c1e: scenes declared kind="clip" with the asset in
// "stock" and no "clip" object must be reported (the worker compiles them
// as mute backgrounds).
func TestFindKindClipAnomalies_FlagsTestimonyShapedScenes(t *testing.T) {
	scenes := []map[string]interface{}{
		{
			"scene_id":         "scene-0062",
			"index":            float64(62),
			"kind":             "clip",
			"text":             "Diga literalmente: ...",
			"duration_seconds": float64(6.36),
			"stock": []interface{}{map[string]interface{}{
				"asset_id": "cliprender_c6b0b4621dba07c8a8951f7e",
				"url":      "velox-drive://1lHGq4PcszySXm9E_Df4-to3byVGlNQfC",
			}},
		},
		{
			"scene_id":         "scene-0002",
			"kind":             "stock",
			"text":             "Narration background",
			"duration_seconds": float64(5),
			"stock": []interface{}{map[string]interface{}{
				"asset_id": "planner:3538656664643330:0",
				"url":      "velox-drive://1qTw9355EFMLdGD2y2x0xH2vdOzHaHWmD",
			}},
		},
		{
			"scene_id":         "scene-0000",
			"kind":             "clip",
			"text":             "Opening scene",
			"duration_seconds": float64(4.5),
			"clip": map[string]interface{}{
				"asset_id": "clip-001",
				"url":      "velox-asset://clips/opening.mp4",
			},
		},
	}
	got := FindKindClipAnomalies(scenes)
	if len(got) != 1 {
		t.Fatalf("anomalies = %v, want exactly one", got)
	}
	if got[0].SceneID != "scene-0062" || got[0].Index != 0 {
		t.Fatalf("anomaly = %+v, want scene-0062 at index 0", got[0])
	}
	if got[0].Reason != SceneKindClipAnomalyReasonKindClipWithoutClipAsset {
		t.Fatalf("reason = %q, want %q", got[0].Reason, SceneKindClipAnomalyReasonKindClipWithoutClipAsset)
	}
}

// TestFindKindClipAnomalies_AcceptsStockBackgrounds ensures genuine
// background scenes (kind=stock, or kind=clip WITH a clip asset) stay
// silent: the soft-deprecation must not flag the shapes production
// relies on.
func TestFindKindClipAnomalies_AcceptsStockBackgrounds(t *testing.T) {
	scenes := []map[string]interface{}{
		{"scene_id": "s-stock", "kind": "stock", "stock": map[string]interface{}{"url": "velox-drive://stock-1"}},
		{"scene_id": "s-clip", "kind": "clip", "clip": map[string]interface{}{"drive_file_id": "drive-1"}},
		{"scene_id": "s-case", "kind": " Clip ", "clip": map[string]interface{}{"url": "velox-drive://c"}},
		{"scene_id": "s-nokind", "text": "no kind at all"},
		nil,
	}
	if got := FindKindClipAnomalies(scenes); len(got) != 0 {
		t.Fatalf("anomalies = %v, want none", got)
	}
}

// TestFindKindClipAnomaliesInPayload_AcceptsBothSceneForms pins the intake
// entry point: the "scenes" list form and the "scenes_json" encoded form
// report identically.
func TestFindKindClipAnomaliesInPayload_AcceptsBothSceneForms(t *testing.T) {
	scene := map[string]interface{}{
		"scene_id": "scene-0257", "kind": "clip", "text": "t",
		"stock": map[string]interface{}{"url": "velox-drive://x"},
	}
	listPayload := map[string]interface{}{"scenes": []interface{}{scene}}
	if got := FindKindClipAnomaliesInPayload(listPayload); len(got) != 1 {
		t.Fatalf("list form anomalies = %v, want one", got)
	}
	jsonPayload := map[string]interface{}{
		"scenes_json": `[{"scene_id":"scene-0257","kind":"clip","stock":{"url":"velox-drive://x"}}]`,
	}
	got := FindKindClipAnomaliesInPayload(jsonPayload)
	if len(got) != 1 || got[0].SceneID != "scene-0257" {
		t.Fatalf("scenes_json form anomalies = %v, want scene-0257", got)
	}
	if got := FindKindClipAnomaliesInPayload(nil); len(got) != 0 {
		t.Fatalf("nil payload anomalies = %v, want none", got)
	}
	if got := FindKindClipAnomaliesInPayload(map[string]interface{}{}); len(got) != 0 {
		t.Fatalf("empty payload anomalies = %v, want none", got)
	}
}
