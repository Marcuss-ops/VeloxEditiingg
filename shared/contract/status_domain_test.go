package contract

import (
	"encoding/json"
	"testing"

	"velox-shared/contract/rendermanifest"
)

func TestNewJobPayloadV2CheckedRejectsLifecycleStatuses(t *testing.T) {
	for _, rawStatus := range []string{"SUCCEEDED", "PUBLISHED", "COMPLETED"} {
		if _, err := NewJobPayloadV2Checked(map[string]any{"status": rawStatus}); err == nil {
			t.Fatalf("checked canonical writer accepted lifecycle status %q", rawStatus)
		}
	}
}

func TestNewJobPayloadV2CheckedUsesTypedRenderManifestBoundary(t *testing.T) {
	raw := map[string]any{
		"render_manifest": map[string]any{
			"schema": "velox.render-manifest.v1",
			"canvas": map[string]any{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
			"assets": []any{
				map[string]any{"id": "clip", "uri": "velox-asset://clip", "kind": "video", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "size_bytes": 1, "duration_ms": 1000},
				map[string]any{"id": "voice", "uri": "velox-asset://voice", "kind": "audio", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "size_bytes": 1, "duration_ms": 1000},
			},
			"tracks": []any{
				map[string]any{"id": "video", "kind": "video", "events": []any{map[string]any{"asset_id": "clip", "timeline_start_ms": 0, "duration_ms": 1000}}},
				map[string]any{"id": "voice", "kind": "voiceover", "events": []any{map[string]any{"asset_id": "voice", "timeline_start_ms": 0, "duration_ms": 1000}}},
			},
			"output": map[string]any{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
		},
	}
	payload, err := NewJobPayloadV2Checked(raw)
	if err != nil {
		t.Fatalf("checked payload rejected valid manifest: %v", err)
	}
	manifest, err := payload.TypedRenderManifest()
	if err != nil || manifest == nil {
		t.Fatalf("TypedRenderManifest() = %#v, %v", manifest, err)
	}
	if manifest.Schema != rendermanifest.Schema {
		t.Fatalf("manifest schema = %q, want %q", manifest.Schema, rendermanifest.Schema)
	}
}

func TestNewJobPayloadV2CheckedRejectsInvalidRenderManifest(t *testing.T) {
	if _, err := NewJobPayloadV2Checked(map[string]any{
		"render_manifest": map[string]any{"schema": "unknown"},
	}); err == nil {
		t.Fatal("checked payload accepted invalid render_manifest")
	}
}

func TestJobPayloadV2DirectJSONReadsLegacyOverloadedStatus(t *testing.T) {
	var payload JobPayloadV2
	if err := json.Unmarshal([]byte(`{"job_id":"legacy-direct","status":"SUCCEEDED"}`), &payload); err != nil {
		t.Fatalf("direct legacy JSON should remain readable: %v", err)
	}
	if payload.Status != InputAssemblyStatus("SUCCEEDED") {
		t.Fatalf("direct legacy status = %q, want preserved raw value", payload.Status)
	}

}

func TestJobPayloadV2FromJSONReadsLegacyOverloadedStatus(t *testing.T) {
	payload, err := JobPayloadV2FromJSON([]byte(`{"job_id":"legacy-1","status":"SUCCEEDED"}`))
	if err != nil {
		t.Fatalf("legacy payload should remain readable: %v", err)
	}
	if payload.Status != InputAssemblyStatus("SUCCEEDED") {
		t.Fatalf("legacy status = %q, want preserved raw value", payload.Status)
	}
	if _, err := payload.ToMap(); err == nil {
		t.Fatal("legacy lifecycle status must not be re-emitted by canonical ToMap")
	}
}

func TestInputAssemblyStatusWireCompatibility(t *testing.T) {
	pending, err := InputAssemblyPending.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal pending: %v", err)
	}
	if string(pending) != `"PENDING"` {
		t.Fatalf("pending wire value = %s, want PENDING", pending)
	}
	completed, err := InputAssemblyCompleted.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal completed: %v", err)
	}
	if string(completed) != `"completed"` {
		t.Fatalf("completed wire value = %s, want completed", completed)
	}
}
