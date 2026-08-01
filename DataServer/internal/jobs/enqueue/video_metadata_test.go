package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"velox-shared/contract/rendermanifest"
)

func TestNormalizeSceneVideoPayloadStripsPublicationMetadataAndPreservesTechnicalRenderMetadata(t *testing.T) {
	payload := map[string]interface{}{
		"video_name": "Renderer job name",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/1.png"},
		},
		"voiceover_paths": []interface{}{"https://example.com/voice.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "social-main", "retry_budget": 3},
		},
		"video_metadata": map[string]interface{}{
			"title":             "Published title",
			"description":       "Published description",
			"tags":              []interface{}{"velox", "test"},
			"privacy_status":    "private",
			"publish_at":        "2026-07-20T18:00:00+02:00",
			"localizations":     map[string]interface{}{"it": map[string]interface{}{"title": "Titolo"}},
			"metadata_override": map[string]interface{}{"title": "Override"},
			"width":             1920,
			"height":            1080,
			"fps_num":           30,
			"fps_den":           1,
			"pixel_format":      "yuv420p",
		},
	}

	out, err := normalizeSceneVideoPayload(payload)
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	metadata, ok := out["video_metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("video_metadata type = %T, want technical map[string]interface{}", out["video_metadata"])
	}
	if metadata["width"] != 1920 || metadata["height"] != 1080 || metadata["pixel_format"] != "yuv420p" {
		t.Fatalf("technical render metadata was not preserved: %#v", metadata)
	}
	for _, key := range []string{"title", "description", "tags", "privacy_status", "publish_at", "localizations", "metadata_override"} {
		if _, present := metadata[key]; present {
			t.Fatalf("publication metadata %q leaked into normalized renderer metadata: %#v", key, metadata[key])
		}
	}

	plan, ok := out["delivery_plan"].([]interface{})
	if !ok || len(plan) != 1 {
		t.Fatalf("delivery_plan = %#v", out["delivery_plan"])
	}
	planItem, ok := plan[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery plan item = %#v", plan[0])
	}
	if planItem["destination_id"] != "social-main" || planItem["retry_budget"] != 3 {
		t.Fatalf("legacy routing fields changed: %#v", planItem)
	}
	if _, present := planItem["metadata"]; present {
		t.Fatalf("publication metadata was attached to delivery plan: %#v", planItem["metadata"])
	}
}

func TestNormalizeSceneVideoPayloadRejectsNullRenderManifest(t *testing.T) {
	_, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":      "Null manifest",
		"script_text":     "Null manifest script",
		"render_manifest": nil,
	})
	if err == nil || !strings.Contains(err.Error(), "render_manifest") {
		t.Fatalf("want render_manifest type error, got %v", err)
	}
}

func TestNormalizeSceneVideoPayloadCompilesStrictRenderManifest(t *testing.T) {
	payload := map[string]interface{}{
		"video_name":      "Strict render",
		"script_text":     "Strict render script",
		"render_manifest": strictNormalizerManifest(),
	}

	out, err := normalizeSceneVideoPayloadContext(context.Background(), payload)
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayloadContext: %v", err)
	}
	planJSON, ok := out["render_plan_json"].(string)
	if !ok || planJSON == "" {
		t.Fatalf("render_plan_json = %#v, want canonical JSON", out["render_plan_json"])
	}
	if err := rendermanifest.ValidateJSON([]byte(planJSON)); err != nil {
		t.Fatalf("compiled render plan is invalid: %v", err)
	}
	sum := sha256.Sum256([]byte(planJSON))
	wantHash := hex.EncodeToString(sum[:])
	if got := out["render_plan_sha256"]; got != wantHash {
		t.Fatalf("render_plan_sha256 = %v, want %s", got, wantHash)
	}
	if _, present := out["layers"]; present {
		t.Fatalf("raw top-level layers leaked beside strict compiled plan: %#v", out["layers"])
	}
}

func TestNormalizeSceneVideoPayloadRejectsCanceledStrictCompile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := normalizeSceneVideoPayloadContext(ctx, map[string]interface{}{
		"video_name":      "Canceled render",
		"script_text":     "Canceled render script",
		"render_manifest": strictNormalizerManifest(),
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled strict compile error, got %v", err)
	}
}

func strictNormalizerManifest() map[string]interface{} {
	const sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	return map[string]interface{}{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []interface{}{
			map[string]interface{}{"id": "clip", "uri": "velox-asset://clip", "kind": "video", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
			map[string]interface{}{"id": "voiceover", "uri": "velox-asset://voiceover", "kind": "audio", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
		},
		"tracks": []interface{}{
			map[string]interface{}{"id": "video", "kind": "video", "events": []interface{}{map[string]interface{}{"asset_id": "clip", "timeline_start_ms": 0, "duration_ms": 1000}}},
			map[string]interface{}{"id": "voiceover", "kind": "voiceover", "events": []interface{}{map[string]interface{}{"asset_id": "voiceover", "timeline_start_ms": 0, "duration_ms": 1000}}},
		},
		"output": map[string]interface{}{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
}

func TestNormalizeSceneVideoPayloadRejectsInvalidVideoMetadata(t *testing.T) {
	payload := map[string]interface{}{
		"video_name": "Test video",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1"},
		},
		"voiceover_paths": []interface{}{"voice.mp3"},
		"video_metadata": map[string]interface{}{
			"privacy_status": "visible-to-everyone",
		},
	}

	_, err := normalizeSceneVideoPayload(payload)
	if err == nil || !strings.Contains(err.Error(), "video_metadata.privacy_status") {
		t.Fatalf("want privacy validation error, got %v", err)
	}
}
