package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"velox-server/internal/config"
)

func TestResolveRenderManifestRef_SubstitutesCanonicalRequest(t *testing.T) {
	t.Parallel()

	body := mustRenderManifestBytes(t, testRenderManifest())
	rawSHA := sha256.Sum256(body)
	rawHex := hex.EncodeToString(rawSHA[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	host := strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]
	h := &Handlers{cfg: &config.Config{AllowedExternalDomains: []string{host}}}
	req := SubmitJobRequest{
		IdempotencyKey: "pg_20260728_manifest_001",
		ManifestRef: &SubmitManifestRef{
			SchemaVersion: "velox.render-manifest.v1",
			URL:           srv.URL,
			SHA256:        rawHex,
		},
	}

	resolved, vErr := h.ResolveRenderManifestRef(context.Background(), req)
	if vErr != nil {
		t.Fatalf("ResolveRenderManifestRef: %v details=%v", vErr, vErr.Details)
	}
	if resolved.ManifestRef != nil {
		t.Fatal("resolved ManifestRef must be nil after substitution")
	}
	if resolved.VideoName != "Manifest Test Video" {
		t.Fatalf("VideoName = %q", resolved.VideoName)
	}
	if resolved.ScriptText != "Manifest script text." {
		t.Fatalf("ScriptText = %q", resolved.ScriptText)
	}
	if len(resolved.Scenes) != 1 {
		t.Fatalf("Scenes len = %d", len(resolved.Scenes))
	}
	scene := resolved.Scenes[0]
	if scene.SceneID != "scene-0" || scene.Index != 0 || scene.Kind != "intro" {
		t.Fatalf("scene identity = %#v", scene)
	}
	if scene.DurationSeconds != 7.2 {
		t.Fatalf("DurationSeconds = %v", scene.DurationSeconds)
	}
	if scene.Clip == nil || scene.Clip.URL != "velox-asset://clips/clip-0.mp4" || scene.Clip.StartMS != 0 || scene.Clip.EndMS != 7200 {
		t.Fatalf("Clip = %#v", scene.Clip)
	}
	if scene.Voiceover == nil || scene.Voiceover.URL != "velox-asset://voiceovers/scene-0.mp3" {
		t.Fatalf("Voiceover = %#v", scene.Voiceover)
	}
	if scene.Subtitles == nil || scene.Subtitles.Format != "ass" || scene.Subtitles.URL != "velox-asset://subtitles/scene-0.ass" {
		t.Fatalf("Subtitles = %#v", scene.Subtitles)
	}
	if len(resolved.DeliveryPlan) != 1 || resolved.DeliveryPlan[0].DestinationID != "drive" {
		t.Fatalf("DeliveryPlan = %#v", resolved.DeliveryPlan)
	}
	if len(resolved.AudioTracks) != 1 {
		t.Fatalf("AudioTracks len = %d; want 1", len(resolved.AudioTracks))
	}
	at := resolved.AudioTracks[0]
	if at.Role != "background_music" || at.Volume != 0.15 || at.SourceURL != "velox-asset://music/bgm-test-001.mp3" {
		t.Fatalf("AudioTracks[0] = %#v", at)
	}
	if resolved.ResolvedManifest == nil || resolved.ResolvedManifestRef == nil || resolved.ResolvedManifestSHA256 != rawHex {
		t.Fatalf("resolved manifest snapshot missing: manifest=%v ref=%v sha=%q", resolved.ResolvedManifest, resolved.ResolvedManifestRef, resolved.ResolvedManifestSHA256)
	}

	if vErr, bad := ValidateSubmitJobRequest(resolved); bad {
		t.Fatalf("resolved request must pass SubmitJob validation: %v details=%v", vErr, vErr.Details)
	}
}

func TestResolveRenderManifestRef_RejectsRawSHA256Mismatch(t *testing.T) {
	t.Parallel()

	body := mustRenderManifestBytes(t, testRenderManifest())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	host := strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]
	h := &Handlers{cfg: &config.Config{AllowedExternalDomains: []string{host}}}
	req := SubmitJobRequest{
		IdempotencyKey: "pg_20260728_manifest_bad_sha",
		ManifestRef: &SubmitManifestRef{
			SchemaVersion: "velox.render-manifest.v1",
			URL:           srv.URL,
			SHA256:        strings.Repeat("0", 64),
		},
	}

	_, vErr := h.ResolveRenderManifestRef(context.Background(), req)
	if vErr == nil {
		t.Fatal("expected validation error")
	}
	found := false
	for _, d := range vErr.Details {
		if d["path"] == "manifest_ref.sha256" && d["issue"] == "mismatch" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected manifest_ref.sha256 mismatch detail, got %#v", vErr.Details)
	}
}

func testRenderManifest() map[string]interface{} {
	return map[string]interface{}{
		"schema_version": "velox.render-manifest.v1",
		"manifest_id":    "pg_test_manifest_001",
		"created_at":     "2026-07-28T14:30:00Z",
		"source": map[string]interface{}{
			"provider":           "pipelinegen",
			"pipelinegen_job_id": "pg_job_001",
			"generation_schema":  1,
		},
		"video": map[string]interface{}{
			"name":          "Manifest Test Video",
			"language":      "en",
			"width":         1920,
			"height":        1080,
			"fps":           30,
			"output_format": "mp4",
		},
		"script": map[string]interface{}{
			"text":           "Manifest script text.",
			"google_doc_url": "https://docs.google.com/document/d/test/edit",
			"language":       "en",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id":    "scene-0",
				"index":       0,
				"kind":        "intro",
				"text":        "Scene zero.",
				"duration_ms": 7200,
				"clip": map[string]interface{}{
					"asset_id":    "clip_0",
					"url":         "velox-asset://clips/clip-0.mp4",
					"sha256":      strings.Repeat("a", 64),
					"start_ms":    0,
					"end_ms":      7200,
					"duration_ms": 7200,
				},
				"voiceover": map[string]interface{}{
					"asset_id":      "voiceover_0",
					"url":           "velox-asset://voiceovers/scene-0.mp3",
					"sha256":        strings.Repeat("b", 64),
					"duration_ms":   7190,
					"language":      "en",
					"drive_file_id": "drive_vo_0",
				},
				"subtitles": map[string]interface{}{
					"asset_id": "subtitles_0",
					"format":   "ass",
					"url":      "velox-asset://subtitles/scene-0.ass",
					"sha256":   strings.Repeat("c", 64),
					"language": "en",
				},
			},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive",
				"priority":       1,
				"retry_budget":   3,
				"metadata": map[string]interface{}{
					"folder_id": "drive_folder_001",
				},
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"asset_id":           "bgm-test-001",
				"source_url":         "velox-asset://music/bgm-test-001.mp3",
				"role":               "background_music",
				"volume":             0.15,
				"start_time_offset":  0,
				"duration_seconds":   12.0,
			},
		},
		"integrity": map[string]interface{}{
			"algorithm":         "sha256",
			"manifest_sha256":   "",
			"scene_count":       1,
			"total_duration_ms": 7200,
		},
	}
}

func mustRenderManifestBytes(t *testing.T, manifest map[string]interface{}) []byte {
	t.Helper()
	sum, err := canonicalManifestIntegritySHA256(manifest)
	if err != nil {
		t.Fatalf("canonicalManifestIntegritySHA256: %v", err)
	}
	manifest["integrity"].(map[string]interface{})["manifest_sha256"] = sum
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return body
}
