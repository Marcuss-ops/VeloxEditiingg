package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
)

func TestCommonAssetResolverColdWarmCacheAcrossMediaKinds(t *testing.T) {
	t.Parallel()

	assets := map[string][]byte{
		"video-001":     []byte("video-bytes"),
		"voiceover-001": []byte("voiceover-bytes"),
		"music-001":     []byte("music-bytes"),
		"effect-001":    []byte("effect-bytes"),
		"subtitle-001":  []byte("[Script Info]\nTitle: captions\n"),
	}
	var mu sync.Mutex
	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetID := strings.TrimPrefix(r.URL.Path, "/api/v1/worker-assets/")
		mu.Lock()
		requests[assetID]++
		mu.Unlock()
		body, ok := assets[assetID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", mediaTypeForAsset(assetID))
		_, _ = w.Write(body)
	}))
	defer server.Close()

	workerDir := t.TempDir()
	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: server.URL, WorkDir: workerDir},
		apiClient: api.NewClient(server.URL),
	}

	payload := map[string]interface{}{
		"render_manifest": map[string]interface{}{
			"assets": []interface{}{
				assetEnvelope("video-001", "video", assets["video-001"]),
				assetEnvelope("voiceover-001", "audio", assets["voiceover-001"]),
				assetEnvelope("music-001", "audio", assets["music-001"]),
				assetEnvelope("effect-001", "audio", assets["effect-001"]),
				assetEnvelope("subtitle-001", "subtitle", assets["subtitle-001"]),
			},
			"tracks": []interface{}{
				map[string]interface{}{"kind": "video", "events": []interface{}{map[string]interface{}{"uri": "velox-asset://video-001"}}},
				map[string]interface{}{"kind": "voiceover", "events": []interface{}{map[string]interface{}{"uri": "velox-asset://voiceover-001"}}},
				map[string]interface{}{"kind": "music", "events": []interface{}{map[string]interface{}{"uri": "velox-asset://music-001"}}},
				map[string]interface{}{"kind": "sfx", "events": []interface{}{map[string]interface{}{"uri": "velox-asset://effect-001"}}},
				map[string]interface{}{"kind": "captions", "events": []interface{}{map[string]interface{}{"uri": "velox-asset://subtitle-001"}}},
			},
		},
	}
	before := mustJSON(t, payload)

	cold, err := w.resolveCommonAssetPayload(context.Background(), payload)
	if err != nil {
		t.Fatalf("cold-cache resolve: %v", err)
	}
	if got := mustJSON(t, payload); got != before {
		t.Fatal("common resolver mutated the source payload")
	}
	for assetID := range assets {
		if requests[assetID] != 1 {
			t.Errorf("cold request count for %s = %d, want 1", assetID, requests[assetID])
		}
	}
	assertResolvedManifestAssets(t, cold, assets, workerDir)

	warm, err := w.resolveCommonAssetPayload(context.Background(), payload)
	if err != nil {
		t.Fatalf("warm-cache resolve: %v", err)
	}
	assertResolvedManifestAssets(t, warm, assets, workerDir)
	for assetID := range assets {
		if requests[assetID] != 1 {
			t.Errorf("warm request count for %s = %d, want unchanged 1", assetID, requests[assetID])
		}
	}

	corruptID := "music-001"
	corruptDigest := sha256Hex(assets[corruptID])
	corruptPath, err := cachedAssetPath(w.assetCacheDir(), corruptID, corruptDigest, int64(len(assets[corruptID])))
	if err != nil || corruptPath == "" {
		t.Fatalf("find cached music asset: path=%q err=%v", corruptPath, err)
	}
	if err := os.WriteFile(corruptPath, bytes.Repeat([]byte("x"), len(assets[corruptID])), 0o644); err != nil {
		t.Fatalf("corrupt cached music asset: %v", err)
	}
	if _, err := w.resolveCommonAssetPayload(context.Background(), payload); err != nil {
		t.Fatalf("corrupt-cache repair: %v", err)
	}
	if requests[corruptID] != 2 {
		t.Fatalf("corrupt asset request count = %d, want 2", requests[corruptID])
	}
	for assetID, count := range requests {
		if assetID != corruptID && count != 1 {
			t.Errorf("unrelated asset %s request count = %d, want 1", assetID, count)
		}
	}
}

func TestCommonAssetResolverRewritesScenesJSONAndPreservesInput(t *testing.T) {
	body := []byte("scene-video-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	w := &Worker{config: &config.WorkerConfig{MasterURL: server.URL, WorkDir: t.TempDir()}, apiClient: api.NewClient(server.URL)}
	payload := map[string]interface{}{
		"scenes_json": `[ {"clip_link":"velox-asset://scene-json-001","sha256":"` + sha256Hex(body) + `","size_bytes":` + fmt.Sprintf("%d", len(body)) + `} ]`,
	}
	before := mustJSON(t, payload)
	resolved, err := w.resolveCommonAssetPayload(context.Background(), payload)
	if err != nil {
		t.Fatalf("resolve scenes_json: %v", err)
	}
	if mustJSON(t, payload) != before {
		t.Fatal("scenes_json resolver mutated the source payload")
	}
	encoded := resolved["scenes_json"].(string)
	if strings.Contains(encoded, "velox-asset://") || !strings.Contains(encoded, w.config.WorkDir) {
		t.Fatalf("scenes_json was not rewritten to a local path: %s", encoded)
	}
}

func TestCommonAssetResolverRejectsRawURLsLocalPathsAndIncompleteMetadata(t *testing.T) {
	w := &Worker{config: &config.WorkerConfig{MasterURL: "http://127.0.0.1:1", WorkDir: t.TempDir()}, apiClient: api.NewClient("http://127.0.0.1:1")}
	cases := []struct {
		name    string
		payload map[string]interface{}
	}{
		{"raw-url", map[string]interface{}{"music": "https://example.test/music.mp3"}},
		{"local-path", map[string]interface{}{"effects": []interface{}{t.TempDir() + "/effect.wav"}}},
		{"missing-integrity", map[string]interface{}{"subtitles": "velox-asset://subtitle-001"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := w.resolveCommonAssetPayload(context.Background(), tc.payload); err == nil {
				t.Fatal("invalid asset reference was accepted")
			}
		})
	}
}

func assetEnvelope(id, kind string, body []byte) map[string]interface{} {
	return map[string]interface{}{
		"id":         id,
		"uri":        "velox-asset://" + id,
		"kind":       kind,
		"sha256":     sha256Hex(body),
		"size_bytes": int64(len(body)),
	}
}

func assertResolvedManifestAssets(t *testing.T, payload map[string]interface{}, assets map[string][]byte, workDir string) {
	t.Helper()
	manifest := payload["render_manifest"].(map[string]interface{})
	resolvedAssets := manifest["assets"].([]interface{})
	for _, raw := range resolvedAssets {
		asset := raw.(map[string]interface{})
		uri := asset["uri"].(string)
		if strings.HasPrefix(uri, "velox-asset://") || !strings.HasPrefix(uri, workDir) {
			t.Fatalf("asset %v was not resolved to a worker-local path: %q", asset["id"], uri)
		}
		if got, err := os.ReadFile(uri); err != nil || !bytes.Equal(got, assets[asset["id"].(string)]) {
			t.Fatalf("resolved bytes for %s invalid: err=%v", asset["id"], err)
		}
	}
}

func mediaTypeForAsset(id string) string {
	switch id {
	case "video-001":
		return "video/mp4"
	case "subtitle-001":
		return "text/plain"
	default:
		return "audio/mpeg"
	}
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func mustJSON(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
