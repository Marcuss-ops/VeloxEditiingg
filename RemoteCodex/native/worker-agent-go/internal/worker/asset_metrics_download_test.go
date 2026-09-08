package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
)

func TestDownloadVeloxAssetWithSHA_ReportsMissHitAndCorruptRedownload(t *testing.T) {
	assetID := "asset-report-001"
	assetBytes := []byte("ID3 per-asset cache report")
	digest := sha256.Sum256(assetBytes)
	expectedSHA := hex.EncodeToString(digest[:])
	requestCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path != "/api/v1/agent/assets/"+assetID {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = io.Copy(w, bytes.NewReader(assetBytes))
	}))
	defer srv.Close()

	workerDir := t.TempDir()
	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: workerDir},
		apiClient: api.NewClient(srv.URL),
	}
	tracker := &assetOperationTracker{}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	expectedSizeBytes := int64(len(assetBytes))

	path, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, expectedSizeBytes)
	if err != nil {
		t.Fatalf("cold resolve: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cached path %q: %v", path, err)
	}

	// A separate cache entry must survive repair of the corrupted asset.
	sentinelPath := filepath.Join(w.assetCacheDir(), "unrelated-asset.mp3")
	if err := os.WriteFile(sentinelPath, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write unrelated cache entry: %v", err)
	}

	pathAgain, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, expectedSizeBytes)
	if err != nil {
		t.Fatalf("warm resolve: %v", err)
	}
	if pathAgain != path {
		t.Fatalf("warm path = %q, want %q", pathAgain, path)
	}

	corruptBytes := bytes.Repeat([]byte{'x'}, len(assetBytes))
	if bytes.Equal(corruptBytes, assetBytes) {
		t.Fatal("test corruption must differ from the valid payload")
	}
	// Promoted blobs are chmod 0444 (immutable from the normal worker); to
	// simulate out-of-band corruption restore write permission first.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("make cache entry writable for corruption: %v", err)
	}
	if err := os.WriteFile(path, corruptBytes, 0o644); err != nil {
		t.Fatalf("corrupt cache entry: %v", err)
	}
	pathAfterCorruption, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, expectedSizeBytes)
	if err != nil {
		t.Fatalf("corrupt-cache resolve: %v", err)
	}
	if got, err := os.ReadFile(pathAfterCorruption); err != nil || string(got) != string(assetBytes) {
		t.Fatalf("redownloaded bytes = %q, err=%v; want %q", got, err, assetBytes)
	}
	if requestCount != 2 {
		t.Fatalf("master request count = %d, want 2 (miss + corrupt redownload)", requestCount)
	}
	if got, err := os.ReadFile(sentinelPath); err != nil || string(got) != "keep me" {
		t.Fatalf("unrelated cache entry = %q, err=%v; it must remain untouched", got, err)
	}

	records := tracker.snapshot()
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	wantStatuses := []string{"miss", "hit", "miss"}
	for i, wantStatus := range wantStatuses {
		record := records[i]
		if record.AssetID != assetID || record.CacheStatus != wantStatus {
			t.Errorf("record[%d] identity/status = %q/%q, want %q/%q", i, record.AssetID, record.CacheStatus, assetID, wantStatus)
		}
		if record.Source != "master_asset_bridge" {
			t.Errorf("record[%d].Source = %q, want master_asset_bridge", i, record.Source)
		}
		if record.LocalPath != pathAfterCorruption {
			t.Errorf("record[%d].LocalPath = %q, want cached path %q", i, record.LocalPath, pathAfterCorruption)
		}
		if record.DownloadStartedAt.IsZero() || record.DownloadCompletedAt.IsZero() {
			t.Errorf("record[%d] missing download timestamps: %+v", i, record)
		}
		if record.DownloadCompletedAt.Before(record.DownloadStartedAt) {
			t.Errorf("record[%d] completed before started: %+v", i, record)
		}
		if !record.SHA256Verified || record.IntegrityCheck != "size_bytes+sha256" || !record.IntegrityValid {
			t.Errorf("record[%d] integrity fields = verified:%v check:%q valid:%v", i, record.SHA256Verified, record.IntegrityCheck, record.IntegrityValid)
		}
	}
	if records[0].DownloadedBytes != int64(len(assetBytes)) || records[2].DownloadedBytes != int64(len(assetBytes)) {
		t.Errorf("miss downloaded bytes = %d/%d, want %d", records[0].DownloadedBytes, records[2].DownloadedBytes, len(assetBytes))
	}
	if records[1].DownloadedBytes != 0 || records[1].DownloadMS != 0 {
		t.Errorf("hit metrics = bytes:%d ms:%d, want 0/0", records[1].DownloadedBytes, records[1].DownloadMS)
	}

	// Phase A1: the per-attempt counters are fed by the canonical resolver
	// sink, not re-derived from the caller: 3 resolutions = 2 misses + 1 hit,
	// with exactly 2 downloads.
	cache := tracker.cacheSnapshot()
	if cache.CacheLookups != 3 || cache.CacheHits != 1 || cache.CacheMisses != 2 || cache.CacheDownloadCount != 2 {
		t.Fatalf("attempt cache counters = %+v, want lookups=3 hits=1 misses=2 downloads=2", cache)
	}
	if cache.CacheDownloadBytes != int64(2*len(assetBytes)) {
		t.Fatalf("attempt cache download bytes = %d, want %d", cache.CacheDownloadBytes, 2*len(assetBytes))
	}
}

func TestDownloadVeloxAssetWithMetadataLogsTerminalHTTPError(t *testing.T) {
	assetID := "asset-terminal-error"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "asset unavailable", http.StatusNotFound)
	}))
	defer srv.Close()

	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-error-test", MasterURL: srv.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(srv.URL),
	}

	var buffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&buffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	if _, err := w.downloadVeloxAssetWithMetadata(context.Background(), assetID, "", 0); err == nil {
		t.Fatal("download unexpectedly succeeded; want terminal HTTP error")
	}

	var event map[string]interface{}
	if err := json.Unmarshal(buffer.Bytes(), &event); err != nil {
		t.Fatalf("terminal error log is not JSON: %v; output=%q", err, buffer.String())
	}
	if event["event"] != "ASSET_CACHE_ACCESS" || event["result"] != "error" {
		t.Fatalf("terminal error event = %#v, want ASSET_CACHE_ACCESS/error", event)
	}
	if event["asset_key"] != assetID {
		t.Errorf("asset_key = %#v, want %q", event["asset_key"], assetID)
	}
}

// TestDownloadVeloxAssetWithMetadataRememberedIntegrityEnablesPartialHits
// locks the contract that partial integrity metadata alone never hits a
// fresh cache, while remembered self-verified integrity (from an earlier
// successful download on the same worker) upgrades later partial-metadata
// accesses to verified hits without re-downloading.
func TestDownloadVeloxAssetWithMetadataRememberedIntegrityEnablesPartialHits(t *testing.T) {
	assetID := "asset-requires-complete-integrity"
	assetBytes := []byte("complete integrity metadata")
	digest := sha256.Sum256(assetBytes)
	expectedSHA := hex.EncodeToString(digest[:])
	requestCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(assetBytes)
	}))
	defer srv.Close()

	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(srv.URL),
	}
	ctx := context.Background()

	completeSize := int64(len(assetBytes))
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, completeSize); err != nil {
		t.Fatalf("complete-metadata download: %v", err)
	}
	cacheDir := w.assetCacheDir()
	// Direct partial-metadata lookups never hit: the content-addressed probe
	// requires the full integrity contract (SHA + size) to address a blob.
	if got, err := cachedAssetPath(cacheDir, expectedSHA, 0); err != nil || got != "" {
		t.Fatalf("SHA-only cache lookup = %q, err=%v; partial metadata must not hit", got, err)
	}
	if got, err := cachedAssetPath(cacheDir, "", completeSize); err != nil || got != "" {
		t.Fatalf("size-only cache lookup = %q, err=%v; partial metadata must not hit", got, err)
	}
	// After the worker remembered the self-verified digest, partial-metadata
	// accesses become verified hits served from cache (no master round-trip).
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, 0); err != nil {
		t.Fatalf("SHA-only download after self-verify: %v", err)
	}
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", completeSize); err != nil {
		t.Fatalf("size-only download after self-verify: %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("master request count = %d, want 1 (partial-metadata hits served from remembered integrity)", requestCount)
	}
	// A fresh Worker (new job/process boundary) has no remembered digest:
	// partial metadata stays a forced miss and re-downloads.
	wFresh := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: w.config.WorkDir},
		apiClient: api.NewClient(srv.URL),
	}
	if _, err := wFresh.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, 0); err != nil {
		t.Fatalf("fresh-worker SHA-only download: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("master request count after fresh worker = %d, want 2", requestCount)
	}
}

// TestDownloadVeloxAssetWithMetadataLegacyReuseAfterSelfVerify locks the
// expected per-worker behaviour for folder-backed assets referenced without
// any integrity metadata: primo accesso → MISS + download, accessi successivi
// → HIT + downloaded_bytes=0 (no repeated downloads of the same stock).
func TestDownloadVeloxAssetWithMetadataLegacyReuseAfterSelfVerify(t *testing.T) {
	assetID := "legacy-asset-no-metadata"
	assetBytes := []byte("legacy stock bytes")
	requestCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(assetBytes)
	}))
	defer srv.Close()

	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(srv.URL),
	}
	ctx := context.Background()

	cold, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", 0)
	if err != nil {
		t.Fatalf("cold resolve: %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("cold request count = %d, want 1", requestCount)
	}
	if _, err := os.Stat(cold); err != nil {
		t.Fatalf("cold cached path %q: %v", cold, err)
	}

	warm, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", 0)
	if err != nil {
		t.Fatalf("warm resolve: %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("warm request count = %d, want 1 (legacy reuse must not re-download)", requestCount)
	}
	if warm != cold {
		t.Fatalf("warm path = %q, want cached path %q", warm, cold)
	}

	// Corrupting the cached file must self-heal: verified mismatch evicts
	// the entry and re-downloads fresh bytes, then remembers the new digest.
	// Promoted blobs are chmod 0444; restore write permission to simulate
	// out-of-band corruption.
	if err := os.Chmod(cold, 0o644); err != nil {
		t.Fatalf("make cached entry writable for corruption: %v", err)
	}
	if err := os.WriteFile(cold, bytes.Repeat([]byte{'x'}, len(assetBytes)), 0o644); err != nil {
		t.Fatalf("corrupt cached entry: %v", err)
	}
	repaired, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", 0)
	if err != nil {
		t.Fatalf("corrupt-cache repair: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("repair request count = %d, want 2", requestCount)
	}
	if got, err := os.ReadFile(repaired); err != nil || string(got) != string(assetBytes) {
		t.Fatalf("repaired bytes = %q, err=%v; want %q", got, err, assetBytes)
	}

	// And the refreshed digest keeps serving verified hits afterwards.
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", 0); err != nil {
		t.Fatalf("post-repair warm resolve: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("post-repair request count = %d, want 2 (no re-download after repair)", requestCount)
	}
}

func TestResolveSceneImagePayload_PropagatesExpectedSHA(t *testing.T) {
	assetID := "scene-asset-report-001"
	assetBytes := []byte("scene image bytes")
	digest := sha256.Sum256(assetBytes)
	expectedSHA := hex.EncodeToString(digest[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = io.Copy(w, bytes.NewReader(assetBytes))
	}))
	defer srv.Close()

	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(srv.URL),
	}
	tracker := &assetOperationTracker{}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	payload := map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"clip_link":  "velox-asset://" + assetID,
			"sha256":     expectedSHA,
			"size_bytes": int64(len(assetBytes)),
		}},
	}

	resolved, err := w.resolveSceneImagePayload(ctx, payload)
	if err != nil {
		t.Fatalf("resolve scene image payload: %v", err)
	}
	scenes := resolved["scenes"].([]interface{})
	scene := scenes[0].(map[string]interface{})
	localPath, ok := scene["clip_link"].(string)
	if !ok || !strings.HasPrefix(localPath, w.config.WorkDir) {
		t.Fatalf("resolved clip_link = %#v, want local path under %q", scene["clip_link"], w.config.WorkDir)
	}
	records := tracker.snapshot()
	if len(records) != 1 || records[0].AssetID != assetID {
		t.Fatalf("scene asset records = %+v, want one record for %s", records, assetID)
	}
	if !records[0].SHA256Verified || records[0].IntegrityCheck != "size_bytes+sha256" || !records[0].IntegrityValid {
		t.Fatalf("scene asset integrity = %+v, want verified size and SHA-256", records[0])
	}
}
