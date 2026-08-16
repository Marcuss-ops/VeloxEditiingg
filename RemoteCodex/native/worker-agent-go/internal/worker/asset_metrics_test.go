package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
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
	// Direct partial-metadata lookups never hit: the legacy branch only
	// reuses a fully verified entry, never a bare asset-ID file.
	if got, err := cachedAssetPath(cacheDir, assetID, expectedSHA, 0); err != nil || got != "" {
		t.Fatalf("SHA-only cache lookup = %q, err=%v; partial metadata must not hit", got, err)
	}
	if got, err := cachedAssetPath(cacheDir, assetID, "", completeSize); err != nil || got != "" {
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

func TestAttachAssetOperationsAddsExistingReportMetrics(t *testing.T) {
	tracker := &assetOperationTracker{}
	tracker.add(AssetOperationRecord{AssetID: "asset-1", CacheStatus: "hit"})
	report := taskrunner.TaskExecutionReport{Metrics: map[string]interface{}{"existing": "preserved"}}

	attachAssetOperations(&report, tracker)

	if report.Metrics["existing"] != "preserved" {
		t.Fatalf("existing metric was not preserved: %#v", report.Metrics)
	}
	records, ok := report.Metrics["asset_operations"].([]AssetOperationRecord)
	if !ok || len(records) != 1 {
		t.Fatalf("asset_operations = %#v, want one typed record", report.Metrics["asset_operations"])
	}
	if records[0].AssetID != "asset-1" || records[0].CacheStatus != "hit" {
		t.Fatalf("record = %+v", records[0])
	}
}

func TestAttachAssetOperationsProjectsResolverCacheCounters(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}
	// The counters are fed exclusively by the canonical resolver sink.
	tracker.recordResolution(downloader.CacheResolution{AssetID: "hit", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
	tracker.recordResolution(downloader.CacheResolution{AssetID: "miss", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound, Downloaded: true, DownloadBytes: 4096, Source: downloader.CacheSourceMaster})
	tracker.add(AssetOperationRecord{AssetID: "hit", CacheStatus: "hit"})
	tracker.add(AssetOperationRecord{AssetID: "miss", CacheStatus: "miss"})
	report := taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			AssetCacheMissCount: 77,
			CacheDownloadCount:  9,
		},
	}
	attachAssetOperations(&report, tracker)
	if report.Metrics["cache.enabled"] != true || report.Metrics["asset.cache.lookups"] != int64(2) || report.Metrics["cache.lookups"] != int64(2) {
		t.Fatalf("cache summary = %#v", report.Metrics)
	}
	if report.Metrics["asset.cache.hit.count"] != int64(1) || report.Metrics["asset.cache.miss.count"] != int64(1) {
		t.Fatalf("cache hit/miss counters = %#v", report.Metrics)
	}
	if report.Metrics["asset.cache.download.count"] != int64(1) || report.Metrics["asset.cache.download.bytes"] != int64(4096) {
		t.Fatalf("cache download counters = %#v", report.Metrics)
	}
	if report.RawMetrics == nil {
		t.Fatal("raw metrics are nil after resolver projection")
	}
	if report.RawMetrics.CacheLookups != 2 || report.RawMetrics.AssetCacheHitCount != 1 || report.RawMetrics.AssetCacheMissCount != 1 || report.RawMetrics.CacheDownloadCount != 1 || report.RawMetrics.CacheDownloadBytes != 4096 || report.RawMetrics.UniqueAssetsRequested != 2 {
		t.Fatalf("raw cache counters = %+v", *report.RawMetrics)
	}
	if report.TypedMetrics != report.RawMetrics {
		t.Fatal("typed metrics must alias the canonical raw envelope")
	}
}

func TestAttachAssetOperationsPreservesAbsentCacheFacts(t *testing.T) {
	report := taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			CacheLookups:          12,
			AssetCacheHitCount:    12,
			AssetCacheMissCount:   0,
			CacheDownloadCount:    2,
			CacheDownloadBytes:    1024,
			UniqueAssetsRequested: 4,
		},
	}
	tracker := &assetOperationTracker{cacheEnabled: true}

	attachAssetOperations(&report, tracker)

	// An idle resolver emitted no fact. Do not turn the absent observation into
	// zeroes or erase facts already supplied by the canonical raw producer.
	got := report.RawMetrics
	if got == nil || got.CacheLookups != 12 || got.AssetCacheHitCount != 12 || got.CacheDownloadCount != 2 || got.CacheDownloadBytes != 1024 || got.UniqueAssetsRequested != 4 {
		t.Fatalf("absent resolver facts changed raw metrics: %+v", got)
	}
}

// TestAttemptCacheMetrics_StartAtZeroPerAttempt locks Phase A1's core
// contract: per-attempt cache accounting starts at zero and is fed only by
// the canonical resolver sink. A warm second attempt never inherits the
// previous attempt's miss/download counters (the plan's example: attempt A
// 169 lookups / 143 hits / 26 misses; attempt B 169 / 169 / 0).
func TestAttemptCacheMetrics_StartAtZeroPerAttempt(t *testing.T) {
	trackerA := &assetOperationTracker{}
	trackerB := &assetOperationTracker{}

	// Attempt A: cold — 143 hits + 26 misses (26 downloads of 1 KiB each).
	for i := 0; i < 169; i++ {
		if i < 143 {
			trackerA.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
		} else {
			trackerA.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound, Downloaded: true, DownloadBytes: 1024, Source: downloader.CacheSourceMaster})
		}
	}
	// Attempt B: warm — every lookup is a verified hit, zero misses. The
	// tracker is a fresh per-attempt accumulator: it must NOT inherit A's
	// 26 misses (the historical worker-cumulative contamination bug).
	for i := 0; i < 169; i++ {
		trackerB.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
	}

	a := trackerA.cacheSnapshot()
	if a.CacheLookups != 169 || a.CacheHits != 143 || a.CacheMisses != 26 || a.CacheDownloadCount != 26 || a.CacheDownloadBytes != 26*1024 {
		t.Fatalf("attempt A counters = %+v, want 169/143/26/26/26624", a)
	}
	b := trackerB.cacheSnapshot()
	if b.CacheLookups != 169 || b.CacheHits != 169 || b.CacheMisses != 0 || b.CacheDownloadCount != 0 || b.CacheDownloadBytes != 0 {
		t.Fatalf("attempt B counters = %+v, want 169/169/0/0/0 (warm second wave must start at zero)", b)
	}
}

func TestAttachAssetOperationsToPhaseMarkersMakesRecordsReportable(t *testing.T) {
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	end := start.Add(250 * time.Millisecond)
	report := taskrunner.TaskExecutionReport{
		Metrics: map[string]interface{}{
			"asset_operations": []AssetOperationRecord{{
				AssetID:             "asset-1",
				CacheStatus:         "miss",
				DownloadStartedAt:   start,
				DownloadCompletedAt: end,
				DownloadMS:          250,
				DownloadedBytes:     42,
				SHA256Verified:      true,
				IntegrityCheck:      "sha256",
				IntegrityValid:      true,
				LocalPath:           "/worker/cache/asset-1.mp3",
				Source:              "master_asset_bridge",
			}},
		},
		PhaseMarkers: []taskrunner.PhaseMarker{{
			Name:        taskrunner.PhasePrefetch,
			StartedAt:   start,
			CompletedAt: end,
			Status:      "ok",
		}},
	}

	attachAssetOperationsToPhaseMarkers(&report)

	if len(report.PhaseMarkers) != 1 {
		t.Fatalf("phase markers = %d, want 1", len(report.PhaseMarkers))
	}
	marker := report.PhaseMarkers[0]
	if marker.Name != taskrunner.PhasePrefetch || marker.Status != "ok" {
		t.Fatalf("marker = %+v, want prefetch/ok", marker)
	}
	if marker.StartedAt != start || marker.CompletedAt != end {
		t.Fatalf("marker timestamps = %s/%s, want %s/%s", marker.StartedAt, marker.CompletedAt, start, end)
	}
	if !strings.Contains(marker.Notes, "asset_operations=") || !strings.Contains(marker.Notes, "asset-1") {
		t.Fatalf("marker notes do not contain asset report: %q", marker.Notes)
	}
}

func TestSubmitTaskResultCarriesAttachedAssetOperations(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-report-test", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey: "scene.composite.v1@1",
		Metrics: map[string]interface{}{
			"asset_operations": []AssetOperationRecord{{
				AssetID:         "asset-wire-1",
				CacheStatus:     "hit",
				DownloadedBytes: 0,
				SHA256Verified:  true,
				IntegrityCheck:  "sha256",
				IntegrityValid:  true,
			}},
		},
		PhaseMarkers: []taskrunner.PhaseMarker{{
			Name:        taskrunner.PhasePrefetch,
			StartedAt:   start,
			CompletedAt: start.Add(time.Second),
			Status:      "ok",
		}},
	}
	attachAssetOperationsToPhaseMarkers(report)
	pte := &PendingTaskExecution{JobID: "job-report-test", ExecutorID: "scene.composite.v1", LeaseID: "lease-report-test"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-report-test", "attempt-report-test", report, nil)

	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v, want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil || len(result.PhaseMarkers) != 1 {
		t.Fatalf("typed task result = %#v, phase markers = %v", message.TypedPayload, result.GetPhaseMarkers())
	}
	if !strings.Contains(result.PhaseMarkers[0].Notes, "asset-wire-1") {
		t.Fatalf("wire phase notes = %q, missing asset report", result.PhaseMarkers[0].Notes)
	}
}

// TestSubmitTaskResult_PhaseTimings: block-1 — the full detailed phase
// stream (proto field 20) is populated on the wire, with lease identity
// stamped from the PendingTaskExecution and all event fields mapping
// through ToProto untouched.
func TestSubmitTaskResult_PhaseTimings(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-phases-test", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey: "scene.composite.v1@1",
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{
				PhaseOrder:      1,
				Component:       "runner",
				Action:          "cache_lookup",
				StartedAt:       start,
				CompletedAt:     start.Add(10 * time.Millisecond),
				DurationMS:      10,
				Status:          "ok",
				Origin:          "worker",
				Scope:           "attempt",
				EventType:       "completed",
				EventName:       "cache_lookup",
				EventIndex:      0,
				Phase:           "cache_lookup",
				ExecutorID:      "scene.composite.v1",
				ExecutorVersion: 1,
			},
			{
				PhaseOrder:      2,
				Component:       "runner",
				Action:          "report",
				StartedAt:       start.Add(10 * time.Millisecond),
				CompletedAt:     start.Add(12 * time.Millisecond),
				DurationMS:      2,
				Status:          "failed",
				ErrorCode:       "EXECUTE_FAILED",
				ErrorMessage:    "boom",
				Origin:          "worker",
				Scope:           "attempt",
				EventType:       "failed",
				EventName:       "report",
				EventIndex:      1,
				Phase:           "report",
				ExecutorID:      "scene.composite.v1",
				ExecutorVersion: 1,
			},
		},
	}
	pte := &PendingTaskExecution{JobID: "job-phases-test", ExecutorID: "scene.composite.v1", LeaseID: "lease-phases-test"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-phases-test", "attempt-phases-test", report, nil)

	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v, want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if len(result.PhaseTimings) != 2 {
		t.Fatalf("PhaseTimings len = %d, want 2", len(result.PhaseTimings))
	}
	first := result.PhaseTimings[0]
	if first.PhaseOrder != 1 || first.Component != "runner" || first.Action != "cache_lookup" {
		t.Errorf("first phase = %d %s.%s", first.PhaseOrder, first.Component, first.Action)
	}
	if first.EventIndex != 0 || first.EventType != "completed" || first.Status != "ok" {
		t.Errorf("first phase event = idx=%d type=%q status=%q", first.EventIndex, first.EventType, first.Status)
	}
	if first.Origin != "worker" || first.Scope != "attempt" || first.Phase != "cache_lookup" {
		t.Errorf("first phase taxonomy = %q/%q phase=%q", first.Origin, first.Scope, first.Phase)
	}
	if first.ExecutorId != "scene.composite.v1" || first.ExecutorVersion != 1 {
		t.Errorf("first phase identity = %s@%d", first.ExecutorId, first.ExecutorVersion)
	}
	if first.DurationMs != 10 {
		t.Errorf("first phase duration_ms = %d, want 10", first.DurationMs)
	}
	second := result.PhaseTimings[1]
	if second.LeaseId != "lease-phases-test" {
		t.Errorf("lease stamped = %q, want lease-phases-test", second.LeaseId)
	}
	if second.EventType != "failed" || second.ErrorCode != "EXECUTE_FAILED" || second.ErrorMessage != "boom" {
		t.Errorf("second phase failure = type=%q code=%q msg=%q", second.EventType, second.ErrorCode, second.ErrorMessage)
	}
}

func TestSubmitTaskResult_PreservesTenDistinctEngineEncodeEventsOnSuccessAndFailure(t *testing.T) {
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	phases := make([]taskrunner.DetailedPhaseTiming, 0, 10)
	for i := 0; i < 10; i++ {
		phases = append(phases, taskrunner.DetailedPhaseTiming{
			PhaseOrder:   i + 1,
			Component:    "engine",
			Action:       "encode",
			Origin:       "engine",
			Scope:        "segment",
			EventType:    "completed",
			EventName:    "engine.encode",
			EventIndex:   int64(100 + i),
			Phase:        "encode",
			SegmentIndex: int32(i),
			StartedAt:    start.Add(time.Duration(i) * time.Millisecond),
			CompletedAt:  start.Add(time.Duration(i+1) * time.Millisecond),
			DurationMS:   1,
			Status:       "ok",
			FramesIn:     int64(90 + i),
			FramesOut:    int64(90 + i),
		})
	}

	for _, tc := range []struct {
		name   string
		status string
		err    error
	}{
		{name: "succeeded", status: "succeeded"},
		{name: "failed", status: "failed", err: errors.New("encoder crashed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{}
			w := &Worker{
				config:    &config.WorkerConfig{WorkerID: "worker-encode-events", ProtocolVersion: "v3"},
				logger:    logger.New(logger.InfoLevel, io.Discard),
				transport: transport,
			}
			report := &taskrunner.TaskExecutionReport{
				Status:         tc.status,
				ExecutorKey:    "scene.composite.v1@1",
				DetailedPhases: phases,
			}
			pte := &PendingTaskExecution{JobID: "job-encode-events", ExecutorID: "scene.composite.v1", ExecutorVersion: 1, LeaseID: "lease-encode-events"}

			wireTestReporter(w, nil).Submit(context.Background(), pte, "task-encode-events", "attempt-encode-events", report, tc.err)

			message, ok := transport.last()
			if !ok {
				t.Fatal("expected a TaskResult message")
			}
			result, ok := message.TypedPayload.(*pb.TaskResult)
			if !ok || result == nil {
				t.Fatalf("typed task result = %#v", message.TypedPayload)
			}
			if result.Status != tc.status {
				t.Fatalf("status = %q, want %q", result.Status, tc.status)
			}
			if len(result.PhaseTimings) != 10 {
				t.Fatalf("phase timings = %d, want 10 distinct engine.encode events", len(result.PhaseTimings))
			}
			for i, phase := range result.PhaseTimings {
				if phase.Component != "engine" || phase.Action != "encode" || phase.Scope != "segment" {
					t.Errorf("phase[%d] identity = %q.%q scope=%q", i, phase.Component, phase.Action, phase.Scope)
				}
				if phase.EventIndex != int64(100+i) || phase.SegmentIndex != int32(i) {
					t.Errorf("phase[%d] event/segment = %d/%d, want %d/%d", i, phase.EventIndex, phase.SegmentIndex, 100+i, i)
				}
				if phase.LeaseId != pte.LeaseID {
					t.Errorf("phase[%d] lease_id = %q, want %q", i, phase.LeaseId, pte.LeaseID)
				}
				if phase.ExecutorId != pte.ExecutorID || phase.ExecutorVersion != 1 {
					t.Errorf("phase[%d] executor identity = %q@%d, want %q@1", i, phase.ExecutorId, phase.ExecutorVersion, pte.ExecutorID)
				}
			}

		})
	}
}

// TestSubmitTaskResult_FailedRenderPreservesDetailedPhases verifies that
// submitTaskResult does not discard the worker report when execution fails.
// Failed renders must still deliver every phase completed before the error,
// including the failed phase itself, so the master can persist partial work.
func TestSubmitTaskResult_FailedRenderPreservesDetailedPhases(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-failed-phases", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey: "scene.composite.v1@1",
		Status:      "failed",
		ErrorCode:   "EXECUTE_FAILED",
		ErrorDetail: "encoder crashed",
		Metrics: map[string]interface{}{
			"engine.frames":             int64(144),
			"engine.speed_x":            float64(1.75),
			"engine.encode_passes":      int64(2),
			"native.total_ms":           int64(2500),
			"quality.ffprobe.valid":     int64(1),
			"quality.black.frame.ratio": float64(0.02),
			"io.disk.read.bytes":        int64(4096),
			"wasted.cpu.ms":             int64(88),
			"wasted.download.bytes":     int64(512),
			"completed.segments":        int64(2),
			"error.component":           "engine",
			"error.phase":               "encode",
		},
		Segments: []taskrunner.SegmentTiming{{
			SegmentIndex: 3, StartedOffsetMS: 1.5, FinishedOffsetMS: 7.25,
			WorkerSlot: 2, CPUThreads: 4, ParallelGroup: "encode-group",
		}},
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{
				PhaseOrder: 1, Component: "runner", Action: "cache_lookup",
				StartedAt: started, CompletedAt: started.Add(5 * time.Millisecond),
				DurationMS: 5, Status: "ok", Origin: "worker", Scope: "attempt",
				EventType: "completed", EventName: "cache_lookup", EventIndex: 0,
				Phase: "cache_lookup", ExecutorID: "scene.composite.v1", ExecutorVersion: 1,
			},
			{
				PhaseOrder: 2, Component: "runner", Action: "execute",
				StartedAt: started.Add(5 * time.Millisecond), CompletedAt: started.Add(25 * time.Millisecond),
				DurationMS: 20, Status: "failed", ErrorCode: "EXECUTE_FAILED",
				ErrorMessage: "encoder crashed", Origin: "worker", Scope: "attempt",
				EventType: "failed", EventName: "execute", EventIndex: 1,
				Phase: "render", ExecutorID: "scene.composite.v1", ExecutorVersion: 1,
			},
		},
	}
	pte := &PendingTaskExecution{
		JobID: "job-failed-phases", ExecutorID: "scene.composite.v1", LeaseID: "lease-failed-phases",
	}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-failed-phases", "attempt-failed-phases", report, errors.New("encoder crashed"))

	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v, want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if result.Status != "failed" {
		t.Fatalf("status = %q, want failed", result.Status)
	}
	if result.ErrorCode != "EXECUTE_FAILED" {
		t.Fatalf("error code = %q, want EXECUTE_FAILED from the failed report", result.ErrorCode)
	}
	if result.ErrorDetail != "encoder crashed" {
		t.Fatalf("error detail = %q, want encoder crashed", result.ErrorDetail)
	}
	if result.ExecutionMetrics == nil || result.ExecutionMetrics.FramesEncoded != 144 || result.ExecutionMetrics.FfmpegSpeedRatio != 1.75 || result.ExecutionMetrics.EncodePasses != 2 || result.ExecutionMetrics.WallClockSeconds != 2.5 || result.ExecutionMetrics.FfprobeValid != 1 || result.ExecutionMetrics.BlackFrameRatio != 0.02 || result.ExecutionMetrics.DiskReadBytes != 4096 || result.ExecutionMetrics.WastedCpuMs != 88 || result.ExecutionMetrics.WastedDownloadBytes != 512 || result.ExecutionMetrics.CompletedSegments != 2 || result.ExecutionMetrics.ErrorComponent != "engine" || result.ExecutionMetrics.ErrorPhase != "encode" {
		t.Fatalf("typed execution metrics = %+v, want native/quality/io/waste fields", result.ExecutionMetrics)
	}
	if len(result.PhaseTimings) != len(report.DetailedPhases) {
		t.Fatalf("phase timings = %d, want %d", len(result.PhaseTimings), len(report.DetailedPhases))
	}
	for i, phase := range result.PhaseTimings {
		if phase.PhaseOrder != int32(i+1) || phase.EventIndex != int64(i) {
			t.Errorf("phase[%d] order/index = %d/%d, want %d/%d", i, phase.PhaseOrder, phase.EventIndex, i+1, i)
		}
	}
	if result.PhaseTimings[1].Status != "failed" || result.PhaseTimings[1].ErrorCode != "EXECUTE_FAILED" {
		t.Fatalf("failed phase = status:%q code:%q", result.PhaseTimings[1].Status, result.PhaseTimings[1].ErrorCode)
	}
	for i, phase := range result.PhaseTimings {
		if phase.LeaseId != pte.LeaseID {
			t.Errorf("phase[%d] lease_id = %q, want %q", i, phase.LeaseId, pte.LeaseID)
		}
	}
	if len(result.SegmentTimings) != 1 {
		t.Fatalf("segment timings = %d, want 1", len(result.SegmentTimings))
	}
	segment := result.SegmentTimings[0]
	if segment.SegmentIndex != 3 || segment.FinishedOffsetMs != 7.25 || segment.WorkerSlot != 2 || segment.CpuThreads != 4 || segment.ParallelGroup != "encode-group" {
		t.Fatalf("failed segment timing = %+v", segment)
	}
}

func TestSubmitTaskResult_ObservabilitySummaryPhasesReachWire(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-summary-wire", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	categories := []string{"audio", "subtitle", "io", "quality", "retry", "waste"}
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey:    "scene.composite.v1@1",
		DetailedPhases: make([]taskrunner.DetailedPhaseTiming, 0, len(categories)),
	}
	for i, category := range categories {
		report.DetailedPhases = append(report.DetailedPhases, taskrunner.DetailedPhaseTiming{
			Origin: "validation", Scope: "attempt", Component: category,
			Action: "summary", Phase: category, EventType: "summary",
			EventName: category, EventIndex: int64(i), Status: "ok",
			MetadataJSON: `{"events":1}`,
		})
	}
	pte := &PendingTaskExecution{JobID: "job-summary-wire", ExecutorID: "scene.composite.v1", LeaseID: "lease-summary-wire"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-summary-wire", "attempt-summary-wire", report, nil)

	message, ok := transport.last()
	if !ok {
		t.Fatal("expected a TaskResult message")
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if len(result.PhaseTimings) != len(categories) {
		t.Fatalf("summary phase timings = %d, want %d", len(result.PhaseTimings), len(categories))
	}
	seen := make(map[string]bool, len(categories))
	for i, phase := range result.PhaseTimings {
		if phase.EventType != "summary" || phase.Action != "summary" || phase.MetadataJson != `{"events":1}` {
			t.Errorf("summary[%d] = type:%q action:%q metadata:%q", i, phase.EventType, phase.Action, phase.MetadataJson)
		}
		if phase.EventIndex != int64(i) {
			t.Errorf("summary[%d] event_index = %d, want %d", i, phase.EventIndex, i)
		}
		seen[phase.Component] = true
	}
	for _, category := range categories {
		if !seen[category] {
			t.Errorf("wire summaries missing category %q", category)
		}
	}
}

func TestSubmitTaskResult_FailedReportWithoutExecutionErrorPreservesFailure(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-failed-report", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	report := &taskrunner.TaskExecutionReport{
		Status:      "failed",
		ErrorCode:   "UPLOAD_FAILED",
		ErrorDetail: "artifact commit rejected",
		DetailedPhases: []taskrunner.DetailedPhaseTiming{{
			PhaseOrder: 1, Component: "runner", Action: "upload", Status: "failed",
			ErrorCode: "UPLOAD_FAILED", ErrorMessage: "artifact commit rejected",
			Origin: "worker", Scope: "attempt", EventType: "failed", EventIndex: 0,
		}},
	}
	pte := &PendingTaskExecution{JobID: "job-failed-report", ExecutorID: "scene.composite.v1", LeaseID: "lease-failed-report"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-failed-report", "attempt-failed-report", report, nil)

	message, ok := transport.last()
	if !ok {
		t.Fatal("expected a TaskResult message")
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if result.Status != "failed" || result.ErrorCode != "UPLOAD_FAILED" || result.ErrorDetail != "artifact commit rejected" {
		t.Fatalf("failure = status:%q code:%q detail:%q", result.Status, result.ErrorCode, result.ErrorDetail)
	}
	if len(result.PhaseTimings) != 1 || result.PhaseTimings[0].Status != "failed" {
		t.Fatalf("phase timings = %+v, want one failed phase", result.PhaseTimings)
	}
}
