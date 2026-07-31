package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/taskrunner"
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
		if r.URL.Path != "/api/v1/worker-assets/"+assetID {
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

	path, err := w.downloadVeloxAssetWithSHA(ctx, assetID, expectedSHA)
	if err != nil {
		t.Fatalf("cold resolve: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cached path %q: %v", path, err)
	}

	pathAgain, err := w.downloadVeloxAssetWithSHA(ctx, assetID, expectedSHA)
	if err != nil {
		t.Fatalf("warm resolve: %v", err)
	}
	if pathAgain != path {
		t.Fatalf("warm path = %q, want %q", pathAgain, path)
	}

	if err := os.WriteFile(path, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt cache entry: %v", err)
	}
	pathAfterCorruption, err := w.downloadVeloxAssetWithSHA(ctx, assetID, expectedSHA)
	if err != nil {
		t.Fatalf("corrupt-cache resolve: %v", err)
	}
	if got, err := os.ReadFile(pathAfterCorruption); err != nil || string(got) != string(assetBytes) {
		t.Fatalf("redownloaded bytes = %q, err=%v; want %q", got, err, assetBytes)
	}
	if requestCount != 2 {
		t.Fatalf("master request count = %d, want 2 (miss + corrupt redownload)", requestCount)
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
		if !record.SHA256Verified || record.IntegrityCheck != "sha256" || !record.IntegrityValid {
			t.Errorf("record[%d] integrity fields = verified:%v check:%q valid:%v", i, record.SHA256Verified, record.IntegrityCheck, record.IntegrityValid)
		}
	}
	if records[0].DownloadedBytes != int64(len(assetBytes)) || records[2].DownloadedBytes != int64(len(assetBytes)) {
		t.Errorf("miss downloaded bytes = %d/%d, want %d", records[0].DownloadedBytes, records[2].DownloadedBytes, len(assetBytes))
	}
	if records[1].DownloadedBytes != 0 || records[1].DownloadMS != 0 {
		t.Errorf("hit metrics = bytes:%d ms:%d, want 0/0", records[1].DownloadedBytes, records[1].DownloadMS)
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
			"clip_link": "velox-asset://" + assetID,
			"sha256":    expectedSHA,
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
	if !records[0].SHA256Verified || records[0].IntegrityCheck != "sha256" || !records[0].IntegrityValid {
		t.Fatalf("scene asset integrity = %+v, want verified SHA-256", records[0])
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

	w.submitTaskResult(context.Background(), pte, "task-report-test", "attempt-report-test", report, nil)

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
