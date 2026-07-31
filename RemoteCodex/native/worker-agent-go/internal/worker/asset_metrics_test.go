package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
}

func TestDownloadVeloxAssetWithMetadataRequiresSizeAndSHAForCacheHit(t *testing.T) {
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
	if got, err := cachedAssetPath(cacheDir, assetID, expectedSHA, 0); err != nil || got != "" {
		t.Fatalf("SHA-only cache lookup = %q, err=%v; partial metadata must not hit", got, err)
	}
	if got, err := cachedAssetPath(cacheDir, assetID, "", completeSize); err != nil || got != "" {
		t.Fatalf("size-only cache lookup = %q, err=%v; partial metadata must not hit", got, err)
	}
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA, 0); err != nil {
		t.Fatalf("SHA-only download: %v", err)
	}
	if _, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, "", completeSize); err != nil {
		t.Fatalf("size-only download: %v", err)
	}
	if requestCount != 3 {
		t.Fatalf("master request count = %d, want 3 (complete + two partial-metadata misses)", requestCount)
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

	w.submitTaskResult(context.Background(), pte, "task-phases-test", "attempt-phases-test", report, nil)

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

	w.submitTaskResult(context.Background(), pte, "task-failed-phases", "attempt-failed-phases", report, errors.New("encoder crashed"))

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

	w.submitTaskResult(context.Background(), pte, "task-failed-report", "attempt-failed-report", report, nil)

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
