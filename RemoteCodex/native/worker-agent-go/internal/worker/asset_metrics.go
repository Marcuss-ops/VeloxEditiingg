package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"velox-worker-agent/internal/taskrunner"
)

// AssetOperationRecord is the report shape for one asset materialization.
// It intentionally contains only JSON-compatible scalar fields so it can be
// embedded in TaskExecutionReport.Metrics without a new wire schema.
type AssetOperationRecord struct {
	AssetID             string    `json:"asset_id"`
	CacheStatus         string    `json:"cache_status"`
	DownloadStartedAt   time.Time `json:"download_started_at"`
	DownloadCompletedAt time.Time `json:"download_completed_at"`
	DownloadMS          int64     `json:"download_ms"`
	DownloadedBytes     int64     `json:"downloaded_bytes"`
	SHA256Verified      bool      `json:"sha256_verified"`
	IntegrityCheck      string    `json:"integrity_check"`
	IntegrityValid      bool      `json:"integrity_valid"`
	LocalPath           string    `json:"local_path"`
	Source              string    `json:"source"`
}

type assetOperationTracker struct {
	mu      sync.Mutex
	records []AssetOperationRecord
}

type assetOperationTrackerKey struct{}

func withAssetOperationTracker(ctx context.Context, tracker *assetOperationTracker) context.Context {
	return context.WithValue(ctx, assetOperationTrackerKey{}, tracker)
}

func assetOperationTrackerFromContext(ctx context.Context) *assetOperationTracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(assetOperationTrackerKey{}).(*assetOperationTracker)
	return tracker
}

func (t *assetOperationTracker) add(record AssetOperationRecord) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.records = append(t.records, record)
	t.mu.Unlock()
}

func (t *assetOperationTracker) snapshot() []AssetOperationRecord {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]AssetOperationRecord(nil), t.records...)
}

func recordAssetOperation(ctx context.Context, record AssetOperationRecord) {
	assetOperationTrackerFromContext(ctx).add(record)
}

func expectedAssetSHA256(fields map[string]interface{}) string {
	if fields == nil {
		return ""
	}
	for _, key := range []string{"sha256", "sha_256", "expected_sha256"} {
		if value, ok := fields[key].(string); ok {
			return value
		}
	}
	return ""
}

func attachAssetOperations(report *taskrunner.TaskExecutionReport, tracker *assetOperationTracker) {
	if report == nil || tracker == nil {
		return
	}
	records := tracker.snapshot()
	if len(records) == 0 {
		return
	}
	if report.Metrics == nil {
		report.Metrics = make(map[string]interface{})
	}
	report.Metrics["asset_operations"] = records
}

// attachAssetOperationsToPhaseMarkers preserves the existing TaskResult wire
// schema while making per-asset records part of the report received by the
// Master. PhaseMarker.Notes is already persisted with the report; the JSON is
// self-describing for operators and downstream parsers.
func attachAssetOperationsToPhaseMarkers(report *taskrunner.TaskExecutionReport) {
	if report == nil || report.Metrics == nil {
		return
	}
	records, ok := report.Metrics["asset_operations"].([]AssetOperationRecord)
	if !ok || len(records) == 0 {
		return
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		return
	}
	notes := fmt.Sprintf("asset_operations=%s", encoded)

	// Normal TaskRunner reports already contain canonical markers. Enrich the
	// prefetch marker so ordering and the one-marker-per-phase invariant remain
	// unchanged; the existing TaskResult builder serializes Notes.
	for i := range report.PhaseMarkers {
		if report.PhaseMarkers[i].Name == taskrunner.PhasePrefetch {
			if report.PhaseMarkers[i].Notes != "" {
				report.PhaseMarkers[i].Notes += " "
			}
			report.PhaseMarkers[i].Notes += notes
			return
		}
	}

	// Do not append a late marker: canonical reports must preserve phase order.
}
