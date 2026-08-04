package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
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

func withCacheAccessContext(ctx context.Context, jobID, role string) context.Context {
	return telemetry.WithCacheAccessContext(ctx, jobID, role)
}

func logAssetCacheAccess(ctx context.Context, workerID, assetKey, result string, downloadedBytes, lookupMS, shaVerifyMS int64) {
	telemetry.LogAssetCacheAccess(ctx, workerID, assetKey, result, downloadedBytes, lookupMS, shaVerifyMS)
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

func cacheAssetKey(assetID, expectedSHA256 string) string {
	if expectedSHA256 != "" {
		return "sha256:" + expectedSHA256
	}
	return assetID
}

func cacheRole(field string) string {
	field = strings.ToLower(field)
	switch {
	case strings.Contains(field, "voiceover"):
		return "voiceover"
	case strings.Contains(field, "stock"), strings.Contains(field, "clip"):
		return "stock"
	case strings.Contains(field, "music"):
		return "music"
	case strings.Contains(field, "effect"), strings.Contains(field, "sfx"):
		return "effect"
	case strings.Contains(field, "image"):
		return "image"
	case strings.Contains(field, "subtitle"), strings.Contains(field, "caption"):
		return "subtitle"
	default:
		return "asset"
	}
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

// expectedAssetSize returns the expected byte count from the asset envelope.
// JSON decoding commonly represents numbers as float64, while typed callers
// may provide int64 or a decimal string, so accept all transport forms.
func integrityCheck(expectedSHA256 string, expectedSizeBytes int64) string {
	if expectedSHA256 != "" && expectedSizeBytes > 0 {
		return "size_bytes+sha256"
	}
	if expectedSHA256 != "" {
		return "sha256"
	}
	if expectedSizeBytes > 0 {
		return "size_bytes"
	}
	return "none"
}

func expectedAssetSize(fields map[string]interface{}) int64 {
	if fields == nil {
		return 0
	}
	for _, key := range []string{"size_bytes", "sizeBytes", "expected_size_bytes", "size"} {
		value, ok := fields[key]
		if !ok {
			continue
		}
		var size int64
		switch typed := value.(type) {
		case int:
			size = int64(typed)
		case int32:
			size = int64(typed)
		case int64:
			size = typed
		case uint:
			size = int64(typed)
		case uint32:
			size = int64(typed)
		case uint64:
			if typed > uint64(^uint64(0)>>1) {
				continue
			}
			size = int64(typed)
		case float64:
			if typed != float64(int64(typed)) {
				continue
			}
			size = int64(typed)
		case json.Number:
			parsed, err := typed.Int64()
			if err != nil {
				continue
			}
			size = parsed
		case string:
			parsed, err := strconv.ParseInt(typed, 10, 64)
			if err != nil {
				continue
			}
			size = parsed
		default:
			continue
		}
		if size > 0 {
			return size
		}
	}
	return 0
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
