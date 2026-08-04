package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

type cacheAccessContextKey struct{}

var cacheAccessLogMu sync.Mutex

type cacheAccessContext struct {
	JobID    string
	Role     string
	WorkerID string
}

// WithCacheAccessContext carries low-cardinality semantic context for cache
// access logs. Job ID is deliberately used only in logs, never as a metric
// label.
func WithCacheAccessContext(ctx context.Context, jobID, role string) context.Context {
	current := cacheAccessContextFromContext(ctx)
	if jobID == "" {
		jobID = current.JobID
	}
	if role == "" {
		role = current.Role
	}
	return context.WithValue(ctx, cacheAccessContextKey{}, cacheAccessContext{
		JobID: jobID, Role: role, WorkerID: current.WorkerID,
	})
}

// WithCacheAccessWorkerID adds the worker identity used only in structured
// logs. It is never exported as a Prometheus label.
func WithCacheAccessWorkerID(ctx context.Context, workerID string) context.Context {
	current := cacheAccessContextFromContext(ctx)
	return context.WithValue(ctx, cacheAccessContextKey{}, cacheAccessContext{
		JobID: current.JobID, Role: current.Role,
		WorkerID: workerIDOr(current.WorkerID, workerID),
	})
}

func workerIDOr(current, next string) string {
	if next != "" {
		return next
	}
	return current
}

// CacheAccessContextFromContext returns the structured-log context carried by
// WithCacheAccessContext.
func CacheAccessContextFromContext(ctx context.Context) (jobID, role string) {
	value := cacheAccessContextFromContext(ctx)
	return value.JobID, value.Role
}

func cacheAccessContextFromContext(ctx context.Context) cacheAccessContext {
	if ctx == nil {
		return cacheAccessContext{}
	}
	value, _ := ctx.Value(cacheAccessContextKey{}).(cacheAccessContext)
	return value
}

// LogAssetCacheAccess emits the canonical per-asset cache access event. The
// asset key and identity fields are log-only; Prometheus labels remain static
// to avoid high cardinality.
func LogAssetCacheAccess(ctx context.Context, workerID, assetKey, result string, downloadedBytes, lookupMS, shaVerifyMS int64) {
	meta := cacheAccessContextFromContext(ctx)
	jobID, role := meta.JobID, meta.Role
	if workerID == "" {
		workerID = meta.WorkerID
	}
	payload := map[string]interface{}{
		"event":            "ASSET_CACHE_ACCESS",
		"job_id":           jobID,
		"worker_id":        workerID,
		"asset_key":        assetKey,
		"role":             role,
		"result":           result,
		"downloaded_bytes": downloadedBytes,
		"lookup_ms":        lookupMS,
		"sha_verify_ms":    shaVerifyMS,
	}
	if encoded, err := json.Marshal(payload); err == nil {
		// Write through the standard logger's writer without its timestamp
		// prefix so each line remains valid JSON for structured collectors.
		// Serialize the complete line because cache accesses can be concurrent.
		cacheAccessLogMu.Lock()
		_, _ = fmt.Fprintln(log.Writer(), string(encoded))
		cacheAccessLogMu.Unlock()
	}
}
