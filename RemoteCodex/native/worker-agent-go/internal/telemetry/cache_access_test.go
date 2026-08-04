package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"testing"
)

func TestCacheAccessContextPreservesWorkerIdentity(t *testing.T) {
	ctx := WithCacheAccessContext(context.Background(), "job-123", "stock")
	ctx = WithCacheAccessWorkerID(ctx, "worker-7")
	ctx = WithCacheAccessContext(ctx, "", "music")

	value := cacheAccessContextFromContext(ctx)
	if value.JobID != "job-123" || value.Role != "music" || value.WorkerID != "worker-7" {
		t.Fatalf("context = %+v, want job/job role/music worker/worker-7", value)
	}
}

func TestLogAssetCacheAccessEmitsCanonicalJSON(t *testing.T) {
	var buffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&buffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	ctx := WithCacheAccessContext(context.Background(), "job-123", "stock")
	ctx = WithCacheAccessWorkerID(ctx, "worker-7")
	LogAssetCacheAccess(ctx, "", "sha256:abc", "miss", 4096, 12, 3)

	var event map[string]interface{}
	if err := json.Unmarshal(buffer.Bytes(), &event); err != nil {
		t.Fatalf("log is not JSON: %v; output=%q", err, buffer.String())
	}
	for key, want := range map[string]interface{}{
		"event": "ASSET_CACHE_ACCESS", "job_id": "job-123", "worker_id": "worker-7",
		"asset_key": "sha256:abc", "role": "stock", "result": "miss",
		"downloaded_bytes": float64(4096), "lookup_ms": float64(12), "sha_verify_ms": float64(3),
	} {
		if event[key] != want {
			t.Errorf("event[%q] = %#v, want %#v", key, event[key], want)
		}
	}
}
