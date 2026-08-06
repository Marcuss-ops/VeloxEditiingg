package workers

import (
	"context"
	"testing"
	"time"

	"velox-shared/identity"
)

func TestGetSchedulableWorkers_ExcludesStaleHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	if err := reg.RegisterWorker(ctx, "w-stale", "stale-worker", "127.0.0.1", map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(1)},
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if err := reg.Heartbeat(ctx, "w-stale", "stale-worker", "", nil); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	reg.mu.Lock()
	id := identity.ParseWorkerID("w-stale")
	info := reg.inMem[id]
	info.LastHB = time.Now().UTC().Add(-(ConnectionStaleThreshold + time.Second)).Format(time.RFC3339)
	reg.inMem[id] = info
	reg.mu.Unlock()

	eligible := reg.GetSchedulableWorkers(ctx)
	if len(eligible) != 0 {
		t.Fatalf("stale worker remained scheduler-eligible: %+v", eligible)
	}
}
