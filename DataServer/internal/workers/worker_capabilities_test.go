package workers

import (
	"context"
	"testing"
)

func TestRegisterWorkerHydratesTypedExecutorRegistryFromLegacyPayload(t *testing.T) {
	reg := New(nil)
	err := reg.RegisterWorker(context.Background(), "worker-typed", "worker", "127.0.0.1", map[string]interface{}{
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(3)},
			},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	info := reg.GetWorker(context.Background(), "worker-typed")
	if info == nil {
		t.Fatal("worker not registered")
	}
	if !info.ExecutorRegistrySnapshot().Has("scene.composite.v1", 3) {
		t.Fatalf("typed registry missing legacy executor: %+v", info.ExecutorRegistrySnapshot().All())
	}
}
