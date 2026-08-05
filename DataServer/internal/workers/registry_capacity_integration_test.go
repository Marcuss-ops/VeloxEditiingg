package workers

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
)

func TestRegistryHydratesCapacityAndStateFromLeaseStore(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/capacity.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	reg := New(db)
	ctx := context.Background()
	caps := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(4)},
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	if err := reg.RegisterWorker(ctx, "w-cap", "worker", "127.0.0.1", caps); err != nil {
		t.Fatal(err)
	}
	session := &store.PersistedSession{
		SessionID: "session-cap",
		WorkerID:  "w-cap",
		TokenHash: "hash-cap",
		IPAddress: "127.0.0.1",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	if err := db.InsertSession(session); err != nil {
		t.Fatal(err)
	}

	expires := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
	if _, err := db.DB().Exec(`INSERT INTO tasks (task_id, job_id, status, worker_id, lease_id, lease_expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"task-cap", "job-cap", "RUNNING", "w-cap", "lease-cap", expires, expires, expires); err != nil {
		t.Fatal(err)
	}

	info := reg.GetWorker(ctx, "w-cap")
	if info == nil {
		t.Fatal("worker missing")
	}
	if !info.SessionActive {
		t.Fatal("worker session must be active for the integration projection")
	}
	if !info.Capacity.Authoritative {
		t.Fatal("capacity must be authoritative after successful lease-store hydration")
	}
	if info.Capacity.MaxSlots != 4 || info.Capacity.ActiveSlots != 1 || info.Capacity.AvailableSlots != 3 {
		t.Fatalf("capacity = %#v, want max=4 active=1 available=3", info.Capacity)
	}
	if info.SchedulingState != SchedulingBusy {
		t.Fatalf("SchedulingState = %q, want %q", info.SchedulingState, SchedulingBusy)
	}
	if info.Health != WorkerHealthBusy {
		t.Fatalf("Health = %q, want %q", info.Health, WorkerHealthBusy)
	}
}
