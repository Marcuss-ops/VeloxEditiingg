package workers

import (
	"context"
	"testing"
	"velox-server/internal/store"
)

func TestRegistryHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	err := reg.Heartbeat(ctx, "w1", "worker-1", "job-1", nil)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	info := reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker to exist")
	}
	// The agent's free-form status string is intentionally NOT stored
	// (worker_state.go): state is derived master-side from the typed
	// dimensions. The heartbeat's current-job signal survives.
	if info.CurrentJob != "job-1" {
		t.Errorf("expected current job job-1, got %s", info.CurrentJob)
	}
}

func TestRegistryHeartbeatRevokedWorker(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(ctx, "w1")

	err := reg.Heartbeat(ctx, "w1", "worker-1", "", nil)
	if err == nil {
		t.Error("expected error for revoked worker heartbeat")
	}
}

func TestRegistryHeartbeatMetadataPersistence(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	ctx := context.Background()

	err = reg.Heartbeat(ctx, "w1", "worker-1", "", map[string]interface{}{
		"code_version":     "v1.0.5",
		"bundle_version":   "v1.0.5",
		"bundle_hash":      "abc123",
		"protocol_version": DefaultWorkerProtocolVersion,
		"engine_version":   "v1.0.5",
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "health_check", "version": float64(1)},
			},
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	reg2 := New(s)
	info := reg2.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker to exist")
	}
	if info.CodeVersion != "v1.0.5" {
		t.Errorf("expected code_version v1.0.5, got %s", info.CodeVersion)
	}
	if info.BundleVersion != "v1.0.5" {
		t.Errorf("expected bundle_version v1.0.5, got %s", info.BundleVersion)
	}
	if info.BundleHash != "abc123" {
		t.Errorf("expected bundle_hash abc123, got %s", info.BundleHash)
	}
	if info.ProtocolVersion != DefaultWorkerProtocolVersion {
		t.Errorf("expected protocol_version %s, got %s", DefaultWorkerProtocolVersion, info.ProtocolVersion)
	}
	if info.EngineVersion != "v1.0.5" {
		t.Errorf("expected engine_version v1.0.5, got %s", info.EngineVersion)
	}
	if !info.ExecutorRegistrySnapshot().Has("health_check", 1) {
		t.Errorf("expected typed executor capability to persist")
	}
}

func TestRegistryHeartbeatJobsCompletedInt64PersistsToMetrics(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_jobs_completed.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	err = reg.Heartbeat(ctx, "w1", "worker-1", "", map[string]interface{}{
		"jobs_completed": int64(7),
		"jobs_failed":    int64(2),
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	info := reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker to exist")
	}
	if got, want := int64FromMap(t, info.Metrics, "jobs_completed"), int64(7); got != want {
		t.Errorf("info.Metrics jobs_completed = %d, want %d", got, want)
	}
	if got, want := int64FromMap(t, info.Metrics, "jobs_failed"), int64(2); got != want {
		t.Errorf("info.Metrics jobs_failed = %d, want %d", got, want)
	}

	// Reload from SQLite and assert persistence.
	reg2 := New(s)
	info2 := reg2.GetWorker(ctx, "w1")
	if info2 == nil {
		t.Fatal("expected worker to exist after reload")
	}
	if got, want := int64FromMap(t, info2.Metrics, "jobs_completed"), int64(7); got != want {
		t.Errorf("reloaded info.Metrics jobs_completed = %d, want %d", got, want)
	}
	if got, want := int64FromMap(t, info2.Metrics, "jobs_failed"), int64(2); got != want {
		t.Errorf("reloaded info.Metrics jobs_failed = %d, want %d", got, want)
	}

	// Verify the SQLite snapshot columns were updated too (regression for
	// the store fallback path).
	var dbCompleted, dbFailed int64
	dbErr := s.DB().QueryRow(
		`SELECT jobs_completed, jobs_failed FROM workers WHERE worker_id = ?`,
		"w1",
	).Scan(&dbCompleted, &dbFailed)
	if dbErr != nil {
		t.Fatalf("failed to read persisted jobs counters: %v", dbErr)
	}
	if dbCompleted != 7 || dbFailed != 2 {
		t.Errorf("persisted jobs_completed=%d jobs_failed=%d, want 7 and 2", dbCompleted, dbFailed)
	}
}
