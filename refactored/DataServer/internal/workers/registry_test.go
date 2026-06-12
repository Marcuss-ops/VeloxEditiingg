package workers

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegistryNewWithPersistence(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}
	if reg.filePath == "" {
		t.Error("expected filePath to be set")
	}
}

func TestRegistryRegisterAndList(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	err := reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	if err != nil {
		t.Fatalf("RegisterWorker failed: %v", err)
	}

	workers := reg.List(ctx)
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}
	if workers[0].WorkerID != "w1" {
		t.Errorf("expected worker ID w1, got %s", workers[0].WorkerID)
	}
}

func TestRegistryRegisterPersistence(t *testing.T) {
	dir := t.TempDir()

	// Register a worker
	reg1 := NewWithPersistence(nil, false, nil, dir)
	_ = reg1.RegisterWorker(context.Background(), "w1", "worker-1", "10.0.0.1", nil)

	// Create new registry from same file
	reg2 := NewWithPersistence(nil, false, nil, dir)
	workers := reg2.List(context.Background())
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker after reload, got %d", len(workers))
	}
	if workers[0].WorkerID != "w1" {
		t.Errorf("expected worker ID w1, got %s", workers[0].WorkerID)
	}
}

func TestRegistryRevokeAndPersist(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(ctx, "w1")

	// Worker should be removed from active list
	workers := reg.List(ctx)
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers after revoke, got %d", len(workers))
	}

	// Revoked list should persist
	revoked := reg.ListRevoked()
	if len(revoked) != 1 {
		t.Fatalf("expected 1 revoked, got %d", len(revoked))
	}

	// Reload and verify revoked persists
	reg2 := NewWithPersistence(nil, false, nil, dir)
	if !reg2.IsRevoked("w1") {
		t.Error("expected w1 to be revoked after reload")
	}
}

func TestRegistryUnrevoke(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)

	_ = reg.RegisterWorker(context.Background(), "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(context.Background(), "w1")
	reg.UnrevokeWorker("w1")

	if reg.IsRevoked("w1") {
		t.Error("expected w1 to not be revoked")
	}

	// Reload and verify
	reg2 := NewWithPersistence(nil, false, nil, dir)
	if reg2.IsRevoked("w1") {
		t.Error("expected w1 to not be revoked after reload")
	}
}

func TestRegistryHeartbeat(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	err := reg.Heartbeat(ctx, "w1", "worker-1", "busy", "job-1", nil)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	info := reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker to exist")
	}
	if info.Status != "busy" {
		t.Errorf("expected status busy, got %s", info.Status)
	}
	if info.CurrentJob != "job-1" {
		t.Errorf("expected current job job-1, got %s", info.CurrentJob)
	}
}

func TestRegistryHeartbeatRevokedWorker(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(ctx, "w1")

	err := reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", nil)
	if err == nil {
		t.Error("expected error for revoked worker heartbeat")
	}
}

func TestRegistryHeartbeatMetadataPersistence(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	err := reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", map[string]interface{}{
		"code_version":     "v1.0.5",
		"bundle_version":   "v1.0.5",
		"bundle_hash":      "abc123",
		"protocol_version": DefaultWorkerProtocolVersion,
		"engine_version":   "v1.0.5",
		"capabilities": map[string]interface{}{
			"ffmpeg":              true,
			"supported_job_types": []string{"health_check"},
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	reg2 := NewWithPersistence(nil, false, nil, dir)
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
	if info.Capabilities == nil || info.Capabilities["ffmpeg"] != true {
		t.Errorf("expected capabilities to persist")
	}
}

func TestRegistryCleanupStaleWorkers(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	// Register a worker
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	// Manually set old heartbeat
	reg.mu.Lock()
	info := reg.inMem["w1"]
	info.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem["w1"] = info
	reg.mu.Unlock()

	count := reg.CleanupStaleWorkers(ctx, time.Hour)
	if count != 1 {
		t.Fatalf("expected 1 cleaned up, got %d", count)
	}

	// Verify persistence
	reg2 := NewWithPersistence(nil, false, nil, dir)
	workers := reg2.List(ctx)
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers after cleanup, got %d", len(workers))
	}
}

func TestRegistryGetSchedulableWorkers(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)

	// Set w1 to drain
	_ = reg.SetWorkerDrain(ctx, "w1", true)

	schedulable := reg.GetSchedulableWorkers(ctx)
	if len(schedulable) != 1 {
		t.Fatalf("expected 1 schedulable worker, got %d", len(schedulable))
	}
	if schedulable[0].WorkerID != "w2" {
		t.Errorf("expected schedulable worker w2, got %s", schedulable[0].WorkerID)
	}
}

func TestRegistryUpdateWorker(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	err := reg.UpdateWorker(ctx, "w1", map[string]interface{}{
		"worker_group": "gpu",
		"code_version": "v1.2.3",
	})
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	info := reg.GetWorker(ctx, "w1")
	if info.WorkerGroup != "gpu" {
		t.Errorf("expected worker_group gpu, got %s", info.WorkerGroup)
	}
	if info.CodeVersion != "v1.2.3" {
		t.Errorf("expected code_version v1.2.3, got %s", info.CodeVersion)
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	// Concurrent registrations
	for i := 0; i < 100; i++ {
		go func(i int) {
			_ = reg.RegisterWorker(ctx, "w"+string(rune('0'+i%10)), "worker", "10.0.0.1", nil)
		}(i)
	}

	// Concurrent heartbeats
	for i := 0; i < 100; i++ {
		go func(i int) {
			_ = reg.Heartbeat(ctx, "w"+string(rune('0'+i%10)), "worker", "idle", "", nil)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		go func() {
			_ = reg.List(ctx)
			_ = reg.GetSchedulableWorkers(ctx)
		}()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestRegistryWorkerGroup(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)

	_ = reg.SetWorkerGroup(ctx, "w1", "gpu")

	groupWorkers := reg.GetWorkersByGroup(ctx, "gpu")
	if len(groupWorkers) != 1 {
		t.Fatalf("expected 1 worker in gpu group, got %d", len(groupWorkers))
	}
	if groupWorkers[0].WorkerID != "w1" {
		t.Errorf("expected worker w1 in gpu group, got %s", groupWorkers[0].WorkerID)
	}
}

func TestRegistryGetActiveWorkers(t *testing.T) {
	dir := t.TempDir()
	reg := NewWithPersistence(nil, false, nil, dir)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	active := reg.GetActiveWorkers(ctx, 5*time.Minute)
	if len(active) != 1 {
		t.Fatalf("expected 1 active worker, got %d", len(active))
	}
}

func TestRegistryFileCorrupt(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "workers.json")
	_ = os.WriteFile(filePath, []byte("not json"), 0644)

	// Should not panic, just log error
	reg := NewWithPersistence(nil, false, nil, dir)
	if reg == nil {
		t.Fatal("expected non-nil registry even with corrupt file")
	}
}
