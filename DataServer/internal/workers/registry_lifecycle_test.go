package workers

import (
	"context"
	"testing"
	"time"
	"velox-server/internal/store"
)

func TestRegistryRegisterAndList(t *testing.T) {
	reg := newTestRegistry(t)
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
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	// Register a worker
	reg1 := New(s)
	_ = reg1.RegisterWorker(context.Background(), "w1", "worker-1", "10.0.0.1", nil)

	// Create new registry from same database
	reg2 := New(s)
	workers := reg2.List(context.Background())
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker after reload, got %d", len(workers))
	}
	if workers[0].WorkerID != "w1" {
		t.Errorf("expected worker ID w1, got %s", workers[0].WorkerID)
	}
}

func TestRegistryRevokeAndPersist(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
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
	reg2 := New(s)
	if !reg2.IsRevoked("w1") {
		t.Error("expected w1 to be revoked after reload")
	}
}

func TestRegistryRevokeFailsClosedWhenPersistenceFails(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	reg := New(s)
	ctx := context.Background()
	if err := reg.RegisterWorker(ctx, "w-revoke-fail", "worker-revoke-fail", "10.0.0.1", nil); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := reg.RevokeWorker(ctx, "w-revoke-fail"); err == nil {
		t.Fatal("RevokeWorker should fail when persistence is unavailable")
	}
	if reg.IsRevoked("w-revoke-fail") {
		t.Fatal("failed revoke must not advance the in-memory revoked projection")
	}
	if reg.GetWorker(ctx, "w-revoke-fail") == nil {
		t.Fatal("failed revoke must keep the in-memory worker projection intact")
	}
}

func TestNewWithErrorFailsClosedWhenPersistenceCannotLoad(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reg, err := NewWithError(s)
	if err == nil {
		t.Fatal("NewWithError should fail when SQLite cannot be read")
	}
	if reg != nil {
		t.Fatalf("NewWithError returned partial registry: %#v", reg)
	}
}

func TestRegistryUnrevokeFailsClosedWhenPersistenceFails(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	reg := New(s)
	ctx := context.Background()
	if err := reg.RegisterWorker(ctx, "w-unrevoke-fail", "worker-unrevoke-fail", "10.0.0.1", nil); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if err := reg.RevokeWorker(ctx, "w-unrevoke-fail"); err != nil {
		t.Fatalf("revoke worker: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := reg.UnrevokeWorker("w-unrevoke-fail"); err == nil {
		t.Fatal("UnrevokeWorker should fail when persistence is unavailable")
	}
	if !reg.IsRevoked("w-unrevoke-fail") {
		t.Fatal("failed unrevoke must keep the in-memory revoked projection")
	}
}

func TestRegistryUnrevoke(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	_ = reg.RegisterWorker(context.Background(), "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(context.Background(), "w1")
	reg.UnrevokeWorker("w1")

	if reg.IsRevoked("w1") {
		t.Error("expected w1 to not be revoked")
	}

	// Reload and verify
	reg2 := New(s)
	if reg2.IsRevoked("w1") {
		t.Error("expected w1 to not be revoked after reload")
	}
}

func TestRegistryUpdateWorker(t *testing.T) {
	reg := newTestRegistry(t)
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
	reg := newTestRegistry(t)
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
			_ = reg.Heartbeat(ctx, "w"+string(rune('0'+i%10)), "worker", "", nil)
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
	reg := newTestRegistry(t)
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
