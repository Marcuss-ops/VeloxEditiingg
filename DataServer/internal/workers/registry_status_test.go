package workers

import (
	"context"
	"testing"
	"time"
	"velox-server/internal/store"
	"velox-shared/identity"
)

func TestRegistryCleanupStaleWorkers(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	ctx := context.Background()

	// Register a worker
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	// Manually set old heartbeat
	reg.mu.Lock()
	info := reg.inMem[identity.ParseWorkerID("w1")]
	info.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem[identity.ParseWorkerID("w1")] = info
	reg.mu.Unlock()

	count := reg.CleanupStaleWorkers(ctx, time.Hour)
	if count != 1 {
		t.Fatalf("expected 1 cleaned up, got %d", count)
	}

	// Verify persistence
	reg2 := New(s)
	workers := reg2.List(ctx)
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers after cleanup, got %d", len(workers))
	}
}

func TestRegistryGetActiveWorkers(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	active := reg.GetActiveWorkers(ctx, 5*time.Minute)
	if len(active) != 1 {
		t.Fatalf("expected 1 active worker, got %d", len(active))
	}
}

func TestRegistryStatusSnapshotSeparatesRegisteredFromLive(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)
	_ = reg.Heartbeat(ctx, "w1", "worker-1", "", nil)
	_ = reg.Heartbeat(ctx, "w2", "worker-2", "", nil)

	reg.mu.Lock()
	stale := reg.inMem[identity.ParseWorkerID("w2")]
	stale.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem[identity.ParseWorkerID("w2")] = stale
	reg.mu.Unlock()

	registered, live := reg.StatusSnapshot(ctx, 5*time.Minute)
	if len(registered) != 2 {
		t.Fatalf("expected 2 registered workers, got %d", len(registered))
	}
	if len(live) != 1 {
		t.Fatalf("expected 1 live worker, got %d", len(live))
	}
	if live[0].WorkerID != "w1" {
		t.Fatalf("expected w1 to be live, got %s", live[0].WorkerID)
	}
}

func TestRegistryGetStaleWorkers(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)
	_ = reg.Heartbeat(ctx, "w1", "worker-1", "", nil)
	_ = reg.Heartbeat(ctx, "w2", "worker-2", "", nil)

	reg.mu.Lock()
	stale := reg.inMem[identity.ParseWorkerID("w2")]
	stale.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem[identity.ParseWorkerID("w2")] = stale
	reg.mu.Unlock()

	staleWorkers := reg.GetStaleWorkers(ctx, 5*time.Minute)
	if len(staleWorkers) != 1 {
		t.Fatalf("expected 1 stale worker, got %d", len(staleWorkers))
	}
	if staleWorkers[0].WorkerID != "w2" {
		t.Fatalf("expected w2 to be stale, got %s", staleWorkers[0].WorkerID)
	}
}

func TestRegistryConnectionStatus_SessionDropAndOldHeartbeat(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_connection_registry.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	ctx := context.Background()

	// insertSession inserts a non-revoked, non-expired worker session.
	insertSession := func(workerID, sessionID string) {
		sess := &store.PersistedSession{
			SessionID: sessionID,
			WorkerID:  workerID,
			TokenHash: "hash-" + sessionID,
			IPAddress: "10.0.0.1",
			ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
		}
		if err := s.InsertSession(sess); err != nil {
			t.Fatalf("InsertSession(%s) failed: %v", sessionID, err)
		}
	}

	// setHB rewinds the worker's last_heartbeat to now-age. Follows the
	// existing pattern in TestRegistryCleanupStaleWorkers (write under
	// the registry's mutex to bypass Heartbeat's mutator path).
	setHB := func(workerID string, age time.Duration) {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		info := reg.inMem[identity.ParseWorkerID(workerID)]
		info.LastHB = time.Now().UTC().Add(-age).Format(time.RFC3339)
		reg.inMem[identity.ParseWorkerID(workerID)] = info
	}

	// ── 1. CONNECTED — fresh session + fresh heartbeat ─────────────
	if err := reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	insertSession("w1", "sess-fresh")

	info := reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker w1 to exist after registration")
	}
	if !info.SessionActive {
		t.Errorf("step 1: expected SessionActive=true with active session; got false")
	}
	if info.ConnectionStatus != StatusConnected {
		t.Errorf("step 1: expected CONNECTED with fresh session+heartbeat; got %q (info=%+v)",
			info.ConnectionStatus, info)
	}

	// ── 2. session_drop while heartbeat is fresh → DISCONNECTED ────
	if err := s.RevokeSession("sess-fresh"); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}

	info = reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker w1 still present after session revoke")
	}
	if info.SessionActive {
		t.Errorf("step 2: expected SessionActive=false after revocation; got true")
	}
	if info.ConnectionStatus != StatusDisconnected {
		t.Errorf("step 2: expected DISCONNECTED on session_drop (heartbeat still fresh); got %q",
			info.ConnectionStatus)
	}

	// ── 3. STALE — fresh session + heartbeat 3min ago ───────────────
	insertSession("w1", "sess-stale")
	setHB("w1", 3*time.Minute)

	info = reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker w1 after second session insert + HB rewind")
	}
	if !info.SessionActive {
		t.Errorf("step 3: expected SessionActive=true; got false")
	}
	if info.ConnectionStatus != StatusStale {
		t.Errorf("step 3: expected STALE with fresh session + 3min-old heartbeat; got %q",
			info.ConnectionStatus)
	}

	// ── 4. DISCONNECTED — heartbeat 6min ago, even with active session
	setHB("w1", 6*time.Minute)

	info = reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker w1")
	}
	if !info.SessionActive {
		t.Errorf("step 4: SessionActive should still be true (worker_sessions unchanged); got false")
	}
	if info.ConnectionStatus != StatusDisconnected {
		t.Errorf("step 4: expected DISCONNECTED with 6min heartbeat age; got %q",
			info.ConnectionStatus)
	}

	// ── 5. DRAINING — drain=true overrides a fresh session/heartbeat ───
	// sess-stale from step 3 is still ACTIVE (not revoked), which is the
	// canonical precondition for a CONNECTED baseline before SetWorkerDrain
	// flips the connection status to DRAINING. Inserting a NEW session
	// here would collide with the still-active sess-stale (different
	// token_hash → ErrWorkerIDCollision), so we deliberately reuse the
	// existing active session + fresh heartbeat instead of calling
	// insertSession again.
	setHB("w1", 0) // fresh

	info = reg.GetWorker(ctx, "w1")
	if info.ConnectionStatus != StatusConnected {
		t.Errorf("step 5 pre-drain: expected CONNECTED baseline; got %q", info.ConnectionStatus)
	}
	if err := reg.SetWorkerDrain(ctx, "w1", true); err != nil {
		t.Fatalf("SetWorkerDrain: %v", err)
	}

	info = reg.GetWorker(ctx, "w1")
	if info.ConnectionStatus != StatusDraining {
		t.Errorf("step 5: expected DRAINING override on fresh session/heartbeat; got %q",
			info.ConnectionStatus)
	}
}

func TestRegistryListPopulatesSessionActive_AcrossFleet(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_list_session.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	defer s.Close()

	reg := New(s)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)

	sess := &store.PersistedSession{
		SessionID: "sess-w1",
		WorkerID:  "w1",
		TokenHash: "hash-w1",
		IPAddress: "10.0.0.1",
		ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
	}
	if err := s.InsertSession(sess); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	list := reg.List(ctx)
	if len(list) != 2 {
		t.Fatalf("expected 2 registered workers, got %d", len(list))
	}

	got := make(map[string]Worker, len(list))
	for _, w := range list {
		got[w.WorkerID.String()] = w
	}
	if !got["w1"].SessionActive {
		t.Errorf("w1.SessionActive: want true (active session inserted); got false (info=%+v)", got["w1"])
	}
	if got["w1"].ConnectionStatus != StatusConnected {
		t.Errorf("w1.ConnectionStatus: want CONNECTED; got %q", got["w1"].ConnectionStatus)
	}
	if got["w2"].SessionActive {
		t.Errorf("w2.SessionActive: want false (no session inserted); got true (info=%+v)", got["w2"])
	}
	if got["w2"].ConnectionStatus != StatusDisconnected {
		t.Errorf("w2.ConnectionStatus: want DISCONNECTED (no session); got %q", got["w2"].ConnectionStatus)
	}
}
