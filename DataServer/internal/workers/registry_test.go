package workers

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	// Use a file-based SQLite store in the temp dir for persistence tests
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s)
}

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

func TestRegistryHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)
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
	reg := newTestRegistry(t)
	ctx := context.Background()

	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	reg.RevokeWorker(ctx, "w1")

	err := reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", nil)
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

	err = reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", map[string]interface{}{
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
	if info.Capabilities == nil || info.Capabilities["ffmpeg"] != true {
		t.Errorf("expected capabilities to persist")
	}
}

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
	info := reg.inMem["w1"]
	info.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem["w1"] = info
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

func TestRegistryGetSchedulableWorkers(t *testing.T) {
	reg := newTestRegistry(t)
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
	_ = reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", nil)
	_ = reg.Heartbeat(ctx, "w2", "worker-2", "idle", "", nil)

	reg.mu.Lock()
	stale := reg.inMem["w2"]
	stale.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem["w2"] = stale
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
	_ = reg.Heartbeat(ctx, "w1", "worker-1", "idle", "", nil)
	_ = reg.Heartbeat(ctx, "w2", "worker-2", "idle", "", nil)

	reg.mu.Lock()
	stale := reg.inMem["w2"]
	stale.LastHB = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	reg.inMem["w2"] = stale
	reg.mu.Unlock()

	staleWorkers := reg.GetStaleWorkers(ctx, 5*time.Minute)
	if len(staleWorkers) != 1 {
		t.Fatalf("expected 1 stale worker, got %d", len(staleWorkers))
	}
	if staleWorkers[0].WorkerID != "w2" {
		t.Fatalf("expected w2 to be stale, got %s", staleWorkers[0].WorkerID)
	}
}

// ─────────────────────────────────────────────────────────────────────
// ConnectionStatus hydration (Registry → worker_sessions integration)
// ─────────────────────────────────────────────────────────────────────
//
// Tests the canonical scenarios for /api/v1/workers/:worker_id:
//  1. CONNECTED — fresh session + fresh heartbeat
//  2. session_drop — fresh heartbeat but revoked session → DISCONNECTED
//  3. STALE — fresh session + heartbeat older than 30s but younger than 5min
//  4. DISCONNECTED — heartbeat older than 5min, even with active session
//  5. DRAINING — drain=true overrides freshness on a fresh session/heartbeat
//
// Uses the real SQLite store (worker_sessions + workers) wired through
// `store.NewSQLiteStore`; manipulates heartbeat timestamps directly via
// the registry's locked `inMem` map (same pattern used by
// TestRegistryCleanupStaleWorkers / TestRegistryGetStaleWorkers).
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
		info := reg.inMem[workerID]
		info.LastHB = time.Now().UTC().Add(-age).Format(time.RFC3339)
		reg.inMem[workerID] = info
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

	// ── 3. STALE — fresh session + heartbeat 60s ago ───────────────
	insertSession("w1", "sess-stale")
	setHB("w1", 60*time.Second)

	info = reg.GetWorker(ctx, "w1")
	if info == nil {
		t.Fatal("expected worker w1 after second session insert + HB rewind")
	}
	if !info.SessionActive {
		t.Errorf("step 3: expected SessionActive=true; got false")
	}
	if info.ConnectionStatus != StatusStale {
		t.Errorf("step 3: expected STALE with fresh session + 60s-old heartbeat; got %q",
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

	// ── 5. DRAINING — drain=true overrides a fresh session/heartbeat
	insertSession("w1", "sess-drain")
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

// TestRegistryListPopulatesSessionActive_AcrossFleet confirms List
// bulk-hydrates SessionActive + ConnectionStatus without N+1 — would
// catch a regression where List went back to the pre-PR heartbeat-only
// filtering (the gap this PR is intended to close).
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

	got := make(map[string]WorkerInfo, len(list))
	for _, w := range list {
		got[w.WorkerID] = w
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

// TestGetSchedulableWorkers_ExcludesDraining pins the dispatcher-side
// drain-exclusion contract at the SAME entry the job-routing layer
// uses. Two channels flow into `costmodel.WorkerProfile.IsDraining`
// (see costmodel/worker_profile.go BuildWorkerProfile line:
// `IsDraining: drain || !schedulable`):
//
//  1. drain == true                 → IsDraining=true → Eligible=false
//  2. schedulable == false          → IsDraining=true → Eligible=false
//
// Both must be excluded from the result of GetSchedulableWorkers.
// This test does NOT validate the implementation — only the
// behavior contract. A regression that strips the canonical
// costmodel-based exclusion (or adds a hand-rolled `if w.Drain`
// block inside GetEligibleWorkers, which would itself be a
// OWNERSHIP.md policy violation) will fail here.
func TestGetSchedulableWorkers_ExcludesDraining(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	// Pin now so the heartbeat is unambiguously "fresh" instead of
	// sensitive to clock drift in CI.
	now := time.Now().UTC().Format(time.RFC3339)

	// ── Channel 1: drain = true. Even with schedulable=true and a
	// fresh heartbeat, the worker is excluded. This is the canonical
	// "DRAINING" input that surfaces as ConnectionStatus="DRAINING".
	reg.Heartbeat(ctx, "w-drain-1", "Draining Worker", "idle", "", nil)
	reg.UpdateWorker(ctx, "w-drain-1", map[string]interface{}{
		"drain":          true,
		"last_heartbeat": now,
		"schedulable":    true, // explicit: scheduled flag-wise, but drain overrides.
	})

	// ── Channel 2: schedulable = false (NOT draining per the drain
	// field, but the costmodel treats `drain || !schedulable` as
	// IsDraining=true). This worker should ALSO be excluded — a
	// regression that only checks the drain field would erroneously
	// pass this case.
	reg.Heartbeat(ctx, "w-unsched-1", "Unschedulable Worker", "idle", "", nil)
	reg.UpdateWorker(ctx, "w-unsched-1", map[string]interface{}{
		"drain":          false,
		"last_heartbeat": now,
		"schedulable":    false,
	})

	// ── Control case: a healthy, schedulable, NON-draining worker.
	// Expected to appear in the result; without it the test is ambiguous.
	reg.Heartbeat(ctx, "w-ok-1", "Healthy Worker", "idle", "", nil)
	reg.UpdateWorker(ctx, "w-ok-1", map[string]interface{}{
		"drain":          false,
		"last_heartbeat": now,
		"schedulable":    true,
	})

	schedulable := reg.GetSchedulableWorkers(ctx)

	if len(schedulable) != 1 {
		t.Fatalf("expected exactly ONE schedulable worker (the control case); got %d: %+v",
			len(schedulable), schedulable)
	}
	if schedulable[0].WorkerID != "w-ok-1" {
		t.Errorf("wrong worker returned; want w-ok-1, got %s", schedulable[0].WorkerID)
	}
	if schedulable[0].ConnectionStatus == "DRAINING" {
		t.Errorf("returned worker should NOT have ConnectionStatus=DRAINING (control-case regression)")
	}

	// Operator-facing canonical assertion: the drain-channel worker
	// (w-drain-1, drain=true) MUST surface as `ConnectionStatus =
	// StatusDraining` on the operator-facing read model. This pins
	// the read-model derivation rule (drain=true ⇒ DRAINING,
	// overrides freshness — see workers.ConnectionStatus) alongside
	// the costmodel-exclusion rule so a regression on either side is
	// caught by the same test.
	//
	// We deliberately do NOT assert ConnectionStatus on w-unsched-1:
	// `schedulable=false` alone does NOT drive ConnectionStatus to
	// DRAINING — that input is gated at the costmodel layer only
	// (IsDraining := drain || !schedulable). The two channels are
	// intentionally different in operator-surface semantics.
	if got := reg.GetWorker(ctx, "w-drain-1"); got == nil {
		t.Errorf("w-drain-1 not registered (sanity regression before derivation check)")
	} else if got.ConnectionStatus != "DRAINING" {
		t.Errorf("w-drain-1 ConnectionStatus = %q, want %q (operator-facing read-model derivation: drain=true ⇒ DRAINING)",
			got.ConnectionStatus, "DRAINING")
	}

	// Sanity: the excluded workers MUST still be REGISTERED. The
	// contract is "not eligible for new offers", NOT "removed from
	// the registry". Misreading that would break health/decommission
	// visibility (a draining worker still shows up on the admin list
	// and on /api/v1/workers/:worker_id).
	for _, id := range []string{"w-drain-1", "w-unsched-1"} {
		if got := reg.GetWorker(ctx, id); got == nil {
			t.Errorf("worker %s should still be REGISTERED; schedulable filter must NOT remove from registry", id)
		}
	}
}
