package workers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-server/internal/store"
)

const (
	masterWorkerAcceptanceID    = "master-worker-acceptance-a"
	masterWorkerAcceptanceToken = "master-worker-acceptance-token-a"
	masterWorkerDuplicateToken  = "master-worker-duplicate-token-b"
)

func masterWorkerAcceptanceCapabilities() map[string]interface{} {
	return map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(2)},
			"executors": []interface{}{
				map[string]interface{}{
					"id":             "scene.composite.v1",
					"version":        float64(1),
					"resource_class": "cpu",
					"temporal_mode":  "frame_local",
					"deterministic":  true,
					"cacheable":      true,
				},
			},
		},
	}
}

func seedMasterWorkerAcceptanceSession(t *testing.T, db *store.SQLiteStore, sessionID, tokenHash string, expiresAt time.Time) {
	t.Helper()
	if err := insertMasterWorkerAcceptanceSessionWithRetry(db, &store.PersistedSession{
		SessionID:   sessionID,
		WorkerID:    masterWorkerAcceptanceID,
		SessionType: "control",
		TokenHash:   tokenHash,
		IPAddress:   "127.0.0.1",
		ExpiresAt:   expiresAt,
	}); err != nil {
		t.Fatalf("InsertSession(%s): %v", sessionID, err)
	}
}

// SQLite serializes concurrent writers. A transient busy error is not an
// admission decision, so the acceptance test retries only that transport-level
// condition and still asserts the typed collision result for all other errors.
func insertMasterWorkerAcceptanceSessionWithRetry(db *store.SQLiteStore, sess *store.PersistedSession) error {
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		err = db.InsertSession(sess)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "locked") {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

func setMasterWorkerAcceptanceHeartbeatAge(t *testing.T, reg *Registry, db *store.SQLiteStore, age time.Duration) {
	t.Helper()
	stamp := time.Now().UTC().Add(-age).Format(time.RFC3339)
	if _, err := db.DB().Exec(`UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?`, stamp, masterWorkerAcceptanceID); err != nil {
		t.Fatalf("rewind persisted heartbeat by %s: %v", age, err)
	}
	reg.mu.Lock()
	info, ok := reg.inMem[masterWorkerAcceptanceID]
	if !ok {
		reg.mu.Unlock()
		t.Fatalf("worker %q missing while rewinding heartbeat", masterWorkerAcceptanceID)
	}
	info.LastHB = stamp
	reg.inMem[masterWorkerAcceptanceID] = info
	reg.mu.Unlock()
}

func assertMasterWorkerAcceptanceIdentity(t *testing.T, reg *Registry, wantWorkerID string) *Worker {
	t.Helper()
	info := reg.GetWorker(context.Background(), wantWorkerID)
	if info == nil {
		t.Fatalf("worker %q missing from registry", wantWorkerID)
	}
	if info.WorkerID.String() != wantWorkerID {
		t.Fatalf("worker identity=%q, want %q", info.WorkerID, wantWorkerID)
	}
	return info
}

// TestMasterWorkerAcceptance_RegistrationDuplicateReconnect verifies the
// real master-side identity/session contract:
//   - registration creates exactly one logical WorkerID;
//   - a second active control session with another credential is rejected;
//   - a new session with the original credential is a legitimate reconnect;
//   - the reconnect leaves exactly one active control session and preserves
//     the same logical worker identity.
func TestMasterWorkerAcceptance_RegistrationDuplicateReconnect(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/master-worker-registration.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	reg := New(db)
	if err := reg.RegisterWorker(ctx, masterWorkerAcceptanceID, "worker-a", "127.0.0.1", masterWorkerAcceptanceCapabilities()); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	seedMasterWorkerAcceptanceSession(t, db, "session-a-1", masterWorkerAcceptanceToken, time.Now().UTC().Add(time.Hour))
	if err := reg.HeartbeatWithSession(ctx, "session-a-1", masterWorkerAcceptanceID, "worker-a", "", nil); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	if got := reg.List(ctx); len(got) != 1 {
		t.Fatalf("registered logical workers=%d, want 1", len(got))
	}
	info := assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if !info.SessionActive || info.ConnectionStatus != StatusConnected {
		t.Fatalf("initial state session_active=%v connection=%q, want true/CONNECTED", info.SessionActive, info.ConnectionStatus)
	}

	duplicateErr := db.InsertSession(&store.PersistedSession{
		SessionID:   "session-b-1",
		WorkerID:    masterWorkerAcceptanceID,
		SessionType: "control",
		TokenHash:   masterWorkerDuplicateToken,
		IPAddress:   "127.0.0.2",
		ExpiresAt:   time.Now().UTC().Add(time.Hour),
	})
	if !errors.Is(duplicateErr, store.ErrWorkerIDCollision) {
		t.Fatalf("duplicate credential error=%v, want ErrWorkerIDCollision", duplicateErr)
	}
	var active int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM worker_sessions WHERE worker_id = ? AND session_type = 'control' AND status = 'ACTIVE'`, masterWorkerAcceptanceID).Scan(&active); err != nil {
		t.Fatalf("active session count after collision: %v", err)
	}
	if active != 1 {
		t.Fatalf("active control sessions after collision=%d, want 1", active)
	}

	seedMasterWorkerAcceptanceSession(t, db, "session-a-2", masterWorkerAcceptanceToken, time.Now().UTC().Add(time.Hour))
	if err := reg.HeartbeatWithSession(ctx, "session-a-2", masterWorkerAcceptanceID, "worker-a", "", nil); err != nil {
		t.Fatalf("reconnect heartbeat: %v", err)
	}
	if got := reg.List(ctx); len(got) != 1 {
		t.Fatalf("logical workers after reconnect=%d, want 1", len(got))
	}
	info = assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if !info.SessionActive || info.ConnectionStatus != StatusConnected {
		t.Fatalf("reconnected state session_active=%v connection=%q, want true/CONNECTED", info.SessionActive, info.ConnectionStatus)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM worker_sessions WHERE worker_id = ? AND session_type = 'control' AND status = 'ACTIVE'`, masterWorkerAcceptanceID).Scan(&active); err != nil {
		t.Fatalf("active session count after reconnect: %v", err)
	}
	if active != 1 {
		t.Fatalf("active control sessions after reconnect=%d, want 1", active)
	}
}

// TestMasterWorkerAcceptance_MasterRestartHeartbeatTTLPlacementRecovery
// simulates a master restart by rebuilding Registry from the same SQLite
// database, then drives the heartbeat through stale and disconnected TTLs.
// It proves stale/offline workers are excluded from new placement and that
// recovery with the same WorkerID restores one eligible logical worker.
// This is intentionally a master-side/store-level acceptance suite: the
// gRPC stream protocol has separate handler tests, while this suite verifies
// the durable identity, liveness and placement contract end to end.
//
// TestMasterWorkerAcceptance_ConcurrentDuplicateAdmission verifies the
// race-facing identity invariant: concurrent session admission attempts for
// one WorkerID may produce one winner, but never two active control sessions.
// Different token hashes represent two physical workers claiming the same
// identity; the store collision gate must reject the losers atomically.
func TestMasterWorkerAcceptance_ConcurrentDuplicateAdmission(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/master-worker-concurrent-admission.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	if _, err := db.DB().Exec(`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES(?,?,?,?,?)`, masterWorkerAcceptanceID, "worker-a", "worker", "{}", time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("seed worker: %v", err)
	}

	const attempts = 8
	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := masterWorkerAcceptanceToken
			if i%2 == 1 {
				token = masterWorkerDuplicateToken
			}
			errs <- insertMasterWorkerAcceptanceSessionWithRetry(db, &store.PersistedSession{
				SessionID:   fmt.Sprintf("concurrent-session-%d", i),
				WorkerID:    masterWorkerAcceptanceID,
				SessionType: "control",
				TokenHash:   token,
				IPAddress:   "127.0.0.1",
				ExpiresAt:   time.Now().UTC().Add(time.Hour),
			})
		}()
	}
	wg.Wait()
	close(errs)

	for admissionErr := range errs {
		if admissionErr != nil && !errors.Is(admissionErr, store.ErrWorkerIDCollision) {
			t.Errorf("unexpected concurrent admission error: %v", admissionErr)
		}
	}
	var active int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM worker_sessions WHERE worker_id=? AND session_type='control' AND status='ACTIVE' AND revoked=0`, masterWorkerAcceptanceID).Scan(&active); err != nil {
		t.Fatalf("active session count: %v", err)
	}
	if active != 1 {
		t.Fatalf("concurrent duplicate admission active sessions=%d, want exactly 1", active)
	}
}

func TestMasterWorkerAcceptance_MasterRestartHeartbeatTTLPlacementRecovery(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/master-worker-restart.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	reg := New(db)
	if err := reg.RegisterWorker(ctx, masterWorkerAcceptanceID, "worker-a", "127.0.0.1", masterWorkerAcceptanceCapabilities()); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	seedMasterWorkerAcceptanceSession(t, db, "session-restart-1", masterWorkerAcceptanceToken, time.Now().UTC().Add(time.Hour))
	if err := reg.HeartbeatWithSession(ctx, "session-restart-1", masterWorkerAcceptanceID, "worker-a", "", nil); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	eligible := reg.GetSchedulableWorkers(ctx)
	if len(eligible) != 1 || eligible[0].WorkerID.String() != masterWorkerAcceptanceID {
		t.Fatalf("initial placement eligible=%+v, want the registered worker", eligible)
	}

	// A new Registry instance is the master-restart boundary. The worker
	// row, capabilities, and session are recovered from SQLite rather than
	// recreated with a new identity.
	reg = New(db)
	info := assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if !info.ExecutorRegistrySnapshot().Has("scene.composite.v1", 1) {
		t.Fatalf("executor capability was not recovered across master restart: %+v", info.ExecutorRegistrySnapshot().All())
	}
	if info.WorkerID.String() != masterWorkerAcceptanceID {
		t.Fatalf("WorkerID after master restart=%q, want %q", info.WorkerID, masterWorkerAcceptanceID)
	}

	// First cross the stale TTL: the worker remains registered but must be
	// excluded from placement before the hard disconnected threshold.
	setMasterWorkerAcceptanceHeartbeatAge(t, reg, db, ConnectionStaleThreshold+time.Second)
	info = assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if info.ConnectionStatus != StatusStale {
		t.Fatalf("stale TTL connection=%q, want STALE", info.ConnectionStatus)
	}
	if got := reg.GetSchedulableWorkers(ctx); len(got) != 0 {
		t.Fatalf("stale worker remained placement-eligible: %+v", got)
	}

	// Then cross the hard disconnected TTL. The registered identity remains
	// visible for operator recovery, but no new lease may be placed on it.
	setMasterWorkerAcceptanceHeartbeatAge(t, reg, db, ConnectionDisconnectedThreshold+time.Second)
	info = assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if info.ConnectionStatus != StatusDisconnected {
		t.Fatalf("disconnected TTL connection=%q, want DISCONNECTED", info.ConnectionStatus)
	}
	if got := reg.GetSchedulableWorkers(ctx); len(got) != 0 {
		t.Fatalf("disconnected worker remained placement-eligible: %+v", got)
	}

	// The worker reconnects with the same identity and existing session
	// credential. The heartbeat refreshes the persisted worker row and the
	// same registry instance sees it as CONNECTED again.
	if err := reg.HeartbeatWithSession(ctx, "session-restart-1", masterWorkerAcceptanceID, "worker-a", "", masterWorkerAcceptanceCapabilities()); err != nil {
		t.Fatalf("recovery heartbeat: %v", err)
	}
	info = assertMasterWorkerAcceptanceIdentity(t, reg, masterWorkerAcceptanceID)
	if info.ConnectionStatus != StatusConnected || !info.SessionActive {
		t.Fatalf("recovered state session_active=%v connection=%q, want true/CONNECTED", info.SessionActive, info.ConnectionStatus)
	}
	if !info.ExecutorRegistrySnapshot().Has("scene.composite.v1", 1) {
		t.Fatalf("executor capability disappeared after recovery heartbeat")
	}
	eligible = reg.GetSchedulableWorkers(ctx)
	if len(eligible) != 1 || eligible[0].WorkerID.String() != masterWorkerAcceptanceID {
		t.Fatalf("recovered placement eligible=%+v, want exactly the original worker", eligible)
	}
	if got := reg.List(ctx); len(got) != 1 || got[0].WorkerID.String() != masterWorkerAcceptanceID {
		t.Fatalf("logical workers after recovery=%+v, want exactly one original identity", got)
	}
}
