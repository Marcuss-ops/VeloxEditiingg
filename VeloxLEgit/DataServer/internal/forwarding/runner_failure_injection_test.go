package forwarding

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// TestTick_DBOutage_PropagatesAsInfrastructure verifies that a DB
// outage during ClaimCreatorForwardings is propagated as an error from
// tick() and classified as ErrInfrastructure by the supervisor. This
// is the P0-02 contract: DB failures must NOT be silently swallowed
// (false-success) — they must escalate so the FailureTracker can
// trigger a restart.
func TestTick_DBOutage_PropagatesAsInfrastructure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cf_outage_test.sqlite")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	// Do NOT register t.Cleanup Close — we close manually below.
	insertTestForwardingRecord(t, db, "cf-db-outage", "openai", "src-outage", "scene.composite.v1", "PENDING")

	// Close the DB to simulate an outage.
	db.Close()

	client := remoteengine.NewClient(remoteengine.Config{URL: "http://localhost:9999"})
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, client, nil, "test-outage")

	err = r.tick(context.Background())
	if err == nil {
		t.Fatal("tick with closed DB should return error, got nil (false-success)")
	}

	classified := supervisor.ClassifyError(err)
	if !errors.Is(classified, supervisor.ErrInfrastructure) {
		t.Errorf("closed DB error should classify as ErrInfrastructure, got %v (classified: %v)", err, classified)
	}
}

// TestTick_MetricsAfterCAS verifies that the Claimed metric is
// incremented only after ClaimCreatorForwardings returns successfully,
// not before. A DB failure must NOT increment Claimed.
func TestTick_MetricsAfterCAS(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cf_metrics_test.sqlite")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	insertTestForwardingRecord(t, db, "cf-metrics", "openai", "src-metrics", "scene.composite.v1", "PENDING")

	// Close DB to force a claim failure.
	db.Close()

	client := remoteengine.NewClient(remoteengine.Config{URL: "http://localhost:9999"})
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, client, nil, "test-metrics")

	_ = r.tick(context.Background())

	if got := r.metrics.Claimed.Load(); got != 0 {
		t.Errorf("Claimed metric = %d after DB failure, want 0 (metric must be post-CAS only)", got)
	}
}

// TestTick_EffectiveClaimBatch_CappedAtConcurrency verifies that when
// ClaimBatch > Concurrency, the effective claim batch passed to
// ClaimCreatorForwardings is capped at Concurrency. This prevents
// leases from being claimed and then sitting behind the semaphore
// without renewal.
func TestTick_EffectiveClaimBatch_CappedAtConcurrency(t *testing.T) {
	db := setupRunnerTestDB(t)

	// Insert 10 PENDING records — more than Concurrency (2).
	for i := 0; i < 10; i++ {
		insertTestForwardingRecord(t, db,
			"cf-batch-"+string(rune('A'+i)),
			"openai", "src-batch-"+string(rune('A'+i)),
			"scene.composite.v1", "PENDING")
	}

	// Configure ClaimBatch=10, Concurrency=2. The runner should cap
	// the claim at 2, not 10.
	cfg := &RunnerConfig{
		PollInterval:    1 * time.Second,
		LeaseDuration:   5 * time.Minute,
		ClaimBatch:      10,
		Concurrency:     2,
		MaxAttempts:     12,
		BackoffSchedule: []time.Duration{30 * time.Second},
	}

	// Use a configured client (pointing to a non-existent server) so
	// tick() proceeds past the IsConfigured guard and actually claims
	// rows. The remote poll will fail, triggering handleRetry.
	client := remoteengine.NewClient(remoteengine.Config{URL: "http://localhost:1"})
	r := NewCreatorForwardingRunner(cfg, db, client, nil, "test-batch")

	err := r.tick(context.Background())
	if err != nil {
		t.Fatalf("tick returned error: %v", err)
	}

	// The Claimed metric should be exactly Concurrency (2), not
	// ClaimBatch (10). This proves the batch was capped at the source.
	if got := r.metrics.Claimed.Load(); got != int64(cfg.Concurrency) {
		t.Errorf("Claimed = %d, want %d (Concurrency cap)", got, cfg.Concurrency)
	}

	// At most Concurrency rows should have been claimed (POLLING).
	// The remaining 8 should still be PENDING.
	pendingCount := 0
	pollingCount := 0
	for i := 0; i < 10; i++ {
		cf, getErr := db.GetCreatorForwarding(context.Background(), "cf-batch-"+string(rune('A'+i)))
		if getErr != nil {
			t.Fatalf("get forwarding: %v", getErr)
		}
		switch cf.Status {
		case "PENDING":
			pendingCount++
		case "POLLING", "RETRY_WAIT":
			pollingCount++
		}
	}
	if pollingCount > cfg.Concurrency {
		t.Errorf("%d rows claimed/processed, want at most %d (Concurrency cap)", pollingCount, cfg.Concurrency)
	}
	if pendingCount < 10-cfg.Concurrency {
		t.Errorf("only %d rows still PENDING, want at least %d", pendingCount, 10-cfg.Concurrency)
	}
}

// TestTick_EffectiveClaimBatch_ParallelNoDeadlock verifies that tick
// with ClaimBatch > Concurrency does not deadlock when the semaphore
// is contended. Uses a configured client so the claim actually happens.
func TestTick_EffectiveClaimBatch_ParallelNoDeadlock(t *testing.T) {
	db := setupRunnerTestDB(t)

	for i := 0; i < 6; i++ {
		insertTestForwardingRecord(t, db,
			"cf-par-"+string(rune('A'+i)),
			"openai", "src-par-"+string(rune('A'+i)),
			"scene.composite.v1", "PENDING")
	}

	cfg := &RunnerConfig{
		PollInterval:    1 * time.Second,
		LeaseDuration:   5 * time.Minute,
		ClaimBatch:      6,
		Concurrency:     3,
		MaxAttempts:     12,
		BackoffSchedule: []time.Duration{30 * time.Second},
	}

	client := remoteengine.NewClient(remoteengine.Config{URL: "http://localhost:1"})
	r := NewCreatorForwardingRunner(cfg, db, client, nil, "test-par")

	done := make(chan struct{})
	go func() {
		_ = r.tick(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// Success — no deadlock.
	case <-time.After(10 * time.Second):
		t.Fatal("tick deadlocked (semaphore not released)")
	}
}

// TestLazyResolver_ConcurrentInit verifies that lazyResolver is
// thread-safe under concurrent first-call access. Without sync.Once,
// two goroutines could both build a minimal Resolver, causing a data
// race on r.resolver. This test runs with -race to detect the race.
func TestLazyResolver_ConcurrentInit(t *testing.T) {
	db := setupRunnerTestDB(t)
	enqueuer := &enqueue.Enqueuer{} // zero-value enqueuer (lazyResolver checks nil on resolver, not enqueuer)

	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, nil, enqueuer, "test-lazy")

	var wg sync.WaitGroup
	var callCount atomic.Int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs := r.lazyResolver()
			if rs != nil {
				callCount.Add(1)
			}
		}()
	}
	wg.Wait()

	// All 20 goroutines should get a non-nil resolver. With sync.Once,
	// exactly one initialisation occurs; all callers get the same pointer.
	if got := callCount.Load(); got != 20 {
		t.Errorf("lazyResolver concurrent init: %d goroutines got non-nil resolver, want 20", got)
	}
}

// TestSetResolver_Idempotent verifies that SetResolver followed by
// lazyResolver returns the injected resolver, not a lazy-built one.
func TestSetResolver_Idempotent(t *testing.T) {
	db := setupRunnerTestDB(t)
	enqueuer := &enqueue.Enqueuer{}

	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, nil, enqueuer, "test-set")

	injected := creatorflow.NewResolverMinimal(enqueuer, db)
	if injected == nil {
		t.Fatal("NewResolverMinimal returned nil")
	}
	r.SetResolver(injected)

	got := r.lazyResolver()
	if got != injected {
		t.Error("lazyResolver after SetResolver should return the injected resolver, not a lazy-built one")
	}
}

// TestEnsureForwarded_IdempotentRepair verifies the EnsureForwarded
// store method: a forwarding row stuck in FORWARDING (simulating a
// crash after Job INSERT but before the FORWARDED CAS) is repaired to
// FORWARDED with the correct target_job_id.
func TestEnsureForwarded_IdempotentRepair(t *testing.T) {
	db := setupRunnerTestDB(t)
	insertTestForwardingRecord(t, db, "cf-repair", "openai", "src-repair", "scene.composite.v1", "PENDING")

	// Simulate a crash: manually move the row to FORWARDING (as if
	// AtomicForwardAndEnqueue started but didn't finish).
	ctx := context.Background()
	_, err := db.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-repair-2",
		SourceProvider:   "openai",
		SourceJobID:      "src-repair-2",
		TargetExecutorID: "scene.composite.v1",
		Status:           "FORWARDING",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}

	// EnsureForwarded should stamp it to FORWARDED.
	err = db.EnsureForwarded(ctx, "cf-repair-2", "job-derived-id-123")
	if err != nil {
		t.Fatalf("EnsureForwarded: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, "cf-repair-2")
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if cf.Status != "FORWARDED" {
		t.Errorf("status = %q, want FORWARDED", cf.Status)
	}
	if cf.TargetJobID != "job-derived-id-123" {
		t.Errorf("target_job_id = %q, want job-derived-id-123", cf.TargetJobID)
	}

	// Second call should be idempotent (no-op, nil error).
	err = db.EnsureForwarded(ctx, "cf-repair-2", "job-derived-id-123")
	if err != nil {
		t.Errorf("idempotent EnsureForwarded should return nil, got %v", err)
	}
}

// TestEnsureForwarded_DivergentJobID verifies that EnsureForwarded
// refuses to overwrite a FORWARDED row that points to a different job.
func TestEnsureForwarded_DivergentJobID(t *testing.T) {
	db := setupRunnerTestDB(t)
	ctx := context.Background()

	_, err := db.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-divergent",
		SourceProvider:   "openai",
		SourceJobID:      "src-divergent",
		TargetExecutorID: "scene.composite.v1",
		Status:           "FORWARDED",
		TargetJobID:      "job-original-456",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}

	err = db.EnsureForwarded(ctx, "cf-divergent", "job-different-789")
	if err == nil {
		t.Fatal("EnsureForwarded with divergent job_id should return error, got nil")
	}
	if !errors.Is(err, store.ErrTransitionConflict) {
		t.Errorf("EnsureForwarded divergent should return ErrTransitionConflict, got %v", err)
	}
}

// TestEnsureForwarded_TerminalState verifies that EnsureForwarded
// refuses to repair a row in FAILED or BLOCKED state.
func TestEnsureForwarded_TerminalState(t *testing.T) {
	db := setupRunnerTestDB(t)
	ctx := context.Background()

	for _, status := range []string{"FAILED", "BLOCKED"} {
		fwdID := "cf-terminal-" + status
		_, err := db.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
			ForwardingID:     fwdID,
			SourceProvider:   "openai",
			SourceJobID:      "src-terminal-" + status,
			TargetExecutorID: "scene.composite.v1",
			Status:           status,
			CreatedAt:        time.Now().UTC().Format(time.RFC3339),
			UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			t.Fatalf("insert forwarding (%s): %v", status, err)
		}

		err = db.EnsureForwarded(ctx, fwdID, "job-repair-attempt")
		if err == nil {
			t.Errorf("EnsureForwarded on %s should return error, got nil", status)
		}
		if !errors.Is(err, store.ErrTransitionConflict) {
			t.Errorf("EnsureForwarded on %s should return ErrTransitionConflict, got %v", status, err)
		}
	}
}

// TestProcessLease_MetricsAfterCAS verifies that the Forwarded metric
// is incremented only after the MarkCreatorForwardingReadyToForward
// CAS returns nil. A CAS failure must NOT increment Forwarded.
//
// This test uses an unconfigured client to verify the metric contract
// without needing a real remote engine. The key assertion is that
// metrics are 0 when no successful CAS occurs.
func TestProcessLease_MetricsAfterCAS(t *testing.T) {
	db := setupRunnerTestDB(t)
	ctx := context.Background()

	// Insert a forwarding and manually claim it.
	insertTestForwardingRecord(t, db, "cf-cas-metrics", "openai", "src-cas", "scene.composite.v1", "PENDING")
	leases, err := db.ClaimCreatorForwardings(ctx, "test-cas", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) == 0 {
		t.Fatalf("claim forwarding: err=%v len=%d", err, len(leases))
	}

	// Build a runner with an unconfigured client — processLease will
	// skip the remote poll and return nil (client not configured check
	// is in tick, not processLease — but processLease will hit the
	// GetPipelineStatus call which returns an error for unconfigured
	// client, triggering handleRetry).
	client := remoteengine.NewClient(remoteengine.Config{URL: "http://localhost:1"})
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, client, nil, "test-cas")

	// processLease with the claimed lease. The unconfigured client will
	// cause GetPipelineStatus to fail, triggering handleRetry.
	// handleRetry will call MarkCreatorForwardingRetry which should
	// succeed (the lease is valid), incrementing Retried.
	err = r.processLease(ctx, leases[0])
	// processLease may return nil (retry handled) or an error (CAS fail).
	// The key assertion is on the metric, not the return value.

	// After processLease: the Forwarded metric should be 0 (no
	// successful forward happened — the remote poll failed).
	if got := r.metrics.Forwarded.Load(); got != 0 {
		t.Errorf("Forwarded metric = %d after failed poll, want 0 (metric must be post-CAS only)", got)
	}

	// The Retried metric should be 1 (handleRetry succeeded after the
	// poll failure, which is a valid CAS).
	if got := r.metrics.Retried.Load(); got != 1 {
		t.Errorf("Retried metric = %d after successful retry CAS, want 1", got)
	}

	// Verify the row is now in RETRY_WAIT.
	cf, err := db.GetCreatorForwarding(ctx, leases[0].ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if cf.Status != "RETRY_WAIT" {
		t.Errorf("status = %q, want RETRY_WAIT", cf.Status)
	}
}
