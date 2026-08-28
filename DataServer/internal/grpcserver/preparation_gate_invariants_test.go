package grpcserver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// ── Expiry-aware mock store ────────────────────────────────────────────────

// expiryMockStore extends mockFutureReservationStore with time-based expiry.
// When now() returns a time after ExpiresAt, ListFutureReservations returns
// empty — simulating the SQLite store's DELETE WHERE expires_at <= now.
type expiryMockStore struct {
	mu          sync.Mutex
	reservation *taskgraph.FutureReservationWithPayload
	payload     []byte
	reserved    bool
	now         func() time.Time
}

func (m *expiryMockStore) TryReserveFutureTask(_ context.Context, r taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reserved {
		return false, nil
	}
	if r.State == "" {
		r.State = taskgraph.ReservationReserved
	}
	m.reservation = &taskgraph.FutureReservationWithPayload{FutureReservation: r, Payload: m.payload}
	m.reserved = true
	return true, nil
}

func (m *expiryMockStore) ReconcileFutureReservations(_ context.Context, _ string, _ []taskgraph.FutureReservation) error {
	return nil
}

func (m *expiryMockStore) ListFutureReservations(_ context.Context, workerID string) ([]taskgraph.FutureReservationWithPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation == nil {
		return nil, nil
	}
	// Simulate expiry: if now() is after ExpiresAt, return empty.
	if m.now != nil && !m.reservation.ExpiresAt.IsZero() && m.now().After(m.reservation.ExpiresAt) {
		return nil, nil
	}
	if workerID != "" && m.reservation.WorkerID != workerID {
		return nil, nil
	}
	return []taskgraph.FutureReservationWithPayload{*m.reservation}, nil
}

func (m *expiryMockStore) FutureTaskPayload(_ context.Context, _ string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.payload, nil
}

func (m *expiryMockStore) TransferFutureTask(_ context.Context, _, _ string, _ taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return true, nil
}

var _ taskgraph.FutureReservationStore = (*expiryMockStore)(nil)

// SetState advances the reservation to the given state for testing
// lifecycle transitions.
func (m *expiryMockStore) SetState(state taskgraph.ReservationState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil {
		m.reservation.State = state
	}
}

// expiryRepo wraps expiryMockStore for the handler's taskRepo.
type expiryRepo struct {
	taskgraph.Repository
	frs *expiryMockStore
}

func (c *expiryRepo) TryReserveFutureTask(ctx context.Context, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TryReserveFutureTask(ctx, r)
}
func (c *expiryRepo) ReconcileFutureReservations(ctx context.Context, w string, rs []taskgraph.FutureReservation) error {
	return c.frs.ReconcileFutureReservations(ctx, w, rs)
}
func (c *expiryRepo) ListFutureReservations(ctx context.Context, w string) ([]taskgraph.FutureReservationWithPayload, error) {
	return c.frs.ListFutureReservations(ctx, w)
}
func (c *expiryRepo) FutureTaskPayload(ctx context.Context, id string) ([]byte, error) {
	return c.frs.FutureTaskPayload(ctx, id)
}
func (c *expiryRepo) TransferFutureTask(ctx context.Context, id, from string, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TransferFutureTask(ctx, id, from, r)
}

// noopProgress is a minimal asset progress sink for tests.
type noopProgress struct{}

func (n *noopProgress) IngestAssetDownloadProgress(_ context.Context, _ store.AssetDownloadProgressRecord) error {
	return nil
}

// buildExpiryHandler creates a handler wired with an expiryMockStore.
func buildExpiryHandler(t *testing.T, frs *expiryMockStore) *Handler {
	t.Helper()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{
		PushMode:            true,
		StrictPrefetchClaim: true,
		FutureAssetPlanTTL:  2 * time.Minute,
	})
	h.taskRepo = &expiryRepo{frs: frs}
	h.SetAssetDownloadProgressSink(&noopProgress{})
	return h
}

// expiryCandidate builds a TaskCandidate for expiry tests.
func expiryCandidate(taskID, jobID string, revision int) *placement.TaskCandidate {
	return &placement.TaskCandidate{
		TaskID:    taskID,
		JobID:     jobID,
		Revision:  revision,
		Executor:  placement.ExecutorKey{ID: "render_batch", Version: 3},
		Priority:  1,
		CreatedAt: time.Now().UTC(),
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 1: Gate blocks claim when no PREPARED evidence
// (Reinforced with structured assertion)
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_GateBlocksWithoutEvidence(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha256   = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size     = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)

	// Without any markPreparedAsset call, gate MUST block.
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("ensurePreparedBeforeClaim error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: gate must block when no PREPARED evidence exists")
	}

	// Verify the prepared map is empty for this reservation.
	h.preparedMu.RLock()
	preparedAssets := h.prepared["future:"+workerID+":"+taskID]
	h.preparedMu.RUnlock()
	if len(preparedAssets) != 0 {
		t.Fatalf("prepared map must be empty, got %d entries", len(preparedAssets))
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 2: Reservation expiry clears gate
// When a reservation expires, ListFutureReservations returns empty → gate
// has nothing to check → returns true (skip).
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_ExpiryClearsGate(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha256   = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size     = int64(1024 * 1024)
	)

	now := time.Now().UTC()
	frs := &expiryMockStore{
		payload: reservationPayload(sha256, size),
		now:     func() time.Time { return now },
	}
	h := buildExpiryHandler(t, frs)

	// Create reservation that expires in 10 seconds.
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    now.Add(10 * time.Second),
	})

	candidate := expiryCandidate(taskID, jobID, 1)

	// Before expiry: gate MUST block (reservation exists, no evidence).
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("before expiry: error = %v", err)
	}
	if prepared {
		t.Fatal("before expiry: gate must block when reservation exists but no evidence")
	}

	// Advance time past expiry.
	now = now.Add(11 * time.Second)

	// After expiry: gate MUST pass (reservation invisible → skip).
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("after expiry: error = %v", err)
	}
	if !prepared {
		t.Fatal("INVARIANT VIOLATED: gate must pass after reservation expiry (reservation invisible to gate)")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 3: Task revision drift invalidates prepared state
// When the task is re-enqueued (revision bumps), evidence at the old
// revision must not satisfy the gate for the new revision.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_RevisionDriftInvalidatesPreparedState(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha256   = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size     = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	// Reservation at revision 1.
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	// Worker prepares at revision 1.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)

	// Gate passes at revision 1.
	candidateR1 := expiryCandidate(taskID, jobID, 1)
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateR1)
	if err != nil {
		t.Fatalf("revision 1: error = %v", err)
	}
	if !prepared {
		t.Fatal("revision 1: gate must pass when evidence matches revision")
	}

	// Task is re-enqueued → revision bumps to 2. The reservation still
	// has TaskRevision=1, but the placement candidate now has Revision=2.
	// The gate compares evidence.TaskRevision (1) against
	// reservation.TaskRevision (1) — they match. However, the candidate
	// Revision (2) represents the CURRENT task state. If the reservation
	// was re-created at revision 2, the old evidence would mismatch.
	//
	// Simulate: create a NEW reservation at revision 2 with the same task.
	frs.mu.Lock()
	frs.reservation = &taskgraph.FutureReservationWithPayload{
		FutureReservation: taskgraph.FutureReservation{
			TaskID:       taskID,
			JobID:        jobID,
			WorkerID:     workerID,
			ReservationID: "future:" + workerID + ":" + taskID,
			TaskRevision: 2, // bumped
			ExpiresAt:    time.Now().UTC().Add(time.Minute),
		},
		Payload: reservationPayload(sha256, size),
	}
	frs.mu.Unlock()

	// Old evidence has TaskRevision=1, new reservation has TaskRevision=2.
	// Gate MUST block: evidence.TaskRevision (1) != reservation.TaskRevision (2).
	candidateR2 := expiryCandidate(taskID, jobID, 2)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateR2)
	if err != nil {
		t.Fatalf("revision 2: error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: gate must block when task_revision drifted (old evidence for new reservation)")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 4: Concurrent claims respect gate
// Multiple goroutines calling ensurePreparedBeforeClaim concurrently
// must all see a consistent view: either all block (no evidence) or
// all pass (evidence present). No goroutine should see a torn state.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_ConcurrentClaimsRespectGate(t *testing.T) {
	const (
		workerID    = "host_57_131_20_173"
		taskID      = "task-B"
		jobID       = "job-B"
		sha256      = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size        = int64(1024 * 1024)
		concurrency = 16
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)

	// ── Phase A: No evidence → all goroutines must see BLOCK ──────────
	var blockedCount atomic.Int32
	var passCount atomic.Int32
	var wg sync.WaitGroup

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
			if err != nil {
				t.Errorf("goroutine error: %v", err)
				return
			}
			if prepared {
				passCount.Add(1)
			} else {
				blockedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if int(blockedCount.Load()) != concurrency {
		t.Fatalf("INVARIANT VIOLATED: all %d goroutines must block without evidence, got %d blocked, %d passed",
			concurrency, blockedCount.Load(), passCount.Load())
	}

	// ── Phase B: Add evidence → all goroutines must see PASS ──────────
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)

	blockedCount.Store(0)
	passCount.Store(0)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
			if err != nil {
				t.Errorf("goroutine error: %v", err)
				return
			}
			if prepared {
				passCount.Add(1)
			} else {
				blockedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if int(passCount.Load()) != concurrency {
		t.Fatalf("INVARIANT VIOLATED: all %d goroutines must pass with evidence, got %d passed, %d blocked",
			concurrency, passCount.Load(), blockedCount.Load())
	}

	t.Logf("concurrent gate invariant verified: %d goroutines consistent in both phases", concurrency)
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 5: Empty asset list skips gate (pass-through)
// A reservation whose payload contains no parseable assets must not block.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_EmptyAssetListSkipsGate(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
	)

	tests := []struct {
		name    string
		payload []byte
	}{
		{"nil_payload", nil},
		{"empty_json", []byte(`{}`)},
		{"no_assets_key", []byte(`{"compiled_render_plan":"..."}`)},
		{"empty_assets_array", []byte(`{"assets":[]}`)},
		{"non_asset_fields", []byte(`{"audio":{},"video":{}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frs := &expiryMockStore{payload: tt.payload}
			h := buildExpiryHandler(t, frs)

			_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
				TaskID:       taskID,
				JobID:        jobID,
				WorkerID:     workerID,
				ReservationID: "future:" + workerID + ":" + taskID,
				TaskRevision: 1,
				ExpiresAt:    time.Now().UTC().Add(time.Minute),
			})

			candidate := expiryCandidate(taskID, jobID, 1)

			// No evidence, but empty asset list → gate must pass.
			prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if !prepared {
				t.Fatalf("INVARIANT VIOLATED: empty asset payload (%s) must not block gate", tt.name)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 6: Evidence state transition — blocked → prepared → blocked
// After evidence is recorded the gate passes; if the evidence is
// invalidated (simulated by clearing the prepared map), the gate blocks
// again. This proves the gate is stateless with respect to previous passes.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_EvidenceStateTransition(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha256   = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size     = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)

	// Step 1: BLOCKED (no evidence).
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("step 1: error = %v", err)
	}
	if prepared {
		t.Fatal("step 1: must be BLOCKED")
	}

	// Step 2: Add evidence → PASS.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)

	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("step 2: error = %v", err)
	}
	if !prepared {
		t.Fatal("step 2: must PASS after evidence recorded")
	}

	// Step 3: Invalidate evidence (clear the prepared map) → BLOCKED again.
	h.preparedMu.Lock()
	delete(h.prepared, "future:"+workerID+":"+taskID)
	h.preparedMu.Unlock()

	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("step 3: error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: gate must block again after evidence is invalidated")
	}

	t.Log("evidence state transition verified: BLOCKED → PASS → BLOCKED")
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 7: Wrong worker cannot claim another worker's reservation
// Even with valid evidence, a claim from a different worker must be
// blocked by the WorkerID check in reservationPrepared.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_WrongWorkerCannotClaim(t *testing.T) {
	const (
		ownerWorker   = "host_57_131_20_173"
		intruderWorker = "host_57_129_132_133"
		taskID        = "task-B"
		jobID         = "job-B"
		sha256        = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size          = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	// Reservation belongs to ownerWorker.
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     ownerWorker,
		ReservationID: "future:" + ownerWorker + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	// Owner prepares the asset.
	h.markPreparedAsset(ownerWorker, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+ownerWorker+":"+taskID)

	// Owner can claim.
	candidate := expiryCandidate(taskID, jobID, 1)
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), ownerWorker, candidate)
	if err != nil {
		t.Fatalf("owner claim: error = %v", err)
	}
	if !prepared {
		t.Fatal("owner must be able to claim")
	}

	// Intruder tries to claim the same task.
	// The gate calls ListFutureReservations(intruderWorker) which returns
	// nothing (reservation belongs to ownerWorker) → gate skips → returns true.
	// BUT the intruder's claim would fail at ensureFutureReservationOwnership
	// (which is called before the gate in the real placement pipeline).
	// For this test, we verify the gate behavior in isolation:
	// the gate correctly skips when no reservation matches the intruder.
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), intruderWorker, candidate)
	if err != nil {
		t.Fatalf("intruder claim: error = %v", err)
	}
	if !prepared {
		t.Fatal("gate should skip (return true) when intruder has no matching reservation")
	}

	// Now simulate: the reservation is transferred to the intruder.
	frs.mu.Lock()
	frs.reservation.WorkerID = intruderWorker
	frs.reservation.ReservationID = "future:" + intruderWorker + ":" + taskID
	frs.mu.Unlock()

	// Intruder has no prepared evidence → gate MUST block.
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), intruderWorker, candidate)
	if err != nil {
		t.Fatalf("intruder claim after transfer: error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: intruder without evidence must be blocked")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 8: SHA256 corruption after preparation invalidates gate
// If the prepared evidence has a different SHA than the reservation
// manifest, the gate must block even though evidence exists.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_SHA256CorruptionAfterPreparation(t *testing.T) {
	const (
		workerID       = "host_57_131_20_173"
		taskID         = "task-B"
		jobID          = "job-B"
		originalSHA    = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		corruptedSHA   = "ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000"
		size           = int64(1024 * 1024)
	)

	// Reservation manifest has the original SHA.
	frs := &expiryMockStore{payload: reservationPayload(originalSHA, size)}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	// Worker reports prepared evidence with CORRUPTED SHA.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       corruptedSHA,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)

	candidate := expiryCandidate(taskID, jobID, 1)

	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: gate must block when prepared evidence SHA differs from reservation manifest")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 9: Multi-asset — partial preparation blocks, full passes
// With a 3-asset reservation, preparing 1 or 2 assets must still block;
// only when all 3 are prepared does the gate pass.
// ──────────────────────────────────────────────────────────────────────────

func TestInvariant_PartialPreparationBlocksFullPasses(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha1     = "1111000000000000000000000000000000000000000000000000000000001111"
		sha2     = "2222000000000000000000000000000000000000000000000000000000002222"
		sha3     = "3333000000000000000000000000000000000000000000000000000000003333"
	)

	payload := []byte(fmt.Sprintf(`{"assets":[
		{"asset_key":"video","asset_id":"video","sha256":"%s","size_bytes":100},
		{"asset_key":"audio","asset_id":"audio","sha256":"%s","size_bytes":200},
		{"asset_key":"subtitle","asset_id":"subtitle","sha256":"%s","size_bytes":50}
	]}`, sha1, sha2, sha3))

	frs := &expiryMockStore{payload: payload}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:       taskID,
		JobID:        jobID,
		WorkerID:     workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision: 1,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)
	resID := "future:" + workerID + ":" + taskID

	// Prepare 1 of 3 → BLOCKED.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "video", SHA256: sha1, SizeBytes: 100,
	}, resID)

	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("1/3: error = %v", err)
	}
	if prepared {
		t.Fatal("1/3: must BLOCK")
	}

	// Prepare 2 of 3 → BLOCKED.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "audio", SHA256: sha2, SizeBytes: 200,
	}, resID)

	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("2/3: error = %v", err)
	}
	if prepared {
		t.Fatal("2/3: must BLOCK")
	}

	// Prepare 3 of 3 → PASS.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "subtitle", SHA256: sha3, SizeBytes: 50,
	}, resID)

	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("3/3: error = %v", err)
	}
	if !prepared {
		t.Fatal("3/3: must PASS when all assets prepared")
	}
}
