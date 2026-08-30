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

func (m *expiryMockStore) UpdateReservationState(_ context.Context, reservationID string, state taskgraph.ReservationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil && m.reservation.ReservationID == reservationID {
		m.reservation.State = state
	}
	return nil
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
func (c *expiryRepo) UpdateReservationState(ctx context.Context, reservationID string, state taskgraph.ReservationState) error {
	return c.frs.UpdateReservationState(ctx, reservationID, state)
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     now.Add(10 * time.Second),
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})

	// Worker prepares at revision 1.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

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
			TaskID:        taskID,
			JobID:         jobID,
			WorkerID:      workerID,
			ReservationID: "future:" + workerID + ":" + taskID,
			TaskRevision:  2, // bumped
			ExpiresAt:     time.Now().UTC().Add(time.Minute),
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
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

	// ── Phase B: Add evidence + advance to PREPARED → all must PASS ──
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

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

			// Do NOT create a reservation — the invariant is that an empty asset
			// list causes the gate to pass even without a reservation.

			candidate := expiryCandidate(taskID, jobID, 1)

			// No reservation, no evidence, empty asset list → gate must pass.
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
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

	// Step 2: Add evidence + advance to PREPARED → PASS.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

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
		ownerWorker    = "host_57_131_20_173"
		intruderWorker = "host_57_129_132_133"
		taskID         = "task-B"
		jobID          = "job-B"
		sha256         = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size           = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	// Reservation belongs to ownerWorker.
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      ownerWorker,
		ReservationID: "future:" + ownerWorker + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})

	// Owner prepares the asset.
	h.markPreparedAsset(ownerWorker, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+ownerWorker+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

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
		workerID     = "host_57_131_20_173"
		taskID       = "task-B"
		jobID        = "job-B"
		originalSHA  = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		corruptedSHA = "ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000"
		size         = int64(1024 * 1024)
	)

	// Reservation manifest has the original SHA.
	frs := &expiryMockStore{payload: reservationPayload(originalSHA, size)}
	h := buildExpiryHandler(t, frs)

	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)
	resID := "future:" + workerID + ":" + taskID

	// Prepare 1 of 3 → BLOCKED (partial evidence).
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "video", SHA256: sha1, SizeBytes: 100,
	}, resID)
	frs.SetState(taskgraph.ReservationPreparing)

	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("1/3: error = %v", err)
	}
	if prepared {
		t.Fatal("1/3: must BLOCK")
	}

	// Prepare 2 of 3 → BLOCKED (still partial).
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
		t.Fatal("3/3: must transition PREPARING to PREPARED and pass when all assets are prepared")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// LIFECYCLE 1: Full RESERVED → PLANNING → PREPARING → PREPARED lifecycle
// Verifies the gate blocks at every intermediate state and passes only
// when the reservation reaches PREPARED.
// ──────────────────────────────────────────────────────────────────────────

func TestLifecycle_FullReservedToPrepared(t *testing.T) {
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})

	candidate := expiryCandidate(taskID, jobID, 1)

	// ── RESERVED: gate blocks (no evidence yet) ──────────────────────
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("RESERVED: error = %v", err)
	}
	if prepared {
		t.Fatal("RESERVED: gate must block")
	}

	// ── PLANNING: gate blocks (plan sent, no evidence) ───────────────
	frs.SetState(taskgraph.ReservationPlanning)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("PLANNING: error = %v", err)
	}
	if prepared {
		t.Fatal("PLANNING: gate must block")
	}

	// ── PREPARING: gate blocks (no complete evidence) ────────────────
	frs.SetState(taskgraph.ReservationPreparing)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("PREPARING: error = %v", err)
	}
	if prepared {
		t.Fatal("PREPARING: gate must block")
	}

	// ── PREPARED: gate passes ────────────────────────────────────────
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "video-fragment", SHA256: sha256, SizeBytes: size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("PREPARED: error = %v", err)
	}
	if !prepared {
		t.Fatal("PREPARED: gate must pass")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// LIFECYCLE 2: EXPIRED state blocks claim
// Once a reservation expires, the gate must block even if evidence exists.
// ──────────────────────────────────────────────────────────────────────────

func TestLifecycle_ExpiredBlocksClaim(t *testing.T) {
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
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})

	// Prepare evidence and advance to PREPARED.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

	candidate := expiryCandidate(taskID, jobID, 1)

	// Gate passes at PREPARED.
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("PREPARED: error = %v", err)
	}
	if !prepared {
		t.Fatal("PREPARED: gate must pass")
	}

	// Advance to EXPIRED.
	frs.SetState(taskgraph.ReservationExpired)

	// Gate must block even though evidence exists.
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("EXPIRED: error = %v", err)
	}
	if prepared {
		t.Fatal("EXPIRED: gate must block")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// LIFECYCLE 3: State constants satisfy invariants
// IsTerminal and CanClaim must be correct for every state.
// ──────────────────────────────────────────────────────────────────────────

func TestLifecycle_StateConstants(t *testing.T) {
	tests := []struct {
		state      taskgraph.ReservationState
		canClaim   bool
		isTerminal bool
	}{
		{taskgraph.ReservationReserved, false, false},
		{taskgraph.ReservationPlanning, false, false},
		{taskgraph.ReservationPreparing, false, false},
		{taskgraph.ReservationPrepared, true, false},
		{taskgraph.ReservationExpired, false, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			if got := tt.state.CanClaim(); got != tt.canClaim {
				t.Fatalf("CanClaim() = %v, want %v", got, tt.canClaim)
			}
			if got := tt.state.IsTerminal(); got != tt.isTerminal {
				t.Fatalf("IsTerminal() = %v, want %v", got, tt.isTerminal)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────
// LIFECYCLE 4: Backward compatibility — empty state is not blocked
// Reservations without an explicit state (legacy) must not be blocked
// by the state machine gate. This preserves the existing behavior.
// ──────────────────────────────────────────────────────────────────────────

func TestLifecycle_EmptyStateNotBlocked(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskID   = "task-B"
		jobID    = "job-B"
		sha256   = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size     = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	// Create reservation with explicit empty state (legacy path).
	// We bypass TryReserveFutureTask to keep state empty.
	frs.mu.Lock()
	frs.reservation = &taskgraph.FutureReservationWithPayload{
		FutureReservation: taskgraph.FutureReservation{
			TaskID:        taskID,
			JobID:         jobID,
			WorkerID:      workerID,
			ReservationID: "future:" + workerID + ":" + taskID,
			TaskRevision:  1,
			ExpiresAt:     time.Now().UTC().Add(time.Minute),
			// State is zero-value (empty string)
		},
		Payload: reservationPayload(sha256, size),
	}
	frs.reserved = true
	frs.mu.Unlock()

	// Add evidence so reservationPrepared passes.
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+workerID+":"+taskID)

	candidate := expiryCandidate(taskID, jobID, 1)

	// Empty state → gate must NOT block (backward compat).
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidate)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !prepared {
		t.Fatal("empty state reservation must not be blocked by state gate")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// LIFECYCLE 5: N+1 lifecycle with state tracking
// Verifies the full N+1 flow with explicit state transitions:
// A claimed → refreshFutureAssetPlan creates B (RESERVED) →
// plan sent (PLANNING) → worker prefetches (PREPARING) →
// all assets ready (PREPARED) → B claimable.
// ──────────────────────────────────────────────────────────────────────────

func TestLifecycle_N1WithStateTracking(t *testing.T) {
	const (
		workerID = "host_57_131_20_173"
		taskB    = "task-B"
		jobB     = "job-B"
		sha256B  = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		sizeB    = int64(677_000_000)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256B, sizeB)}
	h := buildExpiryHandler(t, frs)

	candidateB := expiryCandidate(taskB, jobB, 1)

	// ── Step 1: Create reservation for B (RESERVED) ──────────────────
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:        taskB,
		JobID:         jobB,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskB,
		TaskRevision:  1,
		State:         taskgraph.ReservationReserved,
		ExpiresAt:     time.Now().UTC().Add(2 * time.Minute),
	})

	// Gate blocks at RESERVED.
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateB)
	if err != nil {
		t.Fatalf("step 1: error = %v", err)
	}
	if prepared {
		t.Fatal("step 1 (RESERVED): gate must block")
	}

	// ── Step 2: Plan sent (PLANNING) ────────────────────────────────
	frs.SetState(taskgraph.ReservationPlanning)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateB)
	if err != nil {
		t.Fatalf("step 2: error = %v", err)
	}
	if prepared {
		t.Fatal("step 2 (PLANNING): gate must block")
	}

	// ── Step 3: Worker starts prefetching (PREPARING) ────────────────
	frs.SetState(taskgraph.ReservationPreparing)
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateB)
	if err != nil {
		t.Fatalf("step 3: error = %v", err)
	}
	if prepared {
		t.Fatal("step 3 (PREPARING): gate must block")
	}

	// ── Step 4: All assets ready (PREPARED) ─────────────────────────
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID:       taskB,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256B,
		SizeBytes:    sizeB,
	}, "future:"+workerID+":"+taskB)
	frs.SetState(taskgraph.ReservationPrepared)

	preparedAt := time.Now().UTC()
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), workerID, candidateB)
	if err != nil {
		t.Fatalf("step 4: error = %v", err)
	}
	if !prepared {
		t.Fatal("step 4 (PREPARED): gate must pass")
	}

	// Core invariant: prepared_at < attempt_started_at.
	attemptStartedAt := time.Now().UTC()
	leadMS := attemptStartedAt.Sub(preparedAt).Milliseconds()
	if leadMS < 0 {
		t.Fatalf("invariant violated: prepared_at >= attempt_started_at, lead=%dms", leadMS)
	}
	t.Logf("N+1 lifecycle with state: RESERVED→PLANNING→PREPARING→PREPARED lead=%dms", leadMS)
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 1: verifyAttempt happy path — certificate matches claim
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_HappyPath(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "host_57_131_20_173", 4, "future:host_57_131_20_173:task-B")
	if !ok {
		t.Fatalf("verifyAttempt failed: %s", reason)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 2: verifyAttempt rejects worker mismatch
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_RejectsWorkerMismatch(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "host_57_129_132_133", 4, "future:host_57_131_20_173:task-B")
	if ok {
		t.Fatal("verifyAttempt must reject worker mismatch")
	}
	t.Logf("correctly rejected: %s", reason)
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 3: verifyAttempt rejects revision drift
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_RejectsRevisionDrift(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "host_57_131_20_173", 5, "future:host_57_131_20_173:task-B")
	if ok {
		t.Fatal("verifyAttempt must reject revision drift")
	}
	t.Logf("correctly rejected: %s", reason)
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 4: verifyAttempt rejects incomplete assets
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_RejectsIncompleteAssets(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		TaskRevision:   4,
		AssetsRequired: 3,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "host_57_131_20_173", 4, "future:host_57_131_20_173:task-B")
	if ok {
		t.Fatal("verifyAttempt must reject incomplete assets")
	}
	t.Logf("correctly rejected: %s", reason)
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 5: verifyAttempt rejects zero prepared when assets required
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_RejectsZeroPreparedWithAssetsRequired(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 0,
	}
	ok, reason := verifyAttempt(cert, "host_57_131_20_173", 4, "future:host_57_131_20_173:task-B")
	if ok {
		t.Fatal("verifyAttempt must reject zero prepared when assets required")
	}
	t.Logf("correctly rejected: %s", reason)
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 6: verifyAttempt passes for non-prefetch path (no reservation)
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_PassesForNonPrefetchPath(t *testing.T) {
	cert := preparedJobCertificate{}
	ok, reason := verifyAttempt(cert, "worker-1", 1, "")
	if !ok {
		t.Fatalf("verifyAttempt should pass for non-prefetch path: %s", reason)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 7: verifyAttempt skips worker check when cert WorkerID is empty
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_SkipsWorkerCheckWhenEmpty(t *testing.T) {
	cert := preparedJobCertificate{
		ReservationID:  "future:worker-1:task-1",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "any-worker", 4, "future:worker-1:task-1")
	if !ok {
		t.Fatalf("verifyAttempt should skip worker check when cert WorkerID is empty: %s", reason)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 8: verifyAttempt skips revision check when cert TaskRevision is 0
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_SkipsRevisionCheckWhenZero(t *testing.T) {
	cert := preparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		ReservationID:  "future:host_57_131_20_173:task-B",
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := verifyAttempt(cert, "host_57_131_20_173", 99, "future:host_57_131_20_173:task-B")
	if !ok {
		t.Fatalf("verifyAttempt should skip revision check when cert TaskRevision is 0: %s", reason)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// ATTEMPT 9: Integration — gate blocks when certificate worker doesn't
// match the claiming worker
// ──────────────────────────────────────────────────────────────────────────

func TestVerifyAttempt_GateBlocksOnCertificateWorkerMismatch(t *testing.T) {
	const (
		ownerWorker    = "host_57_131_20_173"
		intruderWorker = "host_57_129_132_133"
		taskID         = "task-B"
		jobID          = "job-B"
		sha256         = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233"
		size           = int64(1024 * 1024)
	)

	frs := &expiryMockStore{payload: reservationPayload(sha256, size)}
	h := buildExpiryHandler(t, frs)

	// Owner creates reservation and prepares assets.
	_, _ = frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      ownerWorker,
		ReservationID: "future:" + ownerWorker + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})
	h.markPreparedAsset(ownerWorker, &preparedAssetEvidence{
		TaskID:       taskID,
		TaskRevision: 1,
		AssetID:      "video-fragment",
		SHA256:       sha256,
		SizeBytes:    size,
	}, "future:"+ownerWorker+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

	// Owner can claim.
	candidate := expiryCandidate(taskID, jobID, 1)
	prepared, err := h.ensurePreparedBeforeClaim(context.Background(), ownerWorker, candidate)
	if err != nil {
		t.Fatalf("owner claim: error = %v", err)
	}
	if !prepared {
		t.Fatal("owner must be able to claim")
	}

	// Intruder tries to claim — reservation belongs to owner, so
	// findReservation returns nothing for intruder → gate skips.
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), intruderWorker, candidate)
	if err != nil {
		t.Fatalf("intruder claim (no reservation): error = %v", err)
	}
	if !prepared {
		t.Fatal("gate should skip when intruder has no matching reservation")
	}

	// Transfer reservation to intruder but keep owner's evidence.
	frs.mu.Lock()
	frs.reservation.WorkerID = intruderWorker
	frs.reservation.ReservationID = "future:" + intruderWorker + ":" + taskID
	frs.mu.Unlock()

	// Intruder has reservation but evidence was from owner → certificate
	// worker mismatch → gate blocks.
	prepared, err = h.ensurePreparedBeforeClaim(context.Background(), intruderWorker, candidate)
	if err != nil {
		t.Fatalf("intruder claim (transferred): error = %v", err)
	}
	if prepared {
		t.Fatal("INVARIANT VIOLATED: gate must block when certificate worker doesn't match claiming worker")
	}
}
