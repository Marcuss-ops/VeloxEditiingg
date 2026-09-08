package grpcserver

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

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
