package grpcserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

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
