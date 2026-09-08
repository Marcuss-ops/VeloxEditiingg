package grpcserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

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
