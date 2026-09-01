package grpcserver

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

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
