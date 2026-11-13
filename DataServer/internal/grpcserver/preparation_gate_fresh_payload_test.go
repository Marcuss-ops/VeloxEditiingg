package grpcserver

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

// TestPreparationGate_ReReadsReservationPayloadAfterFinalize pins the
// FINALIZE→TaskOffer fix: a placement pass can hold a reservation snapshot
// with runtime_assets_pending=true, but the gate must re-read the task spec
// before blocking. Otherwise a task whose FINALIZE already committed stays
// invisible until the next reservation refresh.
func TestPreparationGate_ReReadsReservationPayloadAfterFinalize(t *testing.T) {
	const (
		workerID = "worker-fresh"
		taskID   = "task-fresh"
		jobID    = "job-fresh"
		sha256   = "ddeeff0011223344ddeeff0011223344ddeeff0011223344ddeeff0011223344"
		size     = int64(2048)
	)
	frs := &mockFutureReservationStore{payload: reservationPayload(sha256, size)}
	h := buildHandler(t, true, frs)
	h.taskRepo = &compositeRepo{frs: frs}

	reserved, err := frs.TryReserveFutureTask(context.Background(), taskgraph.FutureReservation{
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		ReservationID: "future:" + workerID + ":" + taskID,
		TaskRevision:  1,
		ExpiresAt:     time.Now().UTC().Add(time.Minute),
	})
	if err != nil || !reserved {
		t.Fatalf("TryReserveFutureTask: reserved=%v err=%v", reserved, err)
	}
	h.markPreparedAsset(workerID, &preparedAssetEvidence{
		TaskID: taskID, TaskRevision: 1, AssetID: "video-fragment", SHA256: sha256, SizeBytes: size,
	}, "future:"+workerID+":"+taskID)
	frs.SetState(taskgraph.ReservationPrepared)

	// The placement pass saw the reservation before FINALIZE. Its cached
	// payload still says the assembly is gated; the live task spec does not.
	snapshot := *frs.reservation
	snapshot.Payload = []byte(`{"runtime_assets_pending":true}`)
	cached := []taskgraph.FutureReservationWithPayload{snapshot}

	gate := h.getPrepGate()
	if gate == nil {
		t.Fatal("preparation gate is not wired")
	}
	decision, err := gate.EnsurePreparedWithReservations(context.Background(), workerID, taskCandidate(taskID, jobID, 1), &cached)
	if err != nil {
		t.Fatalf("EnsurePreparedWithReservations: %v", err)
	}
	if decision != PreparationReady && decision != PreparationNotRequired {
		t.Fatalf("decision = %s, want READY or NOT_REQUIRED after the live payload cleared pending", decision)
	}
}
