package grpcserver

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
)

func TestReservationPreparedRequiresMatchingLineage(t *testing.T) {
	// Wire a minimal handler with a FutureReservationStore so getPrepGate
	// can lazily create the gate. The mock's UpdateReservationState is
	// a no-op; we only need the evidence map to be functional.
	frs := &mockFutureReservationStore{}
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{
		PushMode:            true,
		StrictPrefetchClaim: true,
		FutureAssetPlanTTL:  2 * time.Minute,
	})
	h.taskRepo = &compositeRepo{frs: frs}

	reservation := taskgraph.FutureReservationWithPayload{
		FutureReservation: taskgraph.FutureReservation{
			TaskID: "task-b", WorkerID: "worker-b", ReservationID: "future:worker-b:task-b", TaskRevision: 7,
		},
		Payload: []byte(`{"assets":[{"asset_id":"asset-b","asset_key":"asset-b","sha256":"sha-b","size_bytes":42}]}`),
	}

	prepared := &preparedAssetEvidence{TaskID: "task-b", TaskRevision: 7, AssetID: "asset-b", SHA256: "sha-b", SizeBytes: 42}
	h.markPreparedAsset("worker-b", prepared, reservation.ReservationID)
	ready, err := h.reservationPrepared(context.Background(), "worker-b", reservation)
	if err != nil || !ready {
		t.Fatalf("matching prepared evidence: ready=%v err=%v", ready, err)
	}

	prepared.TaskRevision = 6
	h.markPreparedAsset("worker-b", prepared, "future:worker-b:task-c")
	wrong := reservation
	wrong.ReservationID = "future:worker-b:task-c"
	ready, err = h.reservationPrepared(context.Background(), "worker-b", wrong)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("stale task revision was accepted as prepared")
	}
}
