package api

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"velox-server/internal/store"
)

func TestUpdateWorkerAcceptance_DuplicateInFlightReturnsConflictWithoutSecondOperation(t *testing.T) {
	reg := newRegisteredRegistry(t, "worker-e2e-a")
	pub := &stubPublisher{}
	var mu sync.Mutex
	first := true
	pub.publishFn = func(_ context.Context, _ *store.Operation) error {
		mu.Lock()
		defer mu.Unlock()
		if first {
			first = false
			return nil
		}
		return store.ErrOperationInFlight
	}
	r := updateRoute(newMutationsHandler(reg, pub))
	digest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)
	body := MutationRequest{TargetDigest: digest, Reason: "concurrent update"}

	responses := make(chan int, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- doPOST(t, r, "/api/v1/admin/workers/worker-e2e-a/update", body).Code
		}()
	}
	wg.Wait()
	close(responses)
	var accepted, conflicts int
	for code := range responses {
		switch code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("concurrent update status=%d, want 202 or 409", code)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("concurrent update outcomes accepted=%d conflicts=%d, want 1/1", accepted, conflicts)
	}
	if len(pub.published) != 1 {
		t.Fatalf("published operations=%d, want one accepted operation", len(pub.published))
	}
}

func TestUpdateWorkerAcceptance_InvalidDigestHasNoWorkerOrLedgerSideEffects(t *testing.T) {
	reg := newRegisteredRegistry(t, "worker-e2e-a")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))

	response := doPOST(t, r, "/api/v1/admin/workers/worker-e2e-a/update", MutationRequest{
		TargetDigest: "ghcr.io/marcuss-ops/velox-worker:latest",
		Reason:       "must reject mutable tag",
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid digest status=%d body=%s, want 400", response.Code, response.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("invalid digest published %d operations, want zero", len(pub.published))
	}
	worker := reg.GetWorker(context.Background(), "worker-e2e-a")
	if worker == nil || worker.Drain || worker.Quarantined {
		t.Fatalf("invalid digest changed worker state: %+v", worker)
	}
}
