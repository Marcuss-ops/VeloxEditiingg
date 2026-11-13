package grpcserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

// blockingPlacementRepo blocks the first prefetch candidate query. The offer
// must already be on the session channel while this call is held.
type blockingPlacementRepo struct {
	taskgraph.Repository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingPlacementRepo) ListReadyCandidates(context.Context, int) ([]placement.TaskCandidate, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return nil, nil
}

func (r *blockingPlacementRepo) TryReserveFutureTask(context.Context, taskgraph.FutureReservation) (bool, error) {
	return false, nil
}
func (r *blockingPlacementRepo) ReconcileFutureReservations(context.Context, string, []taskgraph.FutureReservation) error {
	return nil
}
func (r *blockingPlacementRepo) ListFutureReservations(context.Context, string) ([]taskgraph.FutureReservationWithPayload, error) {
	return nil, nil
}
func (r *blockingPlacementRepo) FutureTaskPayload(context.Context, string) ([]byte, error) {
	return nil, nil
}
func (r *blockingPlacementRepo) TransferFutureTask(context.Context, string, string, taskgraph.FutureReservation) (bool, error) {
	return false, nil
}
func (r *blockingPlacementRepo) UpdateReservationState(context.Context, string, taskgraph.ReservationState) error {
	return nil
}

func TestDeliverClaimedTaskQueuesOfferBeforePrefetchRefresh(t *testing.T) {
	repo := &blockingPlacementRepo{entered: make(chan struct{}), release: make(chan struct{})}
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})
	h.taskRepo = repo
	h.SetAssetDownloadProgressSink(&noopAssetProgressSink{})

	sess := &workerSession{workerID: "worker-order", sendCh: make(chan *outboundMessage, 1)}
	tws := &taskgraph.TaskWithSpec{
		Task: taskgraph.Task{ID: "task-order", JobID: "job-order", ExecutorID: "scene.composite.v1", ExecutorVersion: 2},
		SpecPayload: map[string]interface{}{
			"payload_contract_version": contract.PayloadContractVersionCanonical,
			"video_name":               "Offer ordering",
		},
	}
	attempt := &taskattempts.TaskAttempt{ID: "attempt-order", TaskID: "task-order", JobID: "job-order", AttemptNumber: 1}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.deliverClaimedTask(context.Background(), "worker-order", sess, tws, attempt, "lease-order", time.Now())
	}()

	select {
	case out := <-sess.sendCh:
		if out == nil || out.Envelope == nil || out.Envelope.GetTaskOffer() == nil {
			t.Fatalf("deliverClaimedTask did not queue a TaskOffer: %+v", out)
		}
	case <-time.After(time.Second):
		t.Fatal("TaskOffer was delayed by the prefetch refresh")
	}

	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("prefetch refresh was not started after the offer")
	}
	close(repo.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deliverClaimedTask did not finish after the refresh was released")
	}
}
