package grpcserver

import (
	"testing"

	pb "velox-shared/controltransport/pb"
)

func TestDispatchMessage_PrefetchLifecycleSignalsPlacement(t *testing.T) {
	h := &Handler{}
	notifyCh := make(chan struct{}, 1)
	env := &pb.WorkerToMasterEnvelope{
		Msg: &pb.WorkerToMasterEnvelope_PrefetchLifecycleEvent{
			PrefetchLifecycleEvent: &pb.PrefetchLifecycleEvent{
				EventType: "prefetch_prepared",
				WorkerId:  "worker-a",
				JobId:     "job-a",
			},
		},
	}

	if err := h.dispatchMessage("worker-a", "session-a", env, nil, notifyCh); err != nil {
		t.Fatalf("dispatchMessage returned error: %v", err)
	}

	select {
	case <-notifyCh:
		// Expected: preparation evidence must wake placement immediately.
	default:
		t.Fatal("prefetch lifecycle event did not wake placement")
	}
}

func TestSignalTaskOffersCoalescesWakeups(t *testing.T) {
	notifyCh := make(chan struct{}, 1)
	signalTaskOffers(notifyCh)
	signalTaskOffers(notifyCh)

	select {
	case <-notifyCh:
	default:
		t.Fatal("signalTaskOffers did not enqueue a wake-up")
	}
	select {
	case <-notifyCh:
		t.Fatal("signalTaskOffers should coalesce pending wake-ups")
	default:
	}
}
