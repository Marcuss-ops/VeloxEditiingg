package admission

import (
	"errors"
	"testing"
	"time"
)

func TestAdmissionEnforcesLimitsAndReleases(t *testing.T) {
	c := NewController(Limits{MaxRenderConcurrent: 1, MaxTempBytes: 100, MaxScenesPerJob: 10})
	base := Request{ID: "a", BatchID: "batch", Scenes: 2, TempBytes: 50, RenderSlots: 1, WorkerCompatible: true, CredentialUsable: true}
	r, err := c.Reserve(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reserve(Request{ID: "b", BatchID: "batch", TempBytes: 200, WorkerCompatible: true, CredentialUsable: true}); !errors.Is(err, ErrNotAdmitted) {
		t.Fatalf("want admission error, got %v", err)
	}
	r.Release()
	if got := c.Usage().RenderRunning; got != 0 {
		t.Fatalf("render usage = %d", got)
	}
	c.PauseBatch("batch-2")
	if _, err := c.Reserve(Request{ID: "c", BatchID: "batch-2", WorkerCompatible: true, CredentialUsable: true}); err == nil {
		t.Fatal("paused batch admitted")
	}
}

func TestFairQueueAgesAndDoesNotStarveUrgentWork(t *testing.T) {
	q := NewFairQueue(Limits{MaxQueueItems: 100, MaxConsecutivePerBatch: 2})
	old := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(QueueItem{ID: "bulk", BatchID: "bulk", ProjectID: "bulk", SubmittedAt: old}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Enqueue(QueueItem{ID: "urgent", BatchID: "urgent", ProjectID: "ops", Urgent: true, SubmittedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	first, ok := q.Next(time.Now())
	if !ok || first.ID != "urgent" {
		t.Fatalf("first=%+v ok=%v", first, ok)
	}
	q.PauseBatch("bulk")
	if _, ok := q.Next(time.Now()); ok {
		t.Fatal("paused bulk item was dispatched")
	}
}
