package prefetch

import (
	"testing"
	"time"

	"velox-shared/futureasset"
)

func plan(t *testing.T, version uint64, expiry time.Time, jobs ...futureasset.Job) futureasset.Plan {
	t.Helper()
	p, err := futureasset.Build(futureasset.PlannerInput{Version: version, PlanID: "plan", WorkerID: "worker", GeneratedAt: time.Unix(100, 0).UTC(), ExpiresAt: expiry, FutureJobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func job(id, reservation string) futureasset.Job {
	return futureasset.Job{JobID: id, TaskID: "task-" + id, ReservationID: reservation, Assets: []futureasset.AssetManifest{{AssetKey: "asset-" + id, SHA256: "sha", SizeBytes: 1}}}
}

func TestApplyIsVersionedAndReportsReconcile(t *testing.T) {
	c := NewController("worker")
	first := plan(t, 1, time.Now().Add(time.Minute), job("j1", "r1"), job("j2", "r2"))
	got, err := c.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Added) != 2 || got.Stale || c.Version() != 1 {
		t.Fatalf("unexpected first reconcile: %#v", got)
	}
	stale, err := c.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if !stale.Stale || stale.Applied {
		t.Fatalf("equal version was not ignored: %#v", stale)
	}
	second := plan(t, 2, time.Now().Add(time.Minute), job("j2", "r2"), job("j3", "r3"))
	result, err := c.Apply(second)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Removed) != 1 || len(result.Added) != 1 {
		t.Fatalf("unexpected second reconcile: %#v", result)
	}
}

func TestCancelOnlyRemovesMatchingWaiter(t *testing.T) {
	c := NewController("worker")
	_, err := c.Apply(plan(t, 1, time.Now().Add(time.Minute), job("j1", "r1")))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cancel("j1", "wrong", 1) {
		t.Fatal("cancel removed a different reservation")
	}
	if !c.Cancel("j1", "r1", 1) || len(c.ActiveJobs()) != 0 {
		t.Fatal("matching cancellation did not remove the job")
	}
}

func TestExpiredPlanDoesNotStartNewWork(t *testing.T) {
	c := NewController("worker")
	result, err := c.Apply(plan(t, 1, time.Unix(101, 0).UTC(), job("j1", "r1")))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Expired || len(result.Added) != 0 || len(c.ActiveJobs()) != 0 {
		t.Fatalf("expired plan started work: %#v active=%v", result, c.ActiveJobs())
	}
}
