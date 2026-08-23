package workers

import (
	"context"
	"testing"
	"velox-server/internal/store"
)

func TestRegistryGetSchedulableWorkers(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	// Admission fails closed without declared capacity (host.max_parallel_jobs),
	// so the fixture MUST register capability-bearing workers just like the
	// production agent does (worker_info.go derives DeclaredMaxSlots from the
	// capabilities metadata during registration).
	capabilities := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(1)},
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", capabilities)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", capabilities)

	// Set w1 to drain
	_ = reg.SetWorkerDrain(ctx, "w1", true)

	schedulable := reg.GetSchedulableWorkers(ctx)
	if len(schedulable) != 1 {
		t.Fatalf("expected 1 schedulable worker, got %d", len(schedulable))
	}
	if schedulable[0].WorkerID != "w2" {
		t.Errorf("expected schedulable worker w2, got %s", schedulable[0].WorkerID)
	}
}

func TestRegistryResumeGateSurvivesReconnectAndReload(t *testing.T) {
	ctx := context.Background()
	db, err := store.NewSQLiteStore(t.TempDir() + "/resume-gate.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	reg := New(db)
	caps := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(1)},
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	if err := reg.RegisterWorker(ctx, "resume-reconnect", "worker", "127.0.0.1", caps); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerDrain(ctx, "resume-reconnect", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(ctx, "resume-reconnect", "resume-operation-1"); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterWorker(ctx, "resume-reconnect", "worker-reconnected", "127.0.0.2", nil); err != nil {
		t.Fatal(err)
	}

	info := reg.GetWorker(ctx, "resume-reconnect")
	if info == nil || !info.Drain || !info.Resuming {
		t.Fatalf("reconnect lost resume gates: drain=%v resuming=%v", info != nil && info.Drain, info != nil && info.Resuming)
	}
	if got := reg.GetSchedulableWorkers(ctx); len(got) != 0 {
		t.Fatalf("reconnected worker became placement-eligible: %+v", got)
	}

	reloaded := New(db)
	info = reloaded.GetWorker(ctx, "resume-reconnect")
	if info == nil || !info.Drain || !info.Resuming {
		t.Fatalf("master reload lost resume gates: drain=%v resuming=%v", info != nil && info.Drain, info != nil && info.Resuming)
	}
	if got := reloaded.GetSchedulableWorkers(ctx); len(got) != 0 {
		t.Fatalf("reloaded worker became placement-eligible: %+v", got)
	}
}

func TestGetSchedulableWorkers_ExcludesDraining(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	// Register each worker before applying state updates so the test
	// exercises the real persisted registry path.
	workerIDs := []struct {
		id, name string
	}{
		{"w-drain-1", "Draining Worker"},
		{"w-unsched-1", "Unschedulable Worker"},
		{"w-ok-1", "Healthy Worker"},
	}
	for _, worker := range workerIDs {
		if err := reg.RegisterWorker(ctx, worker.id, worker.name, "10.0.0.1", map[string]interface{}{
			"capabilities": map[string]interface{}{
				"host": map[string]interface{}{"max_parallel_jobs": float64(1)},
				"executors": []interface{}{
					map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
				},
			},
		}); err != nil {
			t.Fatalf("RegisterWorker(%s): %v", worker.id, err)
		}
		if err := reg.Heartbeat(ctx, worker.id, worker.name, "", nil); err != nil {
			t.Fatalf("Heartbeat(%s): %v", worker.id, err)
		}
	}

	// Channel 1: drain=true. Even with schedulable=true and a fresh
	// heartbeat, the worker is excluded.
	if err := reg.UpdateWorker(ctx, "w-drain-1", map[string]interface{}{
		"drain":       true,
		"schedulable": true,
	}); err != nil {
		t.Fatalf("UpdateWorker(w-drain-1): %v", err)
	}

	// Channel 2: schedulable=false (not draining), but the costmodel
	// treats drain || !schedulable as IsDraining=true.
	if err := reg.UpdateWorker(ctx, "w-unsched-1", map[string]interface{}{
		"drain":       false,
		"schedulable": false,
	}); err != nil {
		t.Fatalf("UpdateWorker(w-unsched-1): %v", err)
	}

	// Control case: healthy, schedulable, non-draining. It must remain
	// eligible; without this case an empty result would be ambiguous.
	if err := reg.UpdateWorker(ctx, "w-ok-1", map[string]interface{}{
		"drain":       false,
		"schedulable": true,
	}); err != nil {
		t.Fatalf("UpdateWorker(w-ok-1): %v", err)
	}

	schedulable := reg.GetSchedulableWorkers(ctx)

	if len(schedulable) != 1 {
		t.Fatalf("expected exactly ONE schedulable worker (the control case); got %d: %+v",
			len(schedulable), schedulable)
	}
	if schedulable[0].WorkerID != "w-ok-1" {
		t.Errorf("wrong worker returned; want w-ok-1, got %s", schedulable[0].WorkerID)
	}
	if schedulable[0].ConnectionStatus == "DRAINING" {
		t.Errorf("returned worker should NOT have ConnectionStatus=DRAINING (control-case regression)")
	}

	// Operator-facing canonical assertion: the drain-channel worker
	// (w-drain-1, drain=true) MUST surface as `ConnectionStatus =
	// StatusDraining` on the operator-facing read model. This pins
	// the read-model derivation rule (drain=true ⇒ DRAINING,
	// overrides freshness — see workers.ConnectionStatus) alongside
	// the costmodel-exclusion rule so a regression on either side is
	// caught by the same test.
	//
	// We deliberately do NOT assert ConnectionStatus on w-unsched-1:
	// `schedulable=false` alone does NOT drive ConnectionStatus to
	// DRAINING — that input is gated at the costmodel layer only
	// (IsDraining := drain || !schedulable). The two channels are
	// intentionally different in operator-surface semantics.
	if got := reg.GetWorker(ctx, "w-drain-1"); got == nil {
		t.Errorf("w-drain-1 not registered (sanity regression before derivation check)")
	} else if got.ConnectionStatus != "DRAINING" {
		t.Errorf("w-drain-1 ConnectionStatus = %q, want %q (operator-facing read-model derivation: drain=true ⇒ DRAINING)",
			got.ConnectionStatus, "DRAINING")
	}

	// Sanity: the excluded workers MUST still be REGISTERED. The
	// contract is "not eligible for new offers", NOT "removed from
	// the registry". Misreading that would break health/decommission
	// visibility (a draining worker still shows up on the admin list
	// and on /api/v1/workers/:worker_id).
	for _, id := range []string{"w-drain-1", "w-unsched-1"} {
		if got := reg.GetWorker(ctx, id); got == nil {
			t.Errorf("worker %s should still be REGISTERED; schedulable filter must NOT remove from registry", id)
		}
	}
}
