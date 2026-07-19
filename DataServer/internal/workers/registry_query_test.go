// Package workers — RW-PROD-004 §3 A6+A7 tests for HasAtLeastOneLive.
//
// The master-side readiness gate is opt-in (VELOX_REQUIRE_LIVE_WORKERS);
// these tests verify the underlying getter so a regression in heartbeat
// freshness / session-active plumbing is caught independently of the
// opt-in flag.
//
// Build contract note: Registry.inMem is map[string]WorkerInfo (value
// type). Go forbids mutating a map-element value directly; every helper
// below seeds WorkerInfo fields INSIDE the struct literal. The single
// exception is the revoked map[string]bool (a primitive map where
// element-assignment IS valid).
package workers

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/costmodel"
)

// newRegistryWithLastHB seeds an in-memory worker with the supplied
// heartbeat timestamp + sessionActive flag.
//
// Pure test-only helper: does NOT touch SQLite; the test framework
// verifies HasAtLeastOneLive semantics directly without dbStore plumbing
// (dbStore==nil returns DISCONNECTED, which we don't want to assert).
func newRegistryWithLastHB(t *testing.T, workerID string, lastHB time.Time, sessionActive bool) *Registry {
	t.Helper()
	return &Registry{
		inMem: map[string]WorkerInfo{
			workerID: {
				WorkerID:      workerID,
				LastHB:        lastHB.UTC().Format(time.RFC3339),
				Schedulable:   true,
				SessionActive: sessionActive,
				Status:        StatusConnected,
				Capabilities:  map[string]interface{}{},
			},
		},
		revoked: map[string]bool{},
	}
}

const testLiveWorker = "worker-live-A7"

func TestRegistry_HasAtLeastOneLive_Empty(t *testing.T) {
	r := &Registry{
		inMem:   map[string]WorkerInfo{},
		revoked: map[string]bool{},
	}
	if r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("empty fleet must report HasAtLeastOneLive=false")
	}
}

func TestRegistry_HasAtLeastOneLive_Empty_NeverSet(t *testing.T) {
	r := &Registry{
		inMem:   map[string]WorkerInfo{},
		revoked: map[string]bool{},
	}
	if r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("uninitialised registry must report HasAtLeastOneLive=false")
	}
}

func TestRegistry_HasAtLeastOneLive_OneLive(t *testing.T) {
	r := newRegistryWithLastHB(t, testLiveWorker, time.Now().UTC(), true)
	if !r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("fresh-heartbeat worker must satisfy HasAtLeastOneLive")
	}
}

func TestRegistry_HasAtLeastOneLive_AllStale(t *testing.T) {
	// Heartbeat from 5 minutes ago → older than HasAtLeastOneLiveTimeout
	// (150s) AND ConnectionDisconnectedThreshold (5min), so the worker is
	// DISCONNECTED in canonical ConnectionStatus semantics.
	r := newRegistryWithLastHB(t, testLiveWorker, time.Now().UTC().Add(-5*time.Minute), true)
	if r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("stale-heartbeat worker must be excluded from HasAtLeastOneLive")
	}
}

func TestRegistry_HasAtLeastOneLive_Revoked(t *testing.T) {
	// Revoked workers are skipped by GetActiveWorkers via the `r.revoked[w.WorkerID]`
	// guard. LastHB fresh enough to be CONNECTED.
	r := newRegistryWithLastHB(t, testLiveWorker, time.Now().UTC(), true)
	r.revoked[testLiveWorker] = true
	if r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("revoked worker must be excluded from HasAtLeastOneLive even with fresh HB")
	}
}

func TestRegistry_HasAtLeastOneLive_StaleRejected_TwoLiveOk(t *testing.T) {
	// One recently-stale worker + one fresh worker ⇒ the fresh worker
	// keeps the gate satisfied (canonical "any one live is enough"
	// semantics, no per-worker quorum).
	r := &Registry{
		inMem: map[string]WorkerInfo{
			"stale-A": {
				WorkerID:      "stale-A",
				LastHB:        time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339),
				Schedulable:   true,
				SessionActive: true,
				Status:        StatusConnected,
				Capabilities:  map[string]interface{}{},
			},
			testLiveWorker: {
				WorkerID:      testLiveWorker,
				LastHB:        time.Now().UTC().Format(time.RFC3339),
				Schedulable:   true,
				SessionActive: true,
				Status:        StatusConnected,
				Capabilities:  map[string]interface{}{},
			},
		},
		revoked: map[string]bool{},
	}
	if !r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("mixed-stale fleet with at least one fresh worker must satisfy HasAtLeastOneLive")
	}
}

func TestRegistry_HasAtLeastOneLive_NilSafe(t *testing.T) {
	var r *Registry // nil
	if r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("nil registry must not panic and must report false")
	}
}

// TestRegistry_GetEligibleWorkers_DrainExclusionGuardsHasAtLeastOneLive
// ensures that even if drain=true on a fresh worker, the worker's
// "fresh heartbeat + active session" still satisfies HasAtLeastOneLive.
// The caller is the master-side GATE, not the dispatcher; drain is
// orthogonal — the master may still be ready while a worker drains.
func TestRegistry_GetEligibleWorkers_DrainExclusionGuardsHasAtLeastOneLive(t *testing.T) {
	r := &Registry{
		inMem: map[string]WorkerInfo{
			testLiveWorker: {
				WorkerID:      testLiveWorker,
				LastHB:        time.Now().UTC().Format(time.RFC3339),
				Schedulable:   true,
				SessionActive: true,
				Drain:         true,
				Status:        StatusConnected,
				Capabilities:  map[string]interface{}{"max_parallel_jobs": float64(1)},
			},
		},
		revoked: map[string]bool{},
	}
	// Drain should disqualify from eligibility — but the readiness gate
	// checks List/GetActiveWorkers semantics, NOT eligibility.
	if got := r.GetEligibleWorkers(context.Background(), costmodel.DefaultRequirements()); len(got) != 0 {
		t.Fatalf("draining worker should not be eligible; got %d", len(got))
	}
	if !r.HasAtLeastOneLive(context.Background()) {
		t.Fatal("draining worker must STILL satisfy HasAtLeastOneLive (master-side readiness is independent of dispatcher eligibility)")
	}
}
