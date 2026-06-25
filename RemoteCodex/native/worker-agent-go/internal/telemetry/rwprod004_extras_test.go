// Package telemetry — RW-PROD-004 round-2 follow-up tests
//
// MAJOR M1 — StableReasonsTaxonomy: walks every flag permutation and
// asserts the output is a subset of CanonicalReasons. A future refactor
// that adds a reason string outside the canonical list fails this test
// before dashboards can ship.
//
// MAJOR M2 — LiveStillAliveEvenWhenReadyNotReady: drives every reason
// pre-condition to NOT-READY and asserts /health/live is STILL 200
// with status=alive. Catches a regression where liveHandler
// accidentally consults GlobalReady().Snapshot() and inherits the
// not-ready verdict.
//
// MAJOR M3 — LivePropagatesWorkerID: SetHealthWorkerID feeds both
// liveHandler and legacyHealth — this test catches a refactor that
// drops the worker_id plumbing on the live path.
//
// MAJOR M4 — MethodNotAllowed on /health/live + /health: the three
// endpoints are now method-gated symmetrically (POST/PUT/DELETE → 405).
// The existing TestHealth_ReadyMethodNotAllowed covers /health/ready only.
package telemetry

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// canonicalReasonSet is the helper for M1. CanonicalReasons is the
// single exported list; convert once per test for O(N) contains().
func canonicalReasonSet() map[string]bool {
	m := make(map[string]bool, len(CanonicalReasons))
	for _, r := range CanonicalReasons {
		m[r] = true
	}
	return m
}

// TestReady_StableTaxonomy exhaustively emits every reason and
// verifies each emitted string is in CanonicalReasons. Removes the
// "drift-guard surface area" concern from the round-1 review.
func TestReady_StableTaxonomy(t *testing.T) {
	t.Cleanup(ResetForTest)
	permutations := []struct {
		name string
		fn   func()
	}{
		{"empty", func() {
			ResetForTest() // Snapshot starts at zero
		}},
		{"bootstrap_only", func() {
			MarkBootstrapped(true)
		}},
		{"registered_only", func() {
			MarkRegistered(true)
		}},
		{"drain_only", func() {
			MarkDrainMode(true)
		}},
		{"executors_only", func() {
			SetExecutorsCount(2)
		}},
		{"cache_only", func() {
			MarkCacheReady(true)
		}},
		{"blob_only", func() {
			MarkBlobReady(true)
		}},
		{"disk_critical", func() {
			SetDiskState(100, 1000)
		}},
		{"all_known_off", func() {
			MarkRegistered(false)
			MarkBootstrapped(false)
			MarkCacheReady(false)
			MarkBlobReady(false)
			MarkDrainMode(false)
			SetExecutorsCount(0)
			SetDiskState(0, 1<<20)
		}},
	}

	reasonSet := canonicalReasonSet()
	for _, p := range permutations {
		t.Run(p.name, func(t *testing.T) {
			t.Cleanup(ResetForTest)
			p.fn()
			for _, r := range GlobalReady().Snapshot().NotReadyReasons() {
				if !reasonSet[r] {
					t.Errorf("permutation %q emitted non-canonical reason %q; expected subset of %v", p.name, r, CanonicalReasons)
				}
			}
		})
	}
}

// TestReady_AllCanonicalReasonsReachable asserts every canonical reason
// can be reached exactly when its corresponding flip is set (false→true).
// Each subtest sets ONLY that flag and sets the others to "reached true"
// state, so the resulting reasons slice has exactly one entry — the
// specific reason under test.
//
// IMPORTANT: seedAllTrue takes the SUBTEST's t as parameter so each
// subtest gets its own Cleanup stack. Sharing the parent test's
// t.Cleanup here would let `drain_mode` (set by subtest N=3) persist
// into subtests N=4..7, producing false-positive reasons in the
// final assertions. We also explicitly MarkDrainMode(false) inside
// the seed so a previous subtest's flip does not leak.
func TestReady_AllCanonicalReasonsReachable(t *testing.T) {
	seedAllTrue := func(st *testing.T) {
		st.Cleanup(ResetForTest)
		MarkRegistered(true)
		MarkBootstrapped(true)
		MarkCacheReady(true)
		MarkBlobReady(true)
		// Defensive: drain_mode is sticky across rounds; an earlier
		// subtest that flipped drain=true would otherwise leak here.
		MarkDrainMode(false)
		SetExecutorsCount(1)
		SetDiskState(1<<30, 256*1024*1024)
	}
	for _, target := range CanonicalReasons {
		t.Run(target, func(t *testing.T) {
			seedAllTrue(t)
			// Flip the relevant flag back to off / 0 so the snapshot
			// includes ONLY this reason.
			switch target {
			case "bootstrap_not_run":
				MarkBootstrapped(false)
			case "not_registered":
				MarkRegistered(false)
			case "drain_mode":
				MarkDrainMode(true)
			case "executors.empty":
				SetExecutorsCount(0)
			case "cache.not_initialized":
				MarkCacheReady(false)
			case "blob.not_initialized":
				MarkBlobReady(false)
			case "disk.critical":
				SetDiskState(1, 1<<20)
			default:
				t.Fatalf("test missing inverse mapping for canonical reason %q", target)
			}
			reasons := GlobalReady().Snapshot().NotReadyReasons()
			if len(reasons) != 1 || reasons[0] != target {
				t.Fatalf("expected only %q; got %v", target, reasons)
			}
		})
	}
}

// TestHealth_Live_StillAliveEvenWhenReadyNotReady drives every reason
// pre-condition to the "not ready" verdict and asserts /health/live
// is STILL 200 + status="alive". This is the load-bearing invariant
// of the live/ready split — a regression here means monitoring
// tooling treats a healthy process as not-ready, completely
// defeating the purpose of the split.
func TestHealth_Live_StillAliveEvenWhenReadyNotReady(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(false)
	MarkBootstrapped(false)
	MarkDrainMode(true)
	MarkCacheReady(false)
	MarkBlobReady(false)
	SetExecutorsCount(0)
	SetDiskState(0, 1<<20)
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health/live MUST stay 200 even when /health/ready is 503; got %d", resp.StatusCode)
	}
	var out LiveResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Status != "alive" {
		t.Fatalf("status must be \"alive\"; got %q", out.Status)
	}
}

// TestHealth_LiveAlive_PropagatesWorkerID asserts that
// SetHealthWorkerID flows through to the /health/live body.
// A future refactor that drops the worker_id plumbing on live
// would surface as dashboard context loss.
func TestHealth_LiveAlive_PropagatesWorkerID(t *testing.T) {
	t.Cleanup(ResetForTest)
	SetHealthWorkerID("worker-fixed-live-A3")
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out LiveResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.WorkerID != "worker-fixed-live-A3" {
		t.Fatalf("expected worker_id=worker-fixed-live-A3; got %q", out.WorkerID)
	}
}

// TestHealth_LiveMethodNotAllowed is the M4 symmetry half:
// /health/live rejects POST with 405.
func TestHealth_LiveMethodNotAllowed(t *testing.T) {
	t.Cleanup(ResetForTest)
	srv := readyHealthServer(t)
	resp, err := http.Post(srv.URL+"/health/live", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /health/live; got %d", resp.StatusCode)
	}
}

// TestHealth_LegacyMethodNotAllowed is the other M4 half: legacy
// /health rejects POST with 405 (the deprecation adapter should
// still be method-gated like its siblings).
func TestHealth_LegacyMethodNotAllowed(t *testing.T) {
	t.Cleanup(ResetForTest)
	srv := readyHealthServer(t)
	resp, err := http.Post(srv.URL+"/health", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /health; got %d", resp.StatusCode)
	}
}

// TestHealth_Legacy_DeprecationHeader_OnEveryCall asserts the
// X-Velox-Health-Deprecated header is set on EVERY legacy /health
// call (the sync.Once controls only the LOG, not the header). Catches
// a regression where a future "first-call-only" optimisation
// accidentally drops the header on calls 2..N.
func TestHealth_Legacy_DeprecationHeader_OnEveryCall(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)
	srv := readyHealthServer(t)

	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL + "/health")
		if err != nil {
			t.Fatal(err)
		}
		got := resp.Header.Get("X-Velox-Health-Deprecated")
		resp.Body.Close()
		if !strings.Contains(got, "/health/live") {
			t.Fatalf("call %d missing X-Velox-Health-Deprecated; got %q", i, got)
		}
	}
}
