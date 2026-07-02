package registry

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// errProbeFail is the sentinel used by failing-check probes in
// tests. Wrapping lets each probe carry a unique marker so the
// aggregate error message can be parsed position-by-position to
// verify sort order.
var errProbeFail = errors.New("probe failure")

func okProbe() func() error { return func() error { return nil } }

func failProbe(marker string) func() error {
	return func() error { return fmt.Errorf("%w: %s", errProbeFail, marker) }
}

// TestCapabilityRegistry_Readyz_SnapshotOrdering locks down the
// invariants of CapabilityRegistry.Readyz():
//
//  1. Iteration order over failing probes in the aggregated error
//     message is alphabetically sorted by probe name. This makes
//     log diffs across releases reproducible (operators grep /ready
//     output with confidence that failing entry ordering does not
//     change across restarts).
//
//  2. The Readyz() call takes a deterministic snapshot under the
//     RWMutex (RLock + copy + RUnlock) BEFORE invoking any probe.
//     Concurrent Register / Unregister calls during a Readyz pass
//     do not race with the snapshot — verified by running under
//     `go test -race` with concurrent goroutines interleaving.
//
//  3. Register / Readyz / Unregister / Names accept explicit
//     fail-closed inputs (nil registry, empty Name, nil Check).
//
// Run with: `go test -race ./internal/registry/...`
func TestCapabilityRegistry_Readyz_SnapshotOrdering(t *testing.T) {

	t.Run("OrderIsNameSorted", func(t *testing.T) {
		r := NewCapabilityRegistry()
		// Register out of sorted order on purpose.
		if err := r.Register(Probe{Name: "zeta", Check: failProbe("z")}); err != nil {
			t.Fatalf("Register(zeta): %v", err)
		}
		if err := r.Register(Probe{Name: "alpha", Check: failProbe("a")}); err != nil {
			t.Fatalf("Register(alpha): %v", err)
		}
		if err := r.Register(Probe{Name: "mu", Check: failProbe("m")}); err != nil {
			t.Fatalf("Register(mu): %v", err)
		}
		if err := r.Register(Probe{Name: "beta", Check: okProbe()}); err != nil {
			t.Fatalf("Register(beta): %v", err)
		}

		err := r.Readyz()
		if err == nil {
			t.Fatalf("expected non-nil error: 3 of 4 probes fail")
		}
		if !errors.Is(err, ErrCapabilityNotReady) {
			t.Fatalf("expected ErrCapabilityNotReady in chain, got %v", err)
		}

		// Failing set must be exactly {alpha, mu, zeta} (beta is ok).
		// Verify entry order: alpha appears before mu, mu before zeta.
		msg := err.Error()
		idxA := strings.Index(msg, "alpha(")
		idxM := strings.Index(msg, "mu(")
		idxZ := strings.Index(msg, "zeta(")
		if idxA < 0 || idxM < 0 || idxZ < 0 {
			t.Fatalf("error message missing one of alpha/mu/zeta entries: %q", msg)
		}
		if !(idxA < idxM && idxM < idxZ) {
			t.Errorf("expected sorted order alpha→mu→zeta, got: %q", msg)
		}
		// Folded-in guard (replaces the formerly-separate
		// MixedPassFail_OnlyFailingInMessage sub-test): the ok probe
		// must NOT appear in the failing list — a future refactor that
		// re-shapes the failing aggregation should fail this guard.
		if strings.Contains(msg, "beta(") {
			t.Errorf("ok probes must not appear in failing list: %q", msg)
		}
	})

	t.Run("NamesReturnSorted", func(t *testing.T) {
		r := NewCapabilityRegistry()
		_ = r.Register(Probe{Name: "zeta", Check: okProbe()})
		_ = r.Register(Probe{Name: "alpha", Check: okProbe()})
		_ = r.Register(Probe{Name: "mu", Check: okProbe()})

		names := r.Names()
		want := []string{"alpha", "mu", "zeta"}
		if len(names) != len(want) {
			t.Fatalf("Names count: got %d want %d (%v)", len(names), len(want), names)
		}
		for i, n := range want {
			if names[i] != n {
				t.Errorf("Names()[%d]: got %q want %q", i, names[i], n)
			}
		}
		if !sort.StringsAreSorted(names) {
			t.Errorf("Names() must be sorted, got: %v", names)
		}
	})

	t.Run("EmptyRegistry_ReadyzNil", func(t *testing.T) {
		r := NewCapabilityRegistry()
		if err := r.Readyz(); err != nil {
			t.Errorf("empty registry should be ready, got: %v", err)
		}
		if names := r.Names(); len(names) != 0 {
			t.Errorf("empty registry Names should be empty, got: %v", names)
		}
	})

	// Guards — round-2 addition. The registry implements explicit
	// fail-closed inputs for nil registry / empty Name / nil Check.
	// These branches are cheap to test and lock the contract against
	// future refactors that might drop them.
	t.Run("Guards_NilAndEmptyInputs", func(t *testing.T) {
		t.Run("Register_NilRegistry", func(t *testing.T) {
			var r *CapabilityRegistry // nil
			err := r.Register(Probe{Name: "x", Check: okProbe()})
			if err == nil {
				t.Errorf("Register on nil registry must return error")
			}
		})
		t.Run("Register_EmptyName", func(t *testing.T) {
			r := NewCapabilityRegistry()
			err := r.Register(Probe{Name: "", Check: okProbe()})
			if err == nil {
				t.Errorf("Register with empty Name must return error")
			}
		})
		t.Run("Register_NilCheck", func(t *testing.T) {
			r := NewCapabilityRegistry()
			err := r.Register(Probe{Name: "x", Check: nil})
			if err == nil {
				t.Errorf("Register with nil Check must return error")
			}
		})
		t.Run("Readyz_NilRegistry", func(t *testing.T) {
			var r *CapabilityRegistry // nil
			err := r.Readyz()
			if err == nil {
				t.Errorf("Readyz on nil registry must return error")
			} else if !errors.Is(err, ErrCapabilityNotReady) {
				t.Errorf("Readyz on nil registry must wrap ErrCapabilityNotReady, got: %v", err)
			}
		})
		t.Run("Names_NilRegistry", func(t *testing.T) {
			var r *CapabilityRegistry // nil
			if names := r.Names(); names != nil {
				t.Errorf("Names on nil registry must return nil, got: %v", names)
			}
		})
		t.Run("Unregister_NilRegistry", func(t *testing.T) {
			var r *CapabilityRegistry // nil
			// Must NOT panic; Unregister explicitly nil-checks.
			r.Unregister("anything")
		})
		t.Run("Unregister_EmptyName", func(t *testing.T) {
			r := NewCapabilityRegistry()
			// Must NOT panic + must NOT remove any probe.
			_ = r.Register(Probe{Name: "kept", Check: okProbe()})
			r.Unregister("")
			if names := r.Names(); len(names) != 1 || names[0] != "kept" {
				t.Errorf("Unregister(\"\") must be no-op, got: %v", names)
			}
		})
	})

	t.Run("ConcurrentRegisterDuringReadyz_NoRace", func(t *testing.T) {
		// Round-2 refactor: deterministic overlap is enforced with a
		// `start` channel barrier. Without it, fast machines can finish
		// the 240 Register calls before any slow_a/slow_b probe yields.
		// Writers block on <-start; the test closes `start` AFTER the
		// first Readyz pass commits, guaranteeing concurrent Register
		// is in flight while subsequent Readyz passes execute probes.
		r := NewCapabilityRegistry()
		_ = r.Register(Probe{Name: "slow_a", Check: func() error {
			time.Sleep(30 * time.Millisecond)
			return nil
		}})
		_ = r.Register(Probe{Name: "slow_b", Check: func() error {
			time.Sleep(30 * time.Millisecond)
			return nil
		}})

		const (
			concurrentWriters      = 4
			registersPerWriter     = 60
			readyzPassesDuringLoad = 12
		)
		start := make(chan struct{})
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < concurrentWriters; i++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				<-start // Barrier: block until test releases writers.
				for n := 0; n < registersPerWriter; n++ {
					select {
					case <-stop:
						return
					default:
					}
					if err := r.Register(Probe{
						Name:  fmt.Sprintf("concurrent_%d_%03d", seed, n),
						Check: okProbe(),
					}); err != nil {
						t.Errorf("Register(%d,%d): %v", seed, n, err)
						return
					}
				}
			}(i)
		}

		// First Readyz: primes the snapshot window. Must succeed (only
		// slow_a + slow_b are registered, both returning nil).
		if err := r.Readyz(); err != nil {
			t.Errorf("priming Readyz: unexpected err: %v", err)
		}
		// Release writers, then run the remaining passes. From this
		// point onward every Readyz passes must observe at least some
		// concurrent_X_N probes AS WELL AS still-pending ones being
		// added in between — that is the substantive race-cover.
		close(start)
		for pass := 1; pass < readyzPassesDuringLoad; pass++ {
			if err := r.Readyz(); err != nil {
				t.Errorf("Readyz pass %d: unexpected err: %v", pass, err)
			}
		}

		close(stop)
		wg.Wait()

		// Post-storm Readyz: snapshot now includes slow_a, slow_b, plus
		// all concurrent_X_N registrations. Err must be nil because every
		// registered probe (including the just-added concurrent ones) is
		// okProbe().
		if err := r.Readyz(); err != nil {
			t.Errorf("post-storm Readyz: unexpected err: %v", err)
		}

		names := r.Names()
		expectedMin := 2 + concurrentWriters*registersPerWriter
		if len(names) != expectedMin {
			t.Errorf("post-storm Names count: got %d want %d", len(names), expectedMin)
		}
		// Names() must remain sorted even after concurrent Register storm.
		if !sort.StringsAreSorted(names) {
			t.Errorf("post-storm Names() not sorted (len=%d)", len(names))
		}
	})

	t.Run("UnregisterDuringReadyz_NoRace", func(t *testing.T) {
		// Companion test for Unregister: remove a probe while a slow
		// Readyz pass is in flight. -race must remain clean.
		r := NewCapabilityRegistry()
		_ = r.Register(Probe{Name: "slow_a", Check: func() error {
			time.Sleep(40 * time.Millisecond)
			return nil
		}})
		_ = r.Register(Probe{Name: "victim", Check: okProbe()})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Unregister("victim")
		}()
		if err := r.Readyz(); err != nil {
			t.Errorf("Readyz during Unregister: unexpected err: %v", err)
		}
		wg.Wait()
	})

	t.Run("OverwriteExistingName", func(t *testing.T) {
		// Documents the "overwrite permitted" semantics explicitly so a
		// future refactor that locks the contract survives the change
		// because this sub-test pins the behaviour. Used by hot-swap
		// paths (toggling a probe from ok to failing without renaming).
		r := NewCapabilityRegistry()
		if err := r.Register(Probe{Name: "x", Check: okProbe()}); err != nil {
			t.Fatalf("Register(x): %v", err)
		}
		if err := r.Readyz(); err != nil {
			t.Errorf("first Readyz must be nil (ok probe), got: %v", err)
		}
		// Overwrite with a failing check on the same name.
		if err := r.Register(Probe{Name: "x", Check: failProbe("now-broken")}); err != nil {
			t.Fatalf("Register(x overwrite): %v", err)
		}
		if err := r.Readyz(); err == nil {
			t.Errorf("Readyz after overwrite must surface ErrCapabilityNotReady")
		} else if !errors.Is(err, ErrCapabilityNotReady) {
			t.Errorf("overwrite failure must wrap ErrCapabilityNotReady, got: %v", err)
		}
		// Names() count must NOT double-after-overwrite (single X slot).
		names := r.Names()
		if len(names) != 1 || names[0] != "x" {
			t.Errorf("Names() after overwrite: got %v, want [x]", names)
		}
	})
}
