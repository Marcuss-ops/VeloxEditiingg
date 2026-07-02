package registry

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// TestCapabilityRegistry_Readyz_SnapshotOrdering locks down two
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
//     Concurrent Register calls during a Readyz pass do not race
//     with the snapshot — verified by running under `go test -race`
//     with concurrent Register goroutines interleaving with
//     long-running probes.
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
		if strings.Contains(msg, "beta(") {
			t.Errorf("ok probes should not appear in failing list: %q", msg)
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

	t.Run("MixedPassFail_OnlyFailingInMessage", func(t *testing.T) {
		r := NewCapabilityRegistry()
		_ = r.Register(Probe{Name: "ok_a", Check: okProbe()})
		_ = r.Register(Probe{Name: "fail_x", Check: failProbe("x")})
		_ = r.Register(Probe{Name: "ok_b", Check: okProbe()})
		_ = r.Register(Probe{Name: "fail_y", Check: failProbe("y")})
		err := r.Readyz()
		if err == nil {
			t.Fatalf("expected non-nil error")
		}
		msg := err.Error()
		idxX := strings.Index(msg, "fail_x")
		idxY := strings.Index(msg, "fail_y")
		if idxX < 0 || idxY < 0 {
			t.Fatalf("missing failing entries: %q", msg)
		}
		if idxX >= idxY {
			t.Errorf("expected sorted order fail_x→fail_y, got: %q", msg)
		}
		if strings.Contains(msg, "ok_a(") || strings.Contains(msg, "ok_b(") {
			t.Errorf("ok probes should NOT appear in failing list: %q", msg)
		}
	})

	t.Run("ConcurrentRegisterDuringReadyz_NoRace", func(t *testing.T) {
		// This is the headline race-cover test: drive Readyz() concurrently
		// with Register() so that if CapabilityRegistry ever re-introduces
		// a data race (e.g. iterating r.probes without RLock), `go test -race`
		// will catch it here. The two slow_a / slow_b probes widen the
		// snapshot window so the concurrent Register goroutines actually
		// overlap probe execution.
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
		stop := make(chan struct{})
		var (
			wg        sync.WaitGroup
			startedAt atomic.Int32
		)
		for i := 0; i < concurrentWriters; i++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				startedAt.Add(1)
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

		// Run Readyz passes while the writers are hammering Register.
		// Each Readyz takes >= 60ms (slow_a + slow_b serial execution).
		// 12 passes = ~720ms — wide overlap with writer goroutines.
		for pass := 0; pass < readyzPassesDuringLoad; pass++ {
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
}
