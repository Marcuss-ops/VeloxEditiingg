package reconcile

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReconciler struct {
	name string
	// calls counts Reconcile invocations.
	calls *atomic.Int32
	// err returned by Reconcile; nil when not set.
	err error
}

func (f *fakeReconciler) Reconcile(_ context.Context, now time.Time) error {
	if f.calls != nil {
		f.calls.Add(1)
	}
	if !now.IsZero() && f.err != nil {
		return f.err
	}
	return f.err
}

func TestRegistry_RegisterAndOrder(t *testing.T) {
	reg := NewRegistry()
	a := &fakeReconciler{}
	b := &fakeReconciler{}
	if err := reg.Register("A", a); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("B", b); err != nil {
		t.Fatal(err)
	}
	names := reg.Names()
	if len(names) != 2 || names[0] != "A" || names[1] != "B" {
		t.Fatalf("names = %v, want [A B]", names)
	}
}

func TestRegistry_DuplicateRegistrationFails(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("A", &fakeReconciler{}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("A", &fakeReconciler{}); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	if err := reg.Register("", &fakeReconciler{}); err == nil {
		t.Fatal("empty name must fail")
	}
	if err := reg.Register("B", nil); err == nil {
		t.Fatal("nil reconciler must fail")
	}
}

func TestRegistry_ReconcileRunsAllInOrderAndReportsErrors(t *testing.T) {
	reg := NewRegistry()
	calls := &atomic.Int32{}
	first := &fakeReconciler{calls: calls}
	failing := &fakeReconciler{calls: calls, err: errors.New("boom")}
	last := &fakeReconciler{calls: calls}
	if err := reg.Register("first", first); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("failing", failing); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("last", last); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	report := reg.Reconcile(context.Background(), now)
	if report.GeneratedAt != now {
		t.Fatalf("report.GeneratedAt = %v, want %v", report.GeneratedAt, now)
	}
	if len(report.Entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(report.Entries))
	}
	// A failing entry must NOT stop the remaining entries.
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3 (failing entry must not stop the pass)", calls.Load())
	}
	if report.Entries[1].Err == nil || !strings.Contains(report.Entries[1].Err.Error(), "boom") {
		t.Fatalf("failing entry error = %v, want boom", report.Entries[1].Err)
	}
	if err := report.Err(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("report.Err() = %v, want boom", err)
	}
}

func TestRegistry_ReconcileEmptyRegistryIsNoop(t *testing.T) {
	reg := NewRegistry()
	report := reg.Reconcile(context.Background(), time.Now().UTC())
	if len(report.Entries) != 0 {
		t.Fatalf("empty registry produced %d entries", len(report.Entries))
	}
	if err := report.Err(); err != nil {
		t.Fatalf("empty registry error = %v", err)
	}
}
