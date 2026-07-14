// Package completion / coordinator_reconcile_test.go
//
// Per-phase split (declare / progress / complete-upload / commit /
// reconcile) extracted from coordinator_test.go. This file owns the
// FenceTuple utility tests that gate every Coordinator transition —
// including the ReconcileAttempt path (Phase 4.1 supervisor
// repair-forward). The reconcile phase is the last consumer to call
// FenceTuple.Read at entry: it re-validates the (worker_id, lease_id,
// task_revision) tuple before applying SetExpiredByID, so a
// regression in Validate / SQLWhere / SQLArgs breaks the entire
// supervisor surface.
//
// Coverage is small but high-leverage:
//
//   - TestFenceTuple_Validate            (empty-identity + negative-rev
//     rejection for every field)
//   - TestFenceTuple_SQLWhereAndArgs     (predicate string + ordered
//     placeholder values; the
//     inline CAS predicate is
//     revision-strict by
//     construction)
//
// The full FenceTuple.Read / ReadOrMissing gate coverage lives in
// fencing_test.go (extracted as part of the Phase 2.2 central-gate
// land).
package completion

import (
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// FenceTuple tests
// ────────────────────────────────────────────────────────────────────────

func TestFenceTuple_Validate(t *testing.T) {
	good := FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: 1}
	if err := good.Validate(); err != nil {
		t.Errorf("good tuple Validate: got %v, want nil", err)
	}

	cases := []struct {
		name string
		in   FenceTuple
	}{
		{"empty_task", FenceTuple{AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: 1}},
		{"empty_attempt", FenceTuple{TaskID: "t", WorkerID: "w", LeaseID: "l", Revision: 1}},
		{"empty_worker", FenceTuple{TaskID: "t", AttemptID: "a", LeaseID: "l", Revision: 1}},
		{"empty_lease", FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", Revision: 1}},
		{"negative_revision", FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: -1}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.in.Validate()
			if err == nil {
				t.Errorf("Validate on %s: got nil, want error", c.name)
			}
		})
	}
}

func TestFenceTuple_SQLWhereAndArgs(t *testing.T) {
	f := FenceTuple{TaskID: "T1", AttemptID: "A1", WorkerID: "W1", LeaseID: "L1", Revision: 2}
	where := f.SQLWhere()
	want := "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ? AND task_revision = ?"
	if where != want {
		t.Errorf("SQLWhere mismatch: got %q, want %q", where, want)
	}
	gotArgs := f.SQLArgs()
	wantArgs := []any{"T1", "A1", "W1", "L1", 2}
	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("SQLArgs length: got %d, want %d", len(gotArgs), len(wantArgs))
	}
	for i := range gotArgs {
		if gotArgs[i] != wantArgs[i] {
			t.Errorf("SQLArgs[%d]: got %v, want %v", i, gotArgs[i], wantArgs[i])
		}
	}
}
