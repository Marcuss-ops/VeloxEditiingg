package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// TestBuildRecoveryReport_Empty returns false when state has nothing to report.
func TestBuildRecoveryReport_Empty(t *testing.T) {
	s := &persistedState{}
	report, _, ok := BuildRecoveryReport(s)
	if ok || report != nil {
		t.Fatalf("expected (nil,false) for empty state, got (%v, %v)", report, ok)
	}
	report, _, ok = BuildRecoveryReport(nil)
	if ok || report != nil {
		t.Fatalf("expected (nil,false) for nil state, got (%v, %v)", report, ok)
	}
}

// TestBuildRecoveryReport_OnlySeenCommands returns false (vacuous state).
// The contract: we only emit a report when there is JOB-level signal
// (active or pending lease). SeenCommands are RESTORED automatically on
// load but do not require master coordination.
func TestBuildRecoveryReport_OnlySeenCommands(t *testing.T) {
	s := &persistedState{
		SeenCommands: map[string]time.Time{"id:42": time.Now().UTC()},
	}
	report, _, ok := BuildRecoveryReport(s)
	if ok || report != nil {
		t.Fatalf("expected (nil,false) for seen-only state, got (%v, %v)", report, ok)
	}
}

// TestBuildRecoveryReport_WithActiveJobs emits a Struct with the full
// per-job contract.
func TestBuildRecoveryReport_WithActiveJobs(t *testing.T) {
	s := &persistedState{
		SeenCommands: map[string]time.Time{"id:99": time.Now().UTC()},
		SavedAt:      time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC),
		ActiveJobs: map[string]persistedJobInfo{
			"job-A": {JobID: "job-A", JobRunID: "run-A", JobType: "render", LeaseID: "lease-A", StartedAt: "2026-06-19T08:59:50Z"},
		},
		PendingLeaseJobs: map[string]persistedJobInfo{
			"job-B": {JobID: "job-B", JobRunID: "run-B", JobType: "process_video", LeaseID: "lease-B"},
		},
	}
	report, _, ok := BuildRecoveryReport(s)
	if !ok || report == nil {
		t.Fatalf("expected (non-nil, true) for populated state")
	}
	m := report.AsMap()
	if m["schema_version"] != "v1" {
		t.Fatalf("schema_version mismatch: %v", m["schema_version"])
	}
	if m["saved_at"] == "" {
		t.Fatalf("saved_at missing in report: %v", m["saved_at"])
	}
	if got, _ := m["active_jobs_count"].(float64); got != 1 {
		t.Fatalf("active_jobs_count mismatch: %v", m["active_jobs_count"])
	}
	if got, _ := m["pending_leases_count"].(float64); got != 1 {
		t.Fatalf("pending_leases_count mismatch: %v", m["pending_leases_count"])
	}
	active, ok := m["active_jobs"].([]interface{})
	if !ok || len(active) != 1 {
		t.Fatalf("active_jobs missing: %v", m["active_jobs"])
	}
	pending, ok := m["pending_lease_jobs"].([]interface{})
	if !ok || len(pending) != 1 {
		t.Fatalf("pending_lease_jobs missing: %v", m["pending_lease_jobs"])
	}
}

// TestReadPersistedState_RoundTrip writes a state file and re-reads it.
func TestReadPersistedState_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	state := persistedState{
		SeenCommands: map[string]time.Time{"id:7": time.Now().UTC()},
		SavedAt:      time.Now().UTC(),
		ActiveJobs:   map[string]persistedJobInfo{"job-X": {JobID: "job-X", LeaseID: "L1"}},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(stateFilePath(tmp), data, 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadPersistedState(tmp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.ActiveJobs) != 1 || got.ActiveJobs["job-X"].LeaseID != "L1" {
		t.Fatalf("round-trip mismatch: %v", got)
	}
}

// TestReadPersistedState_MissingFile returns os.ErrNotExist (and the worker
// treats this as the no-recovery case).
func TestReadPersistedState_MissingFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "no-such-dir")
	got, err := ReadPersistedState(tmp)
	if err == nil || got != nil {
		t.Fatalf("expected (nil, err) for missing file, got (%v, %v)", got, err)
	}
}

// TestRecoveryVocabularyPin ensures the worker-side action constants match
// the master-side constants byte-for-byte. Constants are mirrored in two
// packages; a drift here would silently cause the worker to ignore valid
// master directives.
func TestRecoveryVocabularyPin(t *testing.T) {
	pairs := []struct{ worker, master string }{
		{RecoveryActionContinue, "CONTINUE"},
		{RecoveryActionCancel, "CANCEL"},
		{RecoveryActionResumeUpload, "RESUME_UPLOAD"},
		{RecoveryActionCleanup, "CLEANUP"},
	}
	for _, p := range pairs {
		if p.worker != p.master {
			t.Fatalf("worker constant %q drifted from expected %q", p.worker, p.master)
		}
	}
}

// TestHandleRecoveryDirective_NilStruct is a no-op (defensive guard).
func TestHandleRecoveryDirective_NilStruct(t *testing.T) {
	w := &Worker{}
	w.handleRecoveryDirective(nil) // must not panic
}

// TestHandleRecoveryDirective_EmptyMapWithoutKey is a no-op (no directive).
func TestHandleRecoveryDirective_EmptyMapWithoutKey(t *testing.T) {
	s, _ := structpb.NewStruct(map[string]interface{}{"unrelated_key": 42})
	w := &Worker{}
	w.handleRecoveryDirective(s) // must not panic
}
