package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"velox-server/internal/jobs"
)

// fakeJobQueueSource is the JobQueueSource double for the supervisor
// tests: one snapshot + a fixed set of terminal completions, with call
// counters so a test can assert the per-tick refresh cadence.
type fakeJobQueueSource struct {
	snapshot jobs.QueueSnapshot
	records  []jobs.CompletionRecord

	snapshotCalls int
	recordsCalls  int
	snapshotErr   error
}

func (f *fakeJobQueueSource) QueueSnapshot(context.Context, time.Time) (jobs.QueueSnapshot, error) {
	f.snapshotCalls++
	return f.snapshot, f.snapshotErr
}

func (f *fakeJobQueueSource) RecentTerminalJobs(context.Context, time.Time, int) ([]jobs.CompletionRecord, error) {
	f.recordsCalls++
	return f.records, nil
}

// TestJobQueueFamiliesAreExposed locks the three queue gauges and the
// completion counters/histogram on the wire, including the phase label
// values the e2e latency attribution is built on.
func TestJobQueueFamiliesAreExposed(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	c.RecordJobQueue(jobs.QueueSnapshot{
		Pending:          3,
		Running:          2,
		OldestPendingAge: 7 * time.Minute,
	})
	created := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	c.RecordJobCompletion(jobs.CompletionRecord{
		ID:          "job-1",
		Status:      jobs.StatusSucceeded,
		CreatedAt:   created,
		StartedAt:   created.Add(time.Minute),
		CompletedAt: created.Add(5 * time.Minute),
	})
	c.RecordJobCompletion(jobs.CompletionRecord{
		ID:     "job-2",
		Status: jobs.StatusFailed,
		// No timestamps: outcome counted, e2e stages stay unobserved
		// (absence is honest, a fabricated zero is not).
	})

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		"velox_jobs_pending 3",
		"velox_jobs_running 2",
		"velox_jobs_oldest_pending_age_seconds 420",
		"velox_job_succeeded_total 1",
		"velox_job_failed_total 1",
		`velox_job_e2e_duration_seconds_bucket{phase="queue",le="60"} 1`,
		`velox_job_e2e_duration_seconds_bucket{phase="execute",le="300"} 1`,
		`velox_job_e2e_duration_seconds_sum{phase="total"} 300`,
		`velox_job_e2e_duration_seconds_count{phase="total"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// The timestamp-less completion contributes its outcome but no
	// observation: only ONE queue/execute/total observation exists.
	for _, phase := range []string{"queue", "execute", "total"} {
		want := `velox_job_e2e_duration_seconds_count{phase="` + phase + `"} 1`
		if !strings.Contains(out, want) {
			t.Errorf("missing %q (a stage with unknown boundaries must not be observed) in:\n%s", want, out)
		}
	}
}

// TestJobQueueFamiliesInvertTimestampsAreDropped: a completion whose
// boundaries move backwards is clock corruption, never a negative
// duration.
func TestJobQueueFamiliesInvertTimestampsAreDropped(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	end := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	c.RecordJobCompletion(jobs.CompletionRecord{
		ID:     "job-inverted",
		Status: jobs.StatusSucceeded,
		// Every boundary runs backwards (master/worker clock skew):
		// started before created, completed before started.
		CreatedAt:   end,
		StartedAt:   end.Add(-time.Minute),
		CompletedAt: end.Add(-2 * time.Minute),
	})
	out := dumpRegistryAll(t, reg)
	if strings.Contains(out, "velox_job_e2e_duration_seconds_count") {
		t.Fatalf("inverted pair produced an observation:\n%s", out)
	}
	if !strings.Contains(out, "velox_job_succeeded_total 1") {
		t.Fatalf("outcome counter missing for the same completion:\n%s", out)
	}
}

// TestSupervisor_StampsJobQueueAndCountsCompletionOnce: an attempt-
// quiet tick (no newly-terminal attempt) must still refresh the queue
// gauges, and the terminal-job completion must be counted exactly once
// even when the source keeps returning it on the next tick (the seenJobs
// dedup).
func TestSupervisor_StampsJobQueueAndCountsCompletionOnce(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{attempts: map[string]*fakeAttemptRecord{}}
	source := &fakeJobQueueSource{
		snapshot: jobs.QueueSnapshot{Pending: 4, Running: 1, OldestPendingAge: 300 * time.Second},
		records: []jobs.CompletionRecord{{
			ID:     "job-done",
			Status: jobs.StatusSucceeded,
		}},
	}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{}, DefaultCostFactors())
	s.SetJobQueueSource(source)
	s.SetLimit(50)

	first := time.Now().UTC()
	if err := s.tickOnce(context.Background(), first.Add(time.Second)); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	second := s.tickOnce(context.Background(), first.Add(2*time.Second))
	if second != nil {
		t.Fatalf("second tick: %v", second)
	}

	if source.snapshotCalls != 2 || source.recordsCalls != 2 {
		t.Fatalf("job-queue source calls = %d snapshots / %d records, want 2/2 (queue refresh is not attempt-driven)",
			source.snapshotCalls, source.recordsCalls)
	}
	out := dumpFamily(t, reg, "velox_jobs_pending")
	if !strings.Contains(out, "velox_jobs_pending 4") {
		t.Errorf("queue gauge not stamped on a quiet tick:\n%s", out)
	}
	if got := dumpFamily(t, reg, "velox_job_succeeded_total"); !strings.Contains(got, "velox_job_succeeded_total 1") {
		t.Errorf("completion counter =\n%s, want exactly 1 after two ticks", got)
	}
}

// TestSupervisor_JobQueueErrorPropagates: a queue-source failure is
// infrastructure (the projection is blind), never a silent no-alert.
func TestSupervisor_JobQueueErrorPropagates(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{attempts: map[string]*fakeAttemptRecord{}}
	source := &fakeJobQueueSource{snapshotErr: context.DeadlineExceeded}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{}, DefaultCostFactors())
	s.SetJobQueueSource(source)

	if err := s.tickOnce(context.Background(), time.Now().UTC().Add(time.Second)); err == nil {
		t.Fatal("queue snapshot failure must surface as a tick error")
	}
}

// TestSupervisor_NilJobQueueSourceIsNoOp: the wiring is optional — a
// supervisor without a source behaves exactly as before (no job family
// rows, no error).
func TestSupervisor_NilJobQueueSourceIsNoOp(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{attempts: map[string]*fakeAttemptRecord{}}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{}, DefaultCostFactors())
	if err := s.tickOnce(context.Background(), time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatalf("tick with no job source: %v", err)
	}
	// Without a source the family must stay un-stamped (no fabricated
	// zero on the wire).
	if out := dumpFamily(t, reg, "velox_jobs_pending"); strings.Contains(out, "velox_jobs_pending 0") {
		t.Fatalf("job family stamped without a source:\n%s", out)
	}
}
