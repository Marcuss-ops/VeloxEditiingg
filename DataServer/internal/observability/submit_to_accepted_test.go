package observability

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// submitBucketJobReader is a JobReader that returns a job with a
// creation timestamp (the Master-clock boundary of the submit→accepted
// bucket).
type submitBucketJobReader struct {
	stubJobReader
	job *jobs.Job
}

func (s *submitBucketJobReader) Get(context.Context, string) (*jobs.Job, error) {
	return s.job, nil
}

// TestSummarizeTask_SubmitToAcceptedBucketRidesExecutionProjectionOnly
// locks the four invariants of the job-level pre-atttempt bucket:
//
//  1. it exists and carries the real submit→accept wait (60s here) —
//     the stretch bucketDefs can never see because the milestone
//     timeline starts at attempt.accepted;
//  2. the attempt-scoped wall_ms / coverage_pct stay untouched (they
//     keep their pinned meaning: attempt lifecycle only);
//  3. the per-attempt waterfall does NOT grow the bucket (job-level
//     data never leaks into the attempt projection);
//  4. it is omitted, never zero-filled, when a boundary is unknown.
func TestSummarizeTask_SubmitToAcceptedBucketRidesExecutionProjectionOnly(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-q"] = &taskgraph.Task{
		ID: "T-q", JobID: "J-q", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	start := time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC)
	end := start.Add(328041 * time.Millisecond)
	attempts.attempts["T-q"] = []taskattempts.TaskAttempt{{
		ID: "A-q", TaskID: "T-q", JobID: "J-q", WorkerID: "worker-01",
		Status: taskattempts.AttemptStatusSucceeded, AttemptNumber: 1,
		StartedAt: &start, CompletedAt: &end,
	}}
	attempts.rawReports = map[string]string{"A-q": realisticAttemptReportJSON}

	// Job submitted exactly 60s before the attempt started.
	svc.WithJobs(&submitBucketJobReader{job: &jobs.Job{
		ID: "J-q", Status: jobs.StatusSucceeded, CreatedAt: start.Add(-time.Minute),
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-q")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.Waterfall == nil {
		t.Fatal("execution waterfall missing despite a durable report")
	}
	bucket := result.Waterfall.SubmitToAccepted
	if bucket == nil {
		t.Fatal("submit_to_accepted bucket missing despite known job.created_at + attempt.started_at")
	}
	if bucket.Name != "submit_to_accepted" || bucket.DurationMS != 60_000 {
		t.Fatalf("bucket = %+v, want submit_to_accepted / 60000ms", bucket)
	}

	// The attempt-scoped contract is untouched: wall is still the
	// attempt lifecycle, coverage still 100% for this tiled timeline.
	attemptSummary := result.Attempts[0]
	if result.Waterfall.WallMS != attemptSummary.DurationMS {
		t.Fatalf("wall_ms = %d, want the attempt lifecycle duration %d (submit wait must not extend it)",
			result.Waterfall.WallMS, attemptSummary.DurationMS)
	}
	if result.Waterfall.CoveragePct != 100 {
		t.Fatalf("coverage_pct = %f, want 100 (the pre-stage is outside accounted_ms)", result.Waterfall.CoveragePct)
	}
	if attemptSummary.AttemptWaterfall == nil {
		t.Fatal("attempt waterfall missing")
	}
	if attemptSummary.AttemptWaterfall.SubmitToAccepted != nil {
		t.Fatalf("job-level bucket leaked into the attempt projection: %+v",
			attemptSummary.AttemptWaterfall.SubmitToAccepted)
	}
}

// TestSummarizeTask_SubmitToAcceptedOmittedWithoutJobTimestamp: an
// unknown job reader or an unknown attempt start leaves the bucket
// absent — a fabricated 0ms wait would read as "no queue at all".
func TestSummarizeTask_SubmitToAcceptedOmittedWithoutJobTimestamp(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-n"] = &taskgraph.Task{
		ID: "T-n", JobID: "J-n", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	start := time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC)
	end := start.Add(time.Second)
	attempts.attempts["T-n"] = []taskattempts.TaskAttempt{{
		ID: "A-n", TaskID: "T-n", JobID: "J-n", WorkerID: "worker-01",
		Status: taskattempts.AttemptStatusSucceeded, AttemptNumber: 1,
		StartedAt: &start, CompletedAt: &end,
	}}
	attempts.rawReports = map[string]string{"A-n": realisticAttemptReportJSON}

	// Jobs reader returns no row (the default stub) → bucket absent.
	result, err := svc.SummarizeTask(context.Background(), "T-n")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.Waterfall == nil {
		t.Fatal("execution waterfall missing")
	}
	if result.Waterfall.SubmitToAccepted != nil {
		t.Fatalf("bucket fabricated without a job row: %+v", result.Waterfall.SubmitToAccepted)
	}
}

// TestSubmitToAcceptedBucketBoundaryRules pins the helper's honesty
// contract directly: missing or inverted pairs produce no bucket.
func TestSubmitToAcceptedBucketBoundaryRules(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name                    string
		submitted, firstAttempt time.Time
		wantNil                 bool
		wantMS                  int64
	}{
		{name: "normal wait", submitted: now, firstAttempt: now.Add(90 * time.Second), wantMS: 90_000},
		{name: "missing submission", submitted: time.Time{}, firstAttempt: now, wantNil: true},
		{name: "missing attempt start", submitted: now, firstAttempt: time.Time{}, wantNil: true},
		{name: "inverted pair", submitted: now, firstAttempt: now.Add(-time.Second), wantNil: true},
		{name: "same instant", submitted: now, firstAttempt: now, wantNil: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SubmitToAcceptedBucket(tc.submitted, tc.firstAttempt)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("bucket = %+v, want nil", got)
				}
				return
			}
			if got == nil || got.DurationMS != tc.wantMS {
				t.Fatalf("bucket = %+v, want %dms", got, tc.wantMS)
			}
		})
	}
}
