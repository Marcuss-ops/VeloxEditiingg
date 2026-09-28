package alertengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/observability"
	"velox-server/internal/taskgraph"
)

// stubQueueJobReader is a JobReader that also satisfies
// observability.JobQueueReader, i.e. the wired production shape
// (*store.SQLiteJobRepository).
type stubQueueJobReader struct {
	stubJobReader
	age time.Duration
	err error
}

func (s *stubQueueJobReader) QueueSnapshot(context.Context, time.Time) (jobs.QueueSnapshot, error) {
	if s.err != nil {
		return jobs.QueueSnapshot{}, s.err
	}
	return jobs.QueueSnapshot{Pending: 1, OldestPendingAge: s.age}, nil
}

// stubPrefetchReader is the observability.PrefetchFailureReader double.
type stubPrefetchReader struct {
	count int64
	err   error
}

func (s *stubPrefetchReader) CountRecentPrefetchFailures(context.Context, time.Time) (int64, error) {
	return s.count, s.err
}

// TestRuleJobPendingAgeHigh_TriggersOnOldestPendingAge: the Milton
// case — one job, PENDING for 7 minutes, workers idle — must alert
// even though every count-based rule stays green.
func TestRuleJobPendingAgeHigh_TriggersOnOldestPendingAge(t *testing.T) {
	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithJobs(&stubQueueJobReader{age: 7 * time.Minute})

	deps := DefaultRuleDeps()
	deps.Obs = obs
	event, err := ruleJobPendingAgeHigh(deps)(context.Background())
	if err != nil {
		t.Fatalf("rule returned error: %v", err)
	}
	if event == nil {
		t.Fatal("7-minute pending job did not fire JobPendingAgeHigh")
	}
	if event.RuleID != "JobPendingAgeHigh" || event.Severity != "warning" {
		t.Fatalf("event = %+v, want RuleID=JobPendingAgeHigh severity=warning", event)
	}
	if event.Labels["age_seconds"] != "420" {
		t.Errorf("age_seconds label = %q, want 420", event.Labels["age_seconds"])
	}
}

// TestRuleJobPendingAgeHigh_SilentBelowThreshold: depth > 0 is normal;
// only the AGE crosses the line.
func TestRuleJobPendingAgeHigh_SilentBelowThreshold(t *testing.T) {
	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithJobs(&stubQueueJobReader{age: 4 * time.Minute})

	deps := DefaultRuleDeps()
	deps.Obs = obs
	event, err := ruleJobPendingAgeHigh(deps)(context.Background())
	if err != nil {
		t.Fatalf("rule returned error: %v", err)
	}
	if event != nil {
		t.Fatalf("4-minute pending job fired %s; threshold is 5 minutes", event.RuleID)
	}
}

// TestRuleJobPendingAgeHigh_SilentWithoutQueueReader: an unwired
// reader is "unknown", never "healthy" or "broken" — the rule stays
// quiet and does not turn missing wiring into an infrastructure error.
func TestRuleJobPendingAgeHigh_SilentWithoutQueueReader(t *testing.T) {
	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithJobs(&stubJobReader{counts: jobs.Counts{jobs.StatusPending: 5}})

	deps := DefaultRuleDeps()
	deps.Obs = obs
	event, err := ruleJobPendingAgeHigh(deps)(context.Background())
	if err != nil {
		t.Fatalf("rule returned error for an unwired reader: %v", err)
	}
	if event != nil {
		t.Fatalf("unwired queue reader fired %s", event.RuleID)
	}
}

// TestRuleJobPendingAgeHigh_ReaderErrorPropagates: a failing read is
// infrastructure — the rule must report it, never convert it into
// "no alert".
func TestRuleJobPendingAgeHigh_ReaderErrorPropagates(t *testing.T) {
	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithJobs(&stubQueueJobReader{age: time.Hour, err: errors.New("db locked")})

	deps := DefaultRuleDeps()
	deps.Obs = obs
	if _, err := ruleJobPendingAgeHigh(deps)(context.Background()); err == nil {
		t.Fatal("queue snapshot failure must surface as a rule error")
	}
}

// TestRulePrefetchFailureSpike_TriggersAtThreshold: 3 failures inside
// the 15-minute window trips the rule (the durable twin of
// velox_prefetch_failures_total).
func TestRulePrefetchFailureSpike_TriggersAtThreshold(t *testing.T) {
	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithPrefetchFailures(&stubPrefetchReader{count: 3})

	deps := DefaultRuleDeps()
	deps.Obs = obs
	event, err := rulePrefetchFailureSpike(deps)(context.Background())
	if err != nil {
		t.Fatalf("rule returned error: %v", err)
	}
	if event == nil {
		t.Fatal("3 prefetch failures in 15m did not fire PrefetchFailureSpike")
	}
	if event.RuleID != "PrefetchFailureSpike" || event.Labels["count"] != "3" {
		t.Fatalf("event = %+v, want RuleID=PrefetchFailureSpike count=3", event)
	}
}

// TestRulePrefetchFailureSpike_SilentBelowThresholdOrUnwired keeps the
// rule quiet on a healthy journal and on deployments that never wired
// the reader.
func TestRulePrefetchFailureSpike_SilentBelowThresholdOrUnwired(t *testing.T) {
	deps := DefaultRuleDeps()

	obs, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	obs.WithPrefetchFailures(&stubPrefetchReader{count: 2})
	deps.Obs = obs
	if event, err := rulePrefetchFailureSpike(deps)(context.Background()); err != nil || event != nil {
		t.Fatalf("2 failures below threshold → event=%v err=%v, want silent", event, err)
	}

	unwired, _ := observability.NewService(&stubTaskReader{tasks: map[string]*taskgraph.Task{}}, &stubAttemptReader{})
	deps.Obs = unwired
	if event, err := rulePrefetchFailureSpike(deps)(context.Background()); err != nil || event != nil {
		t.Fatalf("unwired reader → event=%v err=%v, want silent", event, err)
	}
}
