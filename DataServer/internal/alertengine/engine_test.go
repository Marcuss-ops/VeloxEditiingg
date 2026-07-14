// Package alertengine / engine_test.go
//
// Unit tests for all 5 alert rules + cooldown mechanism.
// Uses stub readers to construct a real observability.Service for
// rule evaluation, avoiding any database dependency.

package alertengine

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/observability"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// ── Stub Readers ─────────────────────────────────────────────────────────

type stubTaskReader struct {
	tasks      map[string]*taskgraph.Task
	listResult []taskgraph.Task
}

func (s *stubTaskReader) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	t, ok := s.tasks[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (s *stubTaskReader) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for _, t := range s.tasks {
		if t.JobID == jobID {
			return t, nil
		}
	}
	return nil, nil
}

func (s *stubTaskReader) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	return s.listResult, nil
}

type stubAttemptReader struct {
	attempts     map[string][]taskattempts.TaskAttempt
	phaseTimings map[string][]taskattempts.PhaseTiming
	metrics      map[string]*taskattempts.AttemptMetrics
}

func (s *stubAttemptReader) Get(_ context.Context, id string) (*taskattempts.TaskAttempt, error) {
	return nil, nil
}

func (s *stubAttemptReader) ListByTaskID(_ context.Context, taskID string) ([]taskattempts.TaskAttempt, error) {
	return s.attempts[taskID], nil
}

func (s *stubAttemptReader) GetPhaseTimings(_ context.Context, attemptID string) ([]taskattempts.PhaseTiming, error) {
	return s.phaseTimings[attemptID], nil
}

func (s *stubAttemptReader) GetMetrics(_ context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	m, ok := s.metrics[attemptID]
	if !ok {
		return nil, nil
	}
	return m, nil
}

func (s *stubAttemptReader) GetCacheStats(_ context.Context, _ string) (*taskattempts.AttemptCacheStats, error) {
	return nil, nil
}

type stubJobReader struct {
	counts jobs.Counts
}

func (s *stubJobReader) Get(_ context.Context, _ string) (*jobs.Job, error) {
	return nil, nil
}

func (s *stubJobReader) List(_ context.Context, _ jobs.Filter) ([]jobs.Job, error) {
	return nil, nil
}

func (s *stubJobReader) Counts(_ context.Context) (jobs.Counts, error) {
	return s.counts, nil
}

type stubWorkerReader struct {
	workers []map[string]any
}

func (s *stubWorkerReader) ListWorkers() ([]map[string]any, error) {
	return s.workers, nil
}

func (s *stubWorkerReader) GetWorker(_ string) (map[string]any, error) {
	return nil, nil
}

// ── Test Helpers ─────────────────────────────────────────────────────────

// newTestObs creates a real observability.Service backed by stub readers.
func newTestObs(
	jobCounts jobs.Counts,
	workers []map[string]any,
	tasks []taskgraph.Task,
	attempts map[string][]taskattempts.TaskAttempt,
	phaseTimings map[string][]taskattempts.PhaseTiming,
	metrics map[string]*taskattempts.AttemptMetrics,
) *observability.Service {
	taskReader := &stubTaskReader{
		tasks:      make(map[string]*taskgraph.Task),
		listResult: tasks,
	}
	for i := range tasks {
		taskReader.tasks[tasks[i].ID] = &tasks[i]
	}

	attemptReader := &stubAttemptReader{
		attempts:     attempts,
		phaseTimings: phaseTimings,
		metrics:      metrics,
	}

	svc, _ := observability.NewService(taskReader, attemptReader)
	svc.WithJobs(&stubJobReader{counts: jobCounts})
	svc.WithWorkers(&stubWorkerReader{workers: workers})
	return svc
}

// defaultsObs returns a healthy observability.Service with baseline data.
func defaultsObs() *observability.Service {
	return newTestObs(
		jobs.Counts{
			jobs.StatusSucceeded: 95,
			jobs.StatusFailed:    5,
			jobs.StatusPending:   3,
			jobs.StatusRunning:   2,
		},
		[]map[string]any{
			{"worker_id": "w-1", "status": "CONNECTED"},
			{"worker_id": "w-2", "status": "CONNECTED"},
		},
		[]taskgraph.Task{
			{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, ExecutorID: "scene.composite.v1"},
		},
		map[string][]taskattempts.TaskAttempt{
			"T-1": {{ID: "A-1", TaskID: "T-1", WorkerID: "w-1", Status: taskattempts.AttemptStatusSucceeded}},
		},
		map[string][]taskattempts.PhaseTiming{
			"A-1": {
				{AttemptID: "A-1", Phase: "render", DurationMS: 30_000},
			},
		},
		map[string]*taskattempts.AttemptMetrics{
			"A-1": {AttemptID: "A-1", FFmpegSpeedRatio: 2.5},
		},
	)
}

// ── Notifier Spy ─────────────────────────────────────────────────────────

type spyNotifier struct {
	mu     sync.Mutex
	alerts []Alert
}

func (s *spyNotifier) Send(_ context.Context, alert Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, alert)
	return nil
}

func (s *spyNotifier) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.alerts)
}

func (s *spyNotifier) last() *Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.alerts) == 0 {
		return nil
	}
	cp := s.alerts[len(s.alerts)-1]
	return &cp
}

// ── Tests: ErrorRate Rule ────────────────────────────────────────────────

func TestRuleErrorRate_Triggers(t *testing.T) {
	obs := newTestObs(
		jobs.Counts{
			jobs.StatusSucceeded: 80,
			jobs.StatusFailed:    20,
		},
		nil, nil, nil, nil, nil,
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.ErrorRatePct = 10.0 // 20% error rate > 10%

	rule := ruleErrorRate(deps)
	alert := rule(context.Background())
	if alert == nil {
		t.Fatal("expected alert when error rate exceeds threshold")
	}
	if alert.Name != "ErrorRateHigh" {
		t.Errorf("Name = %q, want ErrorRateHigh", alert.Name)
	}
	if alert.Severity != "warning" {
		t.Errorf("Severity = %q, want warning", alert.Severity)
	}
}

func TestRuleErrorRate_Healthy(t *testing.T) {
	obs := newTestObs(
		jobs.Counts{
			jobs.StatusSucceeded: 95,
			jobs.StatusFailed:    5,
		},
		nil, nil, nil, nil, nil,
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.ErrorRatePct = 10.0 // 5% error rate < 10%

	rule := ruleErrorRate(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when error rate below threshold, got %s", alert.Name)
	}
}

func TestRuleErrorRate_NilObs(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.Obs = nil
	deps.ErrorRatePct = 1.0

	rule := ruleErrorRate(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when Obs is nil, got %s", alert.Name)
	}
}

// ── Tests: P95 Wall Time Rule ────────────────────────────────────────────

func TestRuleP95WallMs_Triggers(t *testing.T) {
	obs := newTestObs(
		jobs.Counts{},
		nil,
		[]taskgraph.Task{
			{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, ExecutorID: "scene.composite.v1"},
		},
		map[string][]taskattempts.TaskAttempt{
			"T-1": {{ID: "A-1", TaskID: "T-1", WorkerID: "w-1", Status: taskattempts.AttemptStatusSucceeded}},
		},
		map[string][]taskattempts.PhaseTiming{
			"A-1": {
				{AttemptID: "A-1", Phase: "render", DurationMS: 400_000}, // 400s render
			},
		},
		nil,
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.P95WallMs = 300_000 // 400s > 300s threshold

	rule := ruleP95WallMs(deps)
	alert := rule(context.Background())
	if alert == nil {
		t.Fatal("expected alert when P95 render exceeds threshold")
	}
	if alert.Name != "P95WallMsHigh" {
		t.Errorf("Name = %q, want P95WallMsHigh", alert.Name)
	}
}

func TestRuleP95WallMs_NilObs(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.Obs = nil
	deps.P95WallMs = 300_000

	rule := ruleP95WallMs(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when Obs is nil, got %s", alert.Name)
	}
}

func TestRuleP95WallMs_Healthy(t *testing.T) {
	obs := defaultsObs() // 30s render, 300s threshold
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.P95WallMs = 300_000

	rule := ruleP95WallMs(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when P95 below threshold, got %s", alert.Name)
	}
}

// ── Tests: Worker Offline Rule ────────────────────────────────────────────

func TestRuleWorkerOffline_Triggers(t *testing.T) {
	obs := newTestObs(
		jobs.Counts{},
		[]map[string]any{
			{"worker_id": "w-1", "status": "CONNECTED"},
			{"worker_id": "w-2", "status": "DISCONNECTED"},
			{"worker_id": "w-3", "status": "STALE"},
		},
		nil, nil, nil, nil,
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs

	rule := ruleWorkerOffline(deps)
	alert := rule(context.Background())
	if alert == nil {
		t.Fatal("expected alert when workers are offline")
	}
	if alert.Name != "WorkersOffline" {
		t.Errorf("Name = %q, want WorkersOffline", alert.Name)
	}
	if alert.Severity != "critical" {
		t.Errorf("Severity = %q, want critical", alert.Severity)
	}
}

func TestRuleWorkerOffline_Healthy(t *testing.T) {
	obs := defaultsObs() // both workers CONNECTED
	deps := DefaultRuleDeps()
	deps.Obs = obs

	rule := ruleWorkerOffline(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when all workers CONNECTED, got %s", alert.Name)
	}
}

func TestRuleWorkerOffline_NilObs(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.Obs = nil

	rule := ruleWorkerOffline(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when Obs is nil, got %s", alert.Name)
	}
}

// ── Tests: Disk Free Rule ────────────────────────────────────────────────

func TestRuleDiskFree_Triggers(t *testing.T) {
	// Use /tmp which has finite space; set a very high threshold to trigger.
	deps := DefaultRuleDeps()
	deps.DataDir = os.TempDir()
	deps.DiskFreeGB = 1_000_000.0 // impossibly high → alert triggers

	rule := ruleDiskFree(deps)
	alert := rule(context.Background())
	if alert == nil {
		t.Fatal("expected alert when disk free below threshold")
	}
	if alert.Name != "DiskFreeLow" {
		t.Errorf("Name = %q, want DiskFreeLow", alert.Name)
	}
	if alert.Severity != "critical" {
		t.Errorf("Severity = %q, want critical", alert.Severity)
	}
}

func TestRuleDiskFree_Healthy(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.DataDir = os.TempDir()
	deps.DiskFreeGB = 0.0 // always above 0 → no alert

	rule := ruleDiskFree(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when disk free above threshold, got %s", alert.Name)
	}
}

func TestRuleDiskFree_NoDir(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.DataDir = "" // empty → nil returned

	rule := ruleDiskFree(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when DataDir is empty, got %s", alert.Name)
	}
}

// ── Tests: FFmpeg Speed Ratio Rule ───────────────────────────────────────

func TestRuleFFmpegSpeedRatio_Triggers(t *testing.T) {
	obs := newTestObs(
		jobs.Counts{},
		nil,
		[]taskgraph.Task{
			{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, ExecutorID: "scene.composite.v1"},
		},
		map[string][]taskattempts.TaskAttempt{
			"T-1": {{ID: "A-1", TaskID: "T-1", WorkerID: "w-1", Status: taskattempts.AttemptStatusSucceeded}},
		},
		nil,
		map[string]*taskattempts.AttemptMetrics{
			"A-1": {AttemptID: "A-1", FFmpegSpeedRatio: 1.2}, // below 1.5 min
		},
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.FFmpegMin = 1.5

	rule := ruleFFmpegSpeedRatio(deps)
	alert := rule(context.Background())
	if alert == nil {
		t.Fatal("expected alert when ffmpeg speed ratio below minimum")
	}
	if alert.Name != "FFmpegSpeedRatioLow" {
		t.Errorf("Name = %q, want FFmpegSpeedRatioLow", alert.Name)
	}
}

func TestRuleFFmpegSpeedRatio_Healthy(t *testing.T) {
	obs := defaultsObs() // FFmpegSpeedRatio = 2.5, threshold = 1.5
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.FFmpegMin = 1.5

	rule := ruleFFmpegSpeedRatio(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when speed ratio above minimum, got %s", alert.Name)
	}
}

func TestRuleFFmpegSpeedRatio_NoSamples(t *testing.T) {
	// No metrics at all → RecentScalarMetric returns 0 samples.
	obs := newTestObs(
		jobs.Counts{},
		nil,
		[]taskgraph.Task{},
		nil, nil, nil,
	)
	deps := DefaultRuleDeps()
	deps.Obs = obs
	deps.FFmpegMin = 1.5

	rule := ruleFFmpegSpeedRatio(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert with 0 samples, got %s", alert.Name)
	}
}

func TestRuleFFmpegSpeedRatio_NilObs(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.Obs = nil
	deps.FFmpegMin = 1.5

	rule := ruleFFmpegSpeedRatio(deps)
	alert := rule(context.Background())
	if alert != nil {
		t.Errorf("expected no alert when Obs is nil, got %s", alert.Name)
	}
}

// ── Tests: Cooldown Mechanism ─────────────────────────────────────────────

func TestCooldown_SuppressesRepeatedAlerts(t *testing.T) {
	spy := &spyNotifier{}
	engine := New(1*time.Hour, spy) // long tick so Run doesn't interfere
	engine.Cooldown = 1 * time.Hour // long cooldown so it persists for the test

	// Register a rule that always fires.
	engine.AddRule(func(ctx context.Context) *Alert {
		return &Alert{Name: "AlwaysOn", Severity: "warning", Summary: "test"}
	})

	ctx := context.Background()

	// First evaluation: should fire.
	engine.evaluateAll(ctx)
	if spy.count() != 1 {
		t.Fatalf("first evaluation: expected 1 alert, got %d", spy.count())
	}

	// Second evaluation within cooldown: should be suppressed.
	engine.evaluateAll(ctx)
	if spy.count() != 1 {
		t.Errorf("second evaluation: expected 1 alert (suppressed), got %d", spy.count())
	}
}

func TestCooldown_AllowsAfterCooldown(t *testing.T) {
	spy := &spyNotifier{}
	engine := New(1*time.Hour, spy)
	engine.Cooldown = 10 * time.Millisecond // very short cooldown

	engine.AddRule(func(ctx context.Context) *Alert {
		return &Alert{Name: "AlwaysOn", Severity: "warning", Summary: "test"}
	})

	ctx := context.Background()

	// First evaluation: fires.
	engine.evaluateAll(ctx)
	if spy.count() != 1 {
		t.Fatalf("first evaluation: expected 1 alert, got %d", spy.count())
	}

	// Wait for cooldown to expire.
	time.Sleep(20 * time.Millisecond)

	// Second evaluation: should fire again.
	engine.evaluateAll(ctx)
	if spy.count() != 2 {
		t.Errorf("after cooldown: expected 2 alerts, got %d", spy.count())
	}
}

func TestCooldown_DifferentRulesDontSuppressEachOther(t *testing.T) {
	spy := &spyNotifier{}
	engine := New(1*time.Hour, spy)
	engine.Cooldown = 1 * time.Hour

	engine.AddRule(func(ctx context.Context) *Alert {
		return &Alert{Name: "RuleA", Severity: "warning", Summary: "A"}
	})
	engine.AddRule(func(ctx context.Context) *Alert {
		return &Alert{Name: "RuleB", Severity: "warning", Summary: "B"}
	})

	ctx := context.Background()

	engine.evaluateAll(ctx)
	if spy.count() != 2 {
		t.Fatalf("expected 2 alerts from different rules, got %d", spy.count())
	}

	// Second evaluation: both should be suppressed (each in its own cooldown).
	engine.evaluateAll(ctx)
	if spy.count() != 2 {
		t.Errorf("second evaluation: expected 2 alerts (both suppressed), got %d", spy.count())
	}
}

// ── Tests: MakeRules Integration ──────────────────────────────────────────

func TestMakeRules_ReturnsFiveRules(t *testing.T) {
	deps := DefaultRuleDeps()
	deps.Obs = defaultsObs()
	deps.DataDir = os.TempDir()
	deps.DiskFreeGB = 0.0 // avoid flaky failure if /tmp has < 10 GB free

	rules := MakeRules(deps)
	if len(rules) != 5 {
		t.Fatalf("MakeRules returned %d rules, want 5", len(rules))
	}

	ctx := context.Background()
	for i, rule := range rules {
		alert := rule(ctx)
		// With healthy defaults, no rule should fire.
		if alert != nil {
			t.Errorf("rule %d fired unexpectedly: %s", i, alert.Name)
		}
	}
}
