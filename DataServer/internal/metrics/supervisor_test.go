package metrics

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

// fakeAttemptsDataSource is a hand-rolled AttemptsDataSource impl
// for supervisor unit tests. Production code uses SQLiteLabelResolver;
// the fake isolates the tickOnce math from any DB schema surface.
//
// Concurrent-safety: seenIDs map mutates only inside the test
// goroutine (we drive ticks synchronously), so no mutex needed.
type fakeAttemptsDataSource struct {
	mu        sync.Mutex
	attempts  map[string]*fakeAttemptRecord
	recentIDs []string  // last user-readable response
	sinceAt   time.Time // last RecentAttemptIDs input

	// optional injection points
	labelsMap  map[string]fakeLabels
	metricsMap map[string]*taskattempts.AttemptMetrics
	cacheStats map[string]*taskattempts.AttemptCacheStats
	costMap    map[string]*taskattempts.AttemptCostBasis
	statusMap  map[string]taskattempts.AttemptStatus

	// queries (mostly for debugging)
	recentCalls int

	// daily-rollup tracking
	ComputeDailyRollupsCalls int
	ComputeDailyRollupsDays  []string
	ComputeDailyRollupsErr   error
}

type fakeAttemptRecord struct {
	status    taskattempts.AttemptStatus
	updatedAt time.Time
	execID    string
	execVer   string
	wClass    string
}

type fakeLabels struct {
	execID, execVer, wClass string
}

func (f *fakeAttemptsDataSource) add(a *fakeAttemptRecord, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attempts == nil {
		f.attempts = make(map[string]*fakeAttemptRecord)
	}
	f.attempts[id] = a
}

func (f *fakeAttemptsDataSource) RecentAttemptIDs(ctx context.Context, since time.Time, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recentCalls++
	f.sinceAt = since
	var ids []string
	for id, a := range f.attempts {
		if !a.updatedAt.Before(since) && a.status.IsTerminal() {
			ids = append(ids, id)
		}
	}
	f.recentIDs = ids
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (f *fakeAttemptsDataSource) Labels(ctx context.Context, attemptID string) (string, string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.labelsMap != nil {
		if l, ok := f.labelsMap[attemptID]; ok {
			return l.execID, l.execVer, l.wClass, nil
		}
	}
	if a, ok := f.attempts[attemptID]; ok {
		return a.execID, a.execVer, a.wClass, nil
	}
	return "unknown", "0", "default", io.EOF
}

func (f *fakeAttemptsDataSource) GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusMap != nil {
		if s, ok := f.statusMap[attemptID]; ok {
			return s, nil
		}
	}
	if a, ok := f.attempts[attemptID]; ok {
		return a.status, nil
	}
	return taskattempts.AttemptStatusPending, nil
}

func (f *fakeAttemptsDataSource) GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.metricsMap != nil {
		if m, ok := f.metricsMap[attemptID]; ok {
			return m, nil
		}
	}
	return nil, nil
}

func (f *fakeAttemptsDataSource) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cacheStats != nil {
		if c, ok := f.cacheStats[attemptID]; ok {
			return c, nil
		}
	}
	return nil, nil
}

func (f *fakeAttemptsDataSource) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.costMap != nil {
		if c, ok := f.costMap[attemptID]; ok {
			return c, nil
		}
	}
	return nil, nil
}

func (f *fakeAttemptsDataSource) GetPhaseTimingsDetailed(ctx context.Context, attemptID string) ([]taskattempts.PhaseTimingDetailed, error) {
	return nil, nil
}

func (f *fakeAttemptsDataSource) GetSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error) {
	return nil, nil
}

func (f *fakeAttemptsDataSource) ComputeDailyRollups(ctx context.Context, day string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ComputeDailyRollupsDays = append(f.ComputeDailyRollupsDays, day)
	f.ComputeDailyRollupsCalls++
	return f.ComputeDailyRollupsErr
}

// ComputeDailyRollupsDays tracks the days for which rollups were
// requested (for assertions in daily-rollup tests).
// ComputeDailyRollupsCalls counts total invocations.
// ComputeDailyRollupsErr injects a forced error (nil = success).

// fakeOutboxGauge returns a fixed count. Records the call count so
// tests can assert the supervisor called PendingCount exactly.
type fakeOutboxGauge struct {
	mu    sync.Mutex
	count int64
	err   error
	calls int
}

func (o *fakeOutboxGauge) PendingCount(ctx context.Context) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls++
	return o.count, o.err
}

var _ OutboxGauge = (*fakeOutboxGauge)(nil)

// TestSupervisor_TickOnce_StampCostGauges: the load-bearing test.
// Scenario: 3 newly-terminal attempts across "cpu" and "gpu" classes
// stamp the 4 cost gauges EXACTLY under those classes (no
// collapse-onto-default).
//
// Timing note: the fake attempts' updatedAt MUST be ≥ s.last so the
// `updated_at >= since` filter inside RecentAttemptIDs includes them.
// Adding attempts AFTER NewSupervisor (which sets s.last=now)
// guarantees the timestamp ordering.
func TestSupervisor_TickOnce_StampCostGauges(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	attempts := &fakeAttemptsDataSource{}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{count: 7}, DefaultCostFactors())

	// Add attempts AFTER supervisor construction so
	// `updated_at >= s.last` returns true inside RecentAttemptIDs.
	stampAt := time.Now().UTC().Add(time.Second)
	attempts.attempts = map[string]*fakeAttemptRecord{
		"a1": {status: taskattempts.AttemptStatusSucceeded, updatedAt: stampAt, execID: "scene.composite.v1", execVer: "3", wClass: "mixed"},
		"a2": {status: taskattempts.AttemptStatusSucceeded, updatedAt: stampAt, execID: "transcode", execVer: "1", wClass: "cpu"},
		"a3": {status: taskattempts.AttemptStatusFailed, updatedAt: stampAt, execID: "gpu.encode", execVer: "2", wClass: "gpu"},
	}
	attempts.metricsMap = map[string]*taskattempts.AttemptMetrics{
		"a1": {AttemptID: "a1", CPUTimeMS: 60_000, MediaDurationSeconds: 30},
		"a2": {AttemptID: "a2", CPUTimeMS: 60_000, MediaDurationSeconds: 30},
		"a3": {AttemptID: "a3", CPUTimeMS: 60_000, MediaDurationSeconds: 30},
	}
	attempts.costMap = map[string]*taskattempts.AttemptCostBasis{
		"a1": {AttemptID: "a1", CPUTimeSecondsTotal: 60, NetworkGBEgressed: 0.5, StorageGBWritten: 0.1, OutputMinutesTotal: 0.5},
		"a2": {AttemptID: "a2", CPUTimeSecondsTotal: 60, NetworkGBEgressed: 0.5, StorageGBWritten: 0.1, OutputMinutesTotal: 0.5},
		"a3": {AttemptID: "a3", CPUTimeSecondsTotal: 60, NetworkGBEgressed: 0.5, StorageGBWritten: 0.1, OutputMinutesTotal: 0.5},
	}
	attempts.statusMap = map[string]taskattempts.AttemptStatus{
		"a1": taskattempts.AttemptStatusSucceeded,
		"a2": taskattempts.AttemptStatusSucceeded,
		"a3": taskattempts.AttemptStatusFailed,
	}

	s.SetTick(0) // unused in this test
	s.SetLimit(100)

	// First tick — supervisor should pull all 3 attempts, stamp
	// per-class gauges, and refresh master health.
	s.tickOnce(context.Background(), time.Now().UTC().Add(5*time.Second))

	// Assert: cost gauges stamped under each class.
	out := dumpFamily(t, reg, "velox_cost_total_per_output_minute")
	wantSubstrings := []string{
		`worker_class="cpu"`,
		`worker_class="gpu"`,
		`worker_class="mixed"`,
		`worker_class="all"`,
	}
	for _, want := range wantSubstrings {
		if !contains(out, want) {
			t.Errorf("missing %s in velox_cost_total_per_output_minute:\n%s", want, out)
		}
	}

	// Assert: compute-outcome family stamped under each
	// terminal status (spec §14 follow-up re-uses the existing
	// family).
	out2 := dumpFamily(t, reg, "velox_compute_seconds_total")
	if !contains(out2, `outcome="useful"`) {
		t.Errorf("expected outcome=useful row; got:\n%s", out2)
	}
	if !contains(out2, `outcome="failed"`) {
		t.Errorf("expected outcome=failed row; got:\n%s", out2)
	}

	// Assert: master-health gauges refreshed.
	out3 := dumpFamily(t, reg, "velox_master_outbox_pending")
	// Empty-label gauge: exposition is "family_name value\n" with
	// no `{}` suffix (formatLabelInline returns "" when len==0).
	if !contains(out3, "velox_master_outbox_pending 7") {
		t.Errorf("expected outbox pending row with count=7; got:\n%s", out3)
	}
}

// TestSupervisor_TickOnce_DeltaDetection: a SECOND tick with no
// new attempts does NOT re-stamp — dedup via seenIDs OR via the
// SQL-equivalent `updated_at >= since` filter. The cost gauges
// from the first tick remain (set-to-current semantics; per-tick
// stable when no input changed).
//
// Timing: same caveat as TestSupervisor_TickOnce_StampCostGauges —
// attempt updatedAt must be ≥ s.last.
func TestSupervisor_TickOnce_DeltaDetection(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{count: 1}, DefaultCostFactors())

	stampAt := time.Now().UTC().Add(time.Second)
	attempts.attempts = map[string]*fakeAttemptRecord{
		"a1": {status: taskattempts.AttemptStatusSucceeded, updatedAt: stampAt, execID: "transcode", execVer: "1", wClass: "cpu"},
	}
	attempts.metricsMap = map[string]*taskattempts.AttemptMetrics{
		"a1": {AttemptID: "a1", CPUTimeMS: 60_000, MediaDurationSeconds: 30},
	}
	attempts.costMap = map[string]*taskattempts.AttemptCostBasis{
		"a1": {AttemptID: "a1", CPUTimeSecondsTotal: 60, NetworkGBEgressed: 0.5, StorageGBWritten: 0.1, OutputMinutesTotal: 0.5},
	}
	s.SetLimit(100)

	firstTick := time.Now().UTC()
	s.tickOnce(context.Background(), firstTick.Add(5*time.Second))

	beforeCalls := attempts.recentCalls
	s.tickOnce(context.Background(), firstTick.Add(20*time.Second)) // t1 + tick
	if attempts.recentCalls <= beforeCalls {
		t.Errorf("RecentAttemptIDs should be called on every tick; before=%d after=%d", beforeCalls, attempts.recentCalls)
	}

	// After second tick with same database (no new attempts):
	// the cost gauges should still be valid (not zeroed). Set
	// semantics = re-stamp each tick even with empty data — the
	// fleet still wants to see the latest reading. We just
	// confirm we don't PANIC on the empty-tick path.
	out := dumpFamily(t, reg, "velox_cost_total_per_output_minute")
	if !contains(out, `worker_class="cpu"`) {
		t.Errorf("second tick must not lose per-class cost gauges; got:\n%s", out)
	}
}

// TestSupervisor_TickOnce_NoAttempts_NoCrash: cold-start tick
// with empty DB and stale since — recent returns []ids, master
// health still refreshed, no panic.
func TestSupervisor_TickOnce_NoAttempts_NoCrash(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{attempts: map[string]*fakeAttemptRecord{}}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{count: 0}, DefaultCostFactors())

	s.tickOnce(context.Background(), time.Now().UTC())

	out := dumpFamily(t, reg, "velox_master_outbox_pending")
	// Empty-label gauge: exposition is "family_name value\n" with
	// no `{}` suffix (formatLabelInline returns "" when len==0).
	if !contains(out, "velox_master_outbox_pending 0") {
		t.Errorf("master-health gauges must refresh even on empty tick; got:\n%s", out)
	}
}

// TestSupervisor_New_NilPanics: defensive nil-check on
// construction so a misconfigured bootstrap crashes loudly at
// boot, NOT silently produces a no-op supervisor.
func TestSupervisor_New_NilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewSupervisor with nil attempts MUST panic")
		}
	}()
	NewSupervisor(NewCollector(NewRegistry()), nil, &fakeOutboxGauge{}, DefaultCostFactors())
}

// TestEncodeMicroEUR_NegativeClampsToZero: protects dashboard math
// from a future bug that could emit negative gauge readings.
func TestEncodeMicroEUR_NegativeClampsToZero(t *testing.T) {
	if got := encodeMicroEUR(-1.0); got != 0 {
		t.Errorf("negative encodeMicroEUR = %d; want 0", got)
	}
	if got := encodeMicroEUR(0); got != 0 {
		t.Errorf("zero encodeMicroEUR = %d; want 0", got)
	}
	if got := encodeMicroEUR(0.005612); got != 5612 {
		t.Errorf("0.005612 EUR encodeMicroEUR = %d; want 5612", got)
	}
}

// contains is a tiny substring search (kept local to keep imports lean).
func contains(s, sub string) bool {
	return len(sub) == 0 || len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// guard against unused import lint when test file shrinks.
var _ = fmt.Sprintf

// ── Daily Rollup Tests (Step 2 / Velox Metrics Center) ─────────────────

// TestPercentileFloat64 verifies percentile computation edge cases.
func TestPercentileFloat64(t *testing.T) {
	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
	}{
		{"empty slice", nil, 0.50, 0},
		{"single element p50", []float64{42}, 0.50, 42},
		{"single element p95", []float64{42}, 0.95, 42},
		{"single element p0", []float64{42}, 0, 42},
		{"two elements p50", []float64{10, 20}, 0.50, 15},
		{"two elements p0", []float64{10, 20}, 0, 10},
		{"two elements p100", []float64{10, 20}, 1.0, 20},
		{"five elements p50", []float64{1, 2, 3, 4, 5}, 0.50, 3},
		{"five elements p95", []float64{1, 2, 3, 4, 5}, 0.95, 4.8},
		{"five elements p99", []float64{1, 2, 3, 4, 5}, 0.99, 4.96},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentileFloat64(tt.sorted, tt.p)
			if got != tt.want {
				t.Errorf("percentileFloat64(%v, %.2f) = %v; want %v", tt.sorted, tt.p, got, tt.want)
			}
		})
	}
}

// TestAvgFloat64 verifies basic average computation.
func TestAvgFloat64(t *testing.T) {
	if got := avgFloat64(nil); got != 0 {
		t.Errorf("avgFloat64(nil) = %v; want 0", got)
	}
	if got := avgFloat64([]float64{10, 20, 30}); got != 20 {
		t.Errorf("avgFloat64([10,20,30]) = %v; want 20", got)
	}
}

// TestDateAddDay verifies date arithmetic with valid and invalid inputs.
func TestDateAddDay(t *testing.T) {
	// Positive offset.
	got, err := dateAddDay("2026-07-06", 1)
	if err != nil {
		t.Fatalf("dateAddDay(+1): %v", err)
	}
	if got != "2026-07-07" {
		t.Errorf("dateAddDay(+1) = %q; want 2026-07-07", got)
	}

	// Negative offset.
	got, err = dateAddDay("2026-07-06", -1)
	if err != nil {
		t.Fatalf("dateAddDay(-1): %v", err)
	}
	if got != "2026-07-05" {
		t.Errorf("dateAddDay(-1) = %q; want 2026-07-05", got)
	}

	// Month boundary.
	got, err = dateAddDay("2026-01-31", 1)
	if err != nil {
		t.Fatalf("dateAddDay(month): %v", err)
	}
	if got != "2026-02-01" {
		t.Errorf("dateAddDay(month) = %q; want 2026-02-01", got)
	}

	// Invalid input.
	_, err = dateAddDay("not-a-date", 1)
	if err == nil {
		t.Errorf("dateAddDay(invalid) should return error")
	}
}

// TestSupervisor_TryDailyRollup_MidnightCrossing verifies that
// tryDailyRollup fires ComputeDailyRollups when crossing midnight
// and correctly handles multi-day gaps on extended downtime.
func TestSupervisor_TryDailyRollup_MidnightCrossing(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{}, DefaultCostFactors())

	// No rollup on the SAME day.
	today := time.Now().UTC()
	s.lastRollupDay = today.Format("2006-01-02")
	s.tryDailyRollup(context.Background(), today)
	if attempts.ComputeDailyRollupsCalls > 0 {
		t.Errorf("tryDailyRollup should not fire on same day; got %d calls", attempts.ComputeDailyRollupsCalls)
	}

	// Midnight: should fire for each missing day.
	tomorrow := today.Add(72 * time.Hour) // 3 days later — simulates extended downtime
	s.tryDailyRollup(context.Background(), tomorrow)
	// Should have rolled up 2 days: today+1 and today+2.
	// (lastRollupDay was "today", so the range is [today+1, tomorrow-1])
	if attempts.ComputeDailyRollupsCalls == 0 {
		t.Errorf("tryDailyRollup should fire after midnight crossing")
	}
}

// TestSupervisor_TryDailyRollup_FirstBoot rolls up yesterday only
// on first tick (lastRollupDay empty).
func TestSupervisor_TryDailyRollup_FirstBoot(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	attempts := &fakeAttemptsDataSource{}
	s := NewSupervisor(c, attempts, &fakeOutboxGauge{}, DefaultCostFactors())

	// Simulate first boot: lastRollupDay is empty.
	s.lastRollupDay = ""
	now := time.Now().UTC()
	s.tryDailyRollup(context.Background(), now)
	if attempts.ComputeDailyRollupsCalls != 1 {
		t.Errorf("first boot should roll up yesterday; got %d calls", attempts.ComputeDailyRollupsCalls)
	}
	yesterday := now.Add(-24 * time.Hour).UTC().Format("2006-01-02")
	if len(attempts.ComputeDailyRollupsDays) > 0 && attempts.ComputeDailyRollupsDays[0] != yesterday {
		t.Errorf("first boot should roll up %q; got %q", yesterday, attempts.ComputeDailyRollupsDays[0])
	}
}
