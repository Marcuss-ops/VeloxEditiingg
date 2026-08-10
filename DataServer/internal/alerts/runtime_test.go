package alerts_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-server/internal/alerts"
)

type runtimeClaimDedup struct {
	mu       sync.Mutex
	pending  bool
	commits  int
	releases int
}

type runtimeClaim struct {
	dedup *runtimeClaimDedup
	once  sync.Once
}

func (c *runtimeClaim) Commit() {
	c.once.Do(func() {
		c.dedup.mu.Lock()
		c.dedup.pending = false
		c.dedup.commits++
		c.dedup.mu.Unlock()
	})
}
func (c *runtimeClaim) Release() {
	c.once.Do(func() {
		c.dedup.mu.Lock()
		c.dedup.pending = false
		c.dedup.releases++
		c.dedup.mu.Unlock()
	})
}
func (d *runtimeClaimDedup) Claim(alerts.AlertEvent, time.Time) (alerts.Claim, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pending {
		return nil, false
	}
	d.pending = true
	return &runtimeClaim{dedup: d}, true
}

type runtimeSink struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *runtimeSink) Process(context.Context, alerts.AlertEvent) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.err
}
func (s *runtimeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type selectiveRuntimeSink struct {
	calls  int
	failOn string
}

func (s *selectiveRuntimeSink) Process(_ context.Context, event alerts.AlertEvent) error {
	s.calls++
	if event.RuleID == s.failOn {
		return errors.New("selected event failed")
	}
	return nil
}

func TestPipelineDispatchesCanonicalEventThroughDedupAndSinks(t *testing.T) {
	dedup := &runtimeClaimDedup{}
	sink := &runtimeSink{}
	pipeline := alerts.NewPipeline(dedup, sink)
	event := alerts.AlertEvent{Group: alerts.GroupCompute, RuleID: "error_rate", Subject: "error_rate", FiredAt: time.Now().UTC()}

	if err := pipeline.Dispatch(context.Background(), []alerts.AlertEvent{event}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if sink.count() != 1 || dedup.commits != 1 || dedup.releases != 0 {
		t.Fatalf("calls=%d commits=%d releases=%d, want one commit", sink.count(), dedup.commits, dedup.releases)
	}
}

func TestPipelineReleasesFailedClaimForRetry(t *testing.T) {
	dedup := &runtimeClaimDedup{}
	sink := &runtimeSink{err: errors.New("sink unavailable")}
	pipeline := alerts.NewPipeline(dedup, sink)
	event := alerts.AlertEvent{Group: alerts.GroupFleet, RuleID: "disk_pressure", Subject: "worker-1"}

	if err := pipeline.Dispatch(context.Background(), []alerts.AlertEvent{event}); err == nil {
		t.Fatal("failed dispatch must return an error")
	}
	if dedup.releases != 1 || dedup.commits != 0 {
		t.Fatalf("commits=%d releases=%d, want one release", dedup.commits, dedup.releases)
	}
}

func TestPipelineConcurrentDispatchClaimsOnlyOnce(t *testing.T) {
	pipeline := alerts.NewPipeline(alerts.NewCooldownDeduplicator(time.Hour))
	var mu sync.Mutex
	calls := 0
	pipeline.AddSink(alerts.FuncSink(func(context.Context, alerts.AlertEvent) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	}))
	event := alerts.AlertEvent{Group: alerts.GroupCompute, RuleID: "same", Subject: "same", FiredAt: time.Now().UTC()}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pipeline.Dispatch(context.Background(), []alerts.AlertEvent{event})
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("concurrent dispatch calls=%d, want 1", calls)
	}
}

func TestPipelinePreservesSinkStage(t *testing.T) {
	cause := errors.New("webhook down")
	stageErr := alerts.SinkError{Stage: "notifier", Err: cause}
	if got := alerts.StageOf(stageErr); got != "notifier" {
		t.Fatalf("StageOf = %q, want notifier", got)
	}
	if !errors.Is(stageErr, cause) {
		t.Fatalf("SinkError must preserve its cause")
	}
}

func TestPipelineObservesSuccessfulEventsIndependentlyOfFailedSibling(t *testing.T) {
	sink := &selectiveRuntimeSink{failOn: "bad"}
	pipeline := alerts.NewPipeline(alerts.NewCooldownDeduplicator(time.Hour), sink)
	events := []alerts.AlertEvent{
		{Group: alerts.GroupFleet, RuleID: "good", Subject: "worker-1"},
		{Group: alerts.GroupFleet, RuleID: "bad", Subject: "worker-1"},
	}
	if err := pipeline.Dispatch(context.Background(), events); err == nil {
		t.Fatal("Dispatch should report the failed sibling")
	}
	if sink.calls != 2 {
		t.Fatalf("sink calls=%d, want both sibling events attempted", sink.calls)
	}
}

func TestPipelineAfterCommitRetriesSuppressedEvent(t *testing.T) {
	dedup := alerts.NewCooldownDeduplicator(time.Hour)
	primary := &runtimeSink{}
	after := &runtimeSink{err: errors.New("notify unavailable")}
	pipeline := alerts.NewPipeline(dedup, primary)
	pipeline.AddAfterCommitSink(after)
	event := alerts.AlertEvent{Group: alerts.GroupFleet, RuleID: "disk_pressure", Subject: "worker-1", FiredAt: time.Now().UTC()}
	if err := pipeline.Dispatch(context.Background(), []alerts.AlertEvent{event}); err == nil {
		t.Fatal("first dispatch should report after-commit failure")
	}
	if err := pipeline.Dispatch(context.Background(), []alerts.AlertEvent{event}); err == nil {
		t.Fatal("suppressed retry should report after-commit failure")
	}
	if primary.count() != 1 || after.count() != 2 {
		t.Fatalf("primary=%d after_commit=%d, want 1 and 2", primary.count(), after.count())
	}
}

func TestCooldownDeduplicatorSeparatesRuleGroups(t *testing.T) {
	dedup := alerts.NewCooldownDeduplicator(time.Hour)
	now := time.Now().UTC()
	compute := alerts.AlertEvent{Group: alerts.GroupCompute, RuleID: "offline", Subject: "shared-subject"}
	fleet := alerts.AlertEvent{Group: alerts.GroupFleet, RuleID: "offline", Subject: "shared-subject"}
	computeClaim, ok := dedup.Claim(compute, now)
	if !ok {
		t.Fatal("compute should initially claim")
	}
	fleetClaim, ok := dedup.Claim(fleet, now)
	if !ok {
		t.Fatal("fleet should initially claim independently")
	}
	computeClaim.Commit()
	fleetClaim.Commit()
	if _, ok := dedup.Claim(compute, now.Add(time.Minute)); ok {
		t.Fatal("compute duplicate should be suppressed")
	}
	if _, ok := dedup.Claim(fleet, now.Add(time.Minute)); ok {
		t.Fatal("fleet duplicate should be suppressed independently")
	}
}

func TestPipelineSuppliedEventIDReachesSink(t *testing.T) {
	eventID := "stable-event-id"
	var got string
	pipeline := alerts.NewPipeline(nil, alerts.FuncSink(func(_ context.Context, event alerts.AlertEvent) error {
		got = event.EventID
		return nil
	}))
	if err := pipeline.Dispatch(context.Background(), []alerts.AlertEvent{{EventID: eventID, Group: alerts.GroupCompute, RuleID: "rule", Subject: "subject"}}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got != eventID {
		t.Fatalf("sink EventID=%q, want %q", got, eventID)
	}
}

func TestEventIDSeparatesRuleGroups(t *testing.T) {
	now := time.Unix(10, 20).UTC()
	compute := alerts.AlertEvent{Group: alerts.GroupCompute, RuleID: "offline", Subject: "shared-subject", Severity: "warning", FiredAt: now}
	fleet := compute
	fleet.Group = alerts.GroupFleet
	if got, want := alerts.EventIDFor(compute), alerts.EventIDFor(fleet); got == want {
		t.Fatalf("compute and fleet event IDs collided: %q", got)
	}
}

func TestEventIDIsStableAndLabelOrderIndependent(t *testing.T) {
	event := alerts.AlertEvent{Group: alerts.GroupFleet, RuleID: "disk_pressure", Severity: "WARNING", Subject: "worker-1", FiredAt: time.Unix(10, 20).UTC(), Labels: map[string]string{"b": "2", "a": "1"}}
	other := event
	other.Labels = map[string]string{"a": "1", "b": "2"}
	if got, want := alerts.EventIDFor(event), alerts.EventIDFor(other); got != want {
		t.Fatalf("event IDs differ for equivalent labels: %q != %q", got, want)
	}
}
